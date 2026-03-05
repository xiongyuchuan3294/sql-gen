#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Hive connection and execution helpers."""

from __future__ import annotations

import configparser
import contextlib
import json
import locale
import os
import re
import subprocess
import sys
import tempfile
import threading
from collections import OrderedDict
from functools import lru_cache
from pathlib import Path

try:
    from pyhive import hive
except ImportError:  # pragma: no cover - handled at runtime for remote envs
    hive = None

try:
    from impala.dbapi import connect as impala_connect
    IMPALA_AVAILABLE = True
except ImportError:  # pragma: no cover
    impala_connect = None
    IMPALA_AVAILABLE = False


class LocalHiveProcessExecutor:
    SCHEMA_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    @staticmethod
    def _decode_output(payload: bytes | None) -> str:
        if not payload:
            return ""

        encodings = [
            "utf-8",
            locale.getpreferredencoding(False),
            "gbk",
            "cp936",
        ]
        tried = set()
        for encoding in encodings:
            if not encoding or encoding in tried:
                continue
            tried.add(encoding)
            try:
                return payload.decode(encoding)
            except UnicodeDecodeError:
                continue
        return payload.decode("utf-8", errors="replace")

    @classmethod
    def _config(cls, env: str | None = None):
        config = HiveRuntimeConfig.get_env_config(env)
        if not config:
            raise ValueError(f"Hive environment config not found: {env}")

        mode = HiveConnectionManager.normalize_mode(config.get("mode"))
        if not HiveConnectionManager.is_local_mode(mode):
            raise ValueError(
                f"Environment '{env}' is mode='{mode}', not local process mode."
            )

        missing = [
            field for field in ("root", "python", "runner") if not config.get(field)
        ]
        if missing:
            raise ValueError(
                f"Local Hive environment is missing required fields: {', '.join(missing)}"
            )

        root = Path(str(config["root"]))
        python_path = Path(str(config["python"]))
        runner_path = Path(str(config["runner"]))
        return root, python_path, runner_path

    @classmethod
    def _validate_schema(cls, schema: str) -> str:
        if not schema:
            raise ValueError("Schema is required for local Hive execution.")
        if not cls.SCHEMA_RE.match(schema):
            raise ValueError(f"Invalid schema for local Hive execution: {schema}")
        return schema

    @classmethod
    def _build_sql(cls, schema: str, sql: str) -> str:
        schema_name = cls._validate_schema(schema)
        sql_text = sql.strip()
        if not sql_text:
            raise ValueError("SQL is required.")
        return (
            f"CREATE DATABASE IF NOT EXISTS {schema_name};\n"
            f"USE {schema_name};\n"
            f"{sql_text}\n"
        )

    @classmethod
    def _run(cls, schema: str, sql: str, env: str | None = None) -> dict:
        root, python_path, runner_path = cls._config(env)
        if not root.exists():
            raise FileNotFoundError(
                f"Local Hive root not found: {root}. Run the local setup first."
            )
        if not python_path.exists():
            raise FileNotFoundError(
                f"Local Hive Python not found: {python_path}. "
                "Expected the local venv to be created already."
            )
        if not runner_path.exists():
            raise FileNotFoundError(f"Local Hive runner not found: {runner_path}.")

        payload_sql = cls._build_sql(schema, sql)
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".sql",
                encoding="utf-8",
                delete=False,
            ) as handle:
                handle.write(payload_sql)
                temp_path = handle.name

            result = subprocess.run(
                [
                    str(python_path),
                    str(runner_path),
                    "--file",
                    temp_path,
                    "--output-format",
                    "json",
                ],
                cwd=str(root),
                capture_output=True,
                text=False,
            )
        finally:
            if temp_path:
                Path(temp_path).unlink(missing_ok=True)

        stdout = cls._decode_output(result.stdout).strip()
        stderr = cls._decode_output(result.stderr).strip()
        if not stdout:
            message = stderr or "Local Hive runner returned no output."
            raise RuntimeError(message)

        json_line = ""
        for line in stdout.splitlines():
            candidate = line.strip()
            if candidate.startswith("{") and candidate.endswith("}"):
                json_line = candidate
                break
        if not json_line:
            json_line = stdout

        try:
            payload = json.loads(json_line)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "Failed to parse local Hive runner output.\n"
                f"stdout: {stdout[:1000]}\n"
                f"stderr: {stderr[:1000]}"
            ) from exc

        if result.returncode != 0 or payload.get("status") != "ok":
            message = payload.get("error") or stderr or stdout
            raise RuntimeError(message)
        return payload

    @staticmethod
    def _format_result(columns, rows):
        if not columns:
            return ""

        header = "\t".join(columns)
        rendered_rows = []
        for row in rows:
            rendered_rows.append(
                "\t".join("NULL" if cell is None else str(cell) for cell in row)
            )
        if not rendered_rows:
            return header
        return f"{header}\n" + "\n".join(rendered_rows)

    @classmethod
    def execute_query(cls, schema: str, sql: str, env: str | None = None) -> str:
        payload = cls._run(schema, sql, env)
        results = payload.get("results", [])
        if not results:
            return ""
        result = results[-1]
        return cls._format_result(result.get("columns", []), result.get("rows", []))

    @classmethod
    def execute(cls, schema: str, sql: str, env: str | None = None) -> None:
        cls._run(schema, sql, env)

    @classmethod
    def execute_batch(cls, schema: str, sql_list, env: str | None = None) -> bool:
        sql_text = ";\n".join(
            item.strip().rstrip(";") for item in sql_list if item.strip()
        )
        if not sql_text:
            return True
        cls._run(schema, sql_text, env)
        return True

    @staticmethod
    def close_all() -> None:
        return None


class HiveConnectionManager:
    LOCAL_MODE = "local"
    REMOTE_EXACT_MODES = {"remote", "local_hs2"}
    REMOTE_MODE_PREFIXES = ("local_hs2_",)
    _lock = threading.Lock()
    _connections = OrderedDict()

    @classmethod
    def normalize_env(cls, env: str | None = None) -> str:
        normalized_env = str(env or HiveRuntimeConfig.active_env()).strip().lower()
        if not normalized_env:
            raise ValueError("Active Hive environment is empty. Check conf/aml_conf.conf")
        return normalized_env

    @classmethod
    def normalize_mode(cls, mode: str | None) -> str:
        return str(mode or "").strip().lower()

    @classmethod
    def is_local_mode(cls, mode: str | None) -> bool:
        return cls.normalize_mode(mode) == cls.LOCAL_MODE

    @classmethod
    def is_remote_like_mode(cls, mode: str | None) -> bool:
        normalized = cls.normalize_mode(mode)
        if normalized in cls.REMOTE_EXACT_MODES:
            return True
        return normalized.startswith(cls.REMOTE_MODE_PREFIXES)

    @classmethod
    def is_local_env(cls, env: str | None = None) -> bool:
        config = HiveRuntimeConfig.get_env_config(env)
        return cls.is_local_mode(config.get("mode"))

    @classmethod
    def supported_envs(cls):
        return sorted(HiveRuntimeConfig.environments().keys())

    @classmethod
    def get_connection(cls, env: str | None = None, schema="default"):
        normalized_env = cls.normalize_env(env)
        config = HiveRuntimeConfig.get_env_config(normalized_env)
        if not config:
            supported = ", ".join(cls.supported_envs())
            raise ValueError(
                f"Invalid environment: {normalized_env}. Supported environments: {supported}"
            )

        mode = cls.normalize_mode(config.get("mode"))
        if cls.is_local_mode(mode):
            raise ValueError("Local mode does not use a remote Hive connection.")
        if not cls.is_remote_like_mode(mode):
            supported_modes = ", ".join(
                sorted(cls.REMOTE_EXACT_MODES | {cls.LOCAL_MODE})
            )
            raise ValueError(
                f"Environment {normalized_env} has unsupported mode '{mode}'. "
                f"Supported modes: {supported_modes}."
            )

        if hive is None:
            raise ImportError(
                "pyhive is required for remote Hive execution. "
                "Install it or use env='local' for local debugging."
            )

        missing = [
            field for field in ("host", "port", "username") if not config.get(field)
        ]
        if missing:
            raise ValueError(
                f"Environment {normalized_env} is missing required fields: "
                f"{', '.join(missing)}"
            )

        try:
            port = int(config["port"])
        except Exception as exc:
            raise ValueError(
                f"Invalid port for environment {normalized_env}: {config.get('port')}"
            ) from exc

        connection_key = f"{normalized_env}:{schema}"
        with cls._lock:
            if connection_key in cls._connections:
                return cls._connections[connection_key]

            auth = config.get("auth") or ("LDAP" if config.get("password") else "PLAIN")

            # Try impyla first (better Windows support)
            conn = None
            last_error = None

            if IMPALA_AVAILABLE:
                try:
                    # Map auth mechanisms to impyla format
                    impala_auth = "PLAIN"
                    if auth == "LDAP":
                        impala_auth = "LDAP"
                    elif auth in ("NONE", "NOSASL"):
                        impala_auth = "NOSASL"

                    conn_kwargs = {
                        "host": config["host"],
                        "port": port,
                        "database": schema,
                        "auth_mechanism": impala_auth,
                    }
                    # Add password only for LDAP
                    if impala_auth == "LDAP" and config.get("password"):
                        conn_kwargs["password"] = config["password"]
                    else:
                        conn_kwargs["user"] = config["username"]

                    conn = impala_connect(**conn_kwargs)
                    cls._connections[connection_key] = conn
                    return conn
                except Exception as exc:
                    last_error = exc

            # Fallback to pyhive
            if conn is None and hive is not None:
                try:
                    conn = hive.connect(
                        host=config["host"],
                        port=port,
                        auth=auth,
                        username=config["username"],
                        password=config.get("password"),
                        database=schema,
                        configuration=HiveRuntimeConfig.hive_conf(),
                    )
                    cls._connections[connection_key] = conn
                    return conn
                except Exception as exc:
                    last_error = exc

            if last_error:
                raise ConnectionError(f"Hive connection failed: {last_error}") from last_error
            raise ConnectionError("No Hive driver available (impyla/pyhive not installed)")

    @classmethod
    def close_all_connections(cls):
        with cls._lock:
            for key, conn in list(cls._connections.items()):
                try:
                    conn.close()
                except Exception as exc:
                    print(f"Failed to close connection {key}: {exc}")
                del cls._connections[key]


class JdbcHiveUtils:
    @staticmethod
    def execute_query(schema, sql, env: str | None = None):
        normalized_env = HiveConnectionManager.normalize_env(env)
        if HiveConnectionManager.is_local_env(normalized_env):
            return LocalHiveProcessExecutor.execute_query(schema, sql, normalized_env)

        conn = HiveConnectionManager.get_connection(normalized_env, schema)
        with contextlib.closing(conn.cursor()) as cursor:
            try:
                cursor.execute(sql)
                columns = [col[0] for col in cursor.description]
                header = "\t".join(columns)
                rows = []
                for row in cursor.fetchall():
                    row_data = [
                        str(cell) if cell is not None else "NULL" for cell in row
                    ]
                    rows.append("\t".join(row_data))
                return header if not rows else f"{header}\n" + "\n".join(rows)
            except Exception as exc:
                raise RuntimeError(
                    f"Hive query execution failed: {exc}\nSQL: {sql[:500]}"
                ) from exc

    @staticmethod
    def execute(schema, sql, env: str | None = None):
        normalized_env = HiveConnectionManager.normalize_env(env)
        if HiveConnectionManager.is_local_env(normalized_env):
            LocalHiveProcessExecutor.execute(schema, sql, normalized_env)
            return

        conn = HiveConnectionManager.get_connection(normalized_env, schema)
        with contextlib.closing(conn.cursor()) as cursor:
            cursor.execute(sql)
            try:
                conn.commit()
            except Exception:
                pass

    @classmethod
    def execute_batch(cls, schema, sql_list, env: str | None = None):
        normalized_env = HiveConnectionManager.normalize_env(env)
        if HiveConnectionManager.is_local_env(normalized_env):
            return LocalHiveProcessExecutor.execute_batch(
                schema, sql_list, normalized_env
            )

        for index, sql in enumerate(sql_list, 1):
            print(f"Executing statement #{index}: {sql[:100]}...")
            cls.execute(schema, sql, normalized_env)
        return True

    @staticmethod
    def close_all():
        LocalHiveProcessExecutor.close_all()
        HiveConnectionManager.close_all_connections()


class HiveRuntimeConfig:
    DEFAULT_CONFIG_PATH = (
        Path(__file__).resolve().parent.parent / "conf" / "hive_envs.json"
    )
    DEFAULT_AML_CONF_PATH = (
        Path(__file__).resolve().parent.parent / "conf" / "aml_conf.conf"
    )
    ACTIVE_ENV_SECTION = "hive"
    ACTIVE_ENV_OPTION = "active_env"

    @classmethod
    def config_path(cls) -> Path:
        return Path(os.environ.get("HIVE_ENV_CONFIG_PATH", str(cls.DEFAULT_CONFIG_PATH)))

    @classmethod
    @lru_cache(maxsize=1)
    def load(cls) -> dict:
        path = cls.config_path()
        if not path.exists():
            raise FileNotFoundError(
                f"Hive config file not found: {path}. "
                "Please configure conf/hive_envs.json."
            )
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict):
            raise ValueError(f"Invalid Hive config format: {path}")
        return payload

    @classmethod
    def environments(cls) -> dict:
        payload = cls.load()
        environments = payload.get("environments", {})
        if not isinstance(environments, dict):
            raise ValueError(f"Invalid Hive environments config: {cls.config_path()}")
        if not environments:
            raise ValueError(
                f"Hive environments is empty in config file: {cls.config_path()}"
            )
        return environments

    @classmethod
    def active_env(cls) -> str:
        override = os.environ.get("HIVE_ACTIVE_ENV", "").strip().lower()
        if override:
            return override

        conf_path = Path(
            os.environ.get("AML_CONF_PATH", str(cls.DEFAULT_AML_CONF_PATH))
        )
        parser = configparser.ConfigParser()
        if not conf_path.exists():
            raise FileNotFoundError(
                f"AML config file not found: {conf_path}. "
                "Please configure conf/aml_conf.conf."
            )

        parser.read(conf_path, encoding="utf-8")
        selected = parser.get(
            cls.ACTIVE_ENV_SECTION,
            cls.ACTIVE_ENV_OPTION,
            fallback="",
        ).strip().lower()
        if not selected:
            raise ValueError(
                "Missing [hive].active_env in conf/aml_conf.conf. "
                "Please configure an environment name."
            )

        envs = cls.environments()
        if selected not in envs:
            supported = ", ".join(sorted(envs.keys()))
            raise ValueError(
                f"Configured active_env '{selected}' not found in hive_envs.json. "
                f"Supported environments: {supported}"
            )
        return selected

    @classmethod
    def get_env_config(cls, env: str | None) -> dict:
        normalized_env = HiveConnectionManager.normalize_env(env)
        return cls.environments().get(normalized_env, {})

    @classmethod
    def hive_conf(cls) -> dict:
        payload = cls.load()
        hive_conf = payload.get("hive_conf", {})
        if not isinstance(hive_conf, dict):
            raise ValueError(f"Invalid Hive hive_conf config: {cls.config_path()}")
        return hive_conf


if __name__ == "__main__":
    try:
        schema = "imd_aml_safe"
        query = "DESC rrs_aml_bload_base_account_info_fact"
        result = JdbcHiveUtils.execute_query(schema, query)
        print(result[:100000] + ("..." if len(result) > 100000 else ""))
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
    finally:
        JdbcHiveUtils.close_all()
