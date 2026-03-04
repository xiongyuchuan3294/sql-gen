#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Hive connection and execution helpers."""

from __future__ import annotations

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


# Change this to "local" when you want all callers that omit env to use
# the local debug Hive environment by default.
DEFAULT_HIVE_ENV = "local"

# Accept a few common aliases to reduce accidental remote fallback.
HIVE_ENV_ALIASES = {
    "local_test": "local",
}


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
    def _config(cls):
        config = HiveRuntimeConfig.get_env_config(HiveConnectionManager.LOCAL_ENV)
        root = Path(
            os.environ.get(
                "LOCAL_HIVE_ROOT",
                config.get("root", r"D:\workspace\hive-local-test"),
            )
        )
        python_path = Path(
            os.environ.get(
                "LOCAL_HIVE_PYTHON",
                config.get("python", str(root / ".venv" / "Scripts" / "python.exe")),
            )
        )
        runner_path = Path(
            os.environ.get(
                "LOCAL_HIVE_RUNNER",
                config.get("runner", str(root / "run_hive_sql.py")),
            )
        )
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
    def _run(cls, schema: str, sql: str) -> dict:
        root, python_path, runner_path = cls._config()
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
    def execute_query(cls, schema: str, sql: str) -> str:
        payload = cls._run(schema, sql)
        results = payload.get("results", [])
        if not results:
            return ""
        result = results[-1]
        return cls._format_result(result.get("columns", []), result.get("rows", []))

    @classmethod
    def execute(cls, schema: str, sql: str) -> None:
        cls._run(schema, sql)

    @classmethod
    def execute_batch(cls, schema: str, sql_list) -> bool:
        sql_text = ";\n".join(
            item.strip().rstrip(";") for item in sql_list if item.strip()
        )
        if not sql_text:
            return True
        cls._run(schema, sql_text)
        return True

    @staticmethod
    def close_all() -> None:
        return None


class HiveConnectionManager:
    LOCAL_ENV = "local"
    _lock = threading.Lock()
    _connections = OrderedDict()

    @classmethod
    def normalize_env(cls, env: str | None = None) -> str:
        normalized_env = (env or DEFAULT_HIVE_ENV).strip().lower()
        return HIVE_ENV_ALIASES.get(normalized_env, normalized_env)

    @classmethod
    def is_local_env(cls, env: str | None = None) -> bool:
        return cls.normalize_env(env) == cls.LOCAL_ENV

    @classmethod
    def supported_envs(cls):
        return sorted(HiveRuntimeConfig.environments().keys())

    @classmethod
    def get_connection(cls, env: str | None = None, schema="default"):
        normalized_env = cls.normalize_env(env)
        if cls.is_local_env(normalized_env):
            raise ValueError("Local mode does not use a remote Hive connection.")

        if hive is None:
            raise ImportError(
                "pyhive is required for remote Hive execution. "
                "Install it or use env='local' for local debugging."
            )

        connection_key = f"{normalized_env}:{schema}"
        with cls._lock:
            if connection_key in cls._connections:
                return cls._connections[connection_key]

            config = HiveRuntimeConfig.get_env_config(normalized_env)
            if not config:
                supported = ", ".join(cls.supported_envs())
                raise ValueError(
                    f"Invalid environment: {env}. Supported environments: {supported}"
                )
            if config.get("mode", "remote") != "remote":
                raise ValueError(
                    f"Environment {env} is not configured as a remote Hive connection."
                )

            auth = "LDAP" if config.get("password") else None
            try:
                conn = hive.connect(
                    host=config["host"],
                    port=config.get("port", 10000),
                    auth=auth,
                    username=config["username"],
                    password=config.get("password"),
                    database=schema,
                    configuration=HiveRuntimeConfig.hive_conf(),
                )
            except Exception as exc:
                raise ConnectionError(f"Hive connection failed: {exc}") from exc

            cls._connections[connection_key] = conn
            return conn

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
            return LocalHiveProcessExecutor.execute_query(schema, sql)

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
            LocalHiveProcessExecutor.execute(schema, sql)
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
            return LocalHiveProcessExecutor.execute_batch(schema, sql_list)

        for index, sql in enumerate(sql_list, 1):
            print(f"Executing statement #{index}: {sql[:100]}...")
            cls.execute(schema, sql, normalized_env)
        return True

    @staticmethod
    def close_all():
        LocalHiveProcessExecutor.close_all()
        HiveConnectionManager.close_all_connections()


class HiveRuntimeConfig:
    DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "conf" / "hive_envs.json"
    DEFAULT_CONFIG = {
        "hive_conf": {
            "mapreduce.job.queuename": "queue_1006_01",
            "hive.exec.mode.local.auto": "true",
            "hive.exec.mode.local.auto.inputbytes.max": "100000000",
            "hive.exec.mode.local.auto.input.files.max": "50",
        },
        "environments": {
            "local": {
                "mode": "local",
                "root": r"D:\workspace\hive-local-test",
                "python": r"D:\workspace\hive-local-test\.venv\Scripts\python.exe",
                "runner": r"D:\workspace\hive-local-test\run_hive_sql.py",
            },
            "uat": {
                "mode": "remote",
                "host": "172.21.1.168",
                "port": 10000,
                "username": "hduser1009",
                "password": "Hduser1009@1234.#",
            },
        },
    }

    @classmethod
    def config_path(cls) -> Path:
        return Path(os.environ.get("HIVE_ENV_CONFIG_PATH", str(cls.DEFAULT_CONFIG_PATH)))

    @classmethod
    @lru_cache(maxsize=1)
    def load(cls) -> dict:
        path = cls.config_path()
        if not path.exists():
            return cls.DEFAULT_CONFIG
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
        return environments

    @classmethod
    def get_env_config(cls, env: str) -> dict:
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
