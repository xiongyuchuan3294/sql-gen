#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""YAML-driven SQL workflow executor.

Design goal:
1. AI is responsible for semantic parsing (NL -> workflow YAML input).
2. Python is responsible for deterministic workflow SQL generation.
3. Reuse intelligent_sql_generation/scripts/generate.py functions directly.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


CURRENT_DIR = Path(__file__).resolve().parent
SCENARIO_CONFIG_DIR = CURRENT_DIR / "config" / "scenarios"
OUTPUT_DIR = CURRENT_DIR / "output"
INTELLIGENT_SQL_SCRIPT_DIR = CURRENT_DIR.parent / "intelligent_sql_generation" / "scripts"
DB_NAME_HINTS = (
    "imd_aml300_ads_safe",
    "imd_amlai_ads_safe",
    "imd_aml_dm_safe",
    "imd_aml_safe",
    "imd_rdfs_dm_safe",
    "imd_dm_safe",
)

if str(INTELLIGENT_SQL_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(INTELLIGENT_SQL_SCRIPT_DIR))

from generate import (  # noqa: E402
    discover_db_names_by_table,
    extract_partition_from_text,
    prepare_data_diff_params,
    prepare_params,
    render_template,
    validate_join_keys,
    validate_partition_from_metadata,
)


@dataclass
class ScenarioConfig:
    """Scenario definition loaded from config/scenarios/*.yaml."""

    name: str
    description: str
    keywords: list[str] = field(default_factory=list)
    required_params: list[str] = field(default_factory=list)
    defaults: dict[str, Any] = field(default_factory=dict)
    steps: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_yaml(cls, payload: dict[str, Any]) -> "ScenarioConfig":
        scenario_block = payload.get("scenario", {})
        return cls(
            name=str(scenario_block.get("name", "")).strip(),
            description=str(scenario_block.get("description", "")).strip(),
            keywords=list(scenario_block.get("keywords", []) or []),
            required_params=list(payload.get("required_params", []) or []),
            defaults=dict(payload.get("defaults", {}) or {}),
            steps=list(payload.get("steps", []) or []),
        )


class WorkflowEngine:
    """Deterministic workflow SQL generator."""

    def __init__(self, env: str | None = None):
        self.env = env
        self.scenarios = self._load_scenarios()

    def _load_scenarios(self) -> dict[str, ScenarioConfig]:
        scenarios: dict[str, ScenarioConfig] = {}
        if not SCENARIO_CONFIG_DIR.exists():
            return scenarios

        for yaml_file in sorted(SCENARIO_CONFIG_DIR.glob("*.yaml")):
            payload = yaml.safe_load(yaml_file.read_text(encoding="utf-8-sig")) or {}
            if not isinstance(payload, dict):
                continue
            scenario = ScenarioConfig.from_yaml(payload)
            if scenario.name:
                scenarios[scenario.name] = scenario
        return scenarios

    def recognize_scenario(self, user_input: str) -> str | None:
        text = (user_input or "").lower()
        for scenario in self.scenarios.values():
            if any(str(keyword).lower() in text for keyword in scenario.keywords):
                return scenario.name
        return None

    def extract_params_from_text(self, user_input: str) -> dict[str, Any]:
        """Fallback parser for backward compatibility.

        Preferred mode is still semantic YAML input.
        """
        params: dict[str, Any] = {}
        text = user_input or ""

        db, table_name = self._extract_table_from_text(text)
        if db:
            params["db"] = db
        if table_name:
            params["table_name"] = table_name

        raw_partition = self._extract_raw_partition_from_text(text)
        if raw_partition:
            params["partition"] = raw_partition

        join_keys = self._extract_join_keys_from_text(text)
        if join_keys:
            params["join_keys"] = join_keys

        return params

    @staticmethod
    def _extract_table_from_text(user_input: str) -> tuple[str, str]:
        text = user_input or ""

        full_match = re.search(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b", text)
        if full_match:
            return full_match.group(1), full_match.group(2)

        token_pattern = r"\b([A-Za-z_][A-Za-z0-9_]*)\b"
        raw_tokens = re.findall(token_pattern, text)
        reserved = {
            "and",
            "compare",
            "data",
            "diff",
            "for",
            "from",
            "in",
            "join",
            "key",
            "keys",
            "partition",
            "pk",
            "reconcile",
            "scenario",
            "sql",
            "table",
            "workflow",
        }
        for token in raw_tokens:
            lowered = token.lower()
            if lowered in reserved:
                continue
            if lowered in {"ds", "dt", "pt"}:
                continue
            if "_" not in token and not lowered.startswith("t"):
                continue
            if re.fullmatch(r"\d+", token):
                continue
            return "", token
        return "", ""

    @staticmethod
    def _extract_raw_partition_from_text(user_input: str) -> str:
        text = user_input or ""

        raw_partition = ""
        try:
            parsed = extract_partition_from_text(text) or {}
            raw_partition = str(parsed.get("raw_partition") or "").strip()
        except Exception:
            raw_partition = ""
        if raw_partition:
            return raw_partition

        kv_matches = re.findall(
            r"\b([A-Za-z_][A-Za-z0-9_]*)\s*=\s*['\"]?([A-Za-z0-9_:/.-]+)['\"]?",
            text,
        )
        partition_parts: list[str] = []
        for field, value in kv_matches:
            if field.lower() in {"key", "pk"}:
                continue
            partition_parts.append(f"{field}={value}")
        if partition_parts:
            return ",".join(partition_parts)

        date_match = re.search(r"\b(\d{4}[-/]\d{2}[-/]\d{2})\b", text)
        if date_match:
            return f"ds={date_match.group(1).replace('/', '-')}"

        return ""

    @staticmethod
    def _extract_join_keys_from_text(user_input: str) -> list[str]:
        match = re.search(
            r"(?:主键|join\s*key|keys?|pk)\s*(?:是|为|[:：])?\s*(.+)$",
            user_input,
            flags=re.IGNORECASE,
        )
        if not match:
            return []

        raw = match.group(1)
        for stop_word in ("分区", "partition", "workflow", "工作流", "场景", "scenario", "表"):
            idx = raw.lower().find(stop_word.lower())
            if idx >= 0:
                raw = raw[:idx]
                break

        normalized = (
            raw.replace("和", ",")
            .replace("、", ",")
            .replace("，", ",")
            .replace(";", ",")
            .replace("；", ",")
        )
        normalized = re.sub(r"\band\b", ",", normalized, flags=re.IGNORECASE)
        keys = [
            item.strip()
            for item in normalized.split(",")
            if item.strip() and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", item.strip())
        ]
        return keys

    @staticmethod
    def _normalize_join_keys(raw_keys: Any) -> list[str]:
        if raw_keys is None:
            return []
        if isinstance(raw_keys, list):
            flattened: list[str] = []
            for item in raw_keys:
                flattened.extend(WorkflowEngine._normalize_join_keys(item))
            return flattened

        text = str(raw_keys).strip()
        if not text:
            return []

        text = text.replace("和", ",").replace("、", ",").replace("，", ",").replace(";", ",").replace("；", ",")
        text = re.sub(r"\band\b", ",", text, flags=re.IGNORECASE)
        return [part.strip() for part in text.split(",") if part.strip()]

    @staticmethod
    def _normalize_identifier_list(raw_value: Any) -> list[str]:
        if raw_value is None:
            return []
        if isinstance(raw_value, list):
            normalized: list[str] = []
            for item in raw_value:
                normalized.extend(WorkflowEngine._normalize_identifier_list(item))
            return normalized

        text = str(raw_value).strip()
        if not text:
            return []

        text = text.replace("和", ",").replace("、", ",").replace("，", ",").replace(";", ",").replace("；", ",")
        text = re.sub(r"\band\b", ",", text, flags=re.IGNORECASE)
        output: list[str] = []
        for part in text.split(","):
            item = part.strip()
            if not item:
                continue
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", item):
                output.append(item)
        return output

    @staticmethod
    def _normalize_partition(raw_partition: Any) -> str:
        if raw_partition is None:
            return ""

        if isinstance(raw_partition, dict):
            raw_partition = ",".join(f"{k}={v}" for k, v in raw_partition.items())
        elif isinstance(raw_partition, list):
            raw_partition = ",".join(str(item) for item in raw_partition if str(item).strip())

        text = str(raw_partition).strip()
        if not text:
            return ""

        if re.fullmatch(r"\d{4}[-/]\d{2}[-/]\d{2}", text):
            text = f"ds={text}"

        parts: list[str] = []
        for segment in text.split(","):
            piece = segment.strip()
            if not piece:
                continue
            if "=" not in piece:
                if re.fullmatch(r"\d{4}[-/]\d{2}[-/]\d{2}", piece):
                    parts.append(f"ds='{piece.replace('/', '-')}'")
                continue

            field, value = piece.split("=", 1)
            field = field.strip()
            value = value.strip().strip("'\"")
            if not field:
                continue
            parts.append(f"{field}='{value}'")

        return ",".join(parts)

    @staticmethod
    def _partition_to_where(partition: str) -> str:
        if not partition:
            return ""
        return " AND ".join(part.strip() for part in partition.split(",") if part.strip())

    @staticmethod
    def _build_target_partition(partition: str, temp_suffix: str) -> str:
        if not partition:
            return ""
        output: list[str] = []
        for segment in partition.split(","):
            piece = segment.strip()
            if not piece or "=" not in piece:
                continue
            field, value = piece.split("=", 1)
            normalized_value = value.strip().strip("'\"")
            output.append(f"{field.strip()}='{normalized_value}{temp_suffix}'")
        return ",".join(output)

    @staticmethod
    def _substitute_placeholders(text: str, params: dict[str, Any]) -> str:
        def _replace(match: re.Match[str]) -> str:
            key = match.group(1).strip()
            value = params.get(key)
            if value is None:
                return match.group(0)
            return str(value)

        return re.sub(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}", _replace, text)

    def _normalize_table(self, params: dict[str, Any], env: str | None) -> tuple[str, str]:
        db = str(params.get("db", "") or params.get("schema", "")).strip()
        table_name = str(params.get("table_name", "") or params.get("table", "")).strip()

        if table_name and "." in table_name and not db:
            possible_db, simple_table = table_name.split(".", 1)
            if possible_db and simple_table:
                db = possible_db.strip()
                table_name = simple_table.strip()

        if table_name and not db:
            discovered = discover_db_names_by_table(table_name, env=env)
            if discovered:
                params["possible_dbs"] = discovered
                db = self._select_best_db(table_name, discovered)
                params["resolved_db"] = db
            else:
                inferred = self._infer_db_from_table_name(table_name)
                if inferred:
                    db = inferred
                    params["resolved_db"] = db

        return db, table_name

    @staticmethod
    def _infer_db_from_table_name(table_name: str) -> str:
        table_text = str(table_name or "").lower()
        if not table_text:
            return ""

        for db_name in DB_NAME_HINTS:
            short_name = db_name[4:] if db_name.startswith("imd_") else db_name
            if short_name and short_name in table_text:
                return db_name
        return ""

    @staticmethod
    def _select_best_db(table_name: str, discovered_dbs: list[str]) -> str:
        if not discovered_dbs:
            return ""
        if len(discovered_dbs) == 1:
            return discovered_dbs[0]

        table_text = str(table_name or "").lower()
        best_db = discovered_dbs[0]
        best_score = -1
        for db_name in discovered_dbs:
            db_text = str(db_name).lower()
            short_db = db_text[4:] if db_text.startswith("imd_") else db_text
            db_tokens = [token for token in short_db.split("_") if token]

            score = 0
            if short_db and short_db in table_text:
                score += 100
            score += sum(10 for token in db_tokens if token in table_text)
            score += sum(1 for token in db_tokens if re.search(rf"\b{re.escape(token)}\b", table_text))

            if score > best_score:
                best_score = score
                best_db = db_name

        return best_db
    def _enrich_params(
        self,
        scenario: ScenarioConfig,
        raw_params: dict[str, Any],
        env: str | None,
        user_input: str = "",
    ) -> tuple[dict[str, Any], str | None]:
        params = dict(scenario.defaults)
        params.update(raw_params or {})

        db, table_name = self._normalize_table(params, env)
        params["db"] = db
        params["table_name"] = table_name
        params["table_name_full"] = f"{db}.{table_name}" if db and table_name else table_name

        raw_partition = params.get("raw_partition")
        if not raw_partition and params.get("partition"):
            raw_partition = params.get("partition")
        if not raw_partition and user_input:
            raw_partition = self._extract_raw_partition_from_text(user_input)
        params["raw_partition"] = raw_partition or ""

        normalized_partition = self._normalize_partition(raw_partition)
        params["partition"] = normalized_partition

        if db and table_name:
            validation = validate_partition_from_metadata(
                db_name=db,
                table_name=table_name,
                raw_partition=params.get("raw_partition", ""),
                env=env,
            )
            if not validation.get("valid", True):
                return params, str(validation.get("message", "Partition validation failed"))
            formatted_partition = validation.get("formatted_partition")
            if formatted_partition:
                params["partition"] = formatted_partition

        join_keys = self._normalize_join_keys(params.get("join_keys"))
        if not join_keys and user_input:
            join_keys = self._extract_join_keys_from_text(user_input)
        params["join_keys"] = join_keys

        validation_columns = self._normalize_identifier_list(
            params.get("validation_columns") or params.get("columns")
        )
        if not validation_columns:
            validation_columns = list(join_keys)
        params["validation_columns"] = validation_columns

        group_by_columns = self._normalize_identifier_list(params.get("group_by_columns"))
        if not group_by_columns:
            group_by_columns = list(join_keys)
        params["group_by_columns"] = group_by_columns

        having_threshold = params.get("having_threshold", 1)
        try:
            threshold = int(str(having_threshold).strip())
        except Exception:
            threshold = 1
        params["having_threshold"] = threshold if threshold > 0 else 1

        temp_suffix = str(params.get("temp_suffix", "-temp")).strip() or "-temp"
        params["temp_suffix"] = temp_suffix

        params["target_partition"] = self._build_target_partition(params["partition"], temp_suffix)
        params["partition_where"] = self._partition_to_where(params["partition"])
        params["target_partition_where"] = self._partition_to_where(params["target_partition"])

        return params, None

    def _validate_required_params(self, scenario: ScenarioConfig, params: dict[str, Any]) -> str | None:
        for field_name in scenario.required_params:
            value = params.get(field_name)
            if field_name == "join_keys":
                check = validate_join_keys(self._normalize_join_keys(value))
                if not check.get("valid", False):
                    return str(check.get("message", "join_keys is required"))
                continue

            if value is None:
                return f"Missing required parameter: {field_name}"
            if isinstance(value, str) and not value.strip():
                return f"Missing required parameter: {field_name}"
            if isinstance(value, list) and len(value) == 0:
                return f"Missing required parameter: {field_name}"
        return None

    def _build_step_params(
        self,
        step: dict[str, Any],
        params: dict[str, Any],
        env: str | None,
    ) -> dict[str, Any]:
        template = str(step.get("template", "")).strip()
        if not template:
            raise ValueError("Step template is empty")

        if template == "move_partition":
            step_params = {
                "table_name": params["table_name_full"],
                "source_partition": params["partition"],
                "target_partition": params["target_partition"],
            }
            return prepare_params(template, step_params, env=env)

        if template == "data_num":
            role = str(step.get("partition_role", "source")).strip().lower()
            partition_value = (
                params["target_partition_where"] if role == "target" else params["partition_where"]
            )
            step_params = {
                "table_name": params["table_name_full"],
                "partition": partition_value,
            }
            return prepare_params(template, step_params, env=env)

        if template == "data_diff":
            step_params = prepare_data_diff_params(
                db_name=params.get("db", ""),
                table_name=params.get("table_name", ""),
                source_partition=params.get("partition_where", ""),
                target_partition=params.get("target_partition_where", ""),
                join_keys=params.get("join_keys", []),
                env=env,
            )
            if step_params.get("non_partition_columns") and not step_params.get("compare_columns"):
                step_params["compare_columns"] = step_params["non_partition_columns"]
            if not step_params.get("compare_columns"):
                step_params["compare_columns"] = list(params.get("join_keys", []))
            return prepare_params(template, step_params, env=env)

        if template in {"null_checks", "null_rate"}:
            columns = self._normalize_identifier_list(params.get("validation_columns"))
            if not columns:
                columns = self._normalize_identifier_list(params.get("join_keys"))
            if not columns:
                columns = ["id"]
            step_params = {
                "table_name": params["table_name_full"],
                "partition": params.get("partition_where", ""),
                "columns": columns,
            }
            return prepare_params(template, step_params, env=env)

        if template == "repeat_check":
            group_by_columns = self._normalize_identifier_list(params.get("group_by_columns"))
            if not group_by_columns:
                group_by_columns = self._normalize_identifier_list(params.get("join_keys"))
            if not group_by_columns:
                group_by_columns = ["id"]
            step_params = {
                "table_name": params["table_name_full"],
                "partition": params.get("partition_where", ""),
                "group_by_columns": group_by_columns,
                "having_threshold": params.get("having_threshold", 1),
            }
            return prepare_params(template, step_params, env=env)

        # Generic template step with explicit params mapping.
        raw_step_params = dict(step.get("params", {}) or {})
        step_params: dict[str, Any] = {}
        for key, value in raw_step_params.items():
            if isinstance(value, str):
                step_params[key] = self._substitute_placeholders(value, params)
            else:
                step_params[key] = value
        return prepare_params(template, step_params, env=env)

    def _render_steps(
        self,
        scenario: ScenarioConfig,
        params: dict[str, Any],
        env: str | None,
    ) -> tuple[list[str], list[str]]:
        step_names: list[str] = []
        generated_sql: list[str] = []

        for step in scenario.steps:
            name = str(step.get("name", step.get("template", "step"))).strip()
            template = str(step.get("template", "")).strip()
            step_names.append(name)

            step_params = self._build_step_params(step, params, env)
            sql, _ = render_template(template, step_params)
            generated_sql.append(f"-- Step: {name}\n{sql}")

        return step_names, generated_sql

    def execute_from_payload(
        self,
        scenario_name: str,
        payload_params: dict[str, Any],
        *,
        env: str | None = None,
        user_input: str = "",
    ) -> dict[str, Any]:
        scenario = self.scenarios.get(scenario_name)
        if not scenario:
            return {
                "success": False,
                "error": f"Unknown scenario: {scenario_name}",
            }

        effective_env = env or self.env
        params, enrich_error = self._enrich_params(
            scenario=scenario,
            raw_params=payload_params,
            env=effective_env,
            user_input=user_input,
        )
        if enrich_error:
            return {
                "success": False,
                "scenario": scenario.name,
                "error": enrich_error,
                "params": params,
            }

        validate_error = self._validate_required_params(scenario, params)
        if validate_error:
            return {
                "success": False,
                "scenario": scenario.name,
                "error": validate_error,
                "params": params,
            }

        try:
            step_names, generated_sql = self._render_steps(
                scenario=scenario,
                params=params,
                env=effective_env,
            )
        except Exception as exc:
            return {
                "success": False,
                "scenario": scenario.name,
                "error": f"Failed to render workflow SQL: {exc}",
                "params": params,
            }

        return {
            "success": True,
            "scenario": scenario.name,
            "description": scenario.description,
            "env": effective_env,
            "params": params,
            "steps": step_names,
            "generated_sql": generated_sql,
            "message": f"Generated {len(step_names)} workflow steps",
        }

    def execute_from_text(
        self,
        user_input: str,
        explicit_scenario: str | None = None,
        *,
        env: str | None = None,
    ) -> dict[str, Any]:
        scenario_name = explicit_scenario or self.recognize_scenario(user_input)
        if not scenario_name:
            return {
                "success": False,
                "error": f"Cannot recognize scenario from input: {user_input}",
            }
        params = self.extract_params_from_text(user_input)
        return self.execute_from_payload(
            scenario_name=scenario_name,
            payload_params=params,
            env=env,
            user_input=user_input,
        )

    def execute(self, user_input: str, explicit_scenario: str | None = None) -> dict[str, Any]:
        """Backward compatible entrypoint."""
        return self.execute_from_text(user_input=user_input, explicit_scenario=explicit_scenario)

    @staticmethod
    def _resolve_input_yaml_path(yaml_path: Path) -> Path:
        """Resolve user-provided YAML path for replay mode.

        Supports:
        - absolute or relative existing path
        - `output/<file>.yaml` from any cwd
        - `/output/<file>.yaml` shorthand
        - `<file>.yaml` shorthand (auto lookup in sql_workflow/output)
        """
        candidate = Path(str(yaml_path).strip())
        if candidate.exists():
            return candidate

        candidate_text = candidate.as_posix()
        probe_paths: list[Path] = []

        if not candidate.is_absolute():
            probe_paths.append(CURRENT_DIR / candidate)

        if candidate_text.startswith("/output/"):
            probe_paths.append(OUTPUT_DIR / candidate_text.removeprefix("/output/"))
        elif candidate_text.startswith("output/"):
            probe_paths.append(OUTPUT_DIR / candidate_text.removeprefix("output/"))

        if not candidate.is_absolute() and len(candidate.parts) == 1:
            probe_paths.append(OUTPUT_DIR / candidate.name)

        for path in probe_paths:
            if path.exists():
                return path
        return candidate

    def execute_from_yaml(
        self,
        yaml_path: Path,
        explicit_scenario: str | None = None,
        *,
        env: str | None = None,
    ) -> dict[str, Any]:
        resolved_yaml_path = self._resolve_input_yaml_path(yaml_path)
        if not resolved_yaml_path.exists():
            return {
                "success": False,
                "error": f"YAML file not found: {yaml_path}",
            }

        payload = yaml.safe_load(resolved_yaml_path.read_text(encoding="utf-8-sig")) or {}
        if not isinstance(payload, dict):
            return {
                "success": False,
                "error": f"YAML payload must be a mapping: {resolved_yaml_path}",
            }

        scenario_name = explicit_scenario or str(
            payload.get("scenario") or payload.get("workflow") or payload.get("type") or ""
        ).strip()
        if not scenario_name:
            return {
                "success": False,
                "error": f"Scenario is missing in YAML: {resolved_yaml_path}",
            }

        params = payload.get("params")
        if not isinstance(params, dict):
            params = {
                key: value
                for key, value in payload.items()
                if key not in {"scenario", "workflow", "type", "env"}
            }

        effective_env = env or str(payload.get("env") or "").strip() or None
        return self.execute_from_payload(
            scenario_name=scenario_name,
            payload_params=params,
            env=effective_env,
        )

    def format_result(self, result: dict[str, Any]) -> str:
        if not result.get("success"):
            return "\n".join(
                [
                    "## SQL Workflow Failed",
                    f"- {result.get('error', 'Unknown error')}",
                ]
            )

        lines = [
            "## SQL Workflow Result",
            f"**Scenario**: {result.get('description') or result.get('scenario')}",
            f"**Step Count**: {len(result.get('steps', []))}",
            "",
            "### Generated SQL",
        ]

        for index, sql in enumerate(result.get("generated_sql", []), start=1):
            lines.append("")
            lines.append(f"#### Step {index}")
            lines.append("```sql")
            lines.append(sql)
            lines.append("```")
        return "\n".join(lines)

    @staticmethod
    def _sanitize_filename_token(value: str) -> str:
        token = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "").strip()).strip("_")
        return token.lower()

    @classmethod
    def _partition_label(cls, partition: str) -> str:
        cleaned = str(partition or "").replace("'", "").replace('"', "").strip()
        if not cleaned:
            return ""

        labels: list[str] = []
        for segment in cleaned.split(","):
            piece = segment.strip()
            if not piece or "=" not in piece:
                continue
            field, value = piece.split("=", 1)
            field_token = cls._sanitize_filename_token(field)
            value_token = cls._sanitize_filename_token(value)
            if not field_token or not value_token:
                continue
            if field_token == "ds" and re.fullmatch(r"\d{4}_\d{2}_\d{2}", value_token):
                labels.append(value_token.replace("_", ""))
            else:
                labels.append(f"{field_token}_{value_token}")

        return "_".join(labels)

    def _build_default_output_path(self, result: dict[str, Any]) -> Path:
        scenario = self._sanitize_filename_token(str(result.get("scenario") or "workflow")) or "workflow"
        params = result.get("params") or {}
        table_name = str(params.get("table_name") or params.get("table_name_full") or "")
        table_token = self._sanitize_filename_token(table_name)
        partition_token = self._partition_label(str(params.get("partition") or ""))

        if table_token and partition_token:
            filename = f"{scenario}_{table_token}_{partition_token}.sql"
        elif table_token:
            filename = f"{scenario}_{table_token}.sql"
        else:
            filename = f"{scenario}_workflow.sql"

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        return OUTPUT_DIR / filename

    def save_result(self, result: dict[str, Any], output_path: Path | None = None) -> Path | None:
        if not result.get("success"):
            return None

        if output_path is None:
            output_path = self._build_default_output_path(result)
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)

        all_sql = "\n\n".join(result.get("generated_sql", []))
        output_path.write_text(all_sql, encoding="utf-8")
        return output_path

# Backward-compatible alias for existing callers.
ScenarioExecutor = WorkflowEngine


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SQL workflow deterministic generator")
    parser.add_argument("input", nargs="?", help="Natural language input (legacy mode)")
    parser.add_argument("--yaml", help="Semantic YAML input path (preferred mode)")
    parser.add_argument("--scenario", help="Explicit scenario name override")
    parser.add_argument("--env", help="Hive environment")
    parser.add_argument("--output", "-o", help="Output SQL file path")
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Do not write generated SQL file",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    if not args.input and not args.yaml:
        parser.error("Provide either positional input or --yaml")

    engine = WorkflowEngine(env=args.env)

    if args.yaml:
        result = engine.execute_from_yaml(
            yaml_path=Path(args.yaml),
            explicit_scenario=args.scenario,
            env=args.env,
        )
    else:
        result = engine.execute_from_text(
            user_input=args.input,
            explicit_scenario=args.scenario,
            env=args.env,
        )

    print(engine.format_result(result))

    if not args.no_save:
        output_path = Path(args.output) if args.output else None
        saved_path = engine.save_result(result, output_path)
        if saved_path:
            print(f"\nSaved SQL to: {saved_path}")

    return 0 if result.get("success") else 1


if __name__ == "__main__":
    raise SystemExit(main())




