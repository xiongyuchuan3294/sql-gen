#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REMOTE_HIVE_MCP_PATH = Path("/Users/xiongyuc/workspace/hive-mcp-remote")
DEFAULT_SCHEMA = "default"
DEFAULT_PARTITION = "2026-03-08"
DEFAULT_HASH_SEEDS = ["1", "2", "3", "11", "97"]
EXPECTED_COMPARE_COLUMNS = [
    "id",
    "cust_id",
    "user_id",
    "amount",
    "status",
    "updated_at",
    "email",
    "phone",
    "address",
]
GENERATE_SCRIPT = REPO_ROOT / ".claude/skills/intelligent_sql_generation/scripts/generate.py"
SKILL_OUTPUT_DIR = REPO_ROOT / ".claude/skills/intelligent_sql_generation/output"
ARTIFACT_ROOT = SKILL_OUTPUT_DIR / "data_diff_determinism"
REGRESSION_SCRIPT = REPO_ROOT / "tools/regress_all_templates_remote.py"


def ensure(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def load_module(module_name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def build_prompt(context: Any) -> str:
    return f"对比 {context.source_full} 和 {context.target_full} 在 {context.partition} 分区的数据，主键 id。"


def run_iteration(context: Any, remote_hive_mcp_path: Path, hash_seed: str) -> dict[str, Any]:
    prompt = build_prompt(context)
    output_file = SKILL_OUTPUT_DIR / "data_diff_generated.sql"
    resolved_yaml_file = SKILL_OUTPUT_DIR / "data_diff_resolved.yaml"
    call_log_path = ARTIFACT_ROOT / f"data_diff_seed_{hash_seed}.jsonl"

    for path in [output_file, resolved_yaml_file, call_log_path]:
        if path.exists():
            path.unlink()

    env = dict(os.environ)
    env["HIVE_MCP_PATH"] = str(remote_hive_mcp_path)
    env["HIVE_MCP_LOG_STDERR"] = "0"
    env["HIVE_MCP_CALL_LOG_PATH"] = str(call_log_path)
    env["PYTHONHASHSEED"] = str(hash_seed)

    completed = subprocess.run(
        [sys.executable, str(GENERATE_SCRIPT), "--prompt", prompt],
        cwd=str(REPO_ROOT),
        env=env,
        text=True,
        capture_output=True,
    )
    ensure(completed.returncode == 0, f"data_diff generation failed for PYTHONHASHSEED={hash_seed}: {completed.stderr or completed.stdout}")
    ensure(output_file.exists(), f"Generated SQL missing for PYTHONHASHSEED={hash_seed}: {output_file}")
    ensure(resolved_yaml_file.exists(), f"Resolved YAML missing for PYTHONHASHSEED={hash_seed}: {resolved_yaml_file}")

    resolved_payload = yaml.safe_load(resolved_yaml_file.read_text(encoding="utf-8")) or {}
    resolved_params = resolved_payload.get("params") or {}
    compare_columns = list(resolved_params.get("compare_columns") or [])
    ensure(
        compare_columns == EXPECTED_COMPARE_COLUMNS,
        f"Unexpected compare_columns for PYTHONHASHSEED={hash_seed}: {compare_columns}",
    )

    sql_content = output_file.read_text(encoding="utf-8")
    ordered_select = ", ".join(EXPECTED_COMPARE_COLUMNS)
    ensure(
        f"SELECT {ordered_select}" in sql_content,
        f"Generated SQL lost deterministic column order for PYTHONHASHSEED={hash_seed}",
    )

    seed_root = ARTIFACT_ROOT / f"seed_{hash_seed}"
    seed_root.mkdir(parents=True, exist_ok=True)
    sql_copy = seed_root / "data_diff.sql"
    yaml_copy = seed_root / "data_diff.yaml"
    shutil.copyfile(output_file, sql_copy)
    shutil.copyfile(resolved_yaml_file, yaml_copy)

    call_events: list[dict[str, Any]] = []
    if call_log_path.exists():
        for line in call_log_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped:
                call_events.append(json.loads(stripped))
    tool_names = [str(event.get("tool_name", "")) for event in call_events if event.get("event") == "tool_call"]
    ensure("hive_execute_query" in tool_names, f"Expected MCP metadata query for PYTHONHASHSEED={hash_seed}, got {tool_names}")

    return {
        "hash_seed": str(hash_seed),
        "prompt": prompt,
        "compare_columns": compare_columns,
        "sql_path": str(sql_copy),
        "resolved_yaml_path": str(yaml_copy),
        "call_log_path": str(call_log_path),
        "mcp_tool_names": tool_names,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify that data_diff column ordering stays deterministic across Python hash seeds")
    parser.add_argument("--remote-hive-mcp-path", default=str(DEFAULT_REMOTE_HIVE_MCP_PATH))
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--partition", default=DEFAULT_PARTITION)
    parser.add_argument("--hash-seeds", default=",".join(DEFAULT_HASH_SEEDS))
    parser.add_argument("--keep-tables", action="store_true")
    args = parser.parse_args()

    remote_hive_mcp_path = Path(args.remote_hive_mcp_path).expanduser().resolve()
    ensure(remote_hive_mcp_path.exists(), f"Remote Hive MCP path not found: {remote_hive_mcp_path}")

    regression = load_module("data_diff_regression_module", REGRESSION_SCRIPT)
    started_at = int(time.time())
    context = regression.RegressionContext(
        schema=args.schema,
        partition=args.partition,
        source_table=f"codex_diff_det_src_{started_at}",
        target_table=f"codex_diff_det_tgt_{started_at}",
        aux_table=f"codex_diff_det_aux_{started_at}",
    )
    runtime = regression.prepare_runtime(remote_hive_mcp_path)
    hash_seeds = [item.strip() for item in str(args.hash_seeds).split(",") if item.strip()]
    ensure(hash_seeds, "No hash seeds were provided")

    ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
    report_path = ARTIFACT_ROOT / f"report_{started_at}.json"

    try:
        print(f"Using remote Hive MCP path: {remote_hive_mcp_path}")
        print(f"Bootstrapping stability-check tables: {context.source_full}, {context.target_full}, {context.aux_full}")
        regression.bootstrap_sample_tables(runtime, context)

        results = []
        for index, hash_seed in enumerate(hash_seeds, start=1):
            print(f"[{index:02d}/{len(hash_seeds)}] Verifying data_diff with PYTHONHASHSEED={hash_seed}")
            results.append(run_iteration(context, remote_hive_mcp_path, hash_seed))

        unique_orders = {tuple(item["compare_columns"]) for item in results}
        ensure(len(unique_orders) == 1, f"compare_columns changed across runs: {sorted(unique_orders)}")

        report_payload = {
            "started_at": started_at,
            "remote_hive_mcp_path": str(remote_hive_mcp_path),
            "schema": args.schema,
            "partition": args.partition,
            "hash_seeds": hash_seeds,
            "expected_compare_columns": EXPECTED_COMPARE_COLUMNS,
            "results": results,
        }
        report_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8")

        print("\n=== data_diff Determinism Summary ===")
        print(f"- Hash seeds checked: {', '.join(hash_seeds)}")
        print(f"- compare_columns: {EXPECTED_COMPARE_COLUMNS}")
        print(f"- Report JSON: {report_path}")
        print(f"- Artifacts: {ARTIFACT_ROOT}")
        return 0
    finally:
        if not args.keep_tables:
            regression.cleanup_sample_tables(runtime, context)


if __name__ == "__main__":
    raise SystemExit(main())
