#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REMOTE_HIVE_MCP_PATH = Path("/Users/xiongyuc/workspace/hive-mcp-remote")
DEFAULT_SCHEMA = "default"
DEFAULT_PARTITION = "2026-03-08"
EXPECTED_TOOLS = {
    "hive_execute_query",
    "hive_execute_dml",
    "hive_describe_table",
    "hive_show_tables",
}

CN_COMPARE_PROMPT = "对账工作流 {table_full} 的 {partition} 分区 主键 id"
CN_VALIDATION_PROMPT = "校验工作流 {table_full} 的 {partition} 分区 主键 id"
CN_SINGLE_STEP_PROMPT = "对比 {table_full} {partition} 分区 主键 id"


def load_module(module_name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def prepare_runtime(remote_hive_mcp_path: Path) -> tuple[Any, Any, Any]:
    os.environ["HIVE_MCP_PATH"] = str(remote_hive_mcp_path)
    os.environ.setdefault("HIVE_MCP_LOG_STDERR", "0")

    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    generate = load_module(
        "intelligent_sql_generate_module",
        REPO_ROOT / ".claude/skills/intelligent_sql_generation/scripts/generate.py",
    )
    sys.modules["generate"] = generate
    generate.load_hive_runtime.cache_clear()
    runtime, _ = generate.load_hive_runtime()
    if runtime is None:
        raise RuntimeError("Failed to initialize the MCP-backed Hive runtime")

    workflow = load_module(
        "sql_workflow_config_loader_module",
        REPO_ROOT / ".claude/skills/sql_workflow/scripts/config_loader.py",
    )
    return runtime, generate, workflow


def bootstrap_sample_table(hive_runtime: Any, schema: str, table_name: str, partition_value: str) -> None:
    drop_sql = f"DROP TABLE IF EXISTS {schema}.{table_name}"
    create_sql = f"""
    CREATE TABLE {schema}.{table_name} (
      id STRING,
      user_id STRING,
      amount INT,
      status STRING
    )
    PARTITIONED BY (ds STRING)
    STORED AS ORC
    """
    insert_sql = f"""
    INSERT OVERWRITE TABLE {schema}.{table_name} PARTITION (ds='{partition_value}')
    SELECT '1' AS id, 'u1' AS user_id, 100 AS amount, 'ok' AS status
    UNION ALL
    SELECT '2' AS id, 'u2' AS user_id, 50 AS amount, 'hold' AS status
    UNION ALL
    SELECT '3' AS id, 'u3' AS user_id, NULL AS amount, 'bad' AS status
    """
    hive_runtime.execute(schema, drop_sql)
    hive_runtime.execute(schema, create_sql)
    hive_runtime.execute(schema, insert_sql)


def cleanup_sample_table(hive_runtime: Any, schema: str, table_name: str) -> None:
    hive_runtime.execute(schema, f"DROP TABLE IF EXISTS {schema}.{table_name}")


def simulate_single_step(generate: Any, table_full: str, partition_value: str) -> dict[str, Any]:
    raw_prompt = CN_SINGLE_STEP_PROMPT.format(table_full=table_full, partition=partition_value)
    partition_info = generate.extract_partition_from_text(f"{partition_value} 分区")
    schema, table_name = table_full.split(".", 1)
    partition_validation = generate.validate_partition_from_metadata(
        schema,
        table_name,
        str(partition_info.get("raw_partition") or ""),
    )
    params = generate.prepare_params(
        "data_diff",
        {
            "source_table": table_full,
            "target_table": table_full,
            "source_partition": str(partition_validation.get("formatted_partition") or ""),
            "target_partition": str(partition_validation.get("formatted_partition") or ""),
            "join_keys": ["id"],
        },
    )
    rendered_sql, ext = generate.render_template("data_diff", params)
    return {
        "prompt": raw_prompt,
        "ext": ext,
        "params": params,
        "sql": rendered_sql,
    }


def simulate_workflow(workflow: Any, prompt: str) -> dict[str, Any]:
    engine = workflow.WorkflowEngine(env="local")
    return engine.execute_from_text(prompt)


def pop_calls(hive_runtime: Any) -> list[Any]:
    if hasattr(hive_runtime, "pop_call_history"):
        return list(hive_runtime.pop_call_history())
    return []


def print_calls(title: str, calls: list[Any]) -> None:
    print(f"\n=== {title} ===")
    if not calls:
        print("(no MCP calls recorded)")
        return

    for index, call in enumerate(calls, start=1):
        tool_name = getattr(call, "tool_name", "")
        arguments = dict(getattr(call, "arguments", {}) or {})
        schema = arguments.get("schema", "")
        sql = str(arguments.get("sql", "") or "")
        table_name = str(arguments.get("table_name", "") or "")
        if sql:
            compact = " ".join(sql.split())[:220]
            print(f"[{index}] tool={tool_name} schema={schema} sql={compact}")
        elif table_name:
            print(f"[{index}] tool={tool_name} schema={schema} table={table_name}")
        else:
            print(f"[{index}] tool={tool_name} args={arguments}")


def ensure(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Regression for skill-driven SQL generation against hive-mcp-remote via real MCP calls"
    )
    parser.add_argument("--remote-hive-mcp-path", default=str(DEFAULT_REMOTE_HIVE_MCP_PATH))
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--partition", default=DEFAULT_PARTITION)
    parser.add_argument("--keep-table", action="store_true")
    args = parser.parse_args()

    remote_hive_mcp_path = Path(args.remote_hive_mcp_path).expanduser().resolve()
    if not remote_hive_mcp_path.exists():
        raise SystemExit(f"Remote Hive MCP path not found: {remote_hive_mcp_path}")

    table_name = f"codex_skill_reg_{int(time.time())}"
    table_full = f"{args.schema}.{table_name}"

    hive_runtime, generate, workflow = prepare_runtime(remote_hive_mcp_path)
    runtime_info = hive_runtime.describe() if hasattr(hive_runtime, "describe") else {}
    tool_names = set(hive_runtime.list_tools()) if hasattr(hive_runtime, "list_tools") else set()
    ensure(EXPECTED_TOOLS.issubset(tool_names), f"Missing expected MCP tools: {sorted(EXPECTED_TOOLS - tool_names)}")

    try:
        print(f"Using remote Hive MCP path: {remote_hive_mcp_path}")
        print(f"Resolved MCP runtime: {runtime_info}")
        print(f"Initial runtime stats: {hive_runtime.stats() if hasattr(hive_runtime, "stats") else {}}")
        print(f"Discovered tools: {sorted(tool_names)}")
        print(f"Bootstrapping sample table through MCP: {table_full}")
        bootstrap_sample_table(hive_runtime, args.schema, table_name, args.partition)
        pop_calls(hive_runtime)

        single_step = simulate_single_step(generate, table_full, args.partition)
        single_step_calls = pop_calls(hive_runtime)

        compare_prompt = CN_COMPARE_PROMPT.format(table_full=table_full, partition=args.partition)
        compare_result = simulate_workflow(workflow, compare_prompt)
        compare_calls = pop_calls(hive_runtime)

        validation_prompt = CN_VALIDATION_PROMPT.format(table_full=table_full, partition=args.partition)
        validation_result = simulate_workflow(workflow, validation_prompt)
        validation_calls = pop_calls(hive_runtime)

        ensure(bool(single_step.get("sql")), "Single-step SQL generation returned empty SQL")
        ensure(compare_result.get("success") is True, f"Workflow data_compare failed: {compare_result}")
        ensure(validation_result.get("success") is True, f"Workflow data_validation failed: {validation_result}")
        ensure(
            any(getattr(call, "tool_name", "") == "hive_execute_query" for call in single_step_calls),
            "Single-step flow did not call hive_execute_query via MCP",
        )
        ensure(
            any(getattr(call, "tool_name", "") == "hive_execute_query" for call in compare_calls),
            "data_compare flow did not call hive_execute_query via MCP",
        )
        ensure(
            any(getattr(call, "tool_name", "") == "hive_execute_query" for call in validation_calls),
            "data_validation flow did not call hive_execute_query via MCP",
        )

        print("\n=== Simulated Single-Step Skill Request ===")
        print(single_step["prompt"])
        print("\n--- Rendered SQL ---")
        print(single_step["sql"])
        print_calls("MCP calls during single-step generation", single_step_calls)

        print("\n=== Simulated Workflow Request: data_compare ===")
        print(compare_prompt)
        print(
            f"success={compare_result.get('success')} "
            f"scenario={compare_result.get('scenario')} steps={compare_result.get('steps')}"
        )
        print(compare_result.get("generated_sql", [""])[0][:1200])
        print_calls("MCP calls during workflow generation (data_compare)", compare_calls)

        print("\n=== Simulated Workflow Request: data_validation ===")
        print(validation_prompt)
        print(
            f"success={validation_result.get('success')} "
            f"scenario={validation_result.get('scenario')} steps={validation_result.get('steps')}"
        )
        print(validation_result.get("generated_sql", [""])[0][:1200])
        print_calls("MCP calls during workflow generation (data_validation)", validation_calls)

        final_stats = hive_runtime.stats() if hasattr(hive_runtime, "stats") else {}
        ensure(final_stats.get("session_mode") == "shared", f"Unexpected session mode: {final_stats}")
        ensure(final_stats.get("session_starts") == 1, f"Expected one shared MCP session, got: {final_stats}")

        print("\n=== Regression Verdict ===")
        print("- Skill path simulated: intelligent_sql_generation helper flow + sql_workflow natural-language flow")
        print(f"- Remote MCP server root: {remote_hive_mcp_path}")
        print(f"- MCP transport command: {runtime_info.get('command')} {' '.join(runtime_info.get('args', []))}")
        print(f"- Single-step MCP calls recorded: {len(single_step_calls)}")
        print(f"- data_compare MCP calls recorded: {len(compare_calls)}")
        print(f"- Shared session stats: {final_stats}")
        print(f"- data_validation MCP calls recorded: {len(validation_calls)}")
        print("- sql-gen now talks to hive_exec_server.py through the MCP stdio protocol.")
        print("- Chinese trigger phrases still route to the same deterministic SQL templates.")
        return 0
    finally:
        if not args.keep_table:
            cleanup_sample_table(hive_runtime, args.schema, table_name)


if __name__ == "__main__":
    raise SystemExit(main())
