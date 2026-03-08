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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REMOTE_HIVE_MCP_PATH = Path("/Users/xiongyuc/workspace/hive-mcp-remote")
DEFAULT_SCHEMA = "default"
DEFAULT_PARTITION = "2026-03-08"
GENERATE_SCRIPT = REPO_ROOT / ".claude/skills/intelligent_sql_generation/scripts/generate.py"
HIVE_MCP_RUNTIME_ADAPTER = REPO_ROOT / ".claude/skills/intelligent_sql_generation/scripts/hive_mcp_runtime.py"
SKILL_OUTPUT_DIR = REPO_ROOT / ".claude/skills/intelligent_sql_generation/output"
REGRESSION_ROOT = SKILL_OUTPUT_DIR / "template_e2e_regression"
PROMPT_DIR = REGRESSION_ROOT / "prompts"
GENERATED_DIR = REGRESSION_ROOT / "generated"
RESOLVED_DIR = REGRESSION_ROOT / "resolved"
LOG_DIR = REGRESSION_ROOT / "logs"
EXPECTED_DATA_DIFF_COMPARE_COLUMNS = [
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


@dataclass
class RegressionContext:
    schema: str
    partition: str
    source_table: str
    target_table: str
    aux_table: str

    @property
    def source_full(self) -> str:
        return f"{self.schema}.{self.source_table}"

    @property
    def target_full(self) -> str:
        return f"{self.schema}.{self.target_table}"

    @property
    def aux_full(self) -> str:
        return f"{self.schema}.{self.aux_table}"


@dataclass
class TemplateCase:
    template_name: str
    expected_ext: str
    build_prompt: Callable[[RegressionContext], str]
    expected_substrings: Callable[[RegressionContext], list[str]]
    expect_mcp: bool = False
    expected_tools: list[str] = field(default_factory=list)


@dataclass
class CaseResult:
    template_name: str
    prompt: str
    prompt_path: str
    resolved_yaml_path: str
    output_copy: str
    expected_ext: str
    mcp_events: list[dict[str, Any]]
    passed: bool
    stdout: str
    stderr: str


def load_module(module_name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def prepare_runtime(remote_hive_mcp_path: Path) -> Any:
    os.environ["HIVE_MCP_PATH"] = str(remote_hive_mcp_path)
    os.environ.setdefault("HIVE_MCP_LOG_STDERR", "0")
    adapter = load_module("template_regression_hive_mcp_runtime", HIVE_MCP_RUNTIME_ADAPTER)
    runtime, _ = adapter.build_hive_runtime(REPO_ROOT)
    return runtime


def bootstrap_sample_tables(runtime: Any, context: RegressionContext) -> None:
    tables = [context.source_table, context.target_table, context.aux_table]
    for table_name in tables:
        runtime.execute(context.schema, f"DROP TABLE IF EXISTS {context.schema}.{table_name}")

    create_template = """
    CREATE TABLE {schema}.{table_name} (
      id STRING,
      cust_id STRING,
      user_id STRING,
      amount INT,
      status STRING,
      updated_at STRING,
      email STRING,
      phone STRING,
      address STRING
    )
    PARTITIONED BY (ds STRING)
    STORED AS ORC
    """

    insert_source = f"""
    INSERT OVERWRITE TABLE {context.schema}.{context.source_table} PARTITION (ds='{context.partition}')
    SELECT '1', 'c1', 'u1', 100, 'ok', '2026-03-08 01:00:00', 'user1@example.com', '13800000001', 'addr1'
    UNION ALL
    SELECT '2', 'c2', 'u2', 50, 'hold', '2026-03-08 02:00:00', NULL, '13800000002', 'addr2'
    UNION ALL
    SELECT '3', 'c2', 'u3', NULL, 'deleted', '2026-03-08 03:00:00', 'user3@example.com', NULL, 'addr3'
    """

    insert_target = f"""
    INSERT OVERWRITE TABLE {context.schema}.{context.target_table} PARTITION (ds='{context.partition}')
    SELECT '1', 'c1', 'u1', 100, 'ok', '2026-03-08 01:00:00', 'user1@example.com', '13800000001', 'addr1'
    UNION ALL
    SELECT '3', 'c3', 'u3', 70, 'ok', '2026-03-08 03:00:00', 'user3@example.com', '13800000003', 'addr3'
    """

    insert_aux = f"""
    INSERT OVERWRITE TABLE {context.schema}.{context.aux_table} PARTITION (ds='{context.partition}')
    SELECT '8', 'c8', 'u8', 800, 'ok', '2026-03-08 08:00:00', 'user8@example.com', '13800000008', 'addr8'
    UNION ALL
    SELECT '9', 'c9', 'u9', 900, 'review', '2026-03-08 09:00:00', 'user9@example.com', '13800000009', 'addr9'
    """

    for table_name in tables:
        runtime.execute(context.schema, create_template.format(schema=context.schema, table_name=table_name))
    runtime.execute(context.schema, insert_source)
    runtime.execute(context.schema, insert_target)
    runtime.execute(context.schema, insert_aux)


def cleanup_sample_tables(runtime: Any, context: RegressionContext) -> None:
    for table_name in [context.source_table, context.target_table, context.aux_table]:
        runtime.execute(context.schema, f"DROP TABLE IF EXISTS {context.schema}.{table_name}")


def build_cases() -> list[TemplateCase]:
    return [
        TemplateCase(
            template_name="alter_table",
            expected_ext="sql",
            build_prompt=lambda ctx: f"Add column risk_level string and change amount to total_amount int on {ctx.source_full}.",
            expected_substrings=lambda ctx: [
                f"ALTER TABLE {ctx.source_full}",
                "ADD COLUMNS",
                "CHANGE COLUMN amount total_amount int",
            ],
        ),
        TemplateCase(
            template_name="anti_join",
            expected_ext="sql",
            build_prompt=lambda ctx: f"Find rows in {ctx.source_full} that do not exist in {ctx.target_full} for ds={ctx.partition} using key id.",
            expected_substrings=lambda ctx: [
                f"LEFT JOIN {ctx.target_full} t2",
                "t2.id IS NULL",
                f"t2.ds='{ctx.partition}'",
            ],
        ),
        TemplateCase(
            template_name="batch_data_num",
            expected_ext="sql",
            build_prompt=lambda ctx: f"Count rows for {ctx.source_full}, {ctx.target_full}, and {ctx.aux_full} on ds={ctx.partition}.",
            expected_substrings=lambda ctx: [
                f"SELECT '{ctx.source_full}' as table_name",
                f"SELECT '{ctx.target_full}' as table_name",
                f"SELECT '{ctx.aux_full}' as table_name",
            ],
        ),
        TemplateCase(
            template_name="check_field_len",
            expected_ext="sql",
            build_prompt=lambda ctx: f"Find the longest values in column address from {ctx.source_full} for ds={ctx.partition}.",
            expected_substrings=lambda ctx: [
                "length(cast(address as string))",
                f"FROM {ctx.source_full}",
                "LIMIT 5",
            ],
        ),
        TemplateCase(
            template_name="create_temp_partition",
            expected_ext="sql",
            build_prompt=lambda ctx: f"Create an empty temp partition ds={ctx.partition}-temp for {ctx.source_full}.",
            expected_substrings=lambda ctx: [
                f"ALTER TABLE {ctx.source_full} ADD PARTITION (ds='{ctx.partition}-temp')",
            ],
        ),
        TemplateCase(
            template_name="data_clean",
            expected_ext="sql",
            build_prompt=lambda ctx: f"Overwrite {ctx.source_full} for ds={ctx.partition} while filtering out rows where status = 'deleted'.",
            expected_substrings=lambda ctx: [
                f"INSERT OVERWRITE TABLE {ctx.source_full} PARTITION (ds='{ctx.partition}')",
                "status = 'deleted'",
            ],
        ),
        TemplateCase(
            template_name="data_diff",
            expected_ext="sql",
            build_prompt=lambda ctx: f"对比 {ctx.source_full} 和 {ctx.target_full} 在 {ctx.partition} 分区的数据，主键 id。",
            expected_substrings=lambda ctx: [
                "FULL OUTER JOIN",
                "t1.id = t2.id",
                f"FROM {ctx.source_full}",
                f"FROM {ctx.target_full}",
            ],
            expect_mcp=True,
            expected_tools=["hive_execute_query"],
        ),
        TemplateCase(
            template_name="data_num",
            expected_ext="sql",
            build_prompt=lambda ctx: f"Count rows in {ctx.source_full} for ds={ctx.partition}.",
            expected_substrings=lambda ctx: [
                "SELECT COUNT(1) as total_count",
                f"FROM {ctx.source_full}",
            ],
        ),
        TemplateCase(
            template_name="drop_partition",
            expected_ext="sql",
            build_prompt=lambda ctx: f"Generate SQL to drop partition ds={ctx.partition} from {ctx.source_full}.",
            expected_substrings=lambda ctx: [
                f"ALTER TABLE {ctx.source_full} DROP IF EXISTS PARTITION (ds='{ctx.partition}')",
            ],
        ),
        TemplateCase(
            template_name="field_dist",
            expected_ext="sql",
            build_prompt=lambda ctx: f"Show the value distribution of status in {ctx.source_full} for ds={ctx.partition}.",
            expected_substrings=lambda ctx: [
                "GROUP BY status",
                "ORDER BY cnt DESC",
            ],
        ),
        TemplateCase(
            template_name="group_top_n",
            expected_ext="sql",
            build_prompt=lambda ctx: f"Get top 3 rows per cust_id ordered by updated_at desc from {ctx.source_full} for ds={ctx.partition}.",
            expected_substrings=lambda ctx: [
                "row_number() over (partition by cust_id order by updated_at desc)",
                "rn <= 3",
            ],
        ),
        TemplateCase(
            template_name="insert_values",
            expected_ext="sql",
            build_prompt=lambda ctx: f"Insert mock rows into {ctx.source_full} for ds={ctx.partition}.",
            expected_substrings=lambda ctx: [
                f"INSERT INTO TABLE {ctx.source_full} PARTITION (ds='{ctx.partition}')",
                "'mock_id'",
            ],
        ),
        TemplateCase(
            template_name="move_partition",
            expected_ext="sql",
            build_prompt=lambda ctx: f"把 {ctx.source_full} 的 ds={ctx.partition} 分区迁移到 ds={ctx.partition}-temp。",
            expected_substrings=lambda ctx: [
                f"PARTITION (ds='{ctx.partition}-temp')",
                f"WHERE ds='{ctx.partition}'",
            ],
        ),
        TemplateCase(
            template_name="null_checks",
            expected_ext="sql",
            build_prompt=lambda ctx: f"Check whether email, phone, and address contain nulls in {ctx.source_full} for ds={ctx.partition}.",
            expected_substrings=lambda ctx: [
                "email_null_count",
                "phone_null_count",
                "address_null_count",
            ],
        ),
        TemplateCase(
            template_name="null_rate",
            expected_ext="sql",
            build_prompt=lambda ctx: f"Calculate the null rate of email and phone in {ctx.source_full} for ds={ctx.partition}.",
            expected_substrings=lambda ctx: [
                "email_null_rate",
                "phone_null_rate",
                "count(*) as total_count",
            ],
        ),
        TemplateCase(
            template_name="repeat_check",
            expected_ext="sql",
            build_prompt=lambda ctx: f"Check duplicate cust_id values in {ctx.source_full} for ds={ctx.partition}.",
            expected_substrings=lambda ctx: [
                "GROUP BY cust_id",
                "HAVING COUNT(1) > 1",
            ],
        ),
        TemplateCase(
            template_name="union_merge",
            expected_ext="sql",
            build_prompt=lambda ctx: f"Generate a union query that combines {ctx.source_full} and {ctx.target_full} for ds={ctx.partition}.",
            expected_substrings=lambda ctx: [
                "UNION ALL",
                f"FROM {ctx.source_full}",
                f"FROM {ctx.target_full}",
            ],
        ),
        TemplateCase(
            template_name="hdfs_du",
            expected_ext="sh",
            build_prompt=lambda ctx: f"查询 {ctx.source_table} 表 {ctx.partition} 分区的 HDFS 大小。",
            expected_substrings=lambda ctx: [
                "hadoop fs -du -h",
                f"/user/hive/warehouse/hduser1009/{ctx.schema}.db/{ctx.source_table}/ds={ctx.partition}",
            ],
            expect_mcp=True,
            expected_tools=["hive_execute_query"],
        ),
    ]


def parse_call_log(log_path: Path) -> list[dict[str, Any]]:
    if not log_path.exists():
        return []
    events: list[dict[str, Any]] = []
    for line in log_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped:
            events.append(json.loads(stripped))
    return events


def ensure(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def run_case(case: TemplateCase, context: RegressionContext, remote_hive_mcp_path: Path, sequence: int) -> CaseResult:
    prompt = case.build_prompt(context)

    PROMPT_DIR.mkdir(parents=True, exist_ok=True)
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    RESOLVED_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    prompt_path = PROMPT_DIR / f"{sequence:02d}_{case.template_name}.txt"
    prompt_path.write_text(prompt, encoding="utf-8")

    output_file = SKILL_OUTPUT_DIR / f"{case.template_name}_generated.{case.expected_ext}"
    resolved_yaml_file = SKILL_OUTPUT_DIR / f"{case.template_name}_resolved.yaml"
    call_log_path = LOG_DIR / f"{sequence:02d}_{case.template_name}.jsonl"

    for path in [output_file, resolved_yaml_file, call_log_path]:
        if path.exists():
            path.unlink()

    env = dict(os.environ)
    env["HIVE_MCP_PATH"] = str(remote_hive_mcp_path)
    env["HIVE_MCP_LOG_STDERR"] = "0"
    env["HIVE_MCP_CALL_LOG_PATH"] = str(call_log_path)

    completed = subprocess.run(
        [sys.executable, str(GENERATE_SCRIPT), "--prompt", prompt],
        cwd=str(REPO_ROOT),
        env=env,
        text=True,
        capture_output=True,
    )

    ensure(completed.returncode == 0, f"Template {case.template_name} failed: {completed.stderr or completed.stdout}")
    ensure(output_file.exists(), f"Output file not created for {case.template_name}: {output_file}")
    ensure(resolved_yaml_file.exists(), f"Resolved YAML was not created for {case.template_name}: {resolved_yaml_file}")

    resolved_payload = yaml.safe_load(resolved_yaml_file.read_text(encoding="utf-8")) or {}
    ensure(resolved_payload.get("type") == case.template_name, f"Dispatcher resolved {resolved_payload.get('type')} instead of {case.template_name}")
    ensure(str(resolved_payload.get("prompt") or "").strip() == prompt, f"Prompt mismatch in resolved YAML for {case.template_name}")

    if case.template_name == "data_diff":
        resolved_params = resolved_payload.get("params") or {}
        compare_columns = list(resolved_params.get("compare_columns") or [])
        ensure(
            compare_columns == EXPECTED_DATA_DIFF_COMPARE_COLUMNS,
            f"data_diff compare_columns order mismatch: expected {EXPECTED_DATA_DIFF_COMPARE_COLUMNS}, got {compare_columns}",
        )

    content = output_file.read_text(encoding="utf-8")
    for token in case.expected_substrings(context):
        ensure(token in content, f"Output for {case.template_name} missing token: {token}")

    events = parse_call_log(call_log_path)
    tool_events = [event for event in events if event.get("event") == "tool_call"]
    session_events = [event for event in events if event.get("event") == "session_started"]

    if case.expect_mcp:
        ensure(tool_events, f"Expected MCP calls for {case.template_name}, but none were recorded")
        ensure(session_events, f"Expected a session start for {case.template_name}, but none were recorded")
        recorded_tools = {str(event.get("tool_name", "")) for event in tool_events}
        for expected_tool in case.expected_tools:
            ensure(expected_tool in recorded_tools, f"Template {case.template_name} missing MCP tool: {expected_tool}")
    else:
        ensure(not tool_events, f"Template {case.template_name} unexpectedly called MCP: {tool_events}")

    output_copy = GENERATED_DIR / f"{sequence:02d}_{case.template_name}.{case.expected_ext}"
    resolved_copy = RESOLVED_DIR / f"{sequence:02d}_{case.template_name}.yaml"
    shutil.copyfile(output_file, output_copy)
    shutil.copyfile(resolved_yaml_file, resolved_copy)

    return CaseResult(
        template_name=case.template_name,
        prompt=prompt,
        prompt_path=str(prompt_path),
        resolved_yaml_path=str(resolved_copy),
        output_copy=str(output_copy),
        expected_ext=case.expected_ext,
        mcp_events=events,
        passed=True,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def write_reports(results: list[CaseResult], context: RegressionContext, remote_hive_mcp_path: Path, started_at: int) -> tuple[Path, Path]:
    report_payload = {
        "started_at": started_at,
        "remote_hive_mcp_path": str(remote_hive_mcp_path),
        "context": {
            "schema": context.schema,
            "partition": context.partition,
            "source_full": context.source_full,
            "target_full": context.target_full,
            "aux_full": context.aux_full,
        },
        "summary": {
            "total_cases": len(results),
            "mcp_cases": sum(1 for item in results if any(event.get("event") == "tool_call" for event in item.mcp_events)),
            "non_mcp_cases": sum(1 for item in results if not any(event.get("event") == "tool_call" for event in item.mcp_events)),
        },
        "cases": [
            {
                "template_name": item.template_name,
                "prompt": item.prompt,
                "prompt_path": item.prompt_path,
                "resolved_yaml_path": item.resolved_yaml_path,
                "output_copy": item.output_copy,
                "expected_ext": item.expected_ext,
                "mcp_events": item.mcp_events,
                "passed": item.passed,
                "stdout": item.stdout,
                "stderr": item.stderr,
            }
            for item in results
        ],
    }

    json_path = REGRESSION_ROOT / f"report_{started_at}.json"
    md_path = REGRESSION_ROOT / f"report_{started_at}.md"
    json_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# Template E2E Regression Report",
        "",
        f"- remote_hive_mcp_path: `{remote_hive_mcp_path}`",
        f"- partition: `{context.partition}`",
        f"- source_table: `{context.source_full}`",
        f"- target_table: `{context.target_full}`",
        f"- aux_table: `{context.aux_full}`",
        f"- total_cases: `{len(results)}`",
        "",
        "## Cases",
        "",
    ]
    for item in results:
        tool_events = [event for event in item.mcp_events if event.get("event") == "tool_call"]
        lines.extend(
            [
                f"### {item.template_name}",
                f"- prompt: `{item.prompt}`",
                f"- prompt_file: `{item.prompt_path}`",
                f"- resolved_yaml: `{item.resolved_yaml_path}`",
                f"- output: `{item.output_copy}`",
                f"- mcp_tool_calls: `{len(tool_events)}`",
                f"- passed: `{item.passed}`",
                "",
            ]
        )
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run prompt-only end-to-end regression for all SQL and HDFS templates")
    parser.add_argument("--remote-hive-mcp-path", default=str(DEFAULT_REMOTE_HIVE_MCP_PATH))
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--partition", default=DEFAULT_PARTITION)
    parser.add_argument("--keep-tables", action="store_true")
    args = parser.parse_args()

    remote_hive_mcp_path = Path(args.remote_hive_mcp_path).expanduser().resolve()
    ensure(remote_hive_mcp_path.exists(), f"Remote Hive MCP path not found: {remote_hive_mcp_path}")

    started_at = int(time.time())
    context = RegressionContext(
        schema=args.schema,
        partition=args.partition,
        source_table=f"codex_tpl_src_{started_at}",
        target_table=f"codex_tpl_tgt_{started_at}",
        aux_table=f"codex_tpl_aux_{started_at}",
    )
    cases = build_cases()
    runtime = prepare_runtime(remote_hive_mcp_path)

    REGRESSION_ROOT.mkdir(parents=True, exist_ok=True)

    try:
        print(f"Using remote Hive MCP path: {remote_hive_mcp_path}")
        print(f"Bootstrapping regression tables: {context.source_full}, {context.target_full}, {context.aux_full}")
        bootstrap_sample_tables(runtime, context)

        results: list[CaseResult] = []
        for index, case in enumerate(cases, start=1):
            print(f"[{index:02d}/{len(cases)}] Running template: {case.template_name}")
            results.append(run_case(case, context, remote_hive_mcp_path, index))

        json_path, md_path = write_reports(results, context, remote_hive_mcp_path, started_at)

        print("\n=== Regression Summary ===")
        print(f"- Total templates: {len(results)}")
        print(f"- SQL templates: {sum(1 for item in results if item.expected_ext == 'sql')}")
        print(f"- HDFS templates: {sum(1 for item in results if item.expected_ext == 'sh')}")
        print(f"- Report JSON: {json_path}")
        print(f"- Report Markdown: {md_path}")
        print(f"- Prompt files: {PROMPT_DIR}")
        print(f"- Resolved YAML files: {RESOLVED_DIR}")
        print(f"- Generated outputs: {GENERATED_DIR}")
        print(f"- Call logs: {LOG_DIR}")
        return 0
    finally:
        if not args.keep_tables:
            cleanup_sample_tables(runtime, context)


if __name__ == "__main__":
    raise SystemExit(main())
