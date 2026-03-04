#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""SQL Scenario Orchestrator - Main entry point for scenario execution."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Any

# Add parent directory to path for imports
repo_root = Path(__file__).parent.parent.parent
scripts_path = repo_root / "agent_skills" / "sql_generation" / "scripts"
if str(scripts_path) not in sys.path:
    sys.path.insert(0, str(scripts_path))

from scenarios import BaseScenario, ScenarioResult, ScenarioStep, get_scenario, recognize_scenario
from generate import (
    discover_db_names_by_table,
    discover_partition_fields,
    load_hive_runtime,
    render_template,
    validate_partition_params,
)


class ScenarioOrchestrator:
    """Orchestrates SQL scenario execution."""

    def __init__(self, env: str | None = None):
        self.env = env
        self.hive_utils = None
        self.default_env = None
        self._init_hive()

    def _init_hive(self):
        """Initialize Hive connection."""
        self.hive_utils, self.default_env = load_hive_runtime()
        if self.env is None:
            self.env = self.default_env

    def extract_params_from_input(self, user_input: str) -> dict[str, Any]:
        """
        Extract parameters from user input.

        Args:
            user_input: User's natural language input

        Returns:
            Extracted parameters
        """
        params = {}

        # 提取表名（支持 db.table 格式）
        # 匹配如: imd_aml_safe.rrs_aml_risk_rate_current 或 rrs_aml_risk_rate_current
        table_pattern = r"([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]+)"
        match = re.search(table_pattern, user_input)
        if match:
            params["db"] = match.group(1)
            params["table_name"] = match.group(2)
        else:
            # 只找到表名
            simple_table_pattern = r"([a-zA-Z_][a-zA-Z0-9_]+)"
            match = re.search(simple_table_pattern, user_input)
            if match:
                params["table_name"] = match.group(1)

        # 提取分区值
        # 匹配如: ds='2026-02-01' 或 2026-02-01
        partition_pattern = r"([a-zA-Z_][a-zA-Z0-9_]*)=['\"]?([^'\"\\s]+)['\"]?"
        matches = re.findall(partition_pattern, user_input)
        if matches:
            partition_parts = []
            for field, value in matches:
                partition_parts.append(f"{field}='{value}'")
            params["partition"] = ",".join(partition_parts)

        return params

    def enrich_params_with_metadata(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Enrich parameters with metadata from Hive.

        Args:
            params: Parameters extracted from user input

        Returns:
            Enriched parameters
        """
        table_name = params.get("table_name")
        if not table_name:
            return params

        # 如果没有指定数据库，尝试发现
        if not params.get("db"):
            discovered_dbs = discover_db_names_by_table(table_name, env=self.env)
            if len(discovered_dbs) == 1:
                params["db"] = discovered_dbs[0]
                params["discovered_db"] = discovered_dbs[0]
            elif len(discovered_dbs) > 1:
                params["possible_dbs"] = discovered_dbs
                # 暂时使用第一个
                params["db"] = discovered_dbs[0]

        # 查询分区字段
        db = params.get("db")
        if db:
            partition_info = discover_partition_fields(db, table_name, env=self.env)
            params["is_partitioned"] = partition_info["is_partitioned"]
            params["partition_fields"] = partition_info["partition_fields"]

        return params

    def execute_scenario(
        self,
        user_input: str,
        explicit_scenario: str | None = None,
    ) -> ScenarioResult:
        """
        Execute a scenario based on user input.

        Args:
            user_input: User's natural language input
            explicit_scenario: Explicit scenario name (optional)

        Returns:
            Scenario result with generated SQL
        """
        # 1. 识别场景
        scenario_name = explicit_scenario or recognize_scenario(user_input)
        if not scenario_name:
            return ScenarioResult(
                success=False,
                errors=[f"无法识别场景类型: {user_input}"],
                messages=["请明确说明场景类型，如'对比数据'或'校验数据'"],
            )

        # 2. 提取参数
        params = self.extract_params_from_input(user_input)

        # 3. 补充元数据
        params = self.enrich_params_with_metadata(params)

        # 4. 创建场景实例
        scenario = get_scenario(scenario_name, params)
        if not scenario:
            return ScenarioResult(
                success=False,
                errors=[f"未找到场景: {scenario_name}"],
            )

        # 5. 验证参数
        is_valid, error_msg = scenario.validate_params()
        if not is_valid:
            return ScenarioResult(
                success=False,
                errors=[error_msg],
                messages=[error_msg],
            )

        # 6. 获取步骤并生成 SQL
        steps = scenario.get_steps()
        generated_sql = []

        for step in steps:
            try:
                step_params = scenario.prepare_step_params(step)
                sql, _ = render_template(step.template, step_params)
                generated_sql.append(f"-- Step: {step.name}\n{sql}")
            except Exception as e:
                generated_sql.append(f"-- Step: {step.name} (Error: {e})")

        return ScenarioResult(
            success=True,
            steps=steps,
            generated_sql=generated_sql,
            messages=[f"成功生成 {len(steps)} 个步骤的 SQL"],
        )

    def format_result(self, result: ScenarioResult) -> str:
        """
        Format result as markdown for display.

        Args:
            result: Scenario result

        Returns:
            Formatted markdown string
        """
        lines = []

        if not result.success:
            lines.append("## ❌ 执行失败\n")
            for error in result.errors:
                lines.append(f"- {error}")
            return "\n".join(lines)

        lines.append("## ✅ SQL 场景执行结果\n")
        lines.append(f"**场景**: {result.steps[0].name if result.steps else 'N/A'}")
        lines.append(f"**步骤数**: {len(result.steps)}\n")
        lines.append("### 生成的 SQL:\n")

        for i, sql in enumerate(result.generated_sql, 1):
            lines.append(f"#### Step {i}")
            lines.append("```sql")
            lines.append(sql)
            lines.append("```\n")

        return "\n".join(lines)


def main():
    """Main entry point for CLI usage."""
    import argparse

    parser = argparse.ArgumentParser(description="SQL Scenario Execution")
    parser.add_argument("input", help="User's natural language input")
    parser.add_argument("--scenario", help="Explicit scenario name")
    parser.add_argument("--env", help="Hive environment")

    args = parser.parse_args()

    orchestrator = ScenarioOrchestrator(env=args.env)
    result = orchestrator.execute_scenario(args.input, args.scenario)
    print(orchestrator.format_result(result))


if __name__ == "__main__":
    main()
