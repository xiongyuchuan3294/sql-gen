#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Scenario configuration loader and executor.
支持两种参数格式：
- 完整格式：db.table + partition='2026-01-01',hour='23'
- 简单格式：table + ds=2026-01-01（自动格式化）
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

import yaml

# Add scripts path for imports
repo_root = Path(__file__).parent.parent.parent
scripts_path = repo_root / "agent_skills" / "sql_generation" / "scripts"
if str(scripts_path) not in sys.path:
    sys.path.insert(0, str(scripts_path))


class ScenarioConfig:
    """Scenario configuration loaded from YAML."""

    def __init__(self, config: dict):
        self.name = config.get("scenario", {}).get("name", "")
        self.description = config.get("scenario", {}).get("description", "")
        self.keywords = config.get("scenario", {}).get("keywords", [])
        self.params_def = config.get("params", {})
        self.steps = config.get("steps", [])

    @classmethod
    def load_from_file(cls, path: Path) -> "ScenarioConfig":
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        return cls(config)

    @classmethod
    def load_all(cls, config_dir: Path) -> dict[str, "ScenarioConfig"]:
        scenarios = {}
        for yaml_file in config_dir.glob("*.yaml"):
            config = cls.load_from_file(yaml_file)
            scenarios[config.name] = config
        return scenarios


class ScenarioExecutor:
    """Execute scenario based on configuration."""

    def __init__(self, env: str | None = None):
        self.env = env
        self.scenarios: dict[str, ScenarioConfig] = {}
        self._load_scenarios()

    def _load_scenarios(self):
        config_dir = Path(__file__).parent / "config" / "scenarios"
        if config_dir.exists():
            self.scenarios = ScenarioConfig.load_all(config_dir)

    def recognize(self, user_input: str) -> str | None:
        """从用户输入识别场景类型"""
        user_input_lower = user_input.lower()
        for name, config in self.scenarios.items():
            if any(kw.lower() in user_input_lower for kw in config.keywords):
                return name
        return None

    def _normalize_partition(self, raw_partition: str) -> str:
        """格式化分区值：ds=2026-02-01 -> ds='2026-02-01'"""
        if not raw_partition:
            return ""

        parts = []
        for part in raw_partition.split(","):
            if "=" in part:
                k, v = part.split("=", 1)
                v = v.strip().strip("'\"")
                parts.append(f"{k}='{v}'")
            else:
                parts.append(part)
        return ",".join(parts)

    def _build_partition_where(self, partition: str) -> str:
        """构建 WHERE 子句：ds='2026-02-01',hour='23' -> ds='2026-02-01' AND hour='23'"""
        if not partition:
            return ""
        return " AND ".join(partition.split(","))

    def _build_target_partition(self, partition: str, temp_suffix: str) -> str:
        """构建目标分区：在每个分区值后添加 temp_suffix"""
        if not partition:
            return ""

        parts = []
        for part in partition.split(","):
            if "=" in part:
                k, v = part.split("=", 1)
                v = v.strip().strip("'\"")
                parts.append(f"{k}='{v}{temp_suffix}'")
            else:
                parts.append(part)
        return ",".join(parts)

    def _enrich_params(self, params: dict) -> dict:
        """自动丰富参数：
        1. 格式化分区值
        2. 构建 target_partition
        3. 构建 partition_where
        4. 构建 target_partition_where
        5. 构建 join_keys
        """
        result = dict(params)

        # 1. 格式化分区值
        raw_partition = params.get("raw_partition", "")
        if raw_partition:
            result["partition"] = self._normalize_partition(raw_partition)
        else:
            result["partition"] = ""

        # 2. temp_suffix 默认值
        result.setdefault("temp_suffix", "-temp")

        # 3. 构建 target_partition
        if result["partition"]:
            result["target_partition"] = self._build_target_partition(
                result["partition"], result["temp_suffix"]
            )
        else:
            result["target_partition"] = ""

        # 4. 构建 partition_where
        result["partition_where"] = self._build_partition_where(result["partition"])

        # 5. 构建 target_partition_where
        result["target_partition_where"] = self._build_partition_where(
            result["target_partition"]
        )

        # 6. 自动构建完整表名
        db = result.get("db", "")
        table_name = result.get("table_name", "")
        if db and table_name:
            result["table_name_full"] = f"{db}.{table_name}"
        else:
            result["table_name_full"] = table_name

        # 7. join_keys 转换为列表
        join_keys = params.get("join_keys", [])
        if isinstance(join_keys, str):
            result["join_keys"] = [k.strip() for k in join_keys.split(",") if k.strip()]
        elif isinstance(join_keys, list):
            result["join_keys"] = join_keys
        else:
            result["join_keys"] = []

        return result

    def extract_params(self, user_input: str) -> dict[str, Any]:
        """
        从用户输入提取参数
        注意：复杂的参数理解由 AI (SKILL.md) 自动处理，这里只做基础提取
        """
        from generate import extract_partition_from_text

        params = {}

        # 保存原始输入（供 AI 分析）
        params["_user_input"] = user_input

        # 提取 db.table 格式
        table_pattern = r"([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]+)"
        match = re.search(table_pattern, user_input)
        if match:
            params["db"] = match.group(1)
            params["table_name"] = match.group(2)
        else:
            # 只提取表名
            simple_pattern = r"([a-zA-Z_][a-zA-Z0-9_]+)"
            match = re.search(simple_pattern, user_input)
            if match:
                params["table_name"] = match.group(1)

        # 调用 intelligent_sql_generation 提取分区
        partition_result = extract_partition_from_text(user_input)
        if "raw_partition" in partition_result:
            params["raw_partition"] = partition_result["raw_partition"]

        # 基础主键提取（后备）- 主要由 AI (SKILL.md) 自动理解
        # 支持格式："主键 id", "key id", "主键 id 和 name", "主键 id 和case_date", "key id and user_id"
        # 1. 先提取 "主键" 后面的所有内容
        key_section_pattern = r"(?:key|主键)\s+(.+?)(?:\s+(?:分区|数据)|$)"
        key_section_match = re.search(key_section_pattern, user_input, re.IGNORECASE)
        if key_section_match:
            keys_str = key_section_match.group(1).strip()
            # 2. 统一处理 "和" / "and" 分隔（支持有/无空格）
            keys_str = re.sub(r"\s*和\s*", ",", keys_str)  # "和" 前后有空格
            keys_str = re.sub(r"(\w)和", r"\1,", keys_str)  # "cust_id和" -> "cust_id,"
            keys_str = re.sub(r"和(\w)", r",\1", keys_str)  # "和case_date" -> ",case_date"
            keys_str = re.sub(r"\s+and\s+", ",", keys_str)  # " and " -> ","
            keys_str = re.sub(r"(\w)and", r"\1,", keys_str)  # "id and" -> "id,"
            keys_str = re.sub(r"and(\w)", r",\1", keys_str)  # "and user_id" -> ",user_id"
            params["join_keys"] = [k.strip() for k in keys_str.split(",") if k.strip() and k.strip().lower() not in ['和', 'and']]

        return params

    def substitute_params(self, text: str, params: dict) -> str:
        """替换 {{param}} 占位符"""
        if not text:
            return text
        result = text
        for key, value in params.items():
            placeholder = f"{{{{{key}}}}}"
            if placeholder in result:
                result = result.replace(placeholder, str(value))
        return result

    def validate_params(self, params: dict, param_defs: dict) -> tuple[bool, str]:
        """验证必填参数 - 调用 intelligent_sql_generation 的校验逻辑"""
        from generate import (
            validate_join_keys,
            extract_partition_from_text,
            validate_partition_from_metadata,
        )

        # 1. 提取并校验分区参数（调用 MCP 发现元数据）
        user_input = params.get("_user_input", "")
        if user_input and params.get("table_name"):
            # 提取原始分区
            raw_partition = params.get("raw_partition", "")
            db_name = params.get("db", "")
            table_name = params.get("table_name", "")

            if db_name and table_name:
                # 调用元数据校验
                result = validate_partition_from_metadata(
                    db_name, table_name, raw_partition, self.env
                )
                if not result["valid"]:
                    return False, result["message"]

        # 2. 校验必填参数
        for name, defn in param_defs.items():
            if defn.get("required", False):
                value = params.get(name)
                if not value:
                    return False, f"【参数缺失】请提供 {name}"
                # 检查空列表
                if isinstance(value, list) and len(value) == 0:
                    # 对于 join_keys，使用更友好的提示
                    if name == "join_keys":
                        result = validate_join_keys([])
                        return False, result["message"]
                    return False, f"【参数缺失】请提供 {name}"

        # 3. 校验 join_keys
        if "join_keys" in params and isinstance(params.get("join_keys"), list):
            result = validate_join_keys(params["join_keys"])
            if not result["valid"]:
                return False, result["message"]

        return True, ""

    def _prepare_step_params(self, step: dict, params: dict) -> dict:
        """准备步骤参数 - 自动推断常用参数"""
        step_params = {}

        # 从步骤定义获取模板名
        template = step.get("template", "")

        # 根据模板类型自动生成参数
        if template == "move_partition":
            step_params = {
                "table_name": params.get("table_name_full", ""),
                "source_partition": params.get("partition", ""),
                "target_partition": params.get("target_partition", ""),
            }
        elif template == "data_num":
            # 判断是原分区还是 temp 分区
            step_name = step.get("name", "")
            if "temp" in step_name:
                step_params = {
                    "table_name": params.get("table_name_full", ""),
                    "partition": params.get("target_partition_where", ""),
                }
            else:
                step_params = {
                    "table_name": params.get("table_name_full", ""),
                    "partition": params.get("partition_where", ""),
                }
        elif template == "data_diff":
            step_params = {
                "source_table": params.get("table_name_full", ""),
                "target_table": params.get("table_name_full", ""),
                "source_partition": params.get("partition_where", ""),
                "target_partition": params.get("target_partition_where", ""),
                "join_keys": params.get("join_keys", ["id"]),
                "compare_columns": ["*"],
            }
        else:
            # 其他模板，使用通用逻辑
            for key, value in step.get("params", {}).items():
                if isinstance(value, str):
                    value = self.substitute_params(value, params)
                step_params[key] = value

        return step_params

    def execute(self, user_input: str, explicit_scenario: str | None = None) -> dict:
        """执行场景"""
        # 1. 识别场景
        scenario_name = explicit_scenario or self.recognize(user_input)
        if not scenario_name:
            return {
                "success": False,
                "error": f"无法识别场景类型: {user_input}",
                "message": "请明确说明场景类型，如'对比数据'或'校验数据'",
            }

        # 2. 获取场景配置
        config = self.scenarios.get(scenario_name)
        if not config:
            return {"success": False, "error": f"未找到场景: {scenario_name}"}

        # 3. 提取参数
        params = self.extract_params(user_input)

        # 4. 设置默认值
        for name, defn in config.params_def.items():
            if name not in params and "default" in defn:
                params[name] = defn["default"]

        # 5. 自动丰富参数
        params = self._enrich_params(params)

        # 6. 验证参数
        is_valid, error_msg = self.validate_params(params, config.params_def)
        if not is_valid:
            return {"success": False, "error": error_msg, "message": error_msg}

        # 7. 生成 SQL
        from generate import render_template

        generated_sql = []
        for step in config.steps:
            step_name = step.get("name", "")
            template = step.get("template", "")

            # 自动准备参数
            step_params = self._prepare_step_params(step, params)

            try:
                sql, _ = render_template(template, step_params)
                generated_sql.append(f"-- Step: {step_name}\n{sql}")
            except Exception as e:
                generated_sql.append(f"-- Step: {step_name} (Error: {e})")

        return {
            "success": True,
            "scenario": scenario_name,
            "description": config.description,
            "params": params,
            "steps": [s.get("name") for s in config.steps],
            "generated_sql": generated_sql,
            "message": f"成功生成 {len(config.steps)} 个步骤的 SQL",
        }

    def format_result(self, result: dict) -> str:
        """格式化结果为 markdown"""
        lines = []

        if not result.get("success"):
            lines.append("## ❌ 执行失败\n")
            lines.append(f"- {result.get('error')}")
            return "\n".join(lines)

        lines.append("## ✅ SQL 场景执行结果\n")
        lines.append(f"**场景**: {result.get('description', result.get('scenario'))}")
        lines.append(f"**步骤数**: {len(result.get('steps', []))}\n")
        lines.append("### 生成的 SQL:\n")

        for i, sql in enumerate(result.get("generated_sql", []), 1):
            lines.append(f"#### Step {i}")
            lines.append("```sql")
            lines.append(sql)
            lines.append("```\n")

        return "\n".join(lines)

    def save_result(self, result: dict, output_path: Path | None = None) -> Path | None:
        """保存结果到文件"""
        if not result.get("success"):
            return None

        # 默认保存到 output 目录
        if output_path is None:
            output_dir = Path(__file__).parent / "output"
            output_dir.mkdir(exist_ok=True)

            # 生成文件名：场景名_时间戳.sql
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            scenario = result.get("scenario", "scenario")
            output_path = output_dir / f"{scenario}_{timestamp}.sql"

        # 合并所有 SQL
        all_sql = "\n\n".join(result.get("generated_sql", []))

        # 保存文件
        output_path.write_text(all_sql, encoding="utf-8")
        return output_path


def main():
    """CLI 入口"""
    import argparse

    parser = argparse.ArgumentParser(description="SQL 场景执行器")
    parser.add_argument("input", help="用户自然语言输入")
    parser.add_argument("--scenario", help="指定场景类型")
    parser.add_argument("--env", help="Hive 环境")
    parser.add_argument("--output", "-o", help="输出文件路径（默认保存到 output 目录）")

    args = parser.parse_args()

    executor = ScenarioExecutor(env=args.env)
    result = executor.execute(args.input, args.scenario)

    # 打印到控制台
    print(executor.format_result(result))

    # 保存到文件
    output_path = Path(args.output) if args.output else None
    saved_path = executor.save_result(result, output_path)
    if saved_path:
        print(f"\n💾 SQL 已保存到: {saved_path}")


if __name__ == "__main__":
    main()
