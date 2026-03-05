#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Data compare scenario - Compare data between original and temp partitions."""

from __future__ import annotations

from .base import BaseScenario, ScenarioStep


class DataCompareScenario(BaseScenario):
    """Scenario for comparing data between partitions."""

    def get_name(self) -> str:
        return "data_compare"

    def get_description(self) -> str:
        return "对比原分区和 temp 分区的数据差异"

    def validate_params(self) -> tuple[bool, str]:
        """Validate required parameters."""
        if not self.params.get("table_name"):
            return False, "【参数缺失】请提供表名 (table_name)"

        # 检查是否需要分区
        partition_fields = self.params.get("partition_fields", [])
        user_partition = self.params.get("partition", "")

        if partition_fields and not user_partition:
            fields_str = ", ".join(partition_fields)
            return False, f"【参数缺失】该表是分区表，分区字段为: {partition_fields}，请指定分区值"

        # 检查二级分区
        if len(partition_fields) > 1 and user_partition:
            # 用户提供了一级分区，检查是否提供了二级
            first_field = partition_fields[0]
            # 简单检查：如果用户只提供了一个分区值但表有多个分区字段
            if user_partition and "=" in user_partition:
                # 解析用户提供的分区
                provided_fields = []
                for part in user_partition.split(","):
                    if "=" in part:
                        field_name = part.split("=")[0].strip()
                        provided_fields.append(field_name)

                missing = set(partition_fields) - set(provided_fields)
                if missing:
                    return False, f"【参数缺失】该表有二级分区 {list(missing)}，请补充指定"

        return True, ""

    def get_steps(self) -> list[ScenarioStep]:
        """Get list of steps for data comparison."""
        table_name = self.params.get("table_name", "")
        db = self.params.get("db", "")
        partition = self.params.get("partition", "")
        temp_suffix = self.params.get("temp_suffix", "-temp")

        # 构建完整的表名
        full_table_name = f"{db}.{table_name}" if db else table_name

        # 构建目标分区（添加 temp 后缀）
        # 策略：每个分区值后面添加 temp 后缀
        # 例如: ds='2026-02-01',hour='23' -> ds='2026-02-01-temp',hour='23-temp'
        # 注意：Hive 分区语法是 PARTITION(ds='...',hour='...')，逗号分隔
        target_partition_parts = []
        if partition:
            for part in partition.split(","):
                if "=" in part:
                    field, value = part.split("=", 1)
                    # 去掉引号，添加 temp 后缀，再加回去
                    value = value.strip("'\"")
                    target_partition_parts.append(f"{field}='{value}{temp_suffix}'")
                else:
                    target_partition_parts.append(part)
            # Hive 分区语法：ds='...',hour='...'
            target_partition = ",".join(target_partition_parts)
        else:
            target_partition = ""

        # 构建 where 子句用的分区过滤
        partition_where = partition.replace(",", " AND ") if partition else ""

        return [
            ScenarioStep(
                name="创建 temp 分区并复制数据",
                template="move_partition",
                params={
                    "table_name": full_table_name,
                    "source_partition": partition,
                    "target_partition": target_partition,
                },
                description="将原分区数据复制到 temp 分区",
            ),
            ScenarioStep(
                name="统计原分区数据量",
                template="data_num",
                params={
                    "table_name": full_table_name,
                    "partition": partition_where,
                },
                description="统计原分区数据量",
            ),
            ScenarioStep(
                name="统计 temp 分区数据量",
                template="data_num",
                params={
                    "table_name": full_table_name,
                    "partition": target_partition.replace(",", " AND "),
                },
                description="统计 temp 分区数据量",
            ),
            ScenarioStep(
                name="对比原分区和 temp 分区的差异",
                template="data_diff",
                params={
                    "source_table": full_table_name,
                    "target_table": full_table_name,
                    "source_partition": partition_where,
                    "target_partition": target_partition.replace(",", " AND "),
                    "join_keys": self.params.get("join_keys", ["id"]),
                    "compare_columns": self.params.get("compare_columns", ["*"]),
                },
                description="对比两个分区的数据差异",
            ),
        ]
