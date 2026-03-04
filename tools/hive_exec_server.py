#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Hive execution MCP server."""

import logging
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from mcp.server.fastmcp import FastMCP

from tools.hive_client import (
    HiveConnectionManager,
    HiveRuntimeConfig,
    JdbcHiveUtils,
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mcp = FastMCP("hive_exec_server")


def _normalize_env(env: str | None) -> str:
    return HiveConnectionManager.normalize_env(env)


def _query_error(exc: Exception) -> str:
    logger.error("Hive request failed: %s", exc)
    return f"查询失败: {exc}"


@mcp.tool(
    name="hive_execute_query",
    description=(
        "Execute Hive SQL query and return tab-separated results. "
        "env supports remote configs and local debug mode."
    ),
)
def hive_execute_query(
    schema: str,
    sql: str,
    env: str | None = None,
) -> str:
    """Execute a Hive query."""
    normalized_env = _normalize_env(env)
    try:
        logger.info(
            "Execute Hive query. schema=%s env=%s sql=%s",
            schema,
            normalized_env,
            sql[:100],
        )
        return JdbcHiveUtils.execute_query(schema, sql, normalized_env)
    except Exception as exc:
        return _query_error(exc)


@mcp.tool(
    name="hive_describe_table",
    description="Describe a Hive table. env supports remote configs and local debug mode.",
)
def hive_describe_table(
    schema: str,
    table_name: str,
    env: str | None = None,
) -> str:
    """Describe a table structure."""
    normalized_env = _normalize_env(env)
    sql = f"DESCRIBE {table_name}"
    try:
        logger.info(
            "Describe Hive table. schema=%s table=%s env=%s",
            schema,
            table_name,
            normalized_env,
        )
        return JdbcHiveUtils.execute_query(schema, sql, normalized_env)
    except Exception as exc:
        return _query_error(exc)


@mcp.tool(
    name="hive_count_records",
    description="Count rows in a Hive table. env supports remote configs and local debug mode.",
)
def hive_count_records(
    schema: str,
    table_name: str,
    partition_filter: str | None = None,
    env: str | None = None,
) -> str:
    """Count records in a table."""
    normalized_env = _normalize_env(env)
    sql = f"SELECT COUNT(*) AS record_count FROM {table_name}"
    if partition_filter:
        sql += f" WHERE {partition_filter}"
    try:
        logger.info(
            "Count Hive records. schema=%s table=%s env=%s partition=%s",
            schema,
            table_name,
            normalized_env,
            partition_filter,
        )
        return JdbcHiveUtils.execute_query(schema, sql, normalized_env)
    except Exception as exc:
        return _query_error(exc)


@mcp.tool(
    name="hive_preview_data",
    description="Preview Hive table data. env supports remote configs and local debug mode.",
)
def hive_preview_data(
    schema: str,
    table_name: str,
    partition_filter: str | None = None,
    limit: int = 10,
    env: str | None = None,
) -> str:
    """Preview table data."""
    normalized_env = _normalize_env(env)
    sql = f"SELECT * FROM {table_name}"
    if partition_filter:
        sql += f" WHERE {partition_filter}"
    sql += f" LIMIT {limit}"
    try:
        logger.info(
            "Preview Hive data. schema=%s table=%s env=%s limit=%s",
            schema,
            table_name,
            normalized_env,
            limit,
        )
        return JdbcHiveUtils.execute_query(schema, sql, normalized_env)
    except Exception as exc:
        return _query_error(exc)


@mcp.tool(
    name="hive_execute_dml",
    description="Execute Hive DDL/DML. env supports remote configs and local debug mode.",
)
def hive_execute_dml(
    schema: str,
    sql: str,
    env: str | None = None,
) -> str:
    """Execute DDL or DML."""
    normalized_env = _normalize_env(env)
    try:
        logger.info(
            "Execute Hive DML. schema=%s env=%s sql=%s",
            schema,
            normalized_env,
            sql[:100],
        )
        JdbcHiveUtils.execute(schema, sql, normalized_env)
        return "执行成功"
    except Exception as exc:
        logger.error("Hive DML failed: %s", exc)
        return f"执行失败: {exc}"


@mcp.tool(
    name="hive_show_tables",
    description="Show tables in a Hive schema. env supports remote configs and local debug mode.",
)
def hive_show_tables(
    schema: str,
    env: str | None = None,
) -> str:
    """Show all tables in the given schema."""
    normalized_env = _normalize_env(env)
    try:
        logger.info("Show Hive tables. schema=%s env=%s", schema, normalized_env)
        return JdbcHiveUtils.execute_query(schema, "SHOW TABLES", normalized_env)
    except Exception as exc:
        return _query_error(exc)


@mcp.tool(
    name="hive_close_connections",
    description="Close cached remote Hive connections.",
)
def hive_close_connections() -> str:
    """Close cached connections."""
    try:
        logger.info("Close Hive connections")
        JdbcHiveUtils.close_all()
        return "所有连接已关闭"
    except Exception as exc:
        logger.error("Close Hive connections failed: %s", exc)
        return f"关闭连接失败: {exc}"


if __name__ == "__main__":
    logger.info(
        "Start Hive execution MCP server. default_env=%s supported_envs=%s",
        HiveRuntimeConfig.active_env(),
        ",".join(HiveConnectionManager.supported_envs()),
    )
    mcp.run()
