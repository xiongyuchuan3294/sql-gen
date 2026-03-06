#!/usr/bin/env python3
import argparse
import os
import re
import sys
from functools import lru_cache

try:
    import yaml
except ImportError:
    print("Error: PyYAML is required. Install it with `pip install PyYAML`.")
    sys.exit(1)

from jinja2 import Environment, FileSystemLoader, TemplateNotFound


DEFAULT_HDFS_WAREHOUSE_USER = "hduser1009"
HDFS_WAREHOUSE_USER_BY_DB = {
    "imd_aml_safe": "hduser1009",
    "imd_aml_dm_safe": "hduser1009",
    "imd_amlai_ads_safe": "hduser1009",
    "imd_aml300_ads_safe": "hduser1009",
    "imd_dm_safe": "hduser1006",
    "imd_rdfs_dm_safe": "hduser1088",
}


def find_repo_root():
    """Find the root of the sql-gen repository (where tools directory exists)."""
    # 从脚本位置向上查找，找到包含 'tools' 目录的父目录
    current_path = os.path.abspath(os.path.dirname(__file__))
    while current_path:
        parent = os.path.dirname(current_path)
        tools_path = os.path.join(current_path, "tools")
        if os.path.isdir(tools_path):
            return current_path
        if parent == current_path:  # Root reached
            break
        current_path = parent
    # Fallback: 返回 find_project_root() 的父目录
    return os.path.abspath(os.path.join(find_project_root(), ".."))


def find_project_root():
    """Find the root of the sql-gen workspace."""
    # Start from current script location and go up until finding 'agent_skills'
    current_path = os.path.abspath(os.path.dirname(__file__))
    while "agent_skills" in current_path:
        parent = os.path.dirname(current_path)
        if parent == current_path: # Root reached
            break
        current_path = parent
    
    # We expect to be in agent_skills/sql_generation/scripts
    # So root should be .../agent_skills/sql_generation/
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    return root

def load_yaml(path):
    with open(path, 'r', encoding='utf-8-sig') as f:
        return yaml.safe_load(f)


def sanitize_partition(partition):
    if not partition:
        return ""
    return str(partition).replace("'", "").strip().strip("/;")


def normalize_table_name(table_name):
    if not table_name:
        return ""
    candidate = str(table_name).strip()
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", candidate):
        return candidate
    return ""


def parse_db_table(table_full_name):
    """
    解析 db.table 格式的表名

    返回: (db_name, table_name)
    例如: "imd_aml_safe.t_local_hs2_aml_safe_p_ds" -> ("imd_aml_safe", "t_local_hs2_aml_safe_p_ds")
    """
    if not table_full_name:
        return "", ""

    table_full_name = str(table_full_name).strip()

    # 尝试用 . 分割
    if "." in table_full_name:
        parts = table_full_name.rsplit(".", 1)
        if len(parts) == 2:
            db_name = parts[0].strip()
            table_name = parts[1].strip()
            if db_name and table_name:
                return db_name, table_name

    # 如果没有 db 前缀，返回空 db
    return "", table_full_name


def parse_first_column_rows(result_text):
    if not result_text:
        return []

    raw_lines = [line.strip() for line in str(result_text).splitlines() if line.strip()]
    if not raw_lines:
        return []

    # Skip CLI table borders if they appear in output.
    filtered = []
    for line in raw_lines:
        if set(line) <= set("+-| "):
            continue
        filtered.append(line)
    if not filtered:
        return []

    # Hive outputs header + rows; keep rows only.
    data_lines = filtered[1:] if len(filtered) >= 2 else []
    values = []
    for line in data_lines:
        first_col = line.split("\t")[0].strip()
        # 去掉 beeline 返回的单引号（如 'aml_demo' -> aml_demo）
        if first_col and first_col.startswith("'") and first_col.endswith("'"):
            first_col = first_col[1:-1]
        if first_col:
            values.append(first_col)
    return values


@lru_cache(maxsize=1)
def load_hive_runtime():
    repo_root = find_repo_root()
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    # Try external hive-mcp directories first.
    parent_dir = os.path.dirname(repo_root)
    candidate_paths = [
        os.getenv("HIVE_MCP_PATH", "").strip(),
        os.getenv("SQL_GEN_HIVE_MCP_PATH", "").strip(),
        os.path.join(parent_dir, "hive-mcp"),
        os.path.join(parent_dir, "hive-mcp-uat"),
        os.path.expanduser("~/workspace/hive-mcp"),
        os.path.expanduser("~/workspace/hive-mcp-uat"),
        r"D:\workspace\hive-mcp",
        r"D:\workspace\hive-mcp-uat",
    ]
    seen_paths = set()
    valid_paths = []
    for base_path in candidate_paths:
        if not base_path:
            continue
        normalized = os.path.normpath(base_path)
        if normalized in seen_paths or not os.path.isdir(normalized):
            continue
        seen_paths.add(normalized)
        valid_paths.append(normalized)

    # Keep candidate order as priority: earlier item = higher priority.
    for normalized in reversed(valid_paths):
        tools_path = os.path.join(normalized, "tools")
        if os.path.isdir(tools_path) and tools_path not in sys.path:
            sys.path.insert(0, tools_path)
        if normalized not in sys.path:
            sys.path.insert(0, normalized)

    try:
        from hive_client import HiveRuntimeConfig, JdbcHiveUtils
    except Exception:
        try:
            # Repository layout uses tools/hive_client.py.
            from tools.hive_client import HiveRuntimeConfig, JdbcHiveUtils
        except Exception:
            return None, None

    try:
        default_env = HiveRuntimeConfig.active_env()
    except Exception:
        default_env = "local"

    return JdbcHiveUtils, default_env


def discover_db_names_by_table(table_name, env=None):
    table = normalize_table_name(table_name)
    if not table:
        return []

    jdbc_hive_utils, default_env = load_hive_runtime()
    if jdbc_hive_utils is None:
        print(
            "Warning: tools.hive_client is unavailable. Skip table->db discovery.",
            file=sys.stderr,
        )
        return []

    _ = env  # keep signature compatibility; runtime now uses active env only.
    effective_env = (default_env or "local").strip()
    discovered = []

    try:
        databases_output = jdbc_hive_utils.execute_query(
            schema="default",
            sql="SHOW DATABASES",
            env=effective_env,
        )
        databases = parse_first_column_rows(databases_output)
        for db_name in databases:
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", db_name):
                continue
            tables_output = jdbc_hive_utils.execute_query(
                schema=db_name,
                sql=f"SHOW TABLES LIKE '{table}'",
                env=effective_env,
            )
            table_rows = parse_first_column_rows(tables_output)
            if any(value == table for value in table_rows):
                discovered.append(db_name)
    except Exception as exc:
        print(
            f"Warning: failed to discover db for table '{table}' in env '{effective_env}': {exc}",
            file=sys.stderr,
        )
        return []
    finally:
        try:
            jdbc_hive_utils.close_all()
        except Exception:
            pass

    # Keep order and remove duplicates.
    unique = []
    seen = set()
    for db_name in discovered:
        if db_name in seen:
            continue
        seen.add(db_name)
        unique.append(db_name)
    return unique


def discover_partition_fields(db_name, table_name, env=None):
    """
    发现表的分区字段

    返回: {
        "is_partitioned": True/False,
        "partition_fields": ["ds"] 或 ["ds", "hour"]
    }
    """
    if not db_name or not table_name:
        return {"is_partitioned": False, "partition_fields": []}

    jdbc_hive_utils, default_env = load_hive_runtime()
    if jdbc_hive_utils is None:
        return {"is_partitioned": False, "partition_fields": []}

    _ = env  # keep signature compatibility; runtime now uses active env only.
    effective_env = (default_env or "local").strip()

    try:
        # 使用 DESCRIBE FORMATTED 获取分区信息
        result = jdbc_hive_utils.execute_query(
            schema=db_name,
            sql=f"DESCRIBE FORMATTED {table_name}",
            env=effective_env,
        )
        return parse_partition_fields_from_desc(result)
    except Exception as exc:
        print(
            f"Warning: failed to discover partitions for '{db_name}.{table_name}': {exc}",
            file=sys.stderr,
        )
        return {"is_partitioned": False, "partition_fields": []}
    finally:
        try:
            jdbc_hive_utils.close_all()
        except Exception:
            pass


def parse_partition_fields_from_desc(desc_output):
    """
    解析 DESCRIBE FORMATTED 输出，提取分区字段

    返回: {
        "is_partitioned": True/False,
        "partition_fields": ["ds", "hour"]
    }
    """
    if not desc_output:
        return {"is_partitioned": False, "partition_fields": []}

    partitions = []
    in_partition_section = False
    skip_header_line = False
    lines = desc_output.split('\n')

    for line in lines:
        if '# Partition Information' in line:
            in_partition_section = True
            skip_header_line = True  # 下一行是 # col_name data_type comment
            continue

        if in_partition_section:
            stripped = line.strip()

            # 跳过 header 行 (# col_name data_type comment)
            if skip_header_line:
                skip_header_line = False
                continue

            # 检查是否到达下一个 # 开头的新section
            if stripped.startswith('# '):
                break

            # 跳过空行和 NULL 行
            if not stripped or 'NULL' in stripped:
                continue

            # 解析分区字段（取第一列）
            cols = stripped.split('\t')
            if cols and cols[0].strip():
                field_name = cols[0].strip()
                if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", field_name):
                    partitions.append(field_name)

    return {
        "is_partitioned": len(partitions) > 0,
        "partition_fields": partitions
    }


def validate_partition_params(table_metadata, user_partitions):
    """
    校验分区参数

    table_metadata: {
        "is_partitioned": True/False,
        "partition_fields": ["ds", "hour"]
    }
    user_partitions: {
        "ds": "2026-02-01",
        "hour": "23"
    }

    返回: {
        "valid": True/False,
        "message": "错误信息（如果无效）",
        "missing_fields": ["hour"]（缺失的字段）
    }
    """
    if not table_metadata["is_partitioned"]:
        # 非分区表，直接通过
        return {"valid": True, "message": None, "missing_fields": []}

    required_fields = table_metadata["partition_fields"]
    provided_fields = list(user_partitions.keys()) if user_partitions else []

    # 1. 检查是否指定了任何分区
    if not provided_fields:
        return {
            "valid": False,
            "message": f"【参数缺失】该表是分区表，分区字段为: {required_fields}，请指定分区值，如 {required_fields[0]}='2026-02-01'",
            "missing_fields": required_fields
        }

    # 2. 检查是否缺少必需的分区字段
    missing = set(required_fields) - set(provided_fields)
    if missing:
        return {
            "valid": False,
            "message": f"【参数缺失】该表有二级分区 {list(missing)}，请补充指定",
            "missing_fields": list(missing)
        }

    return {"valid": True, "message": None, "missing_fields": []}


def validate_join_keys(join_keys):
    """
    校验主键参数

    join_keys: ["id"] 或 ["id", "user_id"]

    返回: {
        "valid": True/False,
        "message": "错误信息（如果无效）"
    }
    """
    if not join_keys or len(join_keys) == 0:
        return {
            "valid": False,
            "message": "【参数缺失】请提供主键字段（join_keys），用于数据对比"
        }

    return {"valid": True, "message": None}


def extract_partition_from_text(text: str) -> dict:
    """
    从用户输入文本中提取分区参数

    支持格式:
    - ds=2026-02-01
    - ds='2026-02-01'
    - 2026-02-01 分区
    - 的 2026-02-01 分区

    返回: {
        "raw_partition": "ds=2026-02-01,hour=23",
        "partition_fields": ["ds"],  # 推断的分区字段
    }
    """
    import re
    params = {}
    partition_parts = []

    # 格式1: field=value 或 field='value'
    partition_pattern = r"([a-zA-Z_][a-zA-Z0-9_]*)=['\"]?([^'\"\\s]+)['\"]?"
    matches = re.findall(partition_pattern, text)
    for field, value in matches:
        partition_parts.append(f"{field}={value}")

    # 格式2: 自然语言 "2026-02-01 分区"
    if not partition_parts:
        natural_pattern = r"(?:的|在|分区)?\s*(\d{4}[-/]\d{2}[-/]\d{2})\s*(?:分区|号)?"
        natural_matches = re.findall(natural_pattern, text)
        if natural_matches:
            # 推断使用 ds 作为分区字段
            for value in natural_matches:
                partition_parts.append(f"ds={value.replace('/', '-')}")

    # 格式3: hour=23 或 23点
    hour_pattern = r"(?:hour|时)\s*=\s*(\d{1,2})|(\d{1,2})\s*(?:点|时|hour)"
    hour_matches = re.findall(hour_pattern, text)
    for match in hour_matches:
        hour_value = match[0] or match[1]
        partition_parts.append(f"hour={hour_value}")

    if partition_parts:
        params["raw_partition"] = ",".join(partition_parts)

    return params


def validate_partition_from_metadata(db_name: str, table_name: str, raw_partition: str, env=None) -> dict:
    """
    通过元数据校验分区参数

    1. 发现表的分区字段
    2. 校验用户提供的分区是否完整
    3. 如果用户未提供分区，返回提示

    返回: {
        "valid": True/False,
        "message": "错误信息",
        "partition_fields": ["ds", "hour"],
        "formatted_partition": "ds='2026-02-01',hour='23'"
    }
    """
    import re

    # 1. 发现分区字段
    metadata = discover_partition_fields(db_name, table_name, env)
    is_partitioned = metadata.get("is_partitioned", False)
    partition_fields = metadata.get("partition_fields", [])

    # 2. 如果非分区表，直接通过
    if not is_partitioned:
        return {
            "valid": True,
            "message": None,
            "partition_fields": [],
            "is_partitioned": False,
            "formatted_partition": ""
        }

    # 3. 解析用户提供的分区
    user_partitions = {}
    if raw_partition:
        for part in raw_partition.split(","):
            if "=" in part:
                k, v = part.split("=", 1)
                v = v.strip().strip("'\"")
                user_partitions[k] = v

    # 4. 校验是否提供了分区
    if not user_partitions:
        return {
            "valid": False,
            "message": f"【参数缺失】该表是分区表，分区字段为: {partition_fields}，请指定分区值，如 {partition_fields[0]}='2026-02-01'",
            "partition_fields": partition_fields,
            "is_partitioned": True,
            "formatted_partition": ""
        }

    # 5. 校验是否缺少分区字段
    missing = set(partition_fields) - set(user_partitions.keys())
    if missing:
        return {
            "valid": False,
            "message": f"【参数缺失】该表有二级分区 {list(missing)}，请补充指定",
            "partition_fields": partition_fields,
            "is_partitioned": True,
            "formatted_partition": ""
        }

    # 6. 格式化分区
    formatted_parts = []
    for field in partition_fields:
        value = user_partitions.get(field, "")
        if value:
            formatted_parts.append(f"{field}='{value}'")

    return {
        "valid": True,
        "message": None,
        "partition_fields": partition_fields,
        "is_partitioned": True,
        "formatted_partition": ",".join(formatted_parts)
    }


def get_non_partition_columns(db_name: str, table_name: str, env=None) -> list[str]:
    """
    获取表的非分区字段列表

    返回: ["id", "name", "address"]
    """
    if not db_name or not table_name:
        return []

    jdbc_hive_utils, default_env = load_hive_runtime()
    if jdbc_hive_utils is None:
        return []

    _ = env  # keep signature compatibility; runtime now uses active env only.
    effective_env = (default_env or "local").strip()

    try:
        # DESCRIBE table 获取所有字段
        result = jdbc_hive_utils.execute_query(
            schema=db_name,
            sql=f"DESCRIBE {table_name}",
            env=effective_env,
        )

        # 获取分区字段
        partition_info = discover_partition_fields(db_name, table_name, env)
        partition_fields = set(partition_info.get("partition_fields", []))

        # 解析字段（排除分区字段）
        columns = []
        for line in result.split('\n'):
            # 不要 strip 整行，否则 \tNULL\tNULL 会变成 NULL\tNULL
            if not line or line.isspace():
                continue
            # 跳过空行和分隔行
            if '#' in line or 'col_name' in line.lower():
                continue
            cols = line.split('\t')
            if cols and cols[0].strip():
                field_name = cols[0].strip()
                # 跳过分区字段
                if field_name not in partition_fields and re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", field_name):
                    columns.append(field_name)

        return columns
    except Exception as exc:
        print(f"Warning: failed to get columns for '{db_name}.{table_name}': {exc}", file=sys.stderr)
        return []
    finally:
        try:
            jdbc_hive_utils.close_all()
        except Exception:
            pass


def prepare_data_diff_params(db_name, table_name, source_partition, target_partition, join_keys, env=None):
    """
    准备 data_diff 模板的完整参数

    1. 获取表的非分区字段
    2. 构建完整的模板参数

    返回: {
        "source_table": "db.table",
        "target_table": "db.table",
        "source_partition": "ds='2026-02-01'",
        "target_partition": "ds='2026-02-01-temp'",
        "join_keys": ["cust_id"],
        "non_partition_columns": ["id", "name", "address"],
    }
    """
    # 构建完整表名
    table_full = f"{db_name}.{table_name}" if db_name else table_name

    # 获取非分区字段
    non_partition_cols = []
    if db_name and table_name:
        try:
            non_partition_cols = get_non_partition_columns(db_name, table_name, env)
        except Exception as e:
            print(f"Warning: failed to get non-partition columns: {e}", file=sys.stderr)

    # 确保有 join_keys
    if not join_keys:
        join_keys = ["id"]

    # 确保有非分区字段用于对比
    if not non_partition_cols:
        non_partition_cols = join_keys[:1]  # 使用主键作为后备

    return {
        "source_table": table_full,
        "target_table": table_full,
        "source_partition": source_partition,
        "target_partition": target_partition,
        "join_keys": join_keys,
        "non_partition_columns": non_partition_cols,
    }


def expand_hdfs_target(target, env=None):
    """
    Expand a single hdfs_du target to one or more concrete targets.
    Supports:
    - path-only
    - db + table
    - dbs + table
    - table-only (auto discover db from the current Hive env)
    """
    if target.get("path"):
        return [dict(target)]

    table_name = target.get("table")
    db_name = target.get("db")
    db_names = target.get("dbs")

    if db_name and table_name:
        return [dict(target)]

    if isinstance(db_names, list) and table_name:
        expanded = []
        for item in db_names:
            current_db = str(item).strip()
            if not current_db:
                continue
            current_target = dict(target)
            current_target["db"] = current_db
            expanded.append(current_target)
        return expanded

    if table_name and not db_name:
        discovered = discover_db_names_by_table(table_name, env=env)
        if discovered:
            expanded = []
            for discovered_db in discovered:
                current_target = dict(target)
                current_target["db"] = discovered_db
                expanded.append(current_target)
            return expanded

        # Keep table-only target and render path without db segment.
        return [dict(target)]

    return [dict(target)]


def build_hdfs_target_path(target):
    path = target.get("path")
    if path:
        return str(path).rstrip("/;")

    db_name = target.get("db")
    table_name = target.get("table")
    if not table_name:
        return ""

    warehouse_user = target.get("warehouse_user") or HDFS_WAREHOUSE_USER_BY_DB.get(
        db_name,
        DEFAULT_HDFS_WAREHOUSE_USER,
    )
    if db_name:
        hdfs_path = f"/user/hive/warehouse/{warehouse_user}/{db_name}.db/{table_name}"
    else:
        hdfs_path = f"/user/hive/warehouse/{warehouse_user}/{table_name}"

    partition = sanitize_partition(target.get("partition"))
    if partition:
        hdfs_path = f"{hdfs_path}/{partition}"

    return hdfs_path


def prepare_params(template_name, params, env=None):
    prepared_params = dict(params or {})

    # 处理 move_partition, data_num 等模板：自动合并 db.table 为 table_name
    if template_name in ("move_partition", "data_num", "null_checks", "null_rate", "repeat_check", "field_dist", "check_field_len"):
        db = prepared_params.get("db", "")
        table = prepared_params.get("table", "")
        table_name = prepared_params.get("table_name", "")

        # 如果没有 table_name 但有 db 和 table，自动组合
        if not table_name and db and table:
            prepared_params["table_name"] = f"{db}.{table}"

        return prepared_params

    # 处理 data_diff 模板：自动获取表的非分区字段
    if template_name == "data_diff":
        prepared_params = dict(params or {})
        source_table = prepared_params.get("source_table", "")
        target_table = prepared_params.get("target_table", "")
        source_partition = prepared_params.get("source_partition", "")
        target_partition = prepared_params.get("target_partition", "")
        join_keys = prepared_params.get("join_keys", [])

        # 如果用户已指定 compare_columns，直接返回
        if prepared_params.get("compare_columns"):
            return prepared_params

        # 解析 source_table 和 target_table 获取 db 和 table
        source_db, source_table_name = parse_db_table(source_table)
        target_db, target_table_name = parse_db_table(target_table)

        # 获取 source_table 的非分区字段
        source_columns = []
        source_error = None
        if source_db and source_table_name:
            try:
                source_columns = get_non_partition_columns(source_db, source_table_name, env)
            except Exception as e:
                source_error = str(e)
                print(f"Warning: failed to get source columns: {e}", file=sys.stderr)

        # 获取 target_table 的非分区字段
        target_columns = []
        target_error = None
        if target_db and target_table_name:
            try:
                target_columns = get_non_partition_columns(target_db, target_table_name, env)
            except Exception as e:
                target_error = str(e)
                print(f"Warning: failed to get target columns: {e}", file=sys.stderr)

        # 检查是否成功获取字段
        if not source_columns and not target_columns:
            error_msg = (
                f"[ERROR] Failed to automatically get table columns!\n"
                f"  - source_table: {source_table} (error: {source_error or 'no columns found'})\n"
                f"  - target_table: {target_table} (error: {target_error or 'no columns found'})\n"
                f"Please ensure one of the following:\n"
                f"  1. Install required dependencies (pyhive, thrift, sasl) and configure correct Hive environment\n"
                f"  2. Manually specify compare_columns in YAML\n"
                f"\nExample YAML config:\n"
                f"  params:\n"
                f"    compare_columns:\n"
                f"      - cust_id\n"
                f"      - name\n"
                f"      - address\n"
            )
            print(error_msg, file=sys.stderr)
            # 使用 join_keys 作为后备，但给出警告
            compare_columns = join_keys if join_keys else []
            if not compare_columns:
                raise ValueError("无法获取字段信息，且未指定 join_keys，请手动指定 compare_columns 或 join_keys")
        else:
            # 合并字段，去重，保留在两个表中都存在的字段
            if source_columns and target_columns:
                compare_columns = list(set(source_columns) & set(target_columns))
            elif source_columns:
                compare_columns = source_columns
            else:
                compare_columns = target_columns

        prepared_params["compare_columns"] = compare_columns
        return prepared_params

    if template_name != "hdfs_du":
        return params

    prepared_params = dict(params or {})
    prepared_targets = []
    for target in prepared_params.get("targets", []):
        for expanded_target in expand_hdfs_target(target, env=env):
            prepared_target = dict(expanded_target)
            prepared_target["hdfs_path"] = build_hdfs_target_path(prepared_target)
            prepared_targets.append(prepared_target)
    prepared_params["targets"] = prepared_targets
    return prepared_params


def render_template(template_name, params):
    root_dir = find_project_root()
    sql_dir = os.path.join(root_dir, 'templates', 'sql')
    shell_dir = os.path.join(root_dir, 'templates', 'shell')
    
    # Search paths for Jinja2
    env = Environment(loader=FileSystemLoader([sql_dir, shell_dir]))
    
    # Try extensions in order
    candidates = [
        (f"{template_name}.sql", "sql"),
        (f"{template_name}.sh", "sh")
    ]
    
    for filename, ext in candidates:
        try:
            template = env.get_template(filename)
            return template.render(params=params), ext
        except TemplateNotFound:
            continue
            
    print(f"Error: Template {template_name} not found in sql or shell directories.")
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Generate Hive SQL from YAML config.")
    parser.add_argument('--yaml', required=True, help="Path to YAML configuration file")
    parser.add_argument('--template', help="Template name (defaults to 'type' in YAML)")
    
    args = parser.parse_args()
    
    # Load Config
    config = load_yaml(args.yaml)
    
    # Determine Template
    template_name = args.template
    if not template_name:
        template_name = config.get('type')
        if not template_name:
            print("Error: YAML file must specify 'type' or --template argument required.")
            sys.exit(1)
            
    # Render
    params = prepare_params(
        template_name,
        config.get('params', {}),
    )
    content, ext = render_template(template_name, params)
    
    print("-" * 20 + f" Generated {ext.upper()} " + "-" * 20)
    print(content)
    print("-" * 55)

    # Write SQL/SH to output directory (not YAML)
    if ext in ("sql", "sh"):
        output_dir = os.path.join(find_project_root(), 'output')
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"{template_name}_generated.{ext}")
        with open(output_file, 'w', encoding='utf-8', newline='\n') as f:
            f.write(content)
        print(f"Saved to: {output_file}")

if __name__ == "__main__":
    main()
