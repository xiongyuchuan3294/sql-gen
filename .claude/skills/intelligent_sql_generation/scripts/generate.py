#!/usr/bin/env python3
import argparse
import os
import re
import sys
from functools import lru_cache
from pathlib import Path

try:
    import yaml
except ImportError:
    print("Error: PyYAML is required. Install it with `pip install PyYAML`.")
    sys.exit(1)

from jinja2 import Environment, FileSystemLoader, TemplateNotFound

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from prompt_dispatcher import dispatch_prompt


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
    current_path = find_skill_root()
    while True:
        if (current_path / "tools").is_dir():
            return str(current_path)
        if current_path.parent == current_path:
            break
        current_path = current_path.parent
    return str(find_skill_root().parent)


def find_skill_root() -> Path:
    """Return the root directory of the intelligent_sql_generation skill."""
    return Path(__file__).resolve().parents[1]


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
    Parse a table name in db.table format

    Returns: (db_name, table_name)
    Example: "imd_aml_safe.t_local_hs2_aml_safe_p_ds" -> ("imd_aml_safe", "t_local_hs2_aml_safe_p_ds")
    """
    if not table_full_name:
        return "", ""

    table_full_name = str(table_full_name).strip()

    # Split on the last dot when present
    if "." in table_full_name:
        parts = table_full_name.rsplit(".", 1)
        if len(parts) == 2:
            db_name = parts[0].strip()
            table_name = parts[1].strip()
            if db_name and table_name:
                return db_name, table_name

    # If no db prefix exists, return an empty db name
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
        # Strip single quotes returned by beeline (for example 'aml_demo' -> aml_demo)
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

    script_dir = str(Path(__file__).resolve().parent)
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)

    try:
        from hive_mcp_runtime import build_hive_runtime
    except Exception as exc:
        print(f"Warning: failed to import Hive MCP runtime: {exc}", file=sys.stderr)
        return None, None

    try:
        return build_hive_runtime(repo_root)
    except Exception as exc:
        print(f"Warning: failed to initialize Hive MCP runtime: {exc}", file=sys.stderr)
        return None, None


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


def unique_preserve_order(values):
    ordered = []
    seen = set()
    for value in values or []:
        item = str(value or '').strip()
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def stable_shared_columns(primary_columns, secondary_columns):
    secondary_set = {column for column in secondary_columns or [] if column}
    ordered = []
    seen = set()
    for column in primary_columns or []:
        if not column or column not in secondary_set or column in seen:
            continue
        seen.add(column)
        ordered.append(column)
    return ordered


def discover_partition_fields(db_name, table_name, env=None):
    """
    Discover partition fields for a table

    Returns: {
        "is_partitioned": True/False,
        "partition_fields": ["ds"] or ["ds", "hour"]
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
        # Use DESCRIBE FORMATTED to discover partition metadata
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
    Parse DESCRIBE FORMATTED output and extract partition fields

    Returns: {
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
            skip_header_line = True  # The next line is the partition header row
            continue

        if in_partition_section:
            stripped = line.strip()

            # Skip the header row (# col_name data_type comment)
            if skip_header_line:
                skip_header_line = False
                continue

            # Stop when the next section header starts
            if stripped.startswith('# '):
                break

            # Skip empty lines and NULL rows
            if not stripped or 'NULL' in stripped:
                continue

            # Parse the partition field from the first column
            cols = stripped.split('\t')
            if cols and cols[0].strip():
                field_name = cols[0].strip()
                if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", field_name):
                    partitions.append(field_name)

    return {
        "is_partitioned": len(partitions) > 0,
        "partition_fields": partitions
    }



def validate_join_keys(join_keys):
    """
    Validate join-key parameters

    join_keys: ["id"] or ["id", "user_id"]

    Returns: {
        "valid": True/False,
        "message": "error message when invalid"
    }
    """
    if not join_keys or len(join_keys) == 0:
        return {
            "valid": False,
            "message": "Missing join_keys. Please provide primary key fields for data comparison."
        }

    return {"valid": True, "message": None}


def extract_partition_from_text(text: str) -> dict:
    """
    Extract partition parameters from user input text

    Supported formats:
    - ds=2026-02-01
    - ds='2026-02-01'
    - partition 2026-02-01
    - on partition 2026-02-01
    - a Chinese partition phrase such as a date followed by the Chinese word for partition

    Returns: {
        "raw_partition": "ds=2026-02-01,hour=23",
        "partition_fields": ["ds"],  # inferred partition field
    }
    """
    import re
    params = {}
    partition_parts = []

    # Pattern 1: field=value or field='value'
    partition_pattern = r"([a-zA-Z_][a-zA-Z0-9_]*)=['\"]?([^'\"\\s]+)['\"]?"
    matches = re.findall(partition_pattern, text)
    for field, value in matches:
        partition_parts.append(f"{field}={value}")

    # Pattern 2: natural-language partition dates
    if not partition_parts:
        natural_pattern = r"(?:for|on|partition|\u7684|\u5728|\u5206\u533a)?\s*(\d{4}[-/]\d{2}[-/]\d{2})\s*(?:partition|date|\u5206\u533a|\u53f7)?"
        natural_matches = re.findall(natural_pattern, text)
        if natural_matches:
            # Infer ds as the partition field
            for value in natural_matches:
                partition_parts.append(f"ds={value.replace('/', '-')}")

    # Pattern 3: hour=23 or 23 hour
    hour_pattern = r"(?:hour|\u65f6)\s*=\s*(\d{1,2})|(\d{1,2})\s*(?:hour|h|\u70b9|\u65f6)"
    hour_matches = re.findall(hour_pattern, text)
    for match in hour_matches:
        hour_value = match[0] or match[1]
        partition_parts.append(f"hour={hour_value}")

    if partition_parts:
        params["raw_partition"] = ",".join(partition_parts)

    return params


def validate_partition_from_metadata(db_name: str, table_name: str, raw_partition: str, env=None) -> dict:
    """
    Validate partition parameters against metadata

    1. Discover partition fields for a table
    2. Validate whether the provided partition values are complete
    3. Return a hint when partition values are missing

    Returns: {
        "valid": True/False,
        "message": "error message",
        "partition_fields": ["ds", "hour"],
        "formatted_partition": "ds='2026-02-01',hour='23'"
    }
    """
    import re

    # 1. Discover partition fields
    metadata = discover_partition_fields(db_name, table_name, env)
    is_partitioned = metadata.get("is_partitioned", False)
    partition_fields = metadata.get("partition_fields", [])

    # 2. If the table is not partitioned, pass through
    if not is_partitioned:
        return {
            "valid": True,
            "message": None,
            "partition_fields": [],
            "is_partitioned": False,
            "formatted_partition": ""
        }

    # 3. Parse user-provided partition values
    user_partitions = {}
    if raw_partition:
        for part in raw_partition.split(","):
            if "=" in part:
                k, v = part.split("=", 1)
                v = v.strip().strip("'\"")
                user_partitions[k] = v

    # 4. Check whether partition values were provided
    if not user_partitions:
        return {
            "valid": False,
            "message": f"Missing required partition values. This table is partitioned by {partition_fields}. Please provide values such as {partition_fields[0]}='2026-02-01'",
            "partition_fields": partition_fields,
            "is_partitioned": True,
            "formatted_partition": ""
        }

    # 5. Check whether any partition fields are missing
    missing = set(partition_fields) - set(user_partitions.keys())
    if missing:
        return {
            "valid": False,
            "message": f"Missing required partition fields: {list(missing)}. Please provide all partition levels.",
            "partition_fields": partition_fields,
            "is_partitioned": True,
            "formatted_partition": ""
        }

    # 6. Format the partition string
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
    Get the non-partition columns of a table

    Returns: ["id", "name", "address"]
    """
    if not db_name or not table_name:
        return []

    jdbc_hive_utils, default_env = load_hive_runtime()
    if jdbc_hive_utils is None:
        return []

    _ = env  # keep signature compatibility; runtime now uses active env only.
    effective_env = (default_env or "local").strip()

    try:
        # Use DESCRIBE to get all table columns
        result = jdbc_hive_utils.execute_query(
            schema=db_name,
            sql=f"DESCRIBE {table_name}",
            env=effective_env,
        )

        # Fetch partition fields
        partition_info = discover_partition_fields(db_name, table_name, env)
        partition_fields = set(partition_info.get("partition_fields", []))

        # Parse columns and exclude partition fields
        columns = []
        for line in result.split('\n'):
            # Do not strip the whole line; otherwise \tNULL\tNULL becomes NULL\tNULL
            if not line or line.isspace():
                continue
            # Skip empty rows and separator rows
            if '#' in line or 'col_name' in line.lower():
                continue
            cols = line.split('\t')
            if cols and cols[0].strip():
                field_name = cols[0].strip()
                # Skip partition columns
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
    Prepare the full parameter set for the data_diff template

    1. Get the non-partition columns
    2. Build the full template parameters

    Returns: {
        "source_table": "db.table",
        "target_table": "db.table",
        "source_partition": "ds='2026-02-01'",
        "target_partition": "ds='2026-02-01-temp'",
        "join_keys": ["cust_id"],
        "non_partition_columns": ["id", "name", "address"],
    }
    """
    # Build the fully qualified table name
    table_full = f"{db_name}.{table_name}" if db_name else table_name

    # Fetch non-partition columns
    non_partition_cols = []
    if db_name and table_name:
        try:
            non_partition_cols = get_non_partition_columns(db_name, table_name, env)
        except Exception as e:
            print(f"Warning: failed to get non-partition columns: {e}", file=sys.stderr)

    # Ensure join_keys are present
    if not join_keys:
        join_keys = ["id"]

    # Ensure there are non-partition columns to compare
    if not non_partition_cols:
        non_partition_cols = join_keys[:1]  # Fall back to the first join key

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

    # For templates such as move_partition and data_num, automatically combine db + table into table_name
    if template_name in ("move_partition", "data_num", "null_checks", "null_rate", "repeat_check", "field_dist", "check_field_len"):
        db = prepared_params.get("db", "")
        table = prepared_params.get("table", "")
        table_name = prepared_params.get("table_name", "")

        # Auto-compose table_name when db and table are provided separately
        if not table_name and db and table:
            prepared_params["table_name"] = f"{db}.{table}"

        return prepared_params

    # For data_diff, automatically fetch non-partition columns
    if template_name == "data_diff":
        prepared_params = dict(params or {})
        source_table = prepared_params.get("source_table", "")
        target_table = prepared_params.get("target_table", "")
        source_partition = prepared_params.get("source_partition", "")
        target_partition = prepared_params.get("target_partition", "")
        join_keys = prepared_params.get("join_keys", [])

        # Return immediately when compare_columns is already provided
        if prepared_params.get("compare_columns"):
            return prepared_params

        # Parse source_table and target_table into db and table names
        source_db, source_table_name = parse_db_table(source_table)
        target_db, target_table_name = parse_db_table(target_table)

        # Fetch non-partition columns from source_table
        source_columns = []
        source_error = None
        if source_db and source_table_name:
            try:
                source_columns = get_non_partition_columns(source_db, source_table_name, env)
            except Exception as e:
                source_error = str(e)
                print(f"Warning: failed to get source columns: {e}", file=sys.stderr)

        # Fetch non-partition columns from target_table
        target_columns = []
        target_error = None
        if target_db and target_table_name:
            try:
                target_columns = get_non_partition_columns(target_db, target_table_name, env)
            except Exception as e:
                target_error = str(e)
                print(f"Warning: failed to get target columns: {e}", file=sys.stderr)

        # Check whether any columns were discovered successfully
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
            # Fall back to join_keys and keep the warning visible
            compare_columns = join_keys if join_keys else []
            if not compare_columns:
                raise ValueError("Failed to discover columns and no join_keys were provided. Please specify compare_columns or join_keys manually.")
        else:
            # Keep data_diff columns deterministic by preserving the source-table column order.
            if source_columns and target_columns:
                compare_columns = stable_shared_columns(source_columns, target_columns)
            elif source_columns:
                compare_columns = unique_preserve_order(source_columns)
            else:
                compare_columns = unique_preserve_order(target_columns)

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
    skill_root = find_skill_root()
    sql_dir = skill_root / 'assets' / 'templates' / 'sql'
    shell_dir = skill_root / 'assets' / 'templates' / 'shell'
    
    # Search paths for Jinja2
    env = Environment(loader=FileSystemLoader([str(sql_dir), str(shell_dir)]))
    
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

def save_generated_output(template_name, content, ext, resolved_config=None):
    output_dir = find_skill_root() / 'output'
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / f"{template_name}_generated.{ext}"
    with output_file.open('w', encoding='utf-8', newline='\n') as f:
        f.write(content)
    print(f"Saved to: {output_file}")

    if resolved_config is not None:
        resolved_yaml_file = output_dir / f"{template_name}_resolved.yaml"
        with resolved_yaml_file.open('w', encoding='utf-8', newline='\n') as f:
            yaml.safe_dump(resolved_config, f, sort_keys=False, allow_unicode=True)
        print(f"Saved resolved YAML to: {resolved_yaml_file}")


def main():
    parser = argparse.ArgumentParser(description="Generate Hive SQL or shell commands from YAML config or a natural-language prompt.")
    parser.add_argument('--yaml', help="Path to YAML configuration file")
    parser.add_argument('--prompt', help="Natural-language request that should be dispatched to a template")
    parser.add_argument('--template', help="Template name (defaults to 'type' in YAML or dispatcher result)")
    parser.add_argument('--env', help="Hive environment name for metadata-aware preparation")

    args = parser.parse_args()

    if not args.yaml and not args.prompt:
        print("Error: provide either --yaml or --prompt.")
        sys.exit(1)
    if args.yaml and args.prompt:
        print("Error: use either --yaml or --prompt, not both.")
        sys.exit(1)

    resolved_config = None
    if args.prompt:
        dispatch_result = dispatch_prompt(args.prompt, explicit_template=args.template)
        template_name = dispatch_result.get('template_name')
        params_source = dispatch_result.get('params', {})
        resolved_config = {
            'type': template_name,
            'prompt': args.prompt,
            'dispatcher': dispatch_result.get('dispatcher', {}),
            'params': params_source,
        }
        print('-' * 20 + ' Prompt Dispatch ' + '-' * 20)
        print(f"Template: {template_name}")
        print(yaml.safe_dump(resolved_config, sort_keys=False, allow_unicode=True).rstrip())
        print('-' * 55)
    else:
        config = load_yaml(args.yaml)
        template_name = args.template or config.get('type')
        if not template_name:
            print("Error: YAML file must specify 'type' or --template argument required.")
            sys.exit(1)
        params_source = config.get('params', {})
        resolved_config = {
            'type': template_name,
            'params': params_source,
        }

    params = prepare_params(
        template_name,
        params_source,
        env=args.env,
    )
    content, ext = render_template(template_name, params)

    print('-' * 20 + f' Generated {ext.upper()} ' + '-' * 20)
    print(content)
    print('-' * 55)

    if ext in ('sql', 'sh'):
        save_generated_output(
            template_name,
            content,
            ext,
            resolved_config={
                **(resolved_config or {}),
                'params': params,
            },
        )

if __name__ == '__main__':
    main()
