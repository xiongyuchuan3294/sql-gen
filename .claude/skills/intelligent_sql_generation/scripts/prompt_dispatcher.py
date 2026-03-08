#!/usr/bin/env python3
from __future__ import annotations

import re
from typing import Any

IDENTIFIER_PATTERN = r"[A-Za-z_][A-Za-z0-9_]*"
FULL_TABLE_PATTERN = re.compile(rf"\b({IDENTIFIER_PATTERN})\.({IDENTIFIER_PATTERN})\b")
PARTITION_PATTERN = re.compile(rf"\b({IDENTIFIER_PATTERN})\s*=\s*['\"]?([A-Za-z0-9_:/.-]+)['\"]?")
DATE_PATTERN = re.compile(r"\b(\d{4}[-/]\d{2}[-/]\d{2}(?:-temp)?)\b")

RESERVED_TOKENS = {
    "a",
    "add",
    "add_columns",
    "alter",
    "analysis",
    "and",
    "anti",
    "backup",
    "batch",
    "by",
    "calculate",
    "change",
    "check",
    "checks",
    "clean",
    "column",
    "columns",
    "combine",
    "combines",
    "compare",
    "comparison",
    "contain",
    "count",
    "counts",
    "create",
    "data",
    "dataset",
    "datasets",
    "diff",
    "difference",
    "distribution",
    "drop",
    "duplicate",
    "duplicates",
    "empty",
    "exist",
    "exists",
    "field",
    "filter",
    "filtering",
    "find",
    "for",
    "from",
    "generate",
    "get",
    "group",
    "hdfs",
    "in",
    "insert",
    "into",
    "join",
    "key",
    "keys",
    "longest",
    "merge",
    "missing",
    "mock",
    "move",
    "not",
    "null",
    "of",
    "on",
    "order",
    "ordered",
    "out",
    "overwrite",
    "partition",
    "per",
    "pk",
    "primary",
    "query",
    "rate",
    "ranking",
    "records",
    "row",
    "rows",
    "schema",
    "show",
    "size",
    "sql",
    "storage",
    "table",
    "target",
    "temp",
    "that",
    "the",
    "to",
    "top",
    "union",
    "use",
    "using",
    "value",
    "values",
    "volume",
    "where",
    "while",
    "with",
    "校验",
    "主键",
    "为空",
    "分区",
    "分布",
    "删除",
    "创建",
    "前",
    "取前",
    "合并",
    "对比",
    "工作流",
    "差异",
    "排序",
    "插入",
    "数据",
    "查询",
    "检查",
    "比较",
    "清洗",
    "生成",
    "空值",
    "空值率",
    "统计",
    "行数",
    "过滤",
    "迁移",
    "重复",
    "长度",
}

TEMPLATE_RULES: list[tuple[str, list[str]]] = [
    ("hdfs_du", [r"\bhdfs\b", r"storage\s+size", r"hdfs\s+size", r"hdfs\s*大小", r"存储大小"]),
    ("batch_data_num", [r"batch\s+count", r"批量统计", r"count\s+rows\s+for.+,.+(?:and|和)", r"统计.+,.+(?:and|和)"]),
    ("anti_join", [r"anti\s+join", r"missing", r"not\s+exist\s+in", r"not\s+in\s+target", r"不存在于目标", r"缺失"]),
    ("data_diff", [r"compare", r"\bdiff\b", r"difference", r"对比", r"比较"]),
    ("null_rate", [r"null\s+rate", r"空值率"]),
    ("null_checks", [r"contain\s+null", r"contain\s+nulls", r"check\s+whether.+null", r"是否.*空", r"空值检查"]),
    ("repeat_check", [r"duplicate", r"deduplicate", r"重复"]),
    ("field_dist", [r"distribution", r"分布"]),
    ("group_top_n", [r"top\s*\d+", r"ranking", r"top\s+n", r"前\s*\d+"] ),
    ("drop_partition", [r"drop\s+partition", r"删除分区"]),
    ("move_partition", [r"move\s+partition", r"backup\s+partition", r"迁移.*分区", r"移动.*分区", r"分区.*迁移", r"分区.*移动", r"迁到", r"迁至"]),
    ("create_temp_partition", [r"create.+temp\s+partition", r"empty\s+temp\s+partition", r"创建.*临时分区"]),
    ("data_clean", [r"overwrite", r"clean", r"filtering\s+out", r"清洗", r"过滤"]),
    ("insert_values", [r"insert\s+mock", r"mock\s+rows", r"插入.*mock", r"插入测试数据"]),
    ("union_merge", [r"\bunion\b", r"merge", r"合并查询"]),
    ("alter_table", [r"alter\s+table", r"add\s+column", r"change\s+column", r"修改表结构", r"新增列"]),
    ("check_field_len", [r"field\s+length", r"longest\s+values", r"字段长度", r"最长值"]),
    ("data_num", [r"count\s+rows", r"row\s+count", r"volume", r"统计行数"]),
]


def dedupe_preserve_order(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        output.append(normalized)
    return output


def split_identifier_list(raw_text: str) -> list[str]:
    text = str(raw_text or "")
    text = text.replace("和", ",").replace("、", ",").replace("，", ",")
    text = text.replace("；", ",").replace(";", ",")
    text = re.sub(r"\band\b", ",", text, flags=re.IGNORECASE)

    candidates: list[str] = []
    for token in re.findall(rf"\b({IDENTIFIER_PATTERN})\b", text):
        lowered = token.lower()
        if lowered in RESERVED_TOKENS:
            continue
        if lowered in {"ds", "dt", "pt", "hour"}:
            continue
        candidates.append(token)
    return dedupe_preserve_order(candidates)


def extract_full_tables(text: str) -> list[str]:
    return dedupe_preserve_order([f"{db}.{table}" for db, table in FULL_TABLE_PATTERN.findall(text or "")])


def extract_standalone_tables(text: str) -> list[str]:
    tokens = []
    for token in re.findall(rf"\b({IDENTIFIER_PATTERN})\b", text or ""):
        lowered = token.lower()
        if lowered in RESERVED_TOKENS:
            continue
        if lowered in {"ds", "dt", "pt", "hour"}:
            continue
        if re.fullmatch(r"\d+", token):
            continue
        if "_" not in token and not lowered.startswith("t") and not lowered.startswith("codex"):
            continue
        tokens.append(token)
    return dedupe_preserve_order(tokens)


def extract_tables(text: str) -> list[str]:
    full_tables = extract_full_tables(text)
    if full_tables:
        return full_tables
    return extract_standalone_tables(text)


def normalize_partition_clause(raw_partition: str) -> str:
    text = str(raw_partition or "").strip().rstrip("。.,;；")
    if not text:
        return ""
    if "=" not in text and DATE_PATTERN.fullmatch(text):
        return f"ds='{text.replace('/', '-')}'"

    output: list[str] = []
    for segment in text.split(","):
        piece = segment.strip()
        if not piece:
            continue
        if "=" not in piece:
            if DATE_PATTERN.fullmatch(piece):
                output.append(f"ds='{piece.replace('/', '-')}'")
            continue
        field, value = piece.split("=", 1)
        output.append(f"{field.strip()}='{value.strip().strip('"\'')}'")
    return ",".join(output)


def extract_partition_clauses(text: str) -> list[str]:
    clauses = []
    for field, value in PARTITION_PATTERN.findall(text or ""):
        lowered = field.lower()
        if lowered in {"key", "keys", "pk"}:
            continue
        cleaned = value.strip().strip('"\'').rstrip("。.,;；")
        clauses.append(f"{field}='{cleaned}'")
    if clauses:
        return clauses

    dates = dedupe_preserve_order([value.replace("/", "-").rstrip("。.,;；") for value in DATE_PATTERN.findall(text or "")])
    return [f"ds='{value}'" for value in dates]


def extract_join_keys(text: str) -> list[str]:
    match = re.search(
        r"(?:primary\s*key|join\s*key|keys?|pk|主键)\s*(?:是|为|:|：)?\s*(.+)$",
        text or "",
        flags=re.IGNORECASE,
    )
    if not match:
        return []

    raw = match.group(1)
    stop_patterns = [
        r"\bfor\b",
        r"\bfrom\b",
        r"\bin\b",
        r"\bpartition\b",
        r"\bwhere\b",
        r"\bwith\b",
        r"分区",
        r"表",
    ]
    for pattern in stop_patterns:
        stop_match = re.search(pattern, raw, flags=re.IGNORECASE)
        if stop_match:
            raw = raw[:stop_match.start()]
            break
    return split_identifier_list(raw)


def extract_first_partition(text: str, default: str = "") -> str:
    partitions = extract_partition_clauses(text)
    return partitions[0] if partitions else default


def extract_columns_between(text: str, patterns: list[str]) -> list[str]:
    for pattern in patterns:
        match = re.search(pattern, text or "", flags=re.IGNORECASE)
        if match:
            return split_identifier_list(match.group(1))
    return []


def extract_limit(text: str, default: int = 5) -> int:
    match = re.search(r"(?:top|limit|前)\s*(\d+)", text or "", flags=re.IGNORECASE)
    if match:
        return max(1, int(match.group(1)))
    return default


def extract_filter_condition(text: str) -> str:
    patterns = [
        r"where\s+(.+?)(?:\.|$)",
        r"过滤(?:掉)?(?:满足)?(?:条件)?\s+(.+?)(?:\.|$)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text or "", flags=re.IGNORECASE)
        if match:
            return match.group(1).strip().rstrip("。")
    return "1=1"


def extract_order_by(text: str) -> str:
    match = re.search(r"ordered\s+by\s+([A-Za-z_][A-Za-z0-9_]*(?:\s+(?:asc|desc))?)", text or "", flags=re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r"按\s*([A-Za-z_][A-Za-z0-9_]*(?:\s*(?:升序|降序))?)\s*排序", text or "")
    if match:
        raw = match.group(1).replace("降序", "desc").replace("升序", "asc")
        return re.sub(r"\s+", " ", raw).strip()
    return "updated_at desc"


def infer_mock_row() -> list[list[str]]:
    return [["'mock_id'", "'mock_cust'", "'mock_user'", "999", "'ok'", "'2026-03-08 09:00:00'", "'mock@example.com'", "'13800000000'", "'mock_address'"]]


def recognize_template(prompt: str) -> str:
    text = str(prompt or "")
    for template_name, patterns in TEMPLATE_RULES:
        if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns):
            return template_name
    raise ValueError(f"Unable to map prompt to a known template: {prompt}")


def build_params_from_prompt(template_name: str, prompt: str) -> dict[str, Any]:
    text = str(prompt or "")
    tables = extract_tables(text)
    partitions = extract_partition_clauses(text)
    join_keys = extract_join_keys(text)

    if template_name == "data_diff":
        if len(tables) < 2:
            raise ValueError("data_diff requires source and target tables in the prompt")
        return {
            "source_table": tables[0],
            "target_table": tables[1],
            "source_partition": partitions[0] if partitions else "",
            "target_partition": partitions[0] if partitions else "",
            "join_keys": join_keys or ["id"],
        }

    if template_name == "anti_join":
        if len(tables) < 2:
            raise ValueError("anti_join requires source and target tables in the prompt")
        return {
            "source_table": tables[0],
            "target_table": tables[1],
            "source_partition": partitions[0] if partitions else "",
            "target_partition": partitions[0] if partitions else "",
            "join_keys": join_keys or ["id"],
        }

    if template_name == "batch_data_num":
        if len(tables) < 2:
            raise ValueError("batch_data_num requires multiple tables in the prompt")
        partition = partitions[0] if partitions else ""
        return {"tables": [{"name": table_name, "partition": partition} for table_name in tables]}

    if template_name == "check_field_len":
        column_match = re.search(r"(?:column|字段)\s+([A-Za-z_][A-Za-z0-9_]*)", text, flags=re.IGNORECASE)
        column = column_match.group(1) if column_match else "id"
        return {
            "table_name": tables[0],
            "partition": partitions[0] if partitions else "",
            "column": column,
            "limit": extract_limit(text, default=5),
        }

    if template_name == "create_temp_partition":
        return {
            "table_name": tables[0],
            "partition": partitions[-1] if partitions else "ds='temp'",
        }

    if template_name == "data_clean":
        return {
            "table_name": tables[0],
            "partition": partitions[0] if partitions else "",
            "filter_condition": extract_filter_condition(text),
            "columns": [],
        }

    if template_name == "data_num":
        return {
            "table_name": tables[0],
            "partition": partitions[0] if partitions else "",
        }

    if template_name == "drop_partition":
        return {
            "table_name": tables[0],
            "partition": partitions[0] if partitions else "",
            "if_exists": True,
        }

    if template_name == "field_dist":
        columns = extract_columns_between(
            text,
            [
                r"distribution\s+of\s+(.+?)\s+(?:in|from)\s",
                r"(.+?)\s*的分布",
            ],
        )
        return {
            "table_name": tables[0],
            "partition": partitions[0] if partitions else "",
            "columns": columns or ["status"],
            "limit": extract_limit(text, default=100),
        }

    if template_name == "group_top_n":
        partition_by = extract_columns_between(
            text,
            [
                r"per\s+(.+?)\s+ordered\s+by",
                r"每个\s*(.+?)\s*按",
            ],
        )
        return {
            "table_name": tables[0],
            "partition": partitions[0] if partitions else "",
            "partition_by": partition_by or ["cust_id"],
            "order_by": extract_order_by(text),
            "limit_n": extract_limit(text, default=3),
        }

    if template_name == "insert_values":
        return {
            "table_name": tables[0],
            "partition": partitions[0] if partitions else "",
            "data_rows": infer_mock_row(),
        }

    if template_name == "move_partition":
        source_partition = partitions[0] if partitions else ""
        target_partition = partitions[1] if len(partitions) > 1 else source_partition.replace("'", "")
        if target_partition and "=" in target_partition and target_partition == source_partition:
            target_partition = source_partition[:-1] + "-temp'" if source_partition.endswith("'") else source_partition + "-temp"
        return {
            "table_name": tables[0],
            "source_partition": source_partition,
            "target_partition": target_partition,
        }

    if template_name == "null_checks":
        columns = extract_columns_between(
            text,
            [
                r"whether\s+(.+?)\s+contain\s+null",
                r"check\s+whether\s+(.+?)\s+contain",
                r"检查\s*(.+?)\s*(?:是否)?(?:包含)?空",
            ],
        )
        return {
            "table_name": tables[0],
            "partition": partitions[0] if partitions else "",
            "columns": columns or ["id"],
        }

    if template_name == "null_rate":
        columns = extract_columns_between(
            text,
            [
                r"null\s+rate\s+of\s+(.+?)\s+(?:in|from)\s",
                r"(.+?)\s*的空值率",
                r"空值率.*?([A-Za-z_][A-Za-z0-9_]*(?:\s*(?:,|和|and)\s*[A-Za-z_][A-Za-z0-9_]*)+)",
            ],
        )
        return {
            "table_name": tables[0],
            "partition": partitions[0] if partitions else "",
            "columns": columns or ["id"],
        }

    if template_name == "repeat_check":
        columns = extract_columns_between(
            text,
            [
                r"duplicate\s+(.+?)\s+values\s+(?:in|for)",
                r"重复\s*(.+?)\s*(?:值|字段)",
            ],
        )
        return {
            "table_name": tables[0],
            "partition": partitions[0] if partitions else "",
            "group_by_columns": columns or join_keys or ["id"],
            "having_threshold": 1,
        }

    if template_name == "union_merge":
        if len(tables) < 2:
            raise ValueError("union_merge requires at least two tables in the prompt")
        partition = partitions[0] if partitions else ""
        return {
            "union_type": "UNION ALL",
            "queries": [
                {"table_name": table_name, "columns": ["id", "status"], "partition": partition, "condition": "1=1"}
                for table_name in tables[:2]
            ],
        }

    if template_name == "alter_table":
        add_ops = []
        for match in re.finditer(r"add\s+column\s+([A-Za-z_][A-Za-z0-9_]*)\s+([A-Za-z_][A-Za-z0-9_]*)", text, flags=re.IGNORECASE):
            add_ops.append({"name": match.group(1), "type": match.group(2), "comment": "added by prompt dispatcher"})
        operations: list[dict[str, Any]] = []
        if add_ops:
            operations.append({"action": "add_columns", "columns": add_ops})
        for match in re.finditer(r"change(?:\s+column)?\s+([A-Za-z_][A-Za-z0-9_]*)\s+to\s+([A-Za-z_][A-Za-z0-9_]*)\s+([A-Za-z_][A-Za-z0-9_]*)", text, flags=re.IGNORECASE):
            operations.append({
                "action": "change_column",
                "old_name": match.group(1),
                "new_name": match.group(2),
                "type": match.group(3),
            })
        if not operations:
            operations = [{"action": "add_columns", "columns": [{"name": "new_col", "type": "string", "comment": "added by prompt dispatcher"}]}]
        return {
            "table_name": tables[0],
            "operations": operations,
        }

    if template_name == "hdfs_du":
        path_match = re.search(r"(/[^\s;]+)", text)
        if path_match:
            return {"targets": [{"path": path_match.group(1).rstrip('/;')}]}
        table_name = tables[0] if tables else ""
        db, table = (table_name.split(".", 1) if "." in table_name else ("", table_name))
        target: dict[str, Any] = {"partition": partitions[0] if partitions else ""}
        if db and table:
            target["db"] = db
            target["table"] = table
        else:
            target["table"] = table_name
        return {"targets": [target]}

    raise ValueError(f"Unsupported template extraction path: {template_name}")


def dispatch_prompt(prompt: str, explicit_template: str | None = None) -> dict[str, Any]:
    template_name = explicit_template or recognize_template(prompt)
    params = build_params_from_prompt(template_name, prompt)
    return {
        "template_name": template_name,
        "params": params,
        "prompt": prompt,
        "dispatcher": {
            "mode": "prompt_only",
            "explicit_template": explicit_template,
        },
    }
