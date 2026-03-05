#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path


TEMPLATE_KEY_RE = re.compile(r"^(?P<key>[^:\s]+):(?:\s*(?P<value>.*))?$")
PREFERRED_SQL_EXAMPLES = ("data_diff", "null_rate")
PREFERRED_HDFS_EXAMPLES = ("hdfs_du",)


@dataclass(frozen=True)
class TemplateEntry:
    template_type: str
    description: str
    key_params: tuple[str, ...]
    renderer_path: str
    category: str


def find_skill_root() -> Path:
    return Path(__file__).resolve().parent.parent


def read_text(path: Path) -> str:
    for encoding in ("utf-8-sig", "utf-8"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    return path.read_text(encoding="gb18030")


def normalize_scalar(value: str) -> str:
    value = value.strip()
    if value.startswith("#"):
        return ""
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def parse_key_value(text: str) -> tuple[str | None, str]:
    match = TEMPLATE_KEY_RE.match(text.strip())
    if not match:
        return None, ""
    key = match.group("key")
    value = normalize_scalar(match.group("value") or "")
    return key, value


def leading_spaces(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def is_significant(line: str) -> bool:
    stripped = line.strip()
    return bool(stripped) and not stripped.startswith("#")


def unique_append(items: list[str], value: str) -> None:
    if value and value not in items:
        items.append(value)


def inline_value_suffix(value: str) -> str:
    stripped = value.strip()
    if stripped.startswith("["):
        return "[]"
    return ""


def infer_container_suffix(lines: list[str], start_index: int, parent_indent: int) -> str:
    for index in range(start_index + 1, len(lines)):
        line = lines[index]
        if not is_significant(line):
            continue
        indent = leading_spaces(line)
        if indent <= parent_indent:
            return ""
        if line.strip().startswith("- "):
            return "[]"
        return ""
    return ""


def collect_list_child_keys(lines: list[str], item_indent: int) -> list[str]:
    keys: list[str] = []
    saw_scalar_item = False

    for index, line in enumerate(lines):
        if not is_significant(line):
            continue

        indent = leading_spaces(line)
        stripped = line.strip()
        if indent < item_indent:
            break

        if indent == item_indent and stripped.startswith("- "):
            item = stripped[2:].strip()
            child_key, child_value = parse_key_value(item)
            if child_key is None:
                saw_scalar_item = True
                continue
            unique_append(keys, child_key + inline_value_suffix(child_value))
            continue

        if indent == item_indent + 2 and not stripped.startswith("- "):
            child_key, child_value = parse_key_value(stripped)
            if child_key is None:
                continue
            suffix = inline_value_suffix(child_value)
            if not child_value:
                suffix = infer_container_suffix(lines, index, indent)
            unique_append(keys, child_key + suffix)

    if keys:
        return keys
    if saw_scalar_item:
        return ["[]"]
    return []


def collect_mapping_child_keys(lines: list[str], mapping_indent: int) -> list[str]:
    keys: list[str] = []

    for index, line in enumerate(lines):
        if not is_significant(line):
            continue

        indent = leading_spaces(line)
        stripped = line.strip()
        if indent < mapping_indent:
            break
        if indent != mapping_indent or stripped.startswith("- "):
            continue

        child_key, child_value = parse_key_value(stripped)
        if child_key is None:
            continue
        suffix = inline_value_suffix(child_value)
        if not child_value:
            suffix = infer_container_suffix(lines, index, indent)
        unique_append(keys, child_key + suffix)

    return keys


def summarize_param_block(param_name: str, inline_value: str, block_lines: list[str]) -> list[str]:
    if inline_value:
        return [param_name + inline_value_suffix(inline_value)]

    first_significant_index = next(
        (index for index, line in enumerate(block_lines) if is_significant(line)),
        None,
    )
    if first_significant_index is None:
        return [param_name]

    first_line = block_lines[first_significant_index]
    first_indent = leading_spaces(first_line)
    if first_line.strip().startswith("- "):
        child_keys = collect_list_child_keys(block_lines[first_significant_index:], first_indent)
        if child_keys == ["[]"]:
            return [f"{param_name}[]"]
        if child_keys:
            return [f"{param_name}[].{child_key}" for child_key in child_keys]
        return [f"{param_name}[]"]

    child_keys = collect_mapping_child_keys(block_lines[first_significant_index:], first_indent)
    if child_keys:
        return [f"{param_name}.{child_key}" for child_key in child_keys]
    return [param_name]


def extract_key_params(lines: list[str]) -> tuple[str, ...]:
    params_index = next(
        (index for index, line in enumerate(lines) if line.strip() == "params:"),
        None,
    )
    if params_index is None:
        return tuple()

    params_indent = leading_spaces(lines[params_index])
    key_params: list[str] = []
    index = params_index + 1

    while index < len(lines):
        line = lines[index]
        if not is_significant(line):
            index += 1
            continue

        indent = leading_spaces(line)
        if indent <= params_indent:
            break

        if line.strip().startswith("- ") or indent != params_indent + 2:
            index += 1
            continue

        param_key, inline_value = parse_key_value(line.strip())
        if param_key is None:
            index += 1
            continue

        block_end = index + 1
        while block_end < len(lines):
            next_line = lines[block_end]
            if not is_significant(next_line):
                block_end += 1
                continue
            if leading_spaces(next_line) <= indent:
                break
            block_end += 1

        for item in summarize_param_block(param_key, inline_value, lines[index + 1:block_end]):
            unique_append(key_params, item)

        index = block_end

    return tuple(key_params)


def detect_renderer(template_type: str, skill_root: Path) -> tuple[str, str]:
    sql_renderer = skill_root / "templates" / "sql" / f"{template_type}.sql"
    if sql_renderer.exists():
        return "sql", f"templates/sql/{sql_renderer.name}"

    shell_renderer = skill_root / "templates" / "shell" / f"{template_type}.sh"
    if shell_renderer.exists():
        return "shell", f"templates/shell/{shell_renderer.name}"

    return "unknown", "N/A"


def load_template_entry(path: Path, skill_root: Path) -> TemplateEntry:
    lines = read_text(path).splitlines()
    template_type = ""
    description = ""

    for line in lines:
        if not is_significant(line):
            continue
        key, value = parse_key_value(line)
        if key == "type":
            template_type = value
        elif key == "description":
            description = value
        if template_type and description:
            break

    category, renderer_path = detect_renderer(template_type, skill_root)
    return TemplateEntry(
        template_type=template_type,
        description=description,
        key_params=extract_key_params(lines),
        renderer_path=renderer_path,
        category=category,
    )


def format_markdown_cell(text: str) -> str:
    return text.replace("|", "\\|")


def format_key_params(params: tuple[str, ...]) -> str:
    if not params:
        return "`-`"
    return ", ".join(f"`{param}`" for param in params)


def pick_existing(entries: list[TemplateEntry], preferred: tuple[str, ...], category: str) -> str | None:
    names = [entry.template_type for entry in entries if entry.category == category]
    for candidate in preferred:
        if candidate in names:
            return candidate
    return names[0] if names else None


def build_chinese_prompts(entries: list[TemplateEntry]) -> list[str]:
    prompts = [
        "目前有哪些可用的 SQL 模板？",
        "目前有哪些可用的 HDFS 命令模板？",
    ]

    sql_template = pick_existing(entries, PREFERRED_SQL_EXAMPLES, "sql")
    hdfs_template = pick_existing(entries, PREFERRED_HDFS_EXAMPLES, "shell")

    if sql_template:
        prompts.append(f"`{sql_template}` 模板需要哪些参数？")

    if "data_diff" in {entry.template_type for entry in entries if entry.category == "sql"}:
        prompts.append("帮我判断“查 source 和 target 差异”该用哪个模板。")

    if "null_rate" in {entry.template_type for entry in entries if entry.category == "sql"}:
        prompts.append("用 `null_rate` 模板生成 `foo.bar` 在 `ds='2025-01-01'` 分区的空值率 SQL。")
    elif sql_template:
        prompts.append(f"用 `{sql_template}` 模板生成 `foo.bar` 在 `ds='2025-01-01'` 分区的 SQL。")

    if hdfs_template == "hdfs_du":
        prompts.append("用 `hdfs_du` 模板检查 `imd_aml_safe.table_a` 的 `ds='2025-01-01'` 分区大小。")

    return prompts


def build_english_prompts(entries: list[TemplateEntry]) -> list[str]:
    prompts = [
        "What SQL templates are available right now?",
        "What HDFS command templates are available right now?",
    ]

    sql_template = pick_existing(entries, PREFERRED_SQL_EXAMPLES, "sql")
    hdfs_template = pick_existing(entries, PREFERRED_HDFS_EXAMPLES, "shell")

    if sql_template:
        prompts.append(f"What parameters does the `{sql_template}` template require?")
        prompts.append(f"Use the `{sql_template}` template to generate SQL for `foo.bar` on `ds='2025-01-01'`.")

    if hdfs_template:
        prompts.append(f"Use the `{hdfs_template}` template to check `imd_aml_safe.table_a` for `ds='2025-01-01'`.")

    return prompts


def render_table(entries: list[TemplateEntry]) -> str:
    if not entries:
        return "| Type | Purpose | Key Params | Renderer |\n| --- | --- | --- | --- |\n| `-` | `-` | `-` | `-` |"

    lines = [
        "| Type | Purpose | Key Params | Renderer |",
        "| --- | --- | --- | --- |",
    ]
    for entry in entries:
        lines.append(
            "| "
            + f"`{format_markdown_cell(entry.template_type)}` | "
            + f"{format_markdown_cell(entry.description)} | "
            + f"{format_key_params(entry.key_params)} | "
            + f"`{format_markdown_cell(entry.renderer_path)}` |"
        )
    return "\n".join(lines)


def build_markdown(entries: list[TemplateEntry]) -> str:
    sql_entries = [entry for entry in entries if entry.category == "sql"]
    shell_entries = [entry for entry in entries if entry.category == "shell"]
    chinese_prompts = build_chinese_prompts(entries)
    english_prompts = build_english_prompts(entries)

    parts = [
        "<!-- Auto-generated by scripts/generate_template_guide.py. Do not edit manually. -->",
        "",
        "# SQL Generation Template Guide",
        "",
        "This file is the generated quick index for the `intelligent_sql_generation` skill.",
        "It is written with UTF-8 BOM so Chinese examples display correctly in Windows editors and terminals.",
        "",
        "## Source of Truth",
        "- Template definitions: `templates/yaml/*.yaml`",
        "- SQL renderers: `templates/sql/*.sql`",
        "- Shell renderers: `templates/shell/*.sh`",
        "- SQL generator: `scripts/generate.py`",
        "- Guide generator: `scripts/generate_template_guide.py`",
        "- Example outputs: `output/*_generated.*`",
        "",
        "If this guide and the template files disagree, the files under `templates/` are authoritative.",
        "",
        "## How To Regenerate",
        "```bash",
        "python scripts/generate_template_guide.py",
        "```",
        "",
        "## How To Answer Template Questions",
        "1. Group results into `SQL templates` and `HDFS command templates`.",
        "2. For each template, give `type`, `purpose`, and `key params`.",
        "3. If the user asks about one template in detail, open the matching YAML file for the exact parameter contract.",
        "4. If the user gives a business need instead of a template name, recommend the closest template first.",
        "5. Do not invent templates that do not exist in `templates/yaml/`.",
        "",
        f"## Current SQL Templates ({len(sql_entries)})",
        "",
        render_table(sql_entries),
        "",
        f"## Current HDFS Command Templates ({len(shell_entries)})",
        "",
        render_table(shell_entries),
        "",
        "## Chinese Prompt Examples",
    ]

    parts.extend(f"- {prompt}" for prompt in chinese_prompts)
    parts.extend(
        [
            "",
            "## English Prompt Examples",
        ]
    )
    parts.extend(f"- {prompt}" for prompt in english_prompts)
    parts.extend(
        [
            "",
            "## Local Generation Examples",
            "```bash",
            "python scripts/generate_template_guide.py",
            "python scripts/generate.py --yaml templates/yaml/data_diff.yaml",
            "python scripts/generate.py --yaml templates/yaml/hdfs_du.yaml",
            "```",
        ]
    )

    return "\n".join(parts) + "\n"


def generate_guide(output_path: Path) -> None:
    skill_root = find_skill_root()
    yaml_dir = skill_root / "templates" / "yaml"
    entries = sorted(
        (load_template_entry(path, skill_root) for path in yaml_dir.glob("*.yaml")),
        key=lambda entry: entry.template_type,
    )
    output_path.write_text(
        build_markdown(entries),
        encoding="utf-8-sig",
        newline="\n",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate TEMPLATE_GUIDE.md from template files.")
    parser.add_argument(
        "--output",
        default=str(find_skill_root() / "TEMPLATE_GUIDE.md"),
        help="Path to the generated markdown guide.",
    )
    args = parser.parse_args()

    output_path = Path(args.output).resolve()
    generate_guide(output_path)
    print(f"Saved template guide to: {output_path}")


if __name__ == "__main__":
    main()
