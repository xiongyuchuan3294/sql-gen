# sql-gen

This repository is a **skill-first** SQL generation project with three main parts:

- `intelligent_sql_generation`: single-step SQL / HDFS template generation
- `sql_workflow`: YAML-driven multi-step SQL workflow orchestration
- `tools/mysql_client.py`: standalone MySQL query / write helper

## Current Layout

```text
.claude/skills/intelligent_sql_generation/
├── SKILL.md
├── agents/                     # UI metadata for skill discovery
├── scripts/                    # generator scripts
├── references/                 # user guide and template index
├── assets/templates/           # SQL / shell / YAML template assets
└── output/                     # runtime output (gitignored)

.claude/skills/sql_workflow/
├── SKILL.md
├── agents/                     # UI metadata for skill discovery
├── scripts/                    # workflow orchestration scripts
├── assets/scenarios/           # workflow definitions
├── assets/examples/            # example semantic YAML inputs
└── output/                     # runtime output (gitignored)

conf/
└── aml_conf.conf + config.py   # MySQL config loading

tools/
└── mysql_client.py             # MySQL helper
```

## Why It Is Structured This Way

- `scripts/` keeps deterministic logic executable and easy for models to reuse
- `references/` keeps on-demand documentation out of `SKILL.md`
- `assets/` makes templates and workflow definitions easier to discover than scattering them at the skill root
- `output/` keeps generated artifacts local instead of committing them to the repository

## Common Commands

### Regenerate the template guide

```bash
python3 .claude/skills/intelligent_sql_generation/scripts/generate_template_guide.py
```

### Generate SQL / HDFS commands from a template

```bash
python3 .claude/skills/intelligent_sql_generation/scripts/generate.py \
  --yaml .claude/skills/intelligent_sql_generation/assets/templates/yaml/data_diff.yaml
```

### Run a SQL workflow

```bash
python3 .claude/skills/sql_workflow/scripts/orchestrator.py \
  --yaml .claude/skills/sql_workflow/assets/examples/input_example_data_compare.yaml
```

### Use the MySQL helper

```python
from tools.mysql_client import op_mysql

rows = op_mysql("aml_new3", "SELECT 1", op_type="query")
```

### Run remote regression against `hive-mcp-remote`

This regression boots a sample Hive table, sends Chinese natural-language prompts through both skills, verifies that metadata access happens through real MCP stdio tool calls to `hive_exec_server.py`, and checks that all calls reuse one shared MCP session.

For intranet migration, keep Hive connection details inside the external Hive MCP project. In practice this means you only need to update that project's `env.json` or point `HIVE_MCP_PATH` to a different Hive MCP deployment; `sql-gen` does not need Hive connection code changes.
After changing `env.json` or switching `HIVE_MCP_PATH`, restart the current agent / CLI process so the shared MCP session is recreated with the new backend configuration.

```bash
python3 tools/regress_skill_remote.py \
  --remote-hive-mcp-path /Users/xiongyuc/workspace/hive-mcp-remote
```

### Run the repeatable remote CI regression suite

This wrapper compile-checks the core scripts, verifies that `data_diff` keeps a deterministic `compare_columns` order across multiple `PYTHONHASHSEED` values, runs the skill/workflow regression, and then runs the full prompt-only template regression.

```bash
tools/ci_remote_regression.sh /Users/xiongyuc/workspace/hive-mcp-remote
```

## Install

```bash
pip install -r requirements.txt
```
