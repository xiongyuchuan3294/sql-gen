# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a skill-first SQL generation repository focused on:
- single-step SQL / HDFS template generation
- YAML-driven multi-step SQL workflow generation
- standalone MySQL query / write helpers

## Key Directories

```
./
├── conf/
│   ├── aml_conf.conf               # MySQL profiles
│   └── config.py                   # Config loader
├── tools/
│   └── mysql_client.py             # MySQL execution helper
└── .claude/skills/
    ├── intelligent_sql_generation/ # Single-step SQL/HDFS generation skill
    │   └── agents/openai.yaml      # UI metadata
    └── sql_workflow/               # Multi-step workflow generation skill
        └── agents/openai.yaml      # UI metadata
```

## Common Commands

### Install Dependencies
```powershell
pip install -r requirements.txt
```

### SQL Generation
Template-based generation:
```bash
python3 .claude/skills/intelligent_sql_generation/scripts/generate.py --yaml .claude/skills/intelligent_sql_generation/assets/templates/yaml/data_diff.yaml
```

### SQL Workflow
Run a workflow from semantic YAML:
```bash
python3 .claude/skills/sql_workflow/scripts/orchestrator.py --yaml .claude/skills/sql_workflow/assets/examples/input_example_data_compare.yaml
```

### MySQL Execution
```python
from tools.mysql_client import op_mysql

rows = op_mysql("aml_new3", "SELECT 1", op_type="query")
```

Profiles are defined in `conf/aml_conf.conf` under `[mysql]` section.

### Remote Regression
Runs a real MCP stdio session against `hive-mcp-remote/hive_exec_server.py`, bootstraps sample Hive data, verifies that Chinese prompts still resolve into deterministic SQL templates, and confirms the client reuses one shared MCP session.

For environment migration, keep Hive JDBC / beeline / auth settings inside the external Hive MCP project and switch them there via `env.json` or by changing `HIVE_MCP_PATH`.
After that change, restart the current process so the shared MCP session reconnects with the new Hive backend settings.

```bash
python3 tools/regress_skill_remote.py --remote-hive-mcp-path /Users/xiongyuc/workspace/hive-mcp-remote
```

### Repeatable Remote CI Regression
The CI wrapper compile-checks the key scripts, verifies `data_diff` column ordering across multiple `PYTHONHASHSEED` values, runs the skill/workflow MCP regression, and then runs the full prompt-only template regression.

```bash
tools/ci_remote_regression.sh /Users/xiongyuc/workspace/hive-mcp-remote
```

## Architecture

- **Skill Layout**: Each skill now uses `scripts/`, `references/`, `assets/`, and `output/` for clearer model- and user-facing structure
- **SQL Generation**: Uses YAML contracts in `.claude/skills/intelligent_sql_generation/assets/templates/yaml/`
- **Workflow Generation**: Uses scenario definitions in `.claude/skills/sql_workflow/assets/scenarios/`
- **MySQL Client**: Simple wrapper around MySQL connections with profile support

## Notes

- Runtime outputs under each skill's `output/` directory are generated locally and should not be committed.
- Keep local MCP client configuration outside the repository to avoid machine-specific paths in version control.
