# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a SQL generation and execution tool focused on:
- SQL/HDFS command generation from templates
- Hive query and DDL/DML execution via MCP server
- MySQL query and write execution

## Key Directories

```
./
├── conf/                          # Configuration files
│   ├── aml_conf.conf             # MySQL profiles + active Hive env selector
│   ├── config.py                 # Config loader
│   └── hive_envs.json            # Hive local/remote environment mapping
├── tools/                         # Core utilities
│   ├── hive_client.py            # Hive execution utility
│   ├── hive_exec_server.py       # Hive MCP server
│   ├── mysql_client.py           # MySQL execution helper
│   └── test_hive_exec.py        # Hive execution smoke test
└── .claude/skills/               # Claude Code skills
    ├── intelligent_sql_generation/  # SQL template-based generation
    └── sql_scenario_execution/     # Multi-step SQL workflow orchestration
```

## Common Commands

### Install Dependencies
```powershell
pip install -r requirement.txt
```

### SQL Generation
Template-based generation:
```powershell
python .claude/skills/intelligent_sql_generation/scripts/generate.py --yaml .claude/skills/intelligent_sql_generation/templates/yaml/data_diff.yaml
```

### Hive Execution
Start the MCP server:
```powershell
python tools/hive_exec_server.py
```

Test Hive execution:
```powershell
python tools/test_hive_exec.py --env local --schema local_test
```

Hive environments are configured in `conf/hive_envs.json` and selected in `conf/aml_conf.conf` via `[hive].active_env`:
- `local_hs2` - local HiveServer2
- `uat` - remote Hive connection
- `local` - local debug environment

### MySQL Execution
```python
from tools.mysql_client import op_mysql

rows = op_mysql("aml_new3", "SELECT 1", op_type="query")
```

Profiles are defined in `conf/aml_conf.conf` under `[mysql]` section.

## Architecture

- **SQL Generation**: Uses YAML templates in `.claude/skills/intelligent_sql_generation/templates/yaml/` to generate SQL/HDFS commands
- **Hive MCP Server**: Provides Hive query execution via MCP protocol (`hive_exec_server.py`)
- **MySQL Client**: Simple wrapper around MySQL connections with profile support
- **Config System**: INI-style config in `aml_conf.conf` with JSON environment mappings in `hive_envs.json`

## MCP Tools Available

When the Hive MCP server is running, these tools are available:
- `hive_execute_query` - Execute Hive SQL query
- `hive_execute_dml` - Execute Hive DDL/DML
- `hive_describe_table` - Describe a Hive table
- `hive_preview_data` - Preview Hive table data
- `hive_count_records` - Count rows in a Hive table
- `hive_show_tables` - Show tables in a schema
- `hive_close_connections` - Close cached connections
