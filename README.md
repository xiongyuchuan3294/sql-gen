# sql-gen

This repository is now focused on four capabilities only:

- SQL generation from the `agent_skills/sql_generation` template set
- HDFS command generation from the same template set
- Hive query and DDL/DML execution
- MySQL query and write execution

## Kept Components

```text
agent_skills/sql_generation/   SQL and HDFS template generation
conf/aml_conf.conf             MySQL profiles + active Hive env selector
conf/config.py                 Config loader for named profiles
conf/hive_envs.json            Hive local/remote environment mapping
tools/hive_client.py           Hive execution utility
tools/hive_exec_server.py      Hive MCP server
tools/mysql_client.py          MySQL execution helper
tools/test_hive_exec.py        Hive execution smoke test
tools/HIVE_MCP_README.md       Hive MCP usage guide
```

## SQL And HDFS Generation

Template-based generation lives under `agent_skills/sql_generation`.

Common commands:

```powershell
cd D:\workspace\sql-gen\agent_skills\sql_generation
python .\scripts\generate_template_guide.py
python .\scripts\generate.py --yaml .\templates\yaml\data_diff.yaml
python .\scripts\generate.py --yaml .\templates\yaml\hdfs_du.yaml
```

## Hive Execution

Hive environments are configured in `conf/hive_envs.json`.
The active default env is selected in `conf/aml_conf.conf` via `[hive].active_env`.

- `env=uat` uses the remote Hive connection
- `env=local` uses the local debug environment under `D:\workspace\hive-local-test`
- `env=local_hs2` uses the local HiveServer2 endpoint from `conf/hive_envs.json`

Start the MCP server:

```powershell
cd D:\workspace\sql-gen
python .\tools\hive_exec_server.py
python .\tools\test_hive_exec.py --env local --schema local_test
```

## MySQL Execution

MySQL execution is provided by `tools/mysql_client.py`.

It supports:

- named profiles from `conf/aml_conf.conf`
- raw connection strings in `host,port,database,user,password,charset` format

Example:

```python
from tools.mysql_client import op_mysql

rows = op_mysql("aml_new3", "SELECT 1", op_type="query")
```

## Install

```powershell
pip install -r .\requirement.txt
```
