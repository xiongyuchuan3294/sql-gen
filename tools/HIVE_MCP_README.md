# Hive MCP Server

`tools/hive_exec_server.py` provides an MCP server for querying Hive-compatible data sources.

It supports two execution modes:

- `env=uat`: real remote Hive execution for intranet environments
- `env=local`: local debug execution backed by `D:\workspace\hive-local-test`

The server delegates execution to `tools/hive_client.py`.

Default environment selection is controlled by the code variable
`DEFAULT_HIVE_ENV` in `tools/hive_client.py`. It is set to `uat` by default.
If you want all calls that omit `env` to use local mode, change it to `local`.

## Files

```text
tools/
|-- hive_exec_server.py
|-- test_hive_exec.py
`-- HIVE_MCP_README.md

conf/
`-- hive_envs.json
```

## Quick Start

### 1. Start the MCP server

```powershell
cd D:\workspace\sql-gen
python .\tools\hive_exec_server.py
```

### 2. Configure VS Code MCP

Add this to `.vscode/mcp.json`:

```json
{
  "mcpServers": {
    "hive-exec-server": {
      "command": "python",
      "args": ["D:\\workspace\\sql-gen\\tools\\hive_exec_server.py"]
    }
  }
}
```

### 3. Smoke test local mode

First ensure the local debug environment exists under `D:\workspace\hive-local-test`.

Then call the server tools with `env="local"`, or run the local SQL runner directly:

```powershell
cd D:\workspace\hive-local-test
.\run_hive_sql.ps1 --file .\smoke_test.sql
```

## Configuration

All Hive environment mappings are now managed in:

- `conf/hive_envs.json`

This file contains:

- shared `hive_conf`
- remote environments such as `uat`
- the local debug environment `local`

The code also accepts `local_test` as an alias for `local`.

Example:

```json
{
  "hive_conf": {
    "mapreduce.job.queuename": "queue_1006_01"
  },
  "environments": {
    "local": {
      "mode": "local",
      "root": "D:\\workspace\\hive-local-test",
      "python": "D:\\workspace\\hive-local-test\\.venv\\Scripts\\python.exe",
      "runner": "D:\\workspace\\hive-local-test\\run_hive_sql.py"
    },
    "uat": {
      "mode": "remote",
      "host": "172.21.1.168",
      "port": 10000,
      "username": "hduser1009",
      "password": "your-password"
    }
  }
}
```

### Add a new remote environment

Edit `conf/hive_envs.json` and add another entry under `environments`:

```json
{
  "environments": {
    "uat": {
      "mode": "remote",
      "host": "172.21.1.168",
      "port": 10000,
      "username": "hduser1009",
      "password": "..."
    },
    "prod": {
      "mode": "remote",
      "host": "172.21.1.169",
      "port": 10000,
      "username": "hduser2000",
      "password": "..."
    }
  }
}
```

### Override the config path

If needed, point the runtime to another config file:

```powershell
$env:HIVE_ENV_CONFIG_PATH = "D:\custom\hive_envs.json"
python .\tools\hive_exec_server.py
```

### Change the default environment in code

Edit `tools/hive_client.py`:

```python
DEFAULT_HIVE_ENV = "uat"
```

Change it to:

```python
DEFAULT_HIVE_ENV = "local"
```

This affects callers that omit `env`. Explicit `env` parameters still win.

## Supported Tools

### `hive_execute_query`

Execute a Hive SQL query and return tab-separated results.

Parameters:

- `schema`: database name
- `sql`: query text
- `env`: environment name, default `uat`

Example:

```python
{
  "schema": "imd_aml_safe",
  "sql": "SELECT * FROM rrs_aml_risk_rate_current WHERE ds='2025-01-01' LIMIT 10",
  "env": "uat"
}
```

### `hive_describe_table`

Describe a table structure.

Parameters:

- `schema`
- `table_name`
- `env`

### `hive_count_records`

Count records in a table.

Parameters:

- `schema`
- `table_name`
- `partition_filter`
- `env`

### `hive_preview_data`

Preview table data.

Parameters:

- `schema`
- `table_name`
- `partition_filter`
- `limit`
- `env`

### `hive_execute_dml`

Execute DDL or DML.

Parameters:

- `schema`
- `sql`
- `env`

### `hive_show_tables`

Show all tables in a schema.

Parameters:

- `schema`
- `env`

### `hive_close_connections`

Close cached remote connections.

Parameters:

- none

## Usage Examples

### Query a remote Hive table

```python
from tools.hive_client import JdbcHiveUtils

result = JdbcHiveUtils.execute_query(
    schema="imd_aml_safe",
    sql="SELECT * FROM rrs_aml_risk_rate_current WHERE ds='2025-01-01' LIMIT 10",
    env="uat",
)
print(result)
```

### Query the local debug environment

```python
from tools.hive_client import JdbcHiveUtils

result = JdbcHiveUtils.execute_query(
    schema="local_test",
    sql="SELECT * FROM demo_table_smoke ORDER BY id LIMIT 10",
    env="local",
)
print(result)
```

### Show local tables through the MCP server

```python
{
  "schema": "local_test",
  "env": "local"
}
```

### Start and test directly

```powershell
cd D:\workspace\sql-gen
python .\tools\test_hive_exec.py
python .\tools\hive_exec_server.py
```

## Important Notes

1. Use partition filters for partitioned tables whenever possible.
2. Add `LIMIT` when previewing large datasets.
3. `env=local` is for debugging only; do not treat it as a replacement for intranet Hive.
4. Remote credentials and local runner paths now belong in `conf/hive_envs.json`, not in `tools/hive_client.py`.
5. `hive_close_connections` only matters for cached remote connections. Local mode is process-based.

## Troubleshooting

### Connection failed in remote mode

Check:

- the target Hive service is reachable
- the host and port in `conf/hive_envs.json`
- username and password
- whether the selected `env` exists in the config file

### Local mode failed

Check:

- `D:\workspace\hive-local-test` exists
- `.venv\Scripts\python.exe` exists under that directory
- `run_hive_sql.py` exists under that directory
- the `local` entry in `conf/hive_envs.json` points to the correct paths

### VS Code MCP cannot connect

Check:

- `.vscode/mcp.json`
- whether `python .\tools\hive_exec_server.py` starts successfully
- VS Code MCP output logs

## Related Files

- `tools/hive_exec_server.py`
- `tools/hive_client.py`
- `conf/hive_envs.json`
- `tools/test_hive_exec.py`
- `D:\workspace\hive-local-test\README.md`

## References

- MCP: https://modelcontextprotocol.io/
- FastMCP: https://github.com/jlowin/fastmcp
