#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
REMOTE_HIVE_MCP_PATH="${REMOTE_HIVE_MCP_PATH:-${1:-/Users/xiongyuc/workspace/hive-mcp-remote}}"
SCHEMA="${SCHEMA:-default}"
PARTITION="${PARTITION:-2026-03-08}"
DATA_DIFF_HASH_SEEDS="${DATA_DIFF_HASH_SEEDS:-1,2,3,11,97}"

cd "$ROOT_DIR"

echo "[1/4] Compile-check core scripts"
"$PYTHON_BIN" -m py_compile \
  .claude/skills/intelligent_sql_generation/scripts/generate.py \
  .claude/skills/intelligent_sql_generation/scripts/prompt_dispatcher.py \
  .claude/skills/intelligent_sql_generation/scripts/hive_mcp_runtime.py \
  .claude/skills/sql_workflow/scripts/orchestrator.py \
  tools/regress_skill_remote.py \
  tools/regress_all_templates_remote.py \
  tools/check_data_diff_determinism_remote.py

echo "[2/4] Verify data_diff deterministic ordering"
"$PYTHON_BIN" tools/check_data_diff_determinism_remote.py \
  --remote-hive-mcp-path "$REMOTE_HIVE_MCP_PATH" \
  --schema "$SCHEMA" \
  --partition "$PARTITION" \
  --hash-seeds "$DATA_DIFF_HASH_SEEDS"

echo "[3/4] Run skill/workflow MCP regression"
"$PYTHON_BIN" tools/regress_skill_remote.py \
  --remote-hive-mcp-path "$REMOTE_HIVE_MCP_PATH" \
  --schema "$SCHEMA" \
  --partition "$PARTITION"

echo "[4/4] Run full prompt-only template regression"
"$PYTHON_BIN" tools/regress_all_templates_remote.py \
  --remote-hive-mcp-path "$REMOTE_HIVE_MCP_PATH" \
  --schema "$SCHEMA" \
  --partition "$PARTITION"

echo "CI regression suite completed successfully."
