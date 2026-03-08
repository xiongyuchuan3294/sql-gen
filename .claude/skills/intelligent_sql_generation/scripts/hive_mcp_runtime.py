#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import os
import shlex
import sys
from pathlib import Path
from typing import Any


def _extract_root_from_explicit_server() -> Path | None:
    explicit_cwd = os.getenv("HIVE_MCP_SERVER_CWD", "").strip()
    if explicit_cwd:
        candidate = Path(explicit_cwd).expanduser().resolve()
        if (candidate / "hive_exec_server.py").exists():
            return candidate

    explicit_args = shlex.split(os.getenv("HIVE_MCP_SERVER_ARGS", "").strip())
    for arg in explicit_args:
        candidate = Path(arg).expanduser()
        if candidate.name == "hive_exec_server.py" and candidate.exists():
            return candidate.resolve().parent
    return None


def _candidate_runtime_paths(repo_root: str | Path | None = None) -> list[Path]:
    derived_root = _extract_root_from_explicit_server()
    base_repo = Path(repo_root).resolve() if repo_root else Path.cwd().resolve()
    parent_dir = base_repo.parent
    candidate_values = [
        str(derived_root) if derived_root else "",
        os.getenv("HIVE_MCP_PATH", "").strip(),
        os.getenv("SQL_GEN_HIVE_MCP_PATH", "").strip(),
        str(parent_dir / "hive-mcp"),
        str(parent_dir / "hive-mcp-remote"),
        str(parent_dir / "hive-mcp-uat"),
        str(Path("~/workspace/hive-mcp").expanduser()),
        str(Path("~/workspace/hive-mcp-remote").expanduser()),
        str(Path("~/workspace/hive-mcp-uat").expanduser()),
        r"D:\workspace\hive-mcp",
        r"D:\workspace\hive-mcp-remote",
        r"D:\workspace\hive-mcp-uat",
    ]

    discovered: list[Path] = []
    seen: set[str] = set()
    for raw_path in candidate_values:
        if not raw_path:
            continue
        candidate = Path(raw_path).expanduser().resolve()
        key = str(candidate)
        if key in seen or not candidate.is_dir():
            continue
        if not (candidate / "hive_exec_server.py").exists():
            continue
        seen.add(key)
        discovered.append(candidate)
    return discovered


def resolve_hive_mcp_root(repo_root: str | Path | None = None) -> Path:
    for candidate in _candidate_runtime_paths(repo_root):
        client_file = candidate / "hive_mcp_client.py"
        if client_file.exists():
            return candidate
    raise FileNotFoundError(
        "Hive MCP root not found or missing hive_mcp_client.py. "
        "Point HIVE_MCP_PATH to a hive-mcp-remote checkout that includes hive_mcp_client.py."
    )


def _load_external_client_module(server_root: Path) -> Any:
    client_path = server_root / "hive_mcp_client.py"
    module_name = f"hive_mcp_client_{abs(hash(str(client_path)))}"
    cached = sys.modules.get(module_name)
    if cached is not None:
        return cached

    spec = importlib.util.spec_from_file_location(module_name, client_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load Hive MCP client from {client_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def build_hive_runtime(repo_root: str | Path | None = None):
    server_root = resolve_hive_mcp_root(repo_root)
    module = _load_external_client_module(server_root)
    return module.build_hive_runtime(server_root=server_root)
