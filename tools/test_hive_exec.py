#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Minimal Hive execution smoke test."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from tools.hive_client import DEFAULT_HIVE_ENV, JdbcHiveUtils


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test Hive execution.")
    parser.add_argument("--env", default=DEFAULT_HIVE_ENV, help="Hive environment name.")
    parser.add_argument("--schema", default="local_test", help="Schema name.")
    parser.add_argument(
        "--sql",
        default="SELECT * FROM demo_table_smoke ORDER BY id LIMIT 10",
        help="SQL to execute.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = JdbcHiveUtils.execute_query(
            schema=args.schema,
            sql=args.sql,
            env=args.env,
        )
        print(result)
        return 0
    except Exception as exc:
        print(f"Hive smoke test failed: {exc}", file=sys.stderr)
        return 1
    finally:
        JdbcHiveUtils.close_all()


if __name__ == "__main__":
    raise SystemExit(main())
