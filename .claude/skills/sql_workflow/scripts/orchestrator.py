#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Backward-compatible wrapper for the YAML-driven workflow engine."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from config_loader import WorkflowEngine


class ScenarioOrchestrator:
    """Compatibility layer used by existing skill calls."""

    def __init__(self, env: str | None = None):
        self.engine = WorkflowEngine(env=env)

    def execute_scenario(self, user_input: str, explicit_scenario: str | None = None) -> dict:
        return self.engine.execute_from_text(user_input=user_input, explicit_scenario=explicit_scenario)

    def execute_scenario_yaml(self, yaml_path: str | Path, explicit_scenario: str | None = None) -> dict:
        return self.engine.execute_from_yaml(
            yaml_path=Path(yaml_path),
            explicit_scenario=explicit_scenario,
        )

    def format_result(self, result: dict) -> str:
        return self.engine.format_result(result)

    def save_result(self, result: dict, output_path: str | Path | None = None) -> Path | None:
        normalized = Path(output_path) if output_path else None
        return self.engine.save_result(result, normalized)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SQL workflow orchestrator")
    parser.add_argument("input", nargs="?", help="Natural language workflow request")
    parser.add_argument("--yaml", help="Semantic YAML input path (preferred)")
    parser.add_argument("--scenario", help="Explicit scenario name")
    parser.add_argument("--env", help="Hive environment")
    parser.add_argument("--output", "-o", help="Output SQL file path")
    parser.add_argument("--no-save", action="store_true", help="Do not write output SQL file")
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    if not args.input and not args.yaml:
        parser.error("Provide either positional input or --yaml")

    orchestrator = ScenarioOrchestrator(env=args.env)

    if args.yaml:
        result = orchestrator.execute_scenario_yaml(
            yaml_path=args.yaml,
            explicit_scenario=args.scenario,
        )
    else:
        result = orchestrator.execute_scenario(
            user_input=args.input,
            explicit_scenario=args.scenario,
        )

    print(orchestrator.format_result(result))

    if not args.no_save:
        saved_path = orchestrator.save_result(result, args.output)
        if saved_path:
            print(f"\nSaved SQL to: {saved_path}")

    return 0 if result.get("success") else 1


if __name__ == "__main__":
    raise SystemExit(main())
