#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Scenarios package for sql_scenario_execution."""

from .base import BaseScenario, ScenarioResult, ScenarioStep
from .data_compare import DataCompareScenario

# 注册所有场景
SCENARIOS = {
    "data_compare": DataCompareScenario,
    "data_compare_simple": DataCompareScenario,  # 别名
}


def get_scenario(scenario_name: str, params: dict) -> BaseScenario | None:
    """
    Get scenario instance by name.

    Args:
        scenario_name: Name of the scenario
        params: Parameters for the scenario

    Returns:
        Scenario instance or None if not found
    """
    scenario_class = SCENARIOS.get(scenario_name.lower())
    if scenario_class:
        return scenario_class(params)
    return None


def recognize_scenario(user_input: str) -> str | None:
    """
    Recognize scenario from user input.

    Args:
        user_input: User's natural language input

    Returns:
        Scenario name or None
    """
    user_input_lower = user_input.lower()

    # 数据对比场景关键词
    compare_keywords = ["对比", "比较", "差异", "数据对比", "compare", "diff"]
    if any(kw in user_input_lower for kw in compare_keywords):
        return "data_compare"

    # 数据校验场景关键词
    validation_keywords = ["校验", "验证", "检查", "质量", "validation", "check"]
    if any(kw in user_input_lower for kw in validation_keywords):
        return "data_validation"

    return None


__all__ = [
    "BaseScenario",
    "ScenarioResult",
    "ScenarioStep",
    "DataCompareScenario",
    "get_scenario",
    "recognize_scenario",
]
