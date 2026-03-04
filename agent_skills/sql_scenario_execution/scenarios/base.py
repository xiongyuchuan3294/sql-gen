#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Base scenario class for SQL scenario execution."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ScenarioStep:
    """Represents a single step in a scenario."""
    name: str
    template: str
    params: dict[str, Any] = field(default_factory=dict)
    description: str = ""


@dataclass
class ScenarioResult:
    """Result of scenario execution."""
    success: bool
    steps: list[ScenarioStep] = field(default_factory=list)
    generated_sql: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    messages: list[str] = field(default_factory=list)


class BaseScenario(ABC):
    """Base class for all scenarios."""

    def __init__(self, params: dict[str, Any]):
        self.params = params

    @abstractmethod
    def get_name(self) -> str:
        """Get scenario name."""
        pass

    @abstractmethod
    def get_description(self) -> str:
        """Get scenario description."""
        pass

    @abstractmethod
    def get_steps(self) -> list[ScenarioStep]:
        """Get list of steps for this scenario."""
        pass

    def validate_params(self) -> tuple[bool, str]:
        """
        Validate required parameters.

        Returns:
            (is_valid, error_message)
        """
        return True, ""

    def prepare_step_params(self, step: ScenarioStep) -> dict[str, Any]:
        """
        Prepare parameters for a step by substituting variables.

        Args:
            step: The step to prepare parameters for

        Returns:
            Prepared parameters
        """
        params = {}
        for key, value in step.params.items():
            if isinstance(value, str):
                # Substitute variables like {{partition}}
                for param_key, param_value in self.params.items():
                    placeholder = f"{{{{{param_key}}}}}"
                    if placeholder in value:
                        value = value.replace(placeholder, str(param_value))
                params[key] = value
            else:
                params[key] = value
        return params
