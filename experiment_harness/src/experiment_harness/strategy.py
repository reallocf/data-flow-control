"""Experiment strategy interface using the Strategy design pattern."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .results import ExperimentResult


@dataclass
class ExperimentContext:
    """Context object passed to experiment strategy methods."""

    execution_number: int = 0
    is_warmup: bool = False
    database_connection: Any | None = None
    strategy_config: dict[str, Any] | None = None
    shared_state: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Initialize default values."""
        if self.strategy_config is None:
            self.strategy_config = {}


class ExperimentStrategy(ABC):
    """Abstract base class for experiment strategies.

    Subclasses implement this interface to define specific experiment behaviors.
    The harness will call setup() once, execute() multiple times, and teardown() once.
    """

    @abstractmethod
    def setup(self, context: ExperimentContext) -> None:
        """One-time setup before all experiment runs.

        Args:
            context: Experiment context with database connection, system config, etc.
        """

    @abstractmethod
    def execute(self, context: ExperimentContext) -> ExperimentResult:
        """Execute a single experiment run.

        Args:
            context: Experiment context with current execution number and resources.

        Returns:
            ExperimentResult containing timing and custom metrics.
        """

    def teardown(self, _context: ExperimentContext) -> None:
        """Cleanup after all experiment runs.

        Args:
            context: Experiment context with database connection, system config, etc.
        """
        _ = _context

    def get_setting_key(self, _context: ExperimentContext) -> Any | None:
        """Return the setting key for the current execution.

        Strategies can override this to enable per-setting warmup execution order
        in the runner. The return value must be hashable.

        Args:
            _context: Experiment context with execution_number set.

        Returns:
            Hashable setting key for current execution, or None if unsupported.
        """
        _ = _context
        return None

    def get_metrics(self) -> list[str]:
        """Return list of custom metric names this strategy collects.

        Returns:
            List of metric name strings. Default implementation returns empty list.
        """
        return []
