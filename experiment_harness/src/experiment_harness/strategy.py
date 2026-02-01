"""Experiment strategy interface using the Strategy design pattern."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ExperimentContext:
    """Context object passed to experiment strategy methods."""

    execution_number: int = 0
    database_connection: Optional[Any] = None
    system_config: Optional[Dict[str, Any]] = None
    shared_state: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Initialize default values."""
        if self.system_config is None:
            self.system_config = {}


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
    def execute(self, context: ExperimentContext) -> "ExperimentResult":
        """Execute a single experiment run.
        
        Args:
            context: Experiment context with current execution number and resources.
            
        Returns:
            ExperimentResult containing timing and custom metrics.
        """

    def teardown(self, context: ExperimentContext) -> None:
        """Cleanup after all experiment runs.
        
        Args:
            context: Experiment context with database connection, system config, etc.
        """

    def get_metrics(self) -> List[str]:
        """Return list of custom metric names this strategy collects.
        
        Returns:
            List of metric name strings. Default implementation returns empty list.
        """
        return []
