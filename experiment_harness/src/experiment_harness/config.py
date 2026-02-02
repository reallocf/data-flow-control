"""Configuration classes for experiment execution."""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class ExperimentConfig:
    """Configuration for experiment execution.
    
    Attributes:
        num_executions: Number of experiment runs to execute (default: 1)
        num_warmup_runs: Number of warm-up runs to discard (default: 0)
        setup_steps: Optional list of setup functions to run before experiment
        teardown_steps: Optional list of teardown functions to run after experiment
        database_config: Optional database connection parameters
        db_settings: Optional DuckDB settings to apply via SET
        strategy_config: Optional config passed directly to strategies
        output_dir: Directory for CSV output (default: "./results")
        output_filename: Base filename for CSV output (default: "results.csv")
        verbose: Enable verbose logging (default: False)
    """

    num_executions: int = 1
    num_warmup_runs: int = 0
    setup_steps: List[Callable[[], None]] = field(default_factory=list)
    teardown_steps: List[Callable[[], None]] = field(default_factory=list)
    database_config: Optional[Dict[str, Any]] = None
    db_settings: Optional[Dict[str, Any]] = None
    strategy_config: Optional[Dict[str, Any]] = None
    output_dir: str = "./results"
    output_filename: str = "results.csv"
    verbose: bool = False

    def __post_init__(self):
        """Validate configuration values."""
        if self.num_executions < 1:
            raise ValueError("num_executions must be at least 1")
        if self.num_warmup_runs < 0:
            raise ValueError("num_warmup_runs must be non-negative")
        if self.num_warmup_runs >= self.num_executions:
            raise ValueError("num_warmup_runs must be less than num_executions")
