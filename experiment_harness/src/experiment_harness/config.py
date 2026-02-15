"""Configuration classes for experiment execution."""

from dataclasses import dataclass, field
from typing import Any, Callable, Literal, Optional


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
    warmup_mode: Literal["global", "per_setting"] = "global"
    warmup_runs_per_setting: int | None = None
    setup_steps: list[Callable[[], None]] = field(default_factory=list)
    teardown_steps: list[Callable[[], None]] = field(default_factory=list)
    database_config: Optional[dict[str, Any]] = None
    db_settings: Optional[dict[str, Any]] = None
    strategy_config: Optional[dict[str, Any]] = None
    output_dir: str = "./results"
    output_filename: str = "results.csv"
    verbose: bool = False

    def __post_init__(self):
        """Validate configuration values."""
        if self.num_executions < 1:
            raise ValueError("num_executions must be at least 1")
        if self.num_warmup_runs < 0:
            raise ValueError("num_warmup_runs must be non-negative")
        if self.warmup_mode not in {"global", "per_setting"}:
            raise ValueError("warmup_mode must be either 'global' or 'per_setting'")
        if self.warmup_mode == "global" and self.num_warmup_runs >= self.num_executions:
            raise ValueError("num_warmup_runs must be less than num_executions")
        if self.warmup_runs_per_setting is not None and self.warmup_runs_per_setting < 0:
            raise ValueError("warmup_runs_per_setting must be non-negative")
