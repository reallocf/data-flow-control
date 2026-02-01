"""Experiment harness for running experiments using the Strategy design pattern."""

from .config import ExperimentConfig
from .metrics import collect_memory_usage, time_execution
from .results import ExperimentResult, ResultCollector
from .runner import ExperimentRunner
from .strategy import ExperimentContext, ExperimentStrategy

__all__ = [
    "ExperimentConfig",
    "ExperimentContext",
    "ExperimentResult",
    "ExperimentRunner",
    "ExperimentStrategy",
    "ResultCollector",
    "collect_memory_usage",
    "time_execution",
]

__version__ = "0.1.0"
