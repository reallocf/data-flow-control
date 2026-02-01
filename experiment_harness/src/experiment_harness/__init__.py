"""Experiment harness for running experiments using the Strategy design pattern."""

from .strategy import ExperimentStrategy, ExperimentContext
from .config import ExperimentConfig
from .runner import ExperimentRunner
from .results import ExperimentResult, ResultCollector
from .metrics import time_execution, collect_memory_usage

__all__ = [
    "ExperimentStrategy",
    "ExperimentContext",
    "ExperimentConfig",
    "ExperimentRunner",
    "ExperimentResult",
    "ResultCollector",
    "time_execution",
    "collect_memory_usage",
]

__version__ = "0.1.0"
