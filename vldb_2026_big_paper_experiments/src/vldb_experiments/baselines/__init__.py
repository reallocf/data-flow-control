"""Baseline implementations for comparing against SQL rewriter."""

from .logical_baseline import rewrite_query_logical
from .physical_baseline import execute_query_physical

__all__ = [
    "execute_query_physical",
    "rewrite_query_logical",
]
