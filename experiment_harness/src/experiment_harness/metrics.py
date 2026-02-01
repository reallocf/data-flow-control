"""Metrics collection utilities for experiments."""

import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator


@contextmanager
def time_execution() -> Generator[Dict[str, float], None, None]:
    """Context manager for timing code execution.
    
    Yields a dictionary with 'duration_ms' key containing execution time in milliseconds.
    
    Example:
        with time_execution() as timing:
            # code to time
            result = some_function()
        duration = timing['duration_ms']
    """
    start = time.perf_counter()
    timing = {}
    try:
        yield timing
    finally:
        end = time.perf_counter()
        timing['duration_ms'] = (end - start) * 1000.0


def collect_memory_usage() -> Dict[str, float]:
    """Collect current memory usage.
    
    Returns:
        Dictionary with 'memory_mb' key containing memory usage in MB.
        Returns empty dict if psutil is not available.
    """
    try:
        import psutil
        import os
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        return {'memory_mb': memory_info.rss / (1024 * 1024)}
    except ImportError:
        return {}


def count_query_results(cursor: Any) -> Dict[str, int]:
    """Count rows returned by a database query cursor.
    
    Args:
        cursor: Database cursor with query results
        
    Returns:
        Dictionary with 'rows_returned' key containing row count.
    """
    try:
        rows = cursor.fetchall()
        return {'rows_returned': len(rows)}
    except Exception:
        return {'rows_returned': 0}
