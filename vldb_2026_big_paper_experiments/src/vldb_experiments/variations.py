"""Variation generation for microbenchmark experiments."""

import random
from typing import Optional

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False


def sample_zipfian(a: float, size: int, min_val: int = 1, max_val: Optional[int] = None) -> list[int]:
    """Sample from a Zipfian distribution.

    Args:
        a: Zipfian parameter (a > 1, lower = more skewed)
        size: Number of samples
        min_val: Minimum value
        max_val: Maximum value (None for no limit)

    Returns:
        List of sampled integers
    """
    if not NUMPY_AVAILABLE:
        # Fallback to simple random sampling if numpy not available
        return [random.randint(min_val, max_val or 1000) for _ in range(size)]

    # Generate Zipfian samples
    samples = np.random.zipf(a, size)
    # Clip to valid range
    samples = np.clip(samples, min_val, max_val or samples.max())
    return samples.tolist()


def get_policy_threshold_for_rows(
    target_rows_removed: int,
    total_rows: int = 1_000_000,
    value_range: tuple[int, int] = (1, 1_000_000)
) -> int:
    """Calculate policy threshold to remove approximately target_rows_removed rows.

    For a policy like "max(test_data.value) > threshold", we want to remove
    approximately target_rows_removed rows. With the microbenchmark data now using
    value=i for i in [1, total_rows], we can map directly:
    threshold == target_rows_removed.

    Args:
        target_rows_removed: Target number of rows to remove
        total_rows: Total number of rows in table
        value_range: (min_value, max_value) range for the value column

    Returns:
        Threshold value for the policy constraint
    """
    min_val, _max_val = value_range
    max_val = max(total_rows, _max_val)
    if target_rows_removed <= 0:
        return min_val - 1
    # Use direct mapping and clamp to valid domain.
    return max(min_val, min(int(target_rows_removed), max_val - 1))


def generate_variation_parameters(
    query_type: str,
    execution_number: int,
    num_variations: int = 4,
    num_runs_per_variation: int = 5,
    num_query_types: int = 6
) -> dict:
    """Generate variation parameters for a query type.

    Args:
        query_type: Type of query (SELECT, WHERE, JOIN, GROUP_BY, JOIN_GROUP_BY, ORDER_BY)
        execution_number: Current execution number
        num_variations: Number of variations (x values) per query type (default: 4)
        num_runs_per_variation: Number of runs per variation (default: 5)
        num_query_types: Number of different query types (default: 6)

    Returns:
        Dictionary with variation parameters and metrics
    """
    # Structure: 4 variations x 5 runs = 20 executions per query type
    # Execution numbers cycle through query types first, then variations, then runs:
    # Execution 1: SELECT, variation 1, run 1
    # Execution 2: WHERE, variation 1, run 1
    # Execution 3: JOIN, variation 1, run 1
    # Execution 4: GROUP_BY, variation 1, run 1
    # Execution 5: ORDER_BY, variation 1, run 1
    # Execution 6: SELECT, variation 1, run 2
    # Execution 7: WHERE, variation 1, run 2
    # ...
    # Execution 21: SELECT, variation 2, run 1
    # etc.

    # Calculate which "round" of query types we're in (0-indexed)
    # Round 0: executions 1-5 (all variation 1, run 1)
    # Round 1: executions 6-10 (all variation 1, run 2)
    # Round 2: executions 11-15 (all variation 1, run 3)
    # Round 3: executions 16-20 (all variation 1, run 4)
    # Round 4: executions 21-25 (all variation 1, run 5)
    # Round 5: executions 26-30 (all variation 2, run 1)
    # etc.
    round_number = (execution_number - 1) // num_query_types

    # Calculate which variation (0-indexed) and which run (0-indexed)
    variation_index = round_number // num_runs_per_variation
    run_index = round_number % num_runs_per_variation

    # Ensure variation_index is within bounds
    if variation_index >= num_variations:
        variation_index = variation_index % num_variations

    variation_num = variation_index + 1  # 1-indexed for reporting
    run_num = run_index + 1  # 1-indexed for reporting

    if query_type in ["SELECT", "WHERE", "ORDER_BY"]:
        # Vary policy to remove X rows (fixed logarithmic spacing)
        # Values: [0, 1_000_000, 2_000_000, 4_000_000, 8_000_000] rows removed
        rows_to_remove_values = [0, 1_000_000, 2_000_000, 4_000_000, 8_000_000]
        rows_to_remove = rows_to_remove_values[variation_index]

        # Calculate policy threshold
        threshold = get_policy_threshold_for_rows(
            target_rows_removed=rows_to_remove,
            total_rows=10_000_000,
            value_range=(1, 10_000_000),
        )

        return {
            "variation_type": "policy_threshold",
            "rows_to_remove": rows_to_remove,
            "policy_threshold": threshold,
            "variation_index": variation_index,
            "variation_num": variation_num,
            "run_index": run_index,
            "run_num": run_num,
        }

    if query_type == "JOIN":
        # Vary number of join matches (fixed logarithmic spacing)
        # This controls secondary table cardinality in JOIN benchmark.
        # Values: [100, 1000, 10000, 100000, 1000000] rows
        join_matches_values = [100, 1000, 10000, 100000, 1000000]
        join_matches = join_matches_values[variation_index]

        return {
            "variation_type": "join_matches",
            "join_matches": join_matches,
            "variation_index": variation_index,
            "variation_num": variation_num,
            "run_index": run_index,
            "run_num": run_num,
        }

    if query_type == "GROUP_BY":
        # Vary number of groups (fixed logarithmic spacing)
        # This affects the data, not the policy
        # Values: [10, 100, 1000, 10000] groups
        num_groups_values = [10, 100, 1000, 10000]
        num_groups = num_groups_values[variation_index]

        return {
            "variation_type": "num_groups",
            "num_groups": num_groups,
            "variation_index": variation_index,
            "variation_num": variation_num,
            "run_index": run_index,
            "run_num": run_num,
        }

    if query_type == "SIMPLE_AGG":
        # Vary input rows for simple aggregation without GROUP BY.
        # Values: [1k, 10k, 100k, 1m, 10m]
        num_rows_values = [1_000, 10_000, 100_000, 1_000_000, 10_000_000]
        num_rows = num_rows_values[variation_index]

        return {
            "variation_type": "num_rows",
            "num_rows": num_rows,
            "variation_index": variation_index,
            "variation_num": variation_num,
            "run_index": run_index,
            "run_num": run_num,
        }

    if query_type == "JOIN_GROUP_BY":
        join_count_values = [16, 32, 64, 128]
        join_count = join_count_values[variation_index]

        return {
            "variation_type": "join_count",
            "join_count": join_count,
            "variation_index": variation_index,
            "variation_num": variation_num,
            "run_index": run_index,
            "run_num": run_num,
        }

    # Default: no variation
    return {
        "variation_type": "none",
        "variation_index": variation_index,
        "variation_num": variation_num,
        "run_index": run_index,
        "run_num": run_num,
    }
