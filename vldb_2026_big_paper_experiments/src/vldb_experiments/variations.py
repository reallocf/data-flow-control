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
    value_range: tuple[int, int] = (1, 1000)
) -> int:
    """Calculate policy threshold to remove approximately target_rows_removed rows.

    For a policy like "max(test_data.value) > threshold", we want to remove
    approximately target_rows_removed rows. Since values are uniformly distributed
    in [1, 1000], we can calculate the threshold.

    Args:
        target_rows_removed: Target number of rows to remove
        total_rows: Total number of rows in table
        value_range: (min_value, max_value) range for the value column

    Returns:
        Threshold value for the policy constraint
    """
    min_val, max_val = value_range
    value_span = max_val - min_val + 1

    # For "max(value) > threshold", we remove rows where max(value) <= threshold
    # If values are uniformly distributed, threshold should be approximately:
    # threshold = min_val + (target_rows_removed / total_rows) * value_span
    # But we want max(value) > threshold, so we need to invert:
    # We want to keep rows where max(value) > threshold
    # So we remove rows where max(value) <= threshold
    # threshold = min_val + (target_rows_removed / total_rows) * value_span - 1

    # Actually, for max(value) > threshold, we keep rows where the max value in
    # the group/source is > threshold. If we want to remove X rows, we need to
    # set threshold such that X rows have max(value) <= threshold.
    # For uniform distribution: threshold ≈ min_val + (X / total_rows) * value_span

    threshold = min_val + int((target_rows_removed / total_rows) * value_span)
    # Ensure threshold is in valid range
    return max(min_val, min(threshold, max_val - 1))


def generate_variation_parameters(
    query_type: str,
    execution_number: int,
    num_variations: int = 4,
    num_runs_per_variation: int = 5,
    num_query_types: int = 5
) -> dict:
    """Generate variation parameters for a query type.

    Args:
        query_type: Type of query (SELECT, WHERE, JOIN, GROUP_BY, ORDER_BY)
        execution_number: Current execution number
        num_variations: Number of variations (x values) per query type (default: 4)
        num_runs_per_variation: Number of runs per variation (default: 5)
        num_query_types: Number of different query types (default: 5)

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
        # Values: [100, 1000, 10000, 100000] rows removed
        rows_to_remove_values = [100, 1000, 10000, 100000]
        rows_to_remove = rows_to_remove_values[variation_index]

        # Calculate policy threshold
        threshold = get_policy_threshold_for_rows(
            target_rows_removed=rows_to_remove,
            total_rows=1_000_000,
            value_range=(1, 1000)
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
        # This affects the data, not the policy
        # Values: [1000, 10000, 100000, 1000000] matches
        join_matches_values = [1000, 10000, 100000, 1000000]
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

    # Default: no variation
    return {
        "variation_type": "none",
        "variation_index": variation_index,
        "variation_num": variation_num,
        "run_index": run_index,
        "run_num": run_num,
    }
