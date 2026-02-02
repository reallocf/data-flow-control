"""Correctness verification for comparing results across different approaches."""

import math
from typing import Any, Optional


def normalize_results(results: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    """Normalize result set for comparison.

    - Sort by all columns
    - Handle NULL values consistently

    Args:
        results: List of result tuples

    Returns:
        Sorted list of normalized tuples
    """
    if not results:
        return []

    # Convert to list of lists for sorting
    normalized = []
    for row in results:
        normalized_row = []
        for val in row:
            # Normalize NULL values
            if val is None:
                normalized_row.append(None)
            # Normalize floating point for comparison
            elif isinstance(val, float):
                # Round to reasonable precision
                normalized_row.append(round(val, 10))
            else:
                normalized_row.append(val)
        normalized.append(tuple(normalized_row))

    # Sort by all columns
    normalized.sort()

    return normalized


def compare_results(
    dfc_results: list[tuple[Any, ...]],
    logical_results: list[tuple[Any, ...]],
    physical_results: Optional[list[tuple[Any, ...]]] = None
) -> tuple[bool, Optional[str]]:
    """Compare results from policy-enabled approaches.

    Args:
        dfc_results: Results from DFC (SQLRewriter) approach
        logical_results: Results from Logical baseline
        physical_results: Optional results from Physical baseline

    Returns:
        Tuple of (match, error_message)
    """
    # Normalize all result sets
    norm_dfc = normalize_results(dfc_results)
    norm_logical = normalize_results(logical_results)

    # Check row counts
    if len(norm_dfc) != len(norm_logical):
        return False, f"Row count mismatch: dfc={len(norm_dfc)}, logical={len(norm_logical)}"

    # Compare dfc vs logical
    for i, (d_row, l_row) in enumerate(zip(norm_dfc, norm_logical)):
        if not rows_equal(d_row, l_row):
            return False, f"Row {i} mismatch between dfc and logical: {d_row} != {l_row}"

    if physical_results is not None:
        norm_physical = normalize_results(physical_results)
        if len(norm_dfc) != len(norm_physical):
            return False, f"Row count mismatch: dfc={len(norm_dfc)}, physical={len(norm_physical)}"

        # Compare dfc vs physical
        for i, (d_row, p_row) in enumerate(zip(norm_dfc, norm_physical)):
            if not rows_equal(d_row, p_row):
                return False, f"Row {i} mismatch between dfc and physical: {d_row} != {p_row}"

    return True, None


def rows_equal(row1: tuple[Any, ...], row2: tuple[Any, ...]) -> bool:
    """Check if two rows are equal, handling NULLs and floating point.

    Args:
        row1: First row
        row2: Second row

    Returns:
        True if rows are equal
    """
    if len(row1) != len(row2):
        return False

    for v1, v2 in zip(row1, row2):
        # Handle NULL values
        if v1 is None and v2 is None:
            continue
        if v1 is None or v2 is None:
            return False

        # Handle floating point comparison
        if isinstance(v1, float) and isinstance(v2, float):
            if not math.isclose(v1, v2, rel_tol=1e-9, abs_tol=1e-9):
                return False
        elif v1 != v2:
            return False

    return True
