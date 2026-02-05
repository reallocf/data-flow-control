"""Correctness verification for comparing results across different approaches."""

from __future__ import annotations

from decimal import Decimal
import math
from typing import Any


def _normalize_exact(results: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    if not results:
        return []

    normalized = []
    for row in results:
        normalized_row = []
        for val in row:
            if val is None:
                normalized_row.append(None)
            else:
                normalized_row.append(val)
        normalized.append(tuple(normalized_row))

    normalized.sort()
    return normalized


def _normalize_approx(results: list[tuple[Any, ...]], precision: int = 5) -> list[tuple[Any, ...]]:
    if not results:
        return []

    normalized = []
    for row in results:
        normalized_row = []
        for val in row:
            if val is None:
                normalized_row.append(None)
            elif isinstance(val, Decimal):
                normalized_row.append(round(float(val), precision))
            elif isinstance(val, float):
                normalized_row.append(round(val, precision))
            elif isinstance(val, str):
                normalized_row.append(val.strip())
            else:
                normalized_row.append(val)
        normalized.append(tuple(normalized_row))

    normalized.sort()
    return normalized


def compare_results_exact(
    dfc_results: list[tuple[Any, ...]],
    logical_results: list[tuple[Any, ...]],
    physical_results: list[tuple[Any, ...]] | None = None,
) -> tuple[bool, str | None]:
    """Compare results with exact matching (DuckDB-to-DuckDB)."""
    norm_dfc = _normalize_exact(dfc_results)
    norm_logical = _normalize_exact(logical_results)

    if len(norm_dfc) != len(norm_logical):
        return False, f"Row count mismatch: dfc={len(norm_dfc)}, logical={len(norm_logical)}"

    for i, (d_row, l_row) in enumerate(zip(norm_dfc, norm_logical)):
        if not rows_equal_exact(d_row, l_row):
            return False, f"Row {i} mismatch between dfc and logical: {d_row} != {l_row}"

    if physical_results is not None:
        norm_physical = _normalize_exact(physical_results)
        if len(norm_dfc) != len(norm_physical):
            return False, f"Row count mismatch: dfc={len(norm_dfc)}, physical={len(norm_physical)}"

        for i, (d_row, p_row) in enumerate(zip(norm_dfc, norm_physical)):
            if not rows_equal_exact(d_row, p_row):
                return False, f"Row {i} mismatch between dfc and physical: {d_row} != {p_row}"

    return True, None


def compare_results_approx(
    dfc_results: list[tuple[Any, ...]],
    logical_results: list[tuple[Any, ...]],
    physical_results: list[tuple[Any, ...]] | None = None,
    precision: int = 5,
) -> tuple[bool, str | None]:
    """Compare results with approximate matching (DuckDB-to-external)."""
    norm_dfc = _normalize_approx(dfc_results, precision=precision)
    norm_logical = _normalize_approx(logical_results, precision=precision)

    if len(norm_dfc) != len(norm_logical):
        return False, f"Row count mismatch: dfc={len(norm_dfc)}, logical={len(norm_logical)}"

    for i, (d_row, l_row) in enumerate(zip(norm_dfc, norm_logical)):
        if not rows_equal_approx(d_row, l_row, precision=precision):
            return False, f"Row {i} mismatch between dfc and logical: {d_row} != {l_row}"

    if physical_results is not None:
        norm_physical = _normalize_approx(physical_results, precision=precision)
        if len(norm_dfc) != len(norm_physical):
            return False, f"Row count mismatch: dfc={len(norm_dfc)}, physical={len(norm_physical)}"

        for i, (d_row, p_row) in enumerate(zip(norm_dfc, norm_physical)):
            if not rows_equal_approx(d_row, p_row, precision=precision):
                return False, f"Row {i} mismatch between dfc and physical: {d_row} != {p_row}"

    return True, None


def rows_equal_exact(row1: tuple[Any, ...], row2: tuple[Any, ...]) -> bool:
    """Check if two rows are equal, with exact matching."""
    if len(row1) != len(row2):
        return False

    for v1, v2 in zip(row1, row2):
        if v1 is None and v2 is None:
            continue
        if v1 is None or v2 is None:
            return False
        if v1 != v2:
            return False

    return True


def rows_equal_approx(row1: tuple[Any, ...], row2: tuple[Any, ...], precision: int = 5) -> bool:
    """Check if two rows are equal, handling NULLs and approximate numeric equality."""
    if len(row1) != len(row2):
        return False

    for v1, v2 in zip(row1, row2):
        if v1 is None and v2 is None:
            continue
        if v1 is None or v2 is None:
            return False

        if isinstance(v1, Decimal):
            v1 = round(float(v1), precision)
        if isinstance(v2, Decimal):
            v2 = round(float(v2), precision)

        if isinstance(v1, float) or isinstance(v2, float):
            if not math.isclose(float(v1), float(v2), rel_tol=1e-5, abs_tol=1e-5):
                return False
        elif v1 != v2:
            return False

    return True
