"""Helper module to configure DuckDB to use the lineage extension."""

from __future__ import annotations

import os

import duckdb

from vldb_experiments.baselines import smokedduck_helper


def setup_local_smokedduck():
    """Configure DuckDB to use the lineage extension.

    Returns:
        duckdb module with lineage extension available
    """
    os.environ.setdefault("DUCKDB_ALLOW_UNSIGNED_EXTENSIONS", "1")
    smokedduck_helper.ensure_lineage_extension()

    if not hasattr(duckdb, "_original_connect"):
        duckdb._original_connect = duckdb.connect  # type: ignore[attr-defined]

        def _connect(*args, **kwargs):
            config = kwargs.get("config") or {}
            config.setdefault("allow_unsigned_extensions", "true")
            kwargs["config"] = config
            return duckdb._original_connect(*args, **kwargs)  # type: ignore[attr-defined]

        duckdb.connect = _connect  # type: ignore[assignment]

    test_conn = duckdb.connect(":memory:")
    try:
        smokedduck_helper.enable_lineage(test_conn)
    finally:
        test_conn.close()

    smokedduck_helper.duckdb = duckdb
    return duckdb
