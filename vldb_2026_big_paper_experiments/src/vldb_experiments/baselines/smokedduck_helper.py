"""Helper functions for SmokedDuck lineage capture."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import re
import sys
import tempfile
from typing import Any, Optional

import duckdb


def _get_smokedduck_dir() -> Path:
    """Get the path to locally built SmokedDuck (relative to data-flow-control repo root)."""
    # From this file, go up to data-flow-control root, then ../smokedduck
    _current_file = Path(__file__).resolve()
    # Go up: baselines -> vldb_experiments -> src -> vldb_2026_big_paper_experiments -> data-flow-control
    _repo_root = _current_file.parent.parent.parent.parent.parent
    return _repo_root.parent / "smokedduck"


def _setup_smokedduck_path():
    """Set up Python path to find locally built SmokedDuck."""
    smokedduck_dir = _get_smokedduck_dir()
    if smokedduck_dir.exists():
        # Add build/python to path if it exists
        python_path = smokedduck_dir / "build" / "python"
        if python_path.exists():
            python_path_str = str(python_path)
            if python_path_str not in sys.path:
                sys.path.insert(0, python_path_str)

        # Also try build/release/python
        python_path2 = smokedduck_dir / "build" / "release" / "python"
        if python_path2.exists():
            python_path2_str = str(python_path2)
            if python_path2_str not in sys.path:
                sys.path.insert(0, python_path2_str)


def _setup_smokedduck_scripts_path() -> None:
    """Set up Python path to import SmokedDuck lineage query scripts."""
    smokedduck_dir = _get_smokedduck_dir()
    scripts_path = smokedduck_dir / "benchmark" / "smokedduck-scripts" / "smokedduck"
    if scripts_path.exists():
        scripts_path_str = str(scripts_path)
        if scripts_path_str not in sys.path:
            sys.path.insert(0, scripts_path_str)


def is_smokedduck_available() -> bool:
    """Check if SmokedDuck is available (either via import or local build).

    Returns:
        True if SmokedDuck can be imported or found locally

    Raises:
        ImportError: If SmokedDuck is not available
    """
    if importlib.util.find_spec("smokedduck") is not None:
        return True

    _setup_smokedduck_path()
    if importlib.util.find_spec("smokedduck") is not None:
        return True

    # Check if we can use DuckDB with lineage support
    # SmokedDuck may be built into DuckDB itself
    try:
        conn = duckdb.connect(":memory:")
        # Try to enable lineage - if it works, SmokedDuck is available
        with contextlib.suppress(Exception):
            conn.execute("PRAGMA enable_lineage")
            conn.close()
            return True
        conn.close()
    except Exception:
        pass

    # SmokedDuck is REQUIRED - raise error if not found
    smokedduck_dir = _get_smokedduck_dir()
    raise ImportError(
        f"SmokedDuck is REQUIRED but not available. "
        f"Please run ./setup_venv.sh to clone and build SmokedDuck. "
        f"Expected location: {smokedduck_dir}"
    )


def enable_lineage(conn: duckdb.DuckDBPyConnection) -> None:
    """Enable lineage capture in DuckDB connection.

    This is REQUIRED for the physical baseline. Lineage must be enabled before
    executing queries that need provenance tracking.

    Args:
        conn: DuckDB connection (must be SmokedDuck build)

    Raises:
        ImportError: If SmokedDuck is not available
        RuntimeError: If lineage cannot be enabled
    """
    # Verify SmokedDuck is available first
    is_smokedduck_available()

    # Try to enable lineage via PRAGMA
    # SmokedDuck builds lineage support into DuckDB
    # Note: Enabling lineage may start a transaction, so we commit after
    try:
        conn.execute("PRAGMA enable_lineage")
        # Persist lineage tables so operator lineage tables are created.
        with contextlib.suppress(Exception):
            conn.execute("PRAGMA persist_lineage")
        # Commit any transaction started by enabling lineage
        with contextlib.suppress(Exception):
            conn.commit()
        return
    except Exception as e:
        # If pragma doesn't work, try alternative methods
        try:
            # Try alternative pragma syntax
            conn.execute("PRAGMA lineage=on")
            with contextlib.suppress(Exception):
                conn.execute("PRAGMA persist_lineage")
            with contextlib.suppress(Exception):
                conn.commit()
            return
        except Exception:
            # Lineage pragma not available - this is a hard error
            # since the physical baseline REQUIRES lineage
            smokedduck_dir = _get_smokedduck_dir()
            raise RuntimeError(
                "Failed to enable lineage in SmokedDuck. "
                "Lineage support is REQUIRED for the physical baseline. "
                f"Please rebuild SmokedDuck with lineage support: "
                f"cd {smokedduck_dir} && BUILD_LINEAGE=true make -j 4 && BUILD_LINEAGE=true python3 -m pip install ./tools/pythonpkg"
            ) from e


def _run_without_lineage(conn: duckdb.DuckDBPyConnection, action):
    """Run a callable with lineage capture disabled to avoid polluting lineage tables."""
    with contextlib.suppress(Exception):
        conn.execute("PRAGMA disable_lineage")
    try:
        return action()
    finally:
        with contextlib.suppress(Exception):
            enable_lineage(conn)


def get_provenance_tables(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Get list of provenance tables created by SmokedDuck.

    Args:
        conn: DuckDB connection

    Returns:
        List of provenance table names
    """
    # Query system tables to find provenance tables
    # SmokedDuck typically creates tables with specific naming patterns
    try:
        result = conn.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name LIKE '%lineage%' OR table_name LIKE '%provenance%'
        """).fetchall()
        return [row[0] for row in result]
    except Exception:
        # Fallback: return common provenance table names
        return ["lineage", "provenance"]


def extract_provenance_data(conn: duckdb.DuckDBPyConnection, _query_id: Optional[str] = None) -> Any:
    """Extract provenance data for the last executed query.

    Args:
        conn: DuckDB connection
        query_id: Optional query identifier

    Returns:
        Provenance data (format depends on SmokedDuck implementation)
    """
    # This is a placeholder - actual implementation depends on SmokedDuck API
    # SmokedDuck may provide specific functions to retrieve provenance
    provenance_tables = get_provenance_tables(conn)

    if not provenance_tables:
        return None

    # Query the most recent provenance entry
    # This is a simplified approach - adjust based on actual SmokedDuck schema
    try:
        return conn.execute(f"""
            SELECT * FROM {provenance_tables[0]}
            ORDER BY rowid DESC LIMIT 1
        """).fetchall()
    except Exception:
        return None


def get_latest_query_plan(
    conn: duckdb.DuckDBPyConnection,
    query: str,
) -> dict:
    """Fetch the latest plan JSON for a given query string."""
    def _fetch():
        return conn.execute(
            """
            SELECT plan
            FROM duckdb_queries_list()
            WHERE query = ?
            ORDER BY query_id DESC
            LIMIT 1
            """,
            [query],
        ).fetchone()

    result = _run_without_lineage(conn, _fetch)
    if not result:
        raise RuntimeError(f"No lineage plan found for query: {query}")
    (plan_text,) = result
    return json.loads(plan_text)


def _list_lineage_tables(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """List persisted lineage tables without creating new lineage entries."""
    def _fetch():
        rows = conn.execute(
            """
            SELECT table_name
            FROM duckdb_tables()
            WHERE table_name LIKE 'LINEAGE_%'
            """
        ).fetchall()
        return [row[0] for row in rows]

    return _run_without_lineage(conn, _fetch)

def _extract_lineage_query_id(table_names: list[str]) -> int:
    """Extract the lineage query id from a list of lineage table names."""
    if not table_names:
        raise RuntimeError("No lineage tables found. Did lineage capture run with persist enabled?")

    latest_id = None
    for table_name in table_names:
        parts = table_name.split("_", 2)
        if len(parts) < 2 or parts[0] != "LINEAGE":
            continue
        try:
            query_id = int(parts[1])
        except ValueError:
            continue
        latest_id = query_id if latest_id is None else max(latest_id, query_id)

    if latest_id is None:
        raise RuntimeError("Failed to parse lineage query id from lineage tables.")
    return latest_id


def get_latest_lineage_query_id(conn: duckdb.DuckDBPyConnection) -> int:
    """Fetch the latest lineage query id from persisted lineage tables."""
    return _extract_lineage_query_id(_list_lineage_tables(conn))


def build_lineage_query(
    conn: duckdb.DuckDBPyConnection,
    query: str,
    lineage_query_id: int | None = None,
    prune_empty: bool = True,
) -> str:
    """Build a lineage query using SmokedDuck's operator lineage tables."""
    _setup_smokedduck_scripts_path()
    try:
        import lineage_query  # type: ignore
        from operators import OperatorFactory  # type: ignore
        import provenance_models  # type: ignore
    except ImportError as exc:
        smokedduck_dir = _get_smokedduck_dir()
        raise ImportError(
            "SmokedDuck lineage query scripts are not available. "
            f"Expected in {smokedduck_dir}/benchmark/smokedduck-scripts/smokedduck"
        ) from exc

    plan = get_latest_query_plan(conn, query)
    query_id = lineage_query_id if lineage_query_id is not None else get_latest_lineage_query_id(conn)

    if not _plan_has_tables(plan):
        seq_scan_tables = _get_seq_scan_tables_from_profile(conn, query)
        _assign_seq_scan_tables(plan, seq_scan_tables)
    operator_factory = OperatorFactory()
    prov_model = provenance_models.get_prov_model("lineage")

    with contextlib.redirect_stdout(io.StringIO()):
        lineage_sql = lineage_query.get_query(
            query_id,
            plan,
            operator_factory,
            prov_model,
            None,
            None,
            None,
        )
    lineage_sql = _cast_lineage_join_predicates(lineage_sql)
    if prune_empty:
        return _prune_empty_lineage_joins(conn, lineage_sql)
    return lineage_sql


def _cast_lineage_join_predicates(sql: str) -> str:
    """Cast lineage join predicates to VARCHAR to avoid SmokedDuck equality quirks."""
    def _cast_condition(condition: str) -> str:
        def _replace_col(match: re.Match[str]) -> str:
            col = match.group(1)
            prefix = condition[: match.start()].rstrip()
            if prefix.endswith("CAST("):
                return col
            return f"CAST({col} AS VARCHAR)"

        return re.sub(r"(LINEAGE_[A-Za-z0-9_]+\.[A-Za-z0-9_]+)", _replace_col, condition)

    def _replace_on(match: re.Match[str]) -> str:
        condition = match.group(1)
        return "ON " + _cast_condition(condition)

    return re.sub(
        r"ON\s+(.+?)(?=\s+(?:LEFT|RIGHT|FULL)?\s*JOIN\s+|$)",
        _replace_on,
        sql,
        flags=re.DOTALL,
    )


def _lineage_table_count(conn: duckdb.DuckDBPyConnection, table_name: str) -> int:
    def _fetch():
        return conn.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]

    return int(_run_without_lineage(conn, _fetch))


def _prune_empty_lineage_joins(conn: duckdb.DuckDBPyConnection, sql: str) -> str:
    """Remove joins to empty lineage tables by collapsing pass-through operators."""
    if " FROM " not in sql:
        return sql

    select_part, from_part = sql.split(" FROM ", 1)
    tokens = from_part.split(" JOIN ")
    if not tokens:
        return sql

    base_table = tokens[0].strip()
    join_entries: list[list[str]] = []
    for token in tokens[1:]:
        if " ON " not in token:
            return sql
        table, cond = token.split(" ON ", 1)
        join_entries.append([table.strip(), cond.strip()])

    tables = [base_table] + [entry[0] for entry in join_entries]
    empty_tables = {table for table in tables if _lineage_table_count(conn, table) == 0}
    if not empty_tables:
        return sql

    i = 0
    while i < len(join_entries):
        table, cond = join_entries[i]
        if table in empty_tables and i + 1 < len(join_entries):
            _, next_cond = join_entries[i + 1]
            parts = re.split(r"\s*=\s*", cond, maxsplit=1)
            if len(parts) == 2:
                left, right = parts
                replacement_expr = right if f"{table}." in left else left
                next_cond = re.sub(
                    rf"{re.escape(table)}\.[A-Za-z0-9_]+",
                    replacement_expr,
                    next_cond,
                )
                join_entries[i + 1][1] = next_cond.strip()
                join_entries.pop(i)
                continue
        i += 1

    rebuilt_from = base_table
    for table, cond in join_entries:
        rebuilt_from += f" JOIN {table} ON {cond}"
    return f"{select_part} FROM {rebuilt_from}"


def _plan_has_tables(plan_node: dict) -> bool:
    if plan_node.get("table"):
        return True
    return any(_plan_has_tables(child) for child in plan_node.get("children", []))


def _get_seq_scan_tables_from_profile(conn: duckdb.DuckDBPyConnection, query: str) -> list[str]:
    """Run profiling to extract SEQ_SCAN table names from DuckDB JSON plan."""
    seq_scan_tables: list[str] = []
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp_file:
        profile_path = tmp_file.name

    try:
        with contextlib.suppress(Exception):
            conn.execute("PRAGMA disable_lineage")

        conn.execute("PRAGMA enable_profiling='json'")
        conn.execute(f"PRAGMA profiling_output='{profile_path}'")
        conn.execute(query)
        conn.execute("PRAGMA disable_profiling")
        with open(profile_path) as handle:
            profile = json.load(handle)
        seq_scan_tables = _extract_seq_scan_tables_from_profile(profile)
    finally:
        with contextlib.suppress(Exception):
            conn.execute("PRAGMA disable_profiling")
        with contextlib.suppress(Exception):
            enable_lineage(conn)
        with contextlib.suppress(Exception):
            Path(profile_path).unlink(missing_ok=True)
    return seq_scan_tables


def _extract_seq_scan_tables_from_profile(plan_node: dict) -> list[str]:
    tables: list[str] = []
    name = str(plan_node.get("name", "")).strip()
    if name == "SEQ_SCAN":
        extra = plan_node.get("extra_info") or plan_node.get("extra-info") or ""
        if extra:
            first_line = extra.splitlines()[0].strip()
            if first_line:
                tables.append(first_line)
    for child in plan_node.get("children", []):
        tables.extend(_extract_seq_scan_tables_from_profile(child))
    return tables


def _assign_seq_scan_tables(plan_node: dict, table_names: list[str]) -> None:
    remaining = list(table_names)

    def _walk(node: dict) -> None:
        nonlocal remaining
        name = str(node.get("name", "")).strip()
        if name == "SEQ_SCAN":
            node["table"] = remaining.pop(0) if remaining else ""
        for child in node.get("children", []):
            _walk(child)

    _walk(plan_node)
