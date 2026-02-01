"""Helper functions for SmokedDuck lineage capture."""

from pathlib import Path
import sys
from typing import Any, List, Optional

import duckdb


def _get_smokedduck_dir() -> Path:
    """Get the path to locally built SmokedDuck (relative to data-flow-control repo root)."""
    # From this file, go up to data-flow-control root, then ../smokedduck
    _current_file = Path(__file__).resolve()
    # Go up: baselines -> vldb_experiments -> src -> vldb_2026_big_paper_experiments -> data-flow-control
    _repo_root = _current_file.parent.parent.parent.parent
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


def is_smokedduck_available() -> bool:
    """Check if SmokedDuck is available (either via import or local build).
    
    Returns:
        True if SmokedDuck can be imported or found locally
        
    Raises:
        ImportError: If SmokedDuck is not available
    """
    # Try to import smokedduck module
    try:
        import smokedduck
        return True
    except ImportError:
        pass

    # Try setting up local path and importing again
    try:
        _setup_smokedduck_path()
        import smokedduck
        return True
    except ImportError:
        pass

    # Check if we can use DuckDB with lineage support
    # SmokedDuck may be built into DuckDB itself
    try:
        conn = duckdb.connect(":memory:")
        # Try to enable lineage - if it works, SmokedDuck is available
        try:
            conn.execute("PRAGMA enable_lineage")
            conn.close()
            return True
        except:
            conn.close()
    except:
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
        # Commit any transaction started by enabling lineage
        try:
            conn.commit()
        except:
            pass  # commit() may not be available or needed
        return
    except Exception as e:
        # If pragma doesn't work, try alternative methods
        try:
            # Try alternative pragma syntax
            conn.execute("PRAGMA lineage=on")
            try:
                conn.commit()
            except:
                pass
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


def get_provenance_tables(conn: duckdb.DuckDBPyConnection) -> List[str]:
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


def extract_provenance_data(conn: duckdb.DuckDBPyConnection, query_id: Optional[str] = None) -> Any:
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
        result = conn.execute(f"""
            SELECT * FROM {provenance_tables[0]} 
            ORDER BY rowid DESC LIMIT 1
        """).fetchall()
        return result
    except Exception:
        return None
