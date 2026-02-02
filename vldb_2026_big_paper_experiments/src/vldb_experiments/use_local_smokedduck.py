"""
Helper module to configure DuckDB to use the locally built SmokedDuck version.

SmokedDuck is a fork of DuckDB with lineage support built in. When built from source,
it creates DuckDB Python bindings that include lineage functionality.

Usage:
    import use_local_smokedduck
    import duckdb

    # Now duckdb will use the SmokedDuck build (if available)
    conn = duckdb.connect()
    conn.execute("PRAGMA enable_lineage")
"""

import os
from pathlib import Path
import sys

# Path to locally built SmokedDuck (relative to data-flow-control repo root)
# From this file, go up to data-flow-control root, then ../smokedduck
_current_file = Path(__file__).resolve()
# Go up: vldb_experiments -> src -> vldb_2026_big_paper_experiments -> data-flow-control
_repo_root = _current_file.parent.parent.parent.parent
SMOKEDDUCK_DIR = _repo_root.parent / "smokedduck"

def setup_local_smokedduck():
    """Configure environment to use locally built SmokedDuck DuckDB library.

    Returns:
        duckdb module from SmokedDuck build

    Raises:
        FileNotFoundError: If SmokedDuck directory or build does not exist
        ImportError: If SmokedDuck DuckDB module cannot be imported
    """
    if "duckdb" in sys.modules:
        import duckdb
        test_conn = duckdb.connect(":memory:")
        try:
            try:
                test_conn.execute("PRAGMA enable_lineage")
                try:
                    from vldb_experiments.baselines import smokedduck_helper
                    smokedduck_helper.duckdb = duckdb
                except Exception:
                    pass
                return duckdb
            except Exception:
                pass
        finally:
            test_conn.close()

    if not SMOKEDDUCK_DIR.exists():
        raise FileNotFoundError(
            f"SmokedDuck directory not found at {SMOKEDDUCK_DIR}. "
            "SmokedDuck is REQUIRED. Please run ./setup_venv.sh to clone and build it."
        )

    # SmokedDuck builds DuckDB with lineage support
    # Python bindings are installed via pip, so they should be in the Python path
    # But we can also check build directories for development builds
    python_paths = [
        SMOKEDDUCK_DIR / "build" / "python",
        SMOKEDDUCK_DIR / "build" / "release" / "python",
    ]

    # Add Python paths (if they exist, for development builds)
    for path in python_paths:
        if path.exists():
            path_str = str(path)
            if path_str not in sys.path:
                sys.path.insert(0, path_str)

    # Set library path for DuckDB native library
    lib_paths = [
        SMOKEDDUCK_DIR / "build" / "release",
        SMOKEDDUCK_DIR / "build",
    ]

    for lib_path in lib_paths:
        if lib_path.exists():
            lib_path_str = str(lib_path)
            if os.name == "nt":  # Windows
                os.environ["PATH"] = lib_path_str + os.pathsep + os.environ.get("PATH", "")
            elif os.uname().sysname == "Darwin":  # macOS
                os.environ["DYLD_LIBRARY_PATH"] = lib_path_str + os.pathsep + os.environ.get("DYLD_LIBRARY_PATH", "")
            else:  # Linux
                os.environ["LD_LIBRARY_PATH"] = lib_path_str + os.pathsep + os.environ.get("LD_LIBRARY_PATH", "")
            break

    # Try to reload duckdb module to use the local build
    if "duckdb" in sys.modules:
        del sys.modules["duckdb"]

    try:
        import duckdb
        try:
            from vldb_experiments.baselines import smokedduck_helper
            smokedduck_helper.duckdb = duckdb
        except Exception:
            pass
        # Verify this is SmokedDuck by checking if lineage is supported
        # Lineage support is REQUIRED for the physical baseline
        test_conn = duckdb.connect(":memory:")
        try:
            # Try the lineage pragma - may have different names in different SmokedDuck versions
            # Try common variations
            lineage_enabled = False
            for pragma in ["PRAGMA enable_lineage", "PRAGMA lineage=on", "PRAGMA lineage_enabled=true"]:
                try:
                    test_conn.execute(pragma)
                    lineage_enabled = True
                    break
                except Exception:
                    continue

            test_conn.close()
            if not lineage_enabled:
                raise ImportError(
                    "DuckDB module found but lineage support not available. "
                    "This may not be the SmokedDuck build, or lineage support is not enabled. "
                    f"Please rebuild SmokedDuck with lineage support: "
                    f"cd {SMOKEDDUCK_DIR} && BUILD_LINEAGE=true make -j 4 && BUILD_LINEAGE=true python3 -m pip install ./tools/pythonpkg"
                )
            return duckdb
        except ImportError:
            raise
        except Exception as e:
            test_conn.close()
            raise ImportError(
                "DuckDB module found but lineage support not available. "
                "This may not be the SmokedDuck build, or lineage support is not enabled. "
                f"Please rebuild SmokedDuck with lineage support: "
                f"cd {SMOKEDDUCK_DIR} && BUILD_LINEAGE=true make -j 4 && BUILD_LINEAGE=true python3 -m pip install ./tools/pythonpkg"
            ) from e
    except ImportError as e:
        raise ImportError(
            f"Failed to import SmokedDuck DuckDB module: {e}. "
            f"Please run ./setup_venv.sh to clone and build SmokedDuck. "
            f"Expected location: {SMOKEDDUCK_DIR}"
        ) from e

# Do NOT auto-setup when imported - let the caller handle setup
# This allows for explicit error handling
