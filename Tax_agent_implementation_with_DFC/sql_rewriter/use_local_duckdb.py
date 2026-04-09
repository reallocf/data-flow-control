"""
Helper module to configure DuckDB to use the locally built library from extended_duckdb.

Usage:
    import use_local_duckdb
    import duckdb

    # Now duckdb will use the local build
    conn = duckdb.connect()
"""

import contextlib
import os
from pathlib import Path

# Get the path to the extended_duckdb build directory
_SCRIPT_DIR = Path(__file__).parent
_PROJECT_ROOT = _SCRIPT_DIR.parent
_DUCKDB_LIB_PATH = _PROJECT_ROOT / "extended_duckdb" / "build" / "release" / "src"

def setup_local_duckdb():
    """Configure environment to use locally built DuckDB library."""
    lib_path = str(_DUCKDB_LIB_PATH)

    # Check if library exists
    lib_file = None
    if (_DUCKDB_LIB_PATH / "libduckdb.dylib").exists():
        lib_file = str(_DUCKDB_LIB_PATH / "libduckdb.dylib")
    elif (_DUCKDB_LIB_PATH / "libduckdb.so").exists():
        lib_file = str(_DUCKDB_LIB_PATH / "libduckdb.so")
    elif (_DUCKDB_LIB_PATH / "duckdb.dll").exists():
        lib_file = str(_DUCKDB_LIB_PATH / "duckdb.dll")

    if not lib_file:
        raise FileNotFoundError(
            f"DuckDB library not found at {_DUCKDB_LIB_PATH}. "
            "Please run 'make' in the extended_duckdb directory first."
        )

    # Set environment variables
    if os.name == "nt":  # Windows
        os.environ["PATH"] = lib_path + os.pathsep + os.environ.get("PATH", "")
    elif os.uname().sysname == "Darwin":  # macOS
        os.environ["DYLD_LIBRARY_PATH"] = lib_path + os.pathsep + os.environ.get("DYLD_LIBRARY_PATH", "")
    else:  # Linux
        os.environ["LD_LIBRARY_PATH"] = lib_path + os.pathsep + os.environ.get("LD_LIBRARY_PATH", "")

    # Also try DUCKDB_LIBRARY if supported
    os.environ["DUCKDB_LIBRARY"] = lib_file

    return lib_file

# Auto-setup when imported
with contextlib.suppress(FileNotFoundError):
    setup_local_duckdb()
