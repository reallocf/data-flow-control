#!/bin/bash
# Script to set up environment for using locally built DuckDB with sql_rewriter

# Get the absolute path to the extended_duckdb build directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DUCKDB_LIB_PATH="$PROJECT_ROOT/extended_duckdb/build/release/src"

# Check if the library exists
if [ ! -f "$DUCKDB_LIB_PATH/libduckdb.dylib" ] && [ ! -f "$DUCKDB_LIB_PATH/libduckdb.so" ]; then
    echo "Warning: DuckDB library not found at $DUCKDB_LIB_PATH"
    echo "Please run 'make' in the extended_duckdb directory first."
    exit 1
fi

# Set environment variable for macOS
if [[ "$OSTYPE" == "darwin"* ]]; then
    export DYLD_LIBRARY_PATH="$DUCKDB_LIB_PATH:$DYLD_LIBRARY_PATH"
    echo "Set DYLD_LIBRARY_PATH to include: $DUCKDB_LIB_PATH"
# Set environment variable for Linux
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    export LD_LIBRARY_PATH="$DUCKDB_LIB_PATH:$LD_LIBRARY_PATH"
    echo "Set LD_LIBRARY_PATH to include: $DUCKDB_LIB_PATH"
fi

# Also try DUCKDB_LIBRARY if the Python package supports it
export DUCKDB_LIBRARY="$DUCKDB_LIB_PATH/libduckdb.dylib"
if [ ! -f "$DUCKDB_LIBRARY" ]; then
    export DUCKDB_LIBRARY="$DUCKDB_LIB_PATH/libduckdb.so"
fi

echo "Environment configured to use local DuckDB build"
echo "To use this in your shell, run: source $0"

