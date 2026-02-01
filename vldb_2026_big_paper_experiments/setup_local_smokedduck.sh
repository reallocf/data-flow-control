#!/bin/bash
# Setup script to configure environment variables for locally built SmokedDuck/DuckDB
# This should be sourced before running experiments (after venv is activated)
# 
# Note: This script only sets environment variables. For full setup including
# building SmokedDuck, use setup_venv.sh

# Path to locally built SmokedDuck (relative to data-flow-control repo root)
# From vldb_2026_big_paper_experiments, go up to data-flow-control root, then ../smokedduck
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SMOKEDDUCK_DIR="$(cd "$REPO_ROOT/.." && pwd)/smokedduck"

# Check if SmokedDuck directory exists
if [ ! -d "$SMOKEDDUCK_DIR" ]; then
    echo "Error: SmokedDuck directory not found at $SMOKEDDUCK_DIR"
    echo "Please run ./setup_venv.sh to clone and build SmokedDuck"
    return 1 2>/dev/null || exit 1
fi

# Python bindings should be installed via pip, so they're in the Python path
# But we can still add build directories if they exist for development
if [ -d "$SMOKEDDUCK_DIR/build/python" ]; then
    export PYTHONPATH="$SMOKEDDUCK_DIR/build/python:$PYTHONPATH"
elif [ -d "$SMOKEDDUCK_DIR/build/release/python" ]; then
    export PYTHONPATH="$SMOKEDDUCK_DIR/build/release/python:$PYTHONPATH"
fi

# Set LD_LIBRARY_PATH (or DYLD_LIBRARY_PATH on macOS) to find the DuckDB library
if [[ "$OSTYPE" == "darwin"* ]]; then
    export DYLD_LIBRARY_PATH="$SMOKEDDUCK_DIR/build/release:$DYLD_LIBRARY_PATH"
else
    export LD_LIBRARY_PATH="$SMOKEDDUCK_DIR/build/release:$LD_LIBRARY_PATH"
fi

# Also set DUCKDB_LIBRARY environment variable if library exists
if [ -f "$SMOKEDDUCK_DIR/build/release/libduckdb.dylib" ]; then
    export DUCKDB_LIBRARY="$SMOKEDDUCK_DIR/build/release/libduckdb.dylib"
elif [ -f "$SMOKEDDUCK_DIR/build/release/libduckdb.so" ]; then
    export DUCKDB_LIBRARY="$SMOKEDDUCK_DIR/build/release/libduckdb.so"
fi

# Verify SmokedDuck is available (optional check, won't fail if not available)
if python -c "import duckdb; conn = duckdb.connect(':memory:'); conn.execute('PRAGMA enable_lineage'); conn.close()" 2>/dev/null; then
    echo "SmokedDuck environment configured (lineage support verified)"
else
    echo "Warning: SmokedDuck lineage support not verified. Run setup_venv.sh to build/install SmokedDuck."
fi
