#!/bin/bash
# Script to run microbenchmarks with locally built SmokedDuck
# This script sets up the environment and runs the experiments

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Source the setup script to configure SmokedDuck environment
if [ -f "$PROJECT_ROOT/setup_local_smokedduck.sh" ]; then
    source "$PROJECT_ROOT/setup_local_smokedduck.sh"
else
    echo "Warning: setup_local_smokedduck.sh not found, using standard DuckDB"
fi

# Activate virtual environment if it exists
if [ -d "$PROJECT_ROOT/.venv" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

# Run the microbenchmark script
cd "$PROJECT_ROOT"
python scripts/run_microbenchmarks.py "$@"
