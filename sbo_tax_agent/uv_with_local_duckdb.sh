#!/bin/bash
# Wrapper script for uv that automatically sets up the local DuckDB environment

# Source the setup script to configure environment
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/setup_local_duckdb.sh"

# Run uv with all passed arguments
exec uv "$@"

