#!/bin/bash
# Setup script to configure lineage extension for DuckDB
# This should be sourced before running experiments (after venv is activated)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export DUCKDB_ALLOW_UNSIGNED_EXTENSIONS=1
export LINEAGE_INSECURE_SSL=1
export LINEAGE_DUCKDB_VERSION=v1.3.0

if command -v python >/dev/null 2>&1; then
    python - <<'PY'
from vldb_experiments.baselines import smokedduck_helper

smokedduck_helper.ensure_lineage_extension()
print("Lineage extension configured")
PY
else
    echo "Warning: python not found, cannot download lineage extension"
fi
