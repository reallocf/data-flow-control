#!/bin/bash
# Setup script to configure lineage extension for DuckDB
# This should be sourced before running experiments (after venv is activated)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export DUCKDB_ALLOW_UNSIGNED_EXTENSIONS=1
export LINEAGE_INSECURE_SSL=1
export LINEAGE_DUCKDB_VERSION=v1.3.0

PYTHON_BIN=""
if [ -x "$SCRIPT_DIR/.venv/bin/python" ]; then
    PYTHON_BIN="$SCRIPT_DIR/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python)"
fi

if [ -n "$PYTHON_BIN" ]; then
    SCRIPT_DIR="$SCRIPT_DIR" "$PYTHON_BIN" - <<'PY'
import os
import importlib.util
from pathlib import Path

script_dir = Path(os.environ["SCRIPT_DIR"])
helper_path = script_dir / "src" / "vldb_experiments" / "baselines" / "smokedduck_helper.py"
spec = importlib.util.spec_from_file_location("smokedduck_helper_bootstrap", helper_path)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)

module.ensure_lineage_extension()
print("Lineage extension configured")
PY
else
    echo "Warning: python not found, cannot download lineage extension"
fi
