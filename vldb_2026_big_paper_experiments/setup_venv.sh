#!/bin/bash
# Setup script to create and configure the virtual environment for experiments
# Installs dependencies and downloads the lineage extension

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Setting up virtual environment for vldb_2026_big_paper_experiments..."
echo ""

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
else
    echo "Virtual environment already exists."
fi

# Activate virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install local dependencies
echo "Installing local dependencies..."
pip install -e ../sql_rewriter
pip install -e ../experiment_harness
# Avoid pulling standard DuckDB from PyPI; dependencies are already installed above.
pip install -e . --no-deps

# Install other dependencies
echo "Installing other dependencies..."
pip install pandas>=2.0.0
pip install matplotlib>=3.8.0
pip install scikit-learn>=1.3.0
pip install pytest>=8.0.0

# Align DuckDB with the lineage extension build
pip install --force-reinstall duckdb==1.3.0

# Download lineage extension
export DUCKDB_ALLOW_UNSIGNED_EXTENSIONS=1
export LINEAGE_INSECURE_SSL=1
export LINEAGE_DUCKDB_VERSION=v1.3.0
python - <<'PY'
from vldb_experiments.baselines import smokedduck_helper

smokedduck_helper.ensure_lineage_extension()
print("Lineage extension downloaded")
PY

echo ""
echo "✓ Setup complete!"
echo ""
echo "To activate the virtual environment in the future:"
echo "  source .venv/bin/activate"
echo ""
echo "To run experiments:"
echo "  source setup_local_smokedduck.sh  # Configure lineage extension"
echo "  python scripts/run_microbenchmarks.py"
echo "  # or: ./scripts/run_microbenchmarks_with_smokedduck.sh"
