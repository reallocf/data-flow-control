#!/bin/bash
# Setup script to create and configure the virtual environment for experiments
# This replaces uv because we need to install SmokedDuck directly from source

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

# Install other dependencies
echo "Installing other dependencies..."
pip install pandas>=2.0.0
pip install pytest>=8.0.0

# Build and install SmokedDuck
echo ""
echo "Building and installing SmokedDuck..."

# Path to locally built SmokedDuck (relative to data-flow-control repo root)
# From vldb_2026_big_paper_experiments, go up to data-flow-control root, then ../smokedduck
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SMOKEDDUCK_DIR="$(cd "$REPO_ROOT/.." && pwd)/smokedduck"

# Check if SmokedDuck directory exists, clone if not
if [ ! -d "$SMOKEDDUCK_DIR" ]; then
    echo "SmokedDuck directory not found at $SMOKEDDUCK_DIR"
    echo "Cloning SmokedDuck from GitHub (smokedduck-2025-d branch)..."
    
    # Clone the repository
    git clone --branch smokedduck-2025-d --depth 1 https://github.com/cudbg/sd.git "$SMOKEDDUCK_DIR"
    
    if [ $? -ne 0 ]; then
        echo "Error: Failed to clone SmokedDuck repository"
        exit 1
    fi
    
    echo "SmokedDuck repository cloned successfully"
fi

# Function to check if SmokedDuck is built and has Python bindings
check_smokedduck_built() {
    # Check if lineage works - this is the definitive test
    if python -c "import duckdb; conn = duckdb.connect(':memory:'); conn.execute('PRAGMA enable_lineage'); conn.close()" 2>/dev/null; then
        return 0
    fi
    
    # Fallback: check for build artifacts
    local python_bindings_found=false
    if [ -d "$SMOKEDDUCK_DIR/build/python" ] && [ -f "$SMOKEDDUCK_DIR/build/python/duckdb/__init__.py" ]; then
        python_bindings_found=true
    elif [ -d "$SMOKEDDUCK_DIR/build/release/python" ] && [ -f "$SMOKEDDUCK_DIR/build/release/python/duckdb/__init__.py" ]; then
        python_bindings_found=true
    fi
    
    # Check for library file
    local lib_found=false
    if [ -f "$SMOKEDDUCK_DIR/build/release/libduckdb.dylib" ] || [ -f "$SMOKEDDUCK_DIR/build/release/libduckdb.so" ]; then
        lib_found=true
    fi
    
    [ "$python_bindings_found" = true ] && [ "$lib_found" = true ]
}

# Build SmokedDuck if needed
if ! check_smokedduck_built; then
    echo "SmokedDuck not fully built. Building now..."
    echo "This may take several minutes..."
    
    cd "$SMOKEDDUCK_DIR"
    
    # Build SmokedDuck with lineage support
    # BUILD_LINEAGE=true enables lineage capture functionality
    echo "Building SmokedDuck with lineage support..."
    BUILD_LINEAGE=true make -j 4
    
    if [ $? -ne 0 ]; then
        echo "Error: SmokedDuck build failed"
        exit 1
    fi
    
    # Install Python bindings into the virtual environment
    echo "Installing Python bindings into virtual environment..."
    BUILD_LINEAGE=true python -m pip install ./tools/pythonpkg
    
    if [ $? -ne 0 ]; then
        echo "Error: Python bindings installation failed"
        exit 1
    fi
    
    echo "SmokedDuck build and installation completed"
else
    echo "SmokedDuck already built, skipping build step"
    # But ensure it's installed in the venv
    if ! python -c "import duckdb; conn = duckdb.connect(':memory:'); conn.execute('PRAGMA enable_lineage'); conn.close()" 2>/dev/null; then
        echo "SmokedDuck is built but not installed in venv. Installing..."
        cd "$SMOKEDDUCK_DIR"
        BUILD_LINEAGE=true python -m pip install ./tools/pythonpkg
    fi
fi

# Verify build after building
if ! check_smokedduck_built; then
    echo "Error: SmokedDuck build verification failed"
    echo "Python bindings or library not found after build"
    exit 1
fi

echo ""
echo "✓ Setup complete!"
echo ""
echo "To activate the virtual environment in the future:"
echo "  source .venv/bin/activate"
echo ""
echo "To run experiments:"
echo "  source setup_local_smokedduck.sh  # Configure SmokedDuck environment variables"
echo "  python scripts/run_microbenchmarks.py"
echo "  # or: ./scripts/run_microbenchmarks_with_smokedduck.sh"
