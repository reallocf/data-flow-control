# VLDB 2026 Big Paper Experiments

Experiments for evaluating the performance impact of SQL rewriting with Data Flow Control (DFC) policies. This project focuses on microbenchmarks testing core relational operators.

## Overview

This project measures the performance overhead of applying DFC policies to SQL queries. It uses the `experiment_harness` framework to run controlled experiments comparing query execution time with and without policies applied.

## Project Structure

```
vldb_2026_big_paper_experiments/
├── pyproject.toml              # Project configuration
├── README.md                   # This file
├── src/
│   └── vldb_experiments/
│       ├── __init__.py
│       ├── data_setup.py       # Fixed test data creation
│       ├── policy_setup.py     # Policy configuration
│       ├── query_definitions.py # Query definitions for each operator
│       └── strategies/
│           └── microbenchmark_strategy.py # Main experiment strategy
└── scripts/
    └── run_microbenchmarks.py  # Script to run experiments
```

## Installation

This project can be installed with `uv` using local editable sources for `sql-rewriter`, `experiment-harness`, and `shared-sql-utils`. If you need the physical baseline (SmokedDuck lineage), you must ensure the SmokedDuck DuckDB build is installed in the environment; otherwise, use `--disable-physical`.

### Setup Steps

**Quick setup (SmokedDuck physical baseline):**
```bash
cd vldb_2026_big_paper_experiments
./setup_venv.sh
```

This script will:
1. Create a virtual environment (if it doesn't exist)
2. Install all dependencies (sql-rewriter, experiment-harness, pandas, pytest)
3. Build SmokedDuck with lineage support (if not already built)
4. Install SmokedDuck Python bindings into the virtual environment

**Manual setup (SmokedDuck physical baseline):**
```bash
cd vldb_2026_big_paper_experiments
python3 -m venv .venv
source .venv/bin/activate  # On macOS/Linux
# or: .venv\Scripts\activate  # On Windows

pip install --upgrade pip
pip install -e ../sql_rewriter
pip install -e ../experiment_harness
pip install -e ../shared_sql_utils
pip install pandas>=2.0.0
pip install pytest>=8.0.0  # For development

# Build and install SmokedDuck
source setup_local_smokedduck.sh
```

**uv setup (logical/DFC only):**
```bash
cd vldb_2026_big_paper_experiments
uv sync
```

Note: `uv sync` will install standard DuckDB from PyPI. This is fine for logical/DFC baselines but not for SmokedDuck physical baseline.

## Experiment Design

### Test Data

The experiments use a fixed dataset with 1,000,000 rows in a `test_data` table:
- `id` (INTEGER): Primary key, 1 to 1,000,000
- `value` (INTEGER): Numeric value, 1 to 1,000,000
- `category` (VARCHAR): Categorical data ('A', 'B', 'C', 'D', 'E'), cycling through the categories
- `amount` (DOUBLE): Numeric data (value * 10.0)

### Policy

A single source-only DFC policy is used:
```
SOURCE test_data CONSTRAINT max(test_data.value) > 100 ON FAIL REMOVE
```

This policy filters rows where `value <= 100` when applied to queries.

### Query Types

The experiments test the following core relational operators:

1. **SELECT**: Simple table scan
   ```sql
   SELECT * FROM test_data
   ```

2. **WHERE**: Filtered scan
   ```sql
   SELECT * FROM test_data WHERE value > 50
   ```

3. **JOIN**: Join operation
   ```sql
   SELECT t1.id, t2.value 
   FROM test_data t1 
   JOIN test_data t2 ON t1.id = t2.id
   ```

4. **GROUP BY**: Aggregation
   ```sql
   SELECT category, COUNT(*), SUM(amount) 
   FROM test_data 
   GROUP BY category
   ```

5. **ORDER BY**: Sorted scan
   ```sql
   SELECT * FROM test_data ORDER BY value DESC
   ```

## Building and Using SmokedDuck

The physical baseline uses SmokedDuck (a DuckDB fork with lineage support). Since SmokedDuck cannot be installed via pip, we build it from source and install it directly into the virtual environment.

### Building SmokedDuck

The `setup_venv.sh` script handles cloning, building, and installing SmokedDuck automatically during initial setup. It will:

1. Check if SmokedDuck repository exists at `../smokedduck` (relative to data-flow-control repo root)
2. If not, clone it from GitHub (branch `smokedduck-2025-d`):
   ```bash
   git clone --branch smokedduck-2025-d https://github.com/cudbg/sd.git ../smokedduck
   ```
3. Check if SmokedDuck is already built
4. If not, build it with lineage support:
   ```bash
   cd ../smokedduck
   BUILD_LINEAGE=true make -j 4
   BUILD_LINEAGE=true python -m pip install ./tools/pythonpkg
   ```
5. Install SmokedDuck Python bindings into the virtual environment

**Why not use `uv` for SmokedDuck?** `uv` installs standard DuckDB from PyPI, which conflicts with SmokedDuck. Use the venv + SmokedDuck build for physical baseline experiments, or run with `--disable-physical`.

### Using the Local Build

After initial setup with `setup_venv.sh`, you only need to configure environment variables before running experiments. The `setup_local_smokedduck.sh` script sets:
- Library paths for the native DuckDB library (`DYLD_LIBRARY_PATH` or `LD_LIBRARY_PATH`)
- Python path for development builds (if needed)
- `DUCKDB_LIBRARY` environment variable

The code automatically detects and uses the SmokedDuck build. The `use_local_smokedduck.py` module handles:
- Adding the SmokedDuck Python bindings to `sys.path`
- Setting library paths for the native DuckDB library
- Verifying lineage support is available

Always source the environment setup script before running experiments:
```bash
source setup_local_smokedduck.sh
```

**Note**: `setup_local_smokedduck.sh` only sets environment variables. For full setup (venv creation, dependency installation, and SmokedDuck building), use `setup_venv.sh`.

## Running Experiments

Make sure your virtual environment is activated, then run the microbenchmark experiments:

```bash
source .venv/bin/activate  # If not already activated
source setup_local_smokedduck.sh  # Ensure SmokedDuck environment is configured
python scripts/run_microbenchmarks.py
```

Or use the wrapper script that handles both:

```bash
./scripts/run_microbenchmarks_with_smokedduck.sh
```

If you're not using SmokedDuck (uv install), disable the physical baseline:
```bash
python scripts/run_microbenchmarks.py --disable-physical
```

## Linting and Tests

Run from the `vldb_2026_big_paper_experiments` directory using the local venv.

```bash
.venv/bin/python -m ruff check src/ tests/
source setup_local_smokedduck.sh
.venv/bin/python -m pytest
```

## Results

Results are exported to CSV in the `results/` directory (default: `microbenchmark_results_policy{policy_count}.csv`). Each row contains:
- `execution_number`: Execution number
- `timestamp`: When the execution occurred
- `duration_ms`: Total execution time (sum of approaches)
- `query_type`: Which operator was tested (SELECT, WHERE, JOIN, etc.)
- `no_policy_time_ms`: Execution time without policy
- `dfc_time_ms`: DFC total time (rewrite + exec)
- `logical_time_ms`: Logical total time (rewrite + exec)
- `dfc_rewrite_time_ms`, `dfc_exec_time_ms`: DFC rewrite vs execution split
- `logical_rewrite_time_ms`, `logical_exec_time_ms`: Logical rewrite vs execution split

The CSV also includes summary statistics (mean, median, stddev, min, max) for all numeric metrics.

## Experiment Configuration

The default configuration runs:
- 20 executions per query type (100 total executions)
- 2 warm-up runs (discarded)
- Results saved to `./results/microbenchmark_results_policy{policy_count}.csv`

You can modify `scripts/run_microbenchmarks.py` or pass flags:
```bash
python scripts/run_microbenchmarks.py --policy-count 1000 --num-runs-per-variation 5 --warmup-runs 2
```

## Analysis

After running experiments, analyze the results:

1. **Overhead by operator**: Compare `overhead_pct` across different query types
2. **Absolute performance**: Compare `baseline_time_ms` vs `rewritten_time_ms`
3. **Correctness**: Verify `rows_returned_rewritten` matches expected filtered results
4. **Variance**: Check standard deviation to assess consistency

## Dependencies

- `sql-rewriter`: Local editable dependency for SQL rewriting functionality
- `experiment-harness`: Local editable dependency for experiment framework
- `pandas>=2.0.0`: Data manipulation library
- `SmokedDuck`: Custom DuckDB build with lineage support (built from source, not from PyPI)
- `pytest>=8.0.0`: Testing framework (dev dependency)

**Note**: We do not install standard DuckDB from PyPI. SmokedDuck replaces it entirely.

## License

See the main project LICENSE file.
