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
│           ├── microbenchmark_strategy.py # Main experiment strategy
│           └── multi_source_strategy.py  # Multi-source join-chain strategy
└── scripts/
    ├── run_microbenchmarks.py  # Script to run experiments
    ├── run_multi_source_experiment.py # Multi-source experiment runner
    └── generate_multi_source_visualizations.py # Multi-source chart generator
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
SOURCES test_data CONSTRAINT max(test_data.value) > 100 ON FAIL REMOVE
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

## Supported Experiments and Strategies

All experiment strategies live in `src/vldb_experiments/strategies/` and are used by the scripts below.

### DFC on ProvSQL

The ProvSQL baseline deliberately does not invoke Passant's rewrite. It produces one SQL statement
whose materialized CTE evaluates the original query with an explicit row token. A correlated
lateral subquery traverses that token, and for scalar aggregates also traverses each output
cell's `agg_token`; it resolves reachable `lineitem` rows, calculates the policy aggregate, and
filters the CTE row. The aggregate-value channel is required for scalar aggregates, whose
row-level circuit is the constant `one` because their output row exists even over empty input.

Database setup installs two small adapters around existing ProvSQL primitives: a polymorphic
aggregate-token UUID extractor and a circuit-descendant function that runs with provenance
rewriting disabled. Policy evaluation itself is entirely inside each submitted SQL statement;
there is no client-side circuit walk or policy calculation.

The comparison strips ProvSQL's automatic annotation and recovers the current scalar value of
each `agg_token`. Run the 13-query SF 0.01 comparison with:

```bash
.venv/bin/python scripts/run_tpch_provsql_correctness.py --sf 0.01
```

This starts `inriavalda/provsql:latest` on local port 5435, loads TPC-H, and writes per-query SQL,
timings, row counts, and correctness results to
`results/tpch_provsql_monolithic_correctness_sf001.csv`.

### Microbenchmarks (core operators)

- **Strategy:** `MicrobenchmarkStrategy`
- **Focus:** DFC overhead on SELECT/WHERE/JOIN/GROUP BY/ORDER BY
- **Approaches:** no_policy, DFC, logical (CTE), physical (SmokedDuck lineage)
- **Script:** `scripts/run_microbenchmarks.py`

### Multi-Source Join-Chain

- **Strategy:** `MultiSourceStrategy`
- **Focus:** DFC overhead as source-count and join-count scale (join chain with grouped SUM)
- **Approaches:** no_policy, DFC
- **Script:** `scripts/run_multi_source_experiment.py`
- **Visualization:** `scripts/generate_multi_source_visualizations.py`

### Regulation Z Ability-to-Repay Case Study

- **Strategy:** `RegulationZBenchmarkStrategy`
- **Focus:** Relational mortgage-underwriting updates grounded in verified income, complete
  monthly obligations, loan terms, and a product-specific debt-to-income standard
- **Approaches:** DFC, Astra 6, Sol 6.1, Opus 5.5, Fable 5.1, and No Policy
- **LLM provider:** AWS Bedrock Converse for all four model settings
- **Cost attribution:** Project-tagged Bedrock application inference profiles
- **Script:** `scripts/run_regulation_z_benchmark.py`
- **Visualization:** `scripts/generate_regulation_z_benchmark_chart.py`

The default workload contains 1,000 independent, evenly balanced candidate updates. Its invalid
approvals exercise every enforceable policy predicate: claimed or explicitly unverified income,
unverified obligations, omitted debt, an understated payment, evidence-table identity mismatches,
an application-row mismatch, a product-standard mismatch, a missing income record, nonpositive
income, an inconsistent DTI calculation, and a DTI threshold violation. Source mismatches appear
both together and independently for income, obligations, and loan terms. The benchmark is a
narrow, synthetic enforcement case study inspired by 12 CFR 1026.43; it is not a complete
implementation of Regulation Z.

Every Bedrock request starts with a fresh UUID nonce and contains no Bedrock cache checkpoint.
The request omits temperature and other sampling controls so each model uses its provider
defaults. Returned cache-read and cache-write token counts are recorded in the result CSV.

```bash
source setup_local_smokedduck.sh
# Four Bedrock requests: one input evaluated by each model.
.venv/bin/python scripts/run_regulation_z_benchmark.py --num-inputs 1

# Full paper workload: 4,000 Bedrock requests.
.venv/bin/python scripts/run_regulation_z_benchmark.py
.venv/bin/python scripts/generate_regulation_z_benchmark_chart.py
```

`--num-inputs N` (an alias for `--num-updates N`) accepts any positive number. The
runner prints the planned number of Bedrock requests before starting, including measured
runs and warmups, so inexpensive smoke tests can validate credentials, model access, response
parsing, database setup, and result export before a full run.

The runner also writes `<output-stem>.progress.csv` incrementally. Each completed approach/input
is appended and flushed immediately with its correctness, timing, token counts, request ID, and
error status. This file can be tailed during a long run and retains completed work if a later
request fails. Use `--progress-filename` to choose a different filename. The original output CSV
remains the final, wide summary consumed by the visualization script.

Bedrock requests run concurrently within each model, with at most 50 requests in flight. Change
the limit with `--max-concurrency N`, where `N` is between 1 and 50. Incremental rows are written
in completion order; `update_number` and `application_id` identify the original workload item.
Reported per-update model time remains the average request latency, while each model also records
`wall_time_ms` for the parallel batch.

The default Bedrock model IDs may be replaced with the corresponding command-line options,
for example `--astra-model-id <inference-profile-id-or-arn>`.

### Regulation Z v2 Credit-Card Statement Case Study

- **Strategy:** `RegulationZV2BenchmarkStrategy`
- **Focus:** Reconcile a draft periodic statement against its transaction ledger, daily-balance
  interest calculation, and approved purchase APR before publication
- **Approaches:** DFC, Astra 6, Sol 6.1, Luna 6, Opus 5.5, Fable 5.1, and No Policy
- **LLM provider:** AWS Bedrock Converse for all five model settings
- **Cost attribution:** Project-tagged Bedrock application inference profiles
- **Script:** `scripts/run_regulation_z_v2_benchmark.py`
- **Data preparation:** `scripts/prepare_regulation_z_v2_data.py`
- **Visualization:** `scripts/generate_regulation_z_v2_benchmark_chart.py`

The default catalog is a pinned, pseudonymized extract of simple product-wide purchase APRs from
the CFPB Terms of Credit Card Plans survey. Public APRs seed `apr_config`; accounts, transactions,
daily balances, statements, and publication attempts remain synthetic. The public staging table
does not participate in the DFC policy because a survey row is not statement-level evidence.

The default workload has 1,000 balanced statement-publication attempts. Invalid cases independently
exercise incorrect purchase, cash-advance, payment, fee, and interest totals; incorrect new
balances; incorrect disclosed or applied APRs; ledger activity omitted from the statement; and a
missing daily-balance date hidden by a duplicate date.

```bash
source setup_local_smokedduck.sh

# Prepare a fresh pseudonymized catalog, without running the benchmark.
.venv/bin/python scripts/prepare_regulation_z_v2_data.py

# Future smallest Bedrock run: five paid requests.
.venv/bin/python scripts/run_regulation_z_v2_benchmark.py --num-statements 1

# Future full run: 5,000 Bedrock requests.
.venv/bin/python scripts/run_regulation_z_v2_benchmark.py
.venv/bin/python scripts/generate_regulation_z_v2_benchmark_chart.py
```

The v2 runner retains the v1 safety and observability features: provider-default sampling, a
fresh nonce at the beginning of every model prompt, incremental flushed progress rows, selectable
models, configurable input count, and one to 50 simultaneous Bedrock requests. See
`REGULATION_Z_V2.md` for the integration boundary, policy, and scope exclusions.

### LLM Validation Benchmark

This benchmark now uses the local Rust-backed DFC implementation for policy rewriting and ground
truth. Its default Bedrock model set is Astra 6, Sol 6.1, Luna 6, Fable 5.1, and Opus 5.5.
OpenAI-family models use Bedrock's provider-native explicit cache controls, which make the
checkpoint uncacheable; prompts no longer contain nonce text. Anthropic-family requests continue
to use provider defaults without an explicit cache checkpoint.

Before a full LLM validation run, use the smoke-test preset. It runs one query, one policy
count, one query-results prompt, one repetition, and no warmups: one paid request per selected
model. The runner prints the exact number of paid LLM requests before starting and writes smoke
results to a separate file.

```bash
.venv/bin/python scripts/run_llm_validation_experiment.py --smoke-test
```

For a single paid request, select one model. The individual dimensions can also be configured
without the preset using `--queries`, `--policy-counts`, `--runs-per-setting`, `--models`, and
`--prompt-modes`.

```bash
.venv/bin/python scripts/run_llm_validation_experiment.py \
  --smoke-test --models astra
```

Use the `fixed-4-violations` policy variant to hold the ground-truth violation set at Q03, Q04,
Q10, and Q18 for every policy count. This writes to a separate default CSV so it cannot resume
from or overwrite the growing-violation experiment.

```bash
.venv/bin/python scripts/run_llm_validation_experiment.py \
  --policy-variant fixed-4-violations
```

### TPC-H Baseline (selected queries)

- **Strategy:** `TPCHStrategy`
- **Focus:** DFC overhead on supported TPC-H queries (Q01, Q03, Q04, Q05, Q06, Q07, Q08, Q09, Q10, Q12, Q14, Q18, Q19)
- **Approaches:** no_policy, DFC, logical (CTE)
- **Script:** `scripts/run_tpch_experiments.py`

### TPC-H Policy Count (single query)

- **Strategy:** `TPCHPolicyCountStrategy`
- **Focus:** DFC vs logical overhead as policy count scales for a single TPC-H query (default Q01)
- **Approaches:** DFC, logical
- **Script:** `scripts/run_tpch_policy_count_experiment.py`

### TPC-H Policy Count (all supported queries)

- **Strategy:** `TPCHPolicyCountAllQueriesStrategy`
- **Focus:** Fixed policy count applied across all supported TPC-H queries
- **Approaches:** no_policy, DFC, logical
- **Script:** `scripts/run_tpch_policy_count_all_queries.py`

### TPC-H Multi-Database Comparison

- **Strategy:** `TPCHMultiDBStrategy`
- **Focus:** DuckDB (no_policy/DFC/logical) vs external engines (no_policy only)
- **External engines:** Umbra, Postgres, DataFusion, SQL Server (AWS) (configured via `--engine`)
- **Script:** `scripts/run_tpch_multi_db.py`

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

### Multi-Source Join-Chain Experiment

The multi-source experiment varies join counts and source counts (2, 4, 8, 16, 32) and compares No Policy vs DFC. Outputs are suffixed so runs don't overwrite prior results.

Run the experiment:
```bash
source setup_local_smokedduck.sh
python scripts/run_multi_source_experiment.py --suffix <label>
```

Generate charts:
```bash
source setup_local_smokedduck.sh
python scripts/generate_multi_source_visualizations.py --suffix <label>
```

Outputs are written to `results/` as:
- `multi_source_results_<label>.csv`
- `multi_source_exec_time_<label>.png`
- `multi_source_heatmap_<label>.png`

### TPC-H Baseline Experiment

Run the supported TPC-H query suite at one or more scale factors:
```bash
python scripts/run_tpch_experiments.py --sf 1 10
```

### TPC-H Policy Count Experiments

Single-query policy count scaling (defaults to Q01):
```bash
python scripts/run_tpch_policy_count_experiment.py --sf 1 --query 1 --policy-counts 1 10 100 1000
```

Fixed policy count across all supported queries:
```bash
python scripts/run_tpch_policy_count_all_queries.py --sf 1 10 --policy-count 1000
```

### TPC-H Multi-Database Comparison

Compare DuckDB baselines vs external engines (Umbra/Postgres/DataFusion/SQL Server):
```bash
python scripts/run_tpch_multi_db.py --sf 1 --suffix _umbra --engine umbra
```

SQL Server requires AWS setup; see `aws_setup.md`.

**Notes:** SQLite takes ~hours to run TPC-H at sf=0.1, so it was removed from the multi-database comparison. ClickHouse was removed because we could not get it working within our local machine's RAM limits.

### Utility Scripts

- **Diff rewritten SQL:** `scripts/diff_policy_count_sql.py <policy_count>`
- **SmokedDuck wrapper:** `scripts/run_microbenchmarks_with_smokedduck.sh`

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
