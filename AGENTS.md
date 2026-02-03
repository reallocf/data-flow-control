# Agent Guide for Data Flow Control Project

This document contains key principles, best practices, and critical gotchas for working on the Data Flow Control project.

## Project Overview

This project implements Data Flow Control (DFC) policies that can filter or abort queries based on constraints over source/sink tables. The core component is a SQL query rewriter that intercepts queries, applies DFC policies, and executes them against DuckDB. The project also includes applications like the SBO Tax Agent that demonstrate DFC usage.

## Key Architecture Principles

### Policy System

- **Validation split**: Syntax validation at policy creation, catalog validation at registration
- **Policy types**: `DFCPolicy` for standard policies, `AggregateDFCPolicy` for policies evaluated after all data is processed
- **Resolution types**: `REMOVE` filters rows, `KILL` aborts the query, `LLM` uses AI to fix violating rows, `INVALIDATE` marks rows as invalid
- **Column qualification**: All constraint columns must be qualified with table names
- **Source aggregation**: Source columns must be aggregated when source is present (for `DFCPolicy`)
- **Aggregate policies**: `AggregateDFCPolicy` uses inner/outer aggregation patterns and currently only supports `INVALIDATE` resolution

### Query Transformation

- **Aggregation vs scan**: Different strategies - aggregations use HAVING clauses, scans use WHERE clauses
- **Table reference replacement**: Constraints must reference subquery/CTE aliases when source tables are in subqueries
- **Column propagation**: Missing policy columns are automatically added to subquery/CTE SELECT lists

## Critical Gotchas

### sqlglot Expression Mutability

**Always parse fresh when you need a copy** - sqlglot expressions are mutable and modifying a cached expression affects all references:

```python
# Correct: parse fresh
constraint_expr = sqlglot.parse_one(policy.constraint, read="duckdb")

# Or serialize/deserialize
expr_sql = node.this.sql()
expr_copy = sqlglot.parse_one(expr_sql, read="duckdb")
```

### Accessing Clauses

**Use `parsed.args` to access clause objects** - methods like `parsed.having()` return SQL strings, not expression objects:

```python
existing_having_expr = parsed.args.get('having')
existing_where_expr = parsed.args.get('where')
```

### Aggregation Transformation

**Preserve full expressions, not just columns** - when transforming aggregations for non-aggregation queries, use `node.this` to preserve the entire expression:

```python
# Correct: preserves full expression logic
expr_sql = node.this.sql()
expr_copy = sqlglot.parse_one(expr_sql, read="duckdb")
return expr_copy

# Wrong: loses expression logic
col = columns[0]
return col
```

### Table Name Extraction

**Use utility functions** - `column.table` can be various types (str, Identifier, etc.). Use `get_table_name_from_column()` from `sqlglot_utils.py` which handles all cases.

### Function Call Creation

**Use `exp.Anonymous`** - `exp.Function` doesn't exist in sqlglot:

```python
kill_call = exp.Anonymous(this="kill", expressions=[])
```

### Subquery/CTE Structure

- Subqueries in FROM: alias is on the `Subquery` node itself, not a parent `Table`
- CTEs: access via `parsed.args.get('with_')`, not `parsed.with_` (which is a method)
- `CTE.this` is the SELECT expression, `CTE.alias.this` is the Identifier

### Accessing Internal State

**Use public API methods, not private attributes** - Always use public methods like `get_dfc_policies()` and `get_aggregate_policies()` instead of accessing `_policies` or `_aggregate_policies` directly. This maintains encapsulation and allows for future implementation changes.

### Aggregate Policies

**AggregateDFCPolicy is a separate policy type** - Aggregate policies are stored separately from regular policies and use different evaluation logic. They require the `AGGREGATE` keyword when parsing from strings and currently only support `INVALIDATE` resolution. Use `register_policy()` for both types - it automatically detects the policy type and stores it in the appropriate list.

### AWS IAM Explicit Deny Policies

**Explicit deny policies override allow policies** - If you see an error like "with an explicit deny in an identity-based policy", it means there's an IAM policy that explicitly denies the action. Even if you have an allow policy, the explicit deny will block access. You must remove or modify the deny policy to fix this. This is a common issue in enterprise AWS environments with restrictive security policies.

## Code Style Principles

### Comments

- **Remove self-evident comments** that just restate what the code does
- **Keep informative comments** that explain WHY, provide context, or document non-obvious behavior
- **Document design decisions**, especially around sqlglot quirks and workarounds

### DRY Principles

- Extract common logic into helper functions
- Move shared utilities to dedicated modules
- Combine validation methods when they check related things

### Function Organization

- Private helper functions prefixed with `_`
- Group related functionality in the same module
- Keep functions focused and single-purpose

## Testing

### Running Tests

**Run rewriter tests after any changes** - Rewriter tests can be run with `uv run pytest` and should be run after making any Rewriter changes to ensure functionality is preserved.

**Always run tests from the project directory** - `pytest` and virtual environments are project-local. `cd` into the specific project (e.g., `sql_rewriter`, `experiment_harness`, `sbo_tax_agent`). For `vldb_2026_big_paper_experiments`, use the local venv: `.venv/bin/python -m pytest`.

### vldb_2026_big_paper_experiments setup notes

- `vldb_2026_big_paper_experiments` now uses local editable `uv` sources for `sql-rewriter`, `experiment-harness`, and `shared-sql-utils` (no `PYTHONPATH` needed).
- `uv sync` installs standard DuckDB from PyPI, which disables SmokedDuck lineage. For physical baselines, use `setup_venv.sh` + SmokedDuck build or run microbenchmarks with `--disable-physical`.

**Project test commands**:
- `sql_rewriter`: `uv run pytest`
- `experiment_harness`: `uv run --group dev python -m pytest`
- `sbo_tax_agent`: no tests currently
- `vldb_2026_big_paper_experiments`: `source setup_local_smokedduck.sh && .venv/bin/python -m pytest`

### Test Assertions

**Assert full strings, not partial matches** - When comparing strings in tests, always assert full strings instead of looking for partial string matches. This ensures tests are precise and catch unintended changes:

```python
# Correct: assert full string
assert result == "SELECT * FROM table WHERE id = 1"

# Wrong: partial match can hide issues
assert "SELECT" in result
```

## Verifying Correctness

**Always verify correctness before completing changes** - Before finishing any code changes, agents must verify correctness by:

1. **Linting with Ruff** - Run Ruff to check for code style issues, unused imports, and other linting problems:
   ```bash
   # sql_rewriter / experiment_harness / sbo_tax_agent
   python3 -m ruff check .

   # vldb_2026_big_paper_experiments (uses local venv)
   .venv/bin/python -m ruff check src/ tests/

   # Auto-fix issues where possible
   python3 -m ruff check . --fix
   .venv/bin/python -m ruff check src/ tests/ --fix
   ```
   
   Ruff will catch:
   - Unused imports (F401)
   - Code style violations
   - Deprecated Python syntax
   - Unused variables and arguments
   - And many other code quality issues

2. **Running Tests** - Execute the test suite to ensure functionality is preserved:
   ```bash
   # Run all tests
   uv run pytest
   
   # Run tests for a specific module
   uv run pytest tests/test_rewriter.py
   
   # Run tests with verbose output
   uv run pytest -v
   ```
   
   Tests should pass before considering changes complete. If tests fail, fix the issues before proceeding.

**Both linting and tests must pass** - Code changes are not complete until both Ruff linting passes (with no errors or only acceptable warnings) and all relevant tests pass.

## Important Design Decisions

1. **Policy validation split**: Syntax validation at creation, catalog validation at registration
2. **Policy types**: Two policy types - `DFCPolicy` for standard policies applied during query execution, `AggregateDFCPolicy` for policies evaluated after all data is processed
3. **Aggregation vs scan**: Different transformation strategies (HAVING vs WHERE)
4. **Resolution types**: 
   - `KILL` wraps in CASE WHEN to abort query
   - `REMOVE` filters rows
   - `LLM` uses AI to fix violating rows and writes them to stream file
   - `INVALIDATE` marks rows with a 'valid' column
5. **Column qualification required**: Simplifies source/sink identification
6. **Source columns must be aggregated**: Ensures constraints work correctly in aggregation context (for `DFCPolicy`)
7. **Aggregate policies use inner/outer aggregation**: Source columns aggregated twice (inner during query, outer during finalize), sink columns aggregated once during finalize

## Agentic Systems (SBO Tax Agent)

### Streamlit Agentic Loops

**Process one item per rerun for real-time updates** - When building agentic loops in Streamlit, process one transaction/item per `st.rerun()` call. Store the list of items and current index in session state rather than trying to store generators (which aren't serializable). This allows the UI to update in real-time as the agent processes each item.

### AWS Bedrock Tool Use

**Handle multiple conversation rounds** - When using Bedrock with tools, the agent may need multiple API calls: one to request tool use, another after tool execution. Implement a conversation loop that continues until the agent stops requesting tools. Limit iterations to prevent infinite loops.

### Database Tool Design

**Execute through SQLRewriter to respect DFC policies** - When giving an agent access to a database tool, route all SQL queries through the SQLRewriter so that Data Flow Control policies are enforced. This ensures the agent can't bypass policy restrictions.

## Experiment Harness

### Running Experiments

**Use experiment_harness for performance testing** - The `experiment_harness` project provides a reusable framework for running experiments with configurable parameters. Define experiments by implementing the `ExperimentStrategy` interface, then use `ExperimentRunner` to execute them with warm-up runs, multiple executions, and automatic CSV result export.

### Experiment Strategy Pattern

**Implement ExperimentStrategy for custom experiments** - Create experiment strategies by subclassing `ExperimentStrategy` and implementing `setup()`, `execute()`, and `teardown()` methods. The `execute()` method should return an `ExperimentResult` with timing and custom metrics. This allows experiments to be defined in any directory while using the shared harness infrastructure.

### Configuration Best Practices

**Configure experiments with ExperimentConfig** - Use `ExperimentConfig` to set:
- `num_executions`: Number of runs to collect results for
- `num_warmup_runs`: Number of warm-up runs to discard (important for JIT/cache warming)
- `database_config`: DuckDB connection parameters if needed
- `system_config`: System-level parameters (threads, memory limits)
- `output_dir` and `output_filename`: Where to save CSV results

### Metrics Collection

**Collect both timing and custom metrics** - The harness automatically collects execution duration. Use `ExperimentResult.custom_metrics` to add custom metrics like row counts, memory usage, or query-specific measurements. The CSV export includes summary statistics (mean, median, stddev, min, max) for all numeric metrics.

## When in Doubt

1. Check existing tests for similar patterns
2. Look at how similar functionality is implemented
3. Test with simple cases first, then complex ones
4. Remember: sqlglot expressions are mutable - parse fresh when needed
5. Access clauses via `parsed.args`, not via methods that return SQL strings
6. For AWS issues: Check for explicit deny policies if you get access denied errors
