# SQL Rewriter

A SQL query rewriter that intercepts queries, applies Data Flow Control (DFC) policies, and executes them against a DuckDB database. Policies can filter or abort queries based on constraints over source and sink tables.

## Features

- **Query Interception**: Automatically transforms SQL queries before execution
- **Data Flow Control Policies**: Define constraints on data movement between source and sink tables
- **Policy Resolution**: Two modes - `REMOVE` (filter rows) or `KILL` (abort query)
- **Aggregation Support**: Handles both aggregation queries (HAVING clauses) and table scans (WHERE clauses)
- **Subquery and CTE Support**: Automatically handles subqueries and Common Table Expressions (CTEs), adding missing columns needed for policy evaluation
- **DuckDB Integration**: Executes transformed queries against DuckDB with full SQL support

## Installation

This project uses `uv` for package management. To install dependencies:

```bash
uv sync
```

To install with development dependencies (including pytest):

```bash
uv sync --extra dev
```

### Using Local DuckDB Build

If you want to use a locally built DuckDB from the `resolution_ui` submodule (which includes custom extensions), you have several options:

#### Option 1: Use the wrapper script (Recommended)

Use the provided wrapper script that automatically configures the environment:

```bash
./uv_with_local_duckdb.sh sync
./uv_with_local_duckdb.sh run pytest
```

#### Option 2: Source the setup script

Before running uv commands, source the setup script:

```bash
source setup_local_duckdb.sh
uv sync
uv run pytest
```

#### Option 3: Import the Python helper

In your Python code, import the helper module before importing duckdb:

```python
import use_local_duckdb  # Must be imported before duckdb
import duckdb
from sql_rewriter import SQLRewriter

# Now SQLRewriter will use the local DuckDB build
rewriter = SQLRewriter()
```

**Note**: Make sure you've built the DuckDB library first by running `make` in the `resolution_ui` directory.

## Quick Start

```python
from sql_rewriter import SQLRewriter, DFCPolicy, Resolution

# Create a rewriter with an in-memory database
with SQLRewriter() as rewriter:
    # Create a table
    rewriter.execute("CREATE TABLE users (id INTEGER, age INTEGER, name VARCHAR)")
    rewriter.execute("INSERT INTO users VALUES (1, 25, 'Alice'), (2, 17, 'Bob'), (3, 30, 'Charlie')")
    
    # Create a policy: only allow queries where max age >= 18
    policy = DFCPolicy(
        source="users",
        constraint="max(users.age) >= 18",
        on_fail=Resolution.REMOVE,  # Filter out rows that don't meet constraint
    )
    
    # Register the policy
    rewriter.register_policy(policy)
    
    # Query - automatically filtered by policy
    results = rewriter.fetchall("SELECT id, name FROM users")
    # Only returns rows where the aggregation constraint passes
```

## Usage

### Creating Policies

Policies define constraints on data flow. They require either a `source` or `sink` table (or both):

```python
from sql_rewriter import DFCPolicy, Resolution

# Policy with source only
policy1 = DFCPolicy(
    source="users",
    constraint="max(users.age) >= 18",
    on_fail=Resolution.REMOVE,
)

# Policy with source and sink
policy2 = DFCPolicy(
    source="orders",
    sink="analytics",
    constraint="max(orders.total) > 100 AND analytics.status = 'active'",
    on_fail=Resolution.KILL,  # Abort query if constraint fails
)

# Policy with sink only
policy3 = DFCPolicy(
    sink="reports",
    constraint="reports.status = 'approved'",
    on_fail=Resolution.REMOVE,
)
```

### Policy Constraints

- **All columns must be qualified** with table names (e.g., `users.age`, not just `age`)
- **Source columns must be aggregated** when a source table is specified
- **Constraints are SQL expressions** that evaluate to boolean

### Policy Resolution

- **`Resolution.REMOVE`**: Filters out rows/results that don't meet the constraint
- **`Resolution.KILL`**: Aborts the entire query if any row fails the constraint

### Registering Policies

Policies must be registered with a `SQLRewriter` instance. Registration validates that:
- Tables exist in the database
- Columns referenced in constraints exist in their respective tables

```python
rewriter = SQLRewriter()
rewriter.execute("CREATE TABLE users (id INTEGER, age INTEGER)")

policy = DFCPolicy(
    source="users",
    constraint="max(users.age) >= 18",
    on_fail=Resolution.REMOVE,
)

rewriter.register_policy(policy)  # Validates tables and columns exist
```

### Query Execution

The rewriter automatically applies policies to queries:

```python
# Aggregation query - policy applied as HAVING clause
results = rewriter.fetchall("SELECT max(age) FROM users")

# Table scan - policy applied as WHERE clause (aggregations transformed)
results = rewriter.fetchall("SELECT id, name FROM users")

# Subqueries and CTEs - missing columns automatically added
# If a subquery/CTE references a source table but doesn't select all columns
# needed for policy evaluation, they are automatically added to the SELECT list
results = rewriter.fetchall("SELECT sub.name FROM (SELECT name FROM users) AS sub")
```

## Examples

### Filtering Aggregation Results

```python
with SQLRewriter() as rewriter:
    rewriter.execute("CREATE TABLE sales (amount INTEGER, region VARCHAR)")
    rewriter.execute("INSERT INTO sales VALUES (100, 'US'), (50, 'EU'), (200, 'US')")
    
    # Only allow queries where total sales > 150
    policy = DFCPolicy(
        source="sales",
        constraint="sum(sales.amount) > 150",
        on_fail=Resolution.REMOVE,
    )
    rewriter.register_policy(policy)
    
    # This query will be filtered by the policy
    results = rewriter.fetchall("SELECT sum(amount) FROM sales")
    # Only returns results if sum(amount) > 150
```

### Aborting Queries with KILL

```python
with SQLRewriter() as rewriter:
    rewriter.execute("CREATE TABLE sensitive (id INTEGER, level INTEGER)")
    rewriter.execute("INSERT INTO sensitive VALUES (1, 5), (2, 3)")
    
    # Abort query if max level is too high
    policy = DFCPolicy(
        source="sensitive",
        constraint="max(sensitive.level) < 4",
        on_fail=Resolution.KILL,  # Abort if constraint fails
    )
    rewriter.register_policy(policy)
    
    # This will raise an exception because max(level) = 5 >= 4
    try:
        results = rewriter.fetchall("SELECT * FROM sensitive")
    except Exception as e:
        print("Query aborted:", e)
```

## Testing

Run the test suite:

```bash
uv run pytest
```

Or with verbose output:

```bash
uv run pytest -v
```

The test files serve as comprehensive usage examples:
- `test_rewriter.py`: Integration tests (212 tests)
- `test_policy.py`: Policy validation tests (100+ tests)
- `test_rewrite_rule.py`: Rewrite rule tests (36 tests)

## Architecture

- **`rewriter.py`**: Main `SQLRewriter` class - query interception, policy registration, execution
- **`policy.py`**: `DFCPolicy` class - policy definition and validation
- **`rewrite_rule.py`**: Policy application logic - HAVING/WHERE clause injection
- **`sqlglot_utils.py`**: Shared utility functions for sqlglot expressions

## Documentation

For detailed information about:
- Common pitfalls and solutions
- sqlglot-specific gotchas
- Testing patterns
- Code style preferences
- Future enhancements

See [AGENTS.md](AGENTS.md) for a comprehensive guide.

## Development

The rewriter uses `sqlglot` for SQL parsing and transformation. Key areas for extension:
- Additional aggregation transformations in `transform_aggregations_to_columns()`
- New policy validation rules in `DFCPolicy._validate()`
- Support for more SQL features (window functions, recursive CTEs, etc.)

