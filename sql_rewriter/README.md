# SQL Rewriter

A SQL rewriter that intercepts queries, transforms them according to configurable rules, and executes them against a DuckDB database.

## Features

- Intercepts SQL queries before execution
- Transforms queries based on configurable rules
- Executes transformed queries against DuckDB
- Currently adds column "bar" to SELECT statements querying table "foo"

## Installation

This project uses `uv` for package management. To install dependencies:

```bash
uv sync
```

To install with development dependencies (including pytest):

```bash
uv sync --extra dev
```

## Usage

```python
from sql_rewriter import SQLRewriter

# Create a rewriter with an in-memory database
with SQLRewriter() as rewriter:
    # Create a table
    rewriter.execute("CREATE TABLE foo (id INTEGER, name VARCHAR)")
    
    # Insert data
    rewriter.execute("INSERT INTO foo VALUES (1, 'Alice')")
    
    # Query - will automatically add "bar" column if it exists
    results = rewriter.fetchall("SELECT id, name FROM foo")
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

The tests in `test_rewriter.py` also serve as usage examples.

## Development

The rewriter can be extended to add more transformation rules. The `transform_query` method in `rewriter.py` is where transformations are applied.

