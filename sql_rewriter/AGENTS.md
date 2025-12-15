# Agent Guide for SQL Rewriter Project

This document contains key learnings, patterns, and gotchas from the development of this SQL rewriter project. Use this as a reference when working on this codebase.

## Project Overview

This is a SQL query rewriter that intercepts queries, applies Data Flow Control (DFC) policies, and executes them against DuckDB. Policies can filter or abort queries based on constraints over source/sink tables.

## Technology Stack

- **Python 3.8+**: Core language
- **uv**: Package manager (use `uv run pytest`, `uv sync`, etc.)
- **sqlglot**: SQL parsing, transformation, and AST manipulation
- **duckdb**: In-process SQL OLAP database
- **pytest**: Testing framework
- **numpy**: Required dependency for DuckDB Python UDFs

## Project Structure

```
sql_rewriter/
├── __init__.py              # Package exports
├── rewriter.py              # Main SQLRewriter class (query interception, policy registration)
├── policy.py                # DFCPolicy class (policy definition and validation)
├── rewrite_rule.py          # Policy application logic (HAVING/WHERE clause injection)
├── sqlglot_utils.py         # Shared sqlglot utility functions
├── test_rewriter.py         # Comprehensive rewriter tests
├── test_policy.py           # Policy validation tests
├── test_rewrite_rule.py     # Rewrite rule tests
└── pyproject.toml           # Project configuration
```

## Key Architecture Patterns

### 1. Policy System

**DFCPolicy** (`policy.py`):
- Defines constraints on data flow between source and sink tables
- Validates SQL syntax at creation time
- Requires either `source` or `sink` (or both)
- Two resolution types: `REMOVE` (filter rows) or `KILL` (abort query)
- All columns in constraints must be qualified with table names
- Source columns must be aggregated when source is present

**Policy Registration** (`rewriter.py`):
- `register_policy()` validates against DuckDB catalog (tables/columns must exist)
- Runtime validation happens here, not during policy creation
- Policies are stored in `_policies` list

### 2. Query Transformation Flow

1. `transform_query()` parses SQL with `sqlglot.parse_one(query, read="duckdb")`
2. Extracts source tables from FROM/JOIN clauses
3. Finds matching policies for source tables
4. Determines if query is aggregation or scan:
   - Aggregation: Uses `apply_policy_constraints_to_aggregation()` → adds HAVING clauses
   - Scan: Uses `apply_policy_constraints_to_scan()` → transforms aggregations to columns, adds WHERE clauses
5. Returns transformed SQL string

### 3. Rewrite Rules (`rewrite_rule.py`)

**Key Functions**:
- `_wrap_kill_constraint()`: Wraps constraint in `CASE WHEN constraint THEN true ELSE kill() END` for KILL policies
- `_add_clause_to_select()`: Generic function to add HAVING/WHERE clauses, combining with existing if needed
- `transform_aggregations_to_columns()`: Transforms aggregations for non-aggregation queries:
  - `COUNT(*)` → `1`
  - `COUNT_IF(condition)` → `CASE WHEN condition THEN 1 ELSE 0 END`
  - `ARRAY_AGG(column)` → `[column]`
  - `MAX/MIN/SUM/AVG(expr)` → `expr` (preserves full expression, not just first column)

## Critical Gotchas and Solutions

### 1. sqlglot Expression Mutability

**Problem**: sqlglot expressions are mutable. Modifying a cached expression affects all references.

**Solution**: Always parse fresh when you need a copy:
```python
# Parse fresh to avoid mutating cached version
constraint_expr = sqlglot.parse_one(policy.constraint, read="duckdb")
```

Or serialize/deserialize:
```python
expr_sql = node.this.sql()
expr_copy = sqlglot.parse_one(expr_sql, read="duckdb")
```

### 2. Accessing HAVING/WHERE Clauses

**Problem**: `parsed.having()` returns SQL string, not the expression object.

**Solution**: Access via `parsed.args['having']` or `parsed.args['where']`:
```python
existing_having_expr = parsed.args.get('having')
```

### 3. Table Name Extraction from Columns

**Problem**: `column.table` can be `str`, `exp.Identifier`, or other types.

**Solution**: Use `get_table_name_from_column()` from `sqlglot_utils.py` which handles all cases:
```python
from .sqlglot_utils import get_table_name_from_column

table_name = get_table_name_from_column(column)
if isinstance(column.table, exp.Identifier):
    return column.table.name.lower()
elif isinstance(column.table, str):
    return column.table.lower()
else:
    return str(column.table).lower()  # Fallback
```

### 4. Aggregation Transformation Complexity

**Problem**: When transforming `MAX(CASE WHEN ...)` for non-aggregation queries, extracting just the first column loses the expression logic.

**Solution**: Use `node.this` (the full argument expression) instead of `columns[0]`:
```python
# Correct: preserves full expression
expr_sql = node.this.sql()
expr_copy = sqlglot.parse_one(expr_sql, read="duckdb")
return expr_copy

# Wrong: loses expression logic
col = columns[0]
return col
```

### 5. DuckDB UDF Registration

**Problem**: DuckDB requires `numpy` for Python UDFs.

**Solution**: 
- Add `numpy>=1.24.4` to `pyproject.toml` dependencies
- Register UDFs in `__init__`:
```python
def _register_kill_udf(self) -> None:
    def kill() -> bool:
        raise ValueError("KILLing due to dfc policy violation")
    self.conn.create_function('kill', kill, return_type='BOOLEAN')
```

**Note**: DuckDB wraps Python exceptions in `duckdb.InvalidInputException`.

### 6. Table Name Case Sensitivity

**Problem**: DuckDB's `information_schema` stores table names in original case, but comparisons need to be case-insensitive.

**Solution**: Always lowercase both sides:
```python
result = self.conn.execute(
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?",
    [table_name.lower()]
)
```

### 7. Function Call Creation in sqlglot

**Problem**: `exp.Function` doesn't exist. Need to create function calls differently.

**Solution**: Use `exp.Anonymous`:
```python
kill_call = exp.Anonymous(this="kill", expressions=[])
```

## Testing Patterns

### Test Structure

- **test_rewriter.py**: Integration tests for SQLRewriter (212 tests)
- **test_policy.py**: Unit tests for DFCPolicy validation (100+ tests)
- **test_rewrite_rule.py**: Unit tests for rewrite rule functions (36 tests)

### Common Test Patterns

```python
@pytest.fixture
def rewriter():
    rewriter = SQLRewriter()
    rewriter.execute("CREATE TABLE foo (id INTEGER, name VARCHAR)")
    rewriter.execute("INSERT INTO foo VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
    yield rewriter
    rewriter.close()

def test_example(rewriter):
    policy = DFCPolicy(
        source="foo",
        constraint="max(foo.id) > 1",
        on_fail=Resolution.REMOVE,
    )
    rewriter.register_policy(policy)
    
    query = "SELECT id FROM foo"
    transformed = rewriter.transform_query(query)
    result = rewriter.conn.execute(transformed).fetchall()
    
    assert len(result) == 2
```

### Testing KILL Resolution

```python
import duckdb

with pytest.raises(duckdb.InvalidInputException) as exc_info:
    rewriter.conn.execute(transformed).fetchall()
assert "KILLing due to dfc policy violation" in str(exc_info.value)
```

## Code Style Preferences

### Comments

- **Remove self-evident comments**: Comments that just restate what the code does
- **Keep informative comments**: Comments that explain WHY, provide context, or document non-obvious behavior
- **Document design decisions**: Especially around sqlglot quirks and workarounds

### DRY Principles

- Extract common logic into helper functions (e.g., `_wrap_kill_constraint`, `_add_clause_to_select`)
- Move shared utilities to dedicated modules (e.g., `sqlglot_utils.py`)
- Combine validation methods when they check related things

### Function Organization

- Private helper functions prefixed with `_`
- Group related functionality in same module
- Keep functions focused and single-purpose

## Common Tasks

### Adding a New Aggregation Transformation

1. Add case to `transform_aggregations_to_columns()` in `rewrite_rule.py`
2. Consider: What should this aggregation become for a single row?
3. Add test in `test_rewrite_rule.py`
4. Add integration test in `test_rewriter.py` if it affects policy application

### Adding a New Policy Validation Rule

1. Add validation method to `DFCPolicy._validate()` in `policy.py`
2. Add corresponding test in `test_policy.py`
3. Ensure error messages are clear and actionable

### Debugging Query Transformations

1. Use `parsed.sql(pretty=True, dialect="duckdb")` to see transformed SQL
2. Check `parsed.args` to inspect clause structure
3. Use `parsed.find_all(exp.XXX)` to find specific expression types
4. Remember: sqlglot expressions are mutable, so be careful with references

## Important Design Decisions

1. **Policy validation split**: Syntax validation at creation, catalog validation at registration
2. **Aggregation vs scan**: Different transformation strategies (HAVING vs WHERE)
3. **KILL vs REMOVE**: KILL wraps in CASE WHEN to abort query, REMOVE filters rows
4. **Column qualification required**: Simplifies source/sink identification
5. **Source columns must be aggregated**: Ensures constraints work correctly in aggregation context

## Running Tests

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest test_rewriter.py

# Run with verbose output
uv run pytest -v

# Run with short traceback
uv run pytest --tb=short

# Run quietly (just summary)
uv run pytest -q
```

## Dependencies

Always run `uv sync` after modifying `pyproject.toml` to update the lock file.

Key dependencies:
- `sqlglot>=24.0.0`: SQL parsing and transformation
- `duckdb>=1.0.0`: Database engine
- `numpy>=1.24.4`: Required for DuckDB Python UDFs

## Future Enhancements

Areas identified for future work (documented in code comments):
- `ensure_columns_accessible()`: Currently a no-op, should check non-aggregated columns are in SELECT/GROUP BY
- Sink table policy application: Currently only source tables are matched
- More complex SQL features: Window functions, recursive CTEs, etc.

## When in Doubt

1. Check existing tests for similar patterns
2. Look at how similar functionality is implemented
3. Test with simple cases first, then complex ones
4. Remember: sqlglot expressions are mutable - parse fresh when needed
5. Access clauses via `parsed.args`, not via methods that return SQL strings

