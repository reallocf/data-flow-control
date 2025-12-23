# Agent Guide for SQL Rewriter Project

This document contains key learnings, patterns, and gotchas from the development of this SQL rewriter project. Use this as a reference when working on this codebase.

## Project Overview

This is a SQL query rewriter that intercepts queries, applies Data Flow Control (DFC) policies, and executes them against DuckDB. Policies can filter or abort queries based on constraints over source/sink tables.

## Technology Stack

- **Python 3.8+**: Core language
- **uv**: Package manager (use `uv run pytest`, `uv sync`, etc.)
- **sqlglot**: SQL parsing, transformation, and AST manipulation
- **duckdb**: In-process SQL OLAP database
  - Uses locally built DuckDB v1.4.1 from `resolution_ui` submodule
  - Includes custom `external` extension with `EXTERNAL_OPERATOR`
  - See "Using the Local DuckDB Build with Custom Extensions" section for setup
- **pytest**: Testing framework
- **numpy**: Required dependency for DuckDB Python UDFs
- **pandas**: Required for some DuckDB operations (e.g., `.df()` method)

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
- Pre-calculates `_source_columns_needed` at creation time for efficiency (columns needed after aggregation transformation)

**Policy Registration** (`rewriter.py`):
- `register_policy()` validates against DuckDB catalog (tables/columns must exist)
- Runtime validation happens here, not during policy creation
- Policies are stored in `_policies` list

### 2. Query Transformation Flow

1. `transform_query()` parses SQL with `sqlglot.parse_one(query, read="duckdb")`
2. Extracts source tables from FROM/JOIN clauses
3. Finds matching policies for source tables
4. If policies match:
   - `ensure_subqueries_have_constraint_columns()`: Adds missing columns to subquery/CTE SELECT lists
   - Builds table mapping from source tables to subquery/CTE aliases
5. Determines if query is aggregation or scan:
   - Aggregation: Uses `apply_policy_constraints_to_aggregation()` → replaces table references, adds HAVING clauses
   - Scan: Uses `apply_policy_constraints_to_scan()` → transforms aggregations to columns, replaces table references, adds WHERE clauses
6. Returns transformed SQL string

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

### 8. Finding Subqueries in FROM Clauses

**Problem**: Subqueries in FROM clauses have a complex structure in sqlglot. The alias is stored on the Subquery node itself, not on a parent Table.

**Solution**: Find Subquery nodes directly and check their ancestors:
```python
def _get_subqueries_in_from(parsed: exp.Select) -> List[tuple[exp.Subquery, str]]:
    subqueries = []
    all_subqueries = list(parsed.find_all(exp.Subquery))
    for subquery in all_subqueries:
        from_ancestor = subquery.find_ancestor(exp.From)
        if from_ancestor and hasattr(from_ancestor, 'this'):
            from_table = from_ancestor.this
            if isinstance(from_table, exp.Subquery):
                # The alias is on the Subquery itself
                if hasattr(from_table, 'alias') and from_table.alias:
                    alias = from_table.alias.name.lower() if isinstance(from_table.alias, exp.Identifier) else str(from_table.alias).lower()
                    subqueries.append((subquery, alias))
    return subqueries
```

**Key Points**:
- `From.this` can be directly a `Subquery` (not wrapped in a `Table`)
- The alias is stored on the `Subquery` node's `alias` attribute
- The alias can be an `Identifier` or a string

### 9. Accessing CTEs (Common Table Expressions)

**Problem**: `parsed.with_` is a method, not a property. Accessing it directly doesn't work.

**Solution**: Access via `parsed.args.get('with_')`:
```python
def _get_ctes(parsed: exp.Select) -> List[tuple[exp.CTE, str]]:
    ctes = []
    with_clause = parsed.args.get('with_') if hasattr(parsed, 'args') else None
    if with_clause and hasattr(with_clause, 'expressions'):
        for cte in with_clause.expressions:
            if isinstance(cte, exp.CTE):
                # CTE.this is the SELECT expression
                # CTE.alias is a TableAlias, and alias.this is the Identifier
                if hasattr(cte, 'alias') and cte.alias:
                    if hasattr(cte.alias, 'this'):
                        alias_obj = cte.alias.this
                        alias = alias_obj.name.lower() if isinstance(alias_obj, exp.Identifier) else str(alias_obj).lower()
                        ctes.append((cte, alias))
    return ctes
```

**Key Points**:
- Use `parsed.args.get('with_')` to access the WITH clause
- `CTE.this` is the SELECT expression (not `CTE.expression`)
- `CTE.alias` is a `TableAlias`, and `alias.this` is the `Identifier` with the alias name

### 10. Handling Subqueries and CTEs with Missing Policy Columns

**Problem**: When a source table is referenced in a subquery or CTE that doesn't select all columns needed for policy evaluation, the constraint can't be applied in the outer query.

**Solution**: The `ensure_subqueries_have_constraint_columns()` function automatically adds missing columns to subquery/CTE SELECT lists, and `_replace_table_references_in_constraint()` updates constraint expressions to use subquery/CTE aliases instead of original table names.

**Process**:
1. Find all subqueries and CTEs that reference source tables
2. For each policy, check which columns are needed (pre-calculated in `policy._source_columns_needed`)
3. Add missing columns to the subquery/CTE SELECT list
4. Build a mapping from source table names to subquery/CTE aliases
5. Replace table references in constraints (e.g., `foo.id` → `sub.id` or `cte.id`)

**Key Functions**:
- `ensure_subqueries_have_constraint_columns()`: Adds missing columns to SELECT lists
- `_get_source_table_to_alias_mapping()`: Maps source tables to their subquery/CTE aliases
- `_replace_table_references_in_constraint()`: Replaces table references in constraint expressions

**Important**: This happens before policy constraints are applied, so the outer query can correctly reference columns via the subquery/CTE alias.

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

### Standard Tests (sql_rewriter)

```bash
# Run all tests
uv run pytest

# Or with the local DuckDB wrapper (recommended)
./uv_with_local_duckdb.sh run pytest

# Run specific test file
uv run pytest test_rewriter.py

# Run with verbose output
uv run pytest -v

# Run with short traceback
uv run pytest --tb=short

# Run quietly (just summary)
uv run pytest -q
```

### Extension Tests (resolution_ui)

To run tests that use the DuckDB extension:

```bash
cd resolution_ui
source ../sql_rewriter/setup_local_duckdb.sh
../sql_rewriter/.venv/bin/python scripts/test_simple.py
```

Or using the wrapper:
```bash
cd resolution_ui
../sql_rewriter/uv_with_local_duckdb.sh run python scripts/test_simple.py
```

## Using the Local DuckDB Build with Custom Extensions

This project uses a locally built DuckDB from the `resolution_ui` submodule, which includes custom extensions (specifically the `external` extension with the `EXTERNAL_OPERATOR`). This setup allows the SQL rewriter to use modified DuckDB functionality.

### Setup Overview

The `resolution_ui` submodule contains:
- A modified DuckDB build (v1.4.1) with custom extensions
- The `external` extension that provides `EXTERNAL_OPERATOR` functionality
- Build artifacts in `resolution_ui/build/release/`

### Initial Setup

1. **Build the DuckDB library** (if not already built):
   ```bash
   cd resolution_ui
   make
   ```
   This creates:
   - `build/release/duckdb`: DuckDB CLI binary
   - `build/release/src/libduckdb.dylib`: Shared library (macOS) or `.so` (Linux)
   - `build/release/repository/v1.4.1/osx_arm64/external.duckdb_extension`: Extension binary

2. **Install matching DuckDB Python version**:
   ```bash
   cd sql_rewriter
   ./uv_with_local_duckdb.sh pip install "duckdb==1.4.1"
   ```
   **Critical**: The Python package version must match the local build version (1.4.1), otherwise extensions won't load.

### Using the Setup Scripts

Three helper scripts are provided:

#### 1. `setup_local_duckdb.sh`
Sets environment variables to point to the local DuckDB library:
- `DYLD_LIBRARY_PATH` (macOS) or `LD_LIBRARY_PATH` (Linux): Points to the library directory
- `DUCKDB_LIBRARY`: Points to the specific library file

**Usage**:
```bash
source setup_local_duckdb.sh
# Now Python/uv commands will use the local DuckDB
```

#### 2. `uv_with_local_duckdb.sh`
Wrapper script that automatically configures the environment and runs uv commands.

**Usage**:
```bash
./uv_with_local_duckdb.sh sync          # Install dependencies
./uv_with_local_duckdb.sh run pytest     # Run tests
./uv_with_local_duckdb.sh run python    # Run Python scripts
```

#### 3. `use_local_duckdb.py`
Python module that configures DuckDB at runtime. Import it before importing `duckdb`:

```python
import use_local_duckdb  # Must be before duckdb import
import duckdb
from sql_rewriter import SQLRewriter

# Now SQLRewriter uses the local DuckDB build
rewriter = SQLRewriter()
```

### Running Tests with the Extension

To run tests in `resolution_ui/scripts/` that use the extension:

```bash
cd resolution_ui
source ../sql_rewriter/setup_local_duckdb.sh
../sql_rewriter/.venv/bin/python scripts/test_simple.py
```

Or using the wrapper:
```bash
cd resolution_ui
../sql_rewriter/uv_with_local_duckdb.sh run python scripts/test_simple.py
```

### The External Operator Extension

The `external` extension provides the `EXTERNAL_OPERATOR` which enables:
- Union operations with external data sources
- Streaming data integration
- Custom operator logic for data flow control

**Loading the extension**:
```python
import duckdb

con = duckdb.connect(
    database=":memory:",
    config={"allow_unsigned_extensions": "true"},
)

# Load the extension
EXT_PATH = "resolution_ui/build/release/repository/v1.4.1/osx_arm64/external.duckdb_extension"
con.execute(f"LOAD '{EXT_PATH}'")
```

**Key Points**:
- Extension path is platform-specific (e.g., `osx_arm64` for macOS ARM)
- Must enable `allow_unsigned_extensions` to load local extensions
- Extension version (v1.4.1) must match DuckDB version exactly

### Version Matching Requirements

**Critical**: The following must all match:
1. Local DuckDB build version (from `resolution_ui/build/release/duckdb --version`)
2. DuckDB Python package version (from `pip list | grep duckdb`)
3. Extension version (embedded in extension binary)

If versions don't match, you'll see errors like:
```
The file was built specifically for DuckDB version 'v1.4.1' and can only be loaded with that version of DuckDB. (this version of DuckDB is 'v1.4.3')
```

**Solution**: Install the matching Python package version:
```bash
./uv_with_local_duckdb.sh pip install "duckdb==1.4.1"
```

### Troubleshooting

**Problem**: Extension not found
- **Solution**: Run `make` in `resolution_ui` to build the extension

**Problem**: Version mismatch error
- **Solution**: Install matching DuckDB Python version (see above)

**Problem**: Library not found errors
- **Solution**: Ensure `setup_local_duckdb.sh` is sourced or use `uv_with_local_duckdb.sh` wrapper

**Problem**: `numpy` or `pandas` not installed
- **Solution**: Install required dependencies:
  ```bash
  ./uv_with_local_duckdb.sh pip install numpy pandas
  ```

### Integration with SQL Rewriter

When using `SQLRewriter` with the local DuckDB build:
- The rewriter automatically uses the configured DuckDB instance
- Custom extensions are available if loaded before creating the rewriter
- All standard SQL rewriter functionality works with the custom build

**Example**:
```python
import use_local_duckdb  # Configure environment
import duckdb
from sql_rewriter import SQLRewriter

# Load extension if needed
con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute(f"LOAD '{EXT_PATH}'")

# Create rewriter (uses the same DuckDB instance)
rewriter = SQLRewriter()
# ... use rewriter as normal
```

## Dependencies

Always run `uv sync` after modifying `pyproject.toml` to update the lock file.

Key dependencies:
- `sqlglot>=24.0.0`: SQL parsing and transformation
- `duckdb>=1.0.0`: Database engine (use 1.4.1 for local build compatibility)
- `numpy>=1.24.4`: Required for DuckDB Python UDFs
- `pandas`: Required for some DuckDB operations (e.g., `.df()` method)

## Future Enhancements

Areas identified for future work (documented in code comments):
- `ensure_columns_accessible()`: Currently a no-op, should check non-aggregated columns are in SELECT/GROUP BY
- Sink table policy application: Currently only source tables are matched
- More complex SQL features: Window functions, recursive CTEs, etc.
- Nested subqueries: Currently handles one level of subqueries/CTEs, but deeply nested structures may need additional work

## When in Doubt

1. Check existing tests for similar patterns
2. Look at how similar functionality is implemented
3. Test with simple cases first, then complex ones
4. Remember: sqlglot expressions are mutable - parse fresh when needed
5. Access clauses via `parsed.args`, not via methods that return SQL strings

