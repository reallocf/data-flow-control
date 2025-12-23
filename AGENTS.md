# Agent Guide for SQL Rewriter Project

This document contains key principles, best practices, and critical gotchas for working on this SQL rewriter project.

## Project Overview

This is a SQL query rewriter that intercepts queries, applies Data Flow Control (DFC) policies, and executes them against DuckDB. Policies can filter or abort queries based on constraints over source/sink tables.

## Key Architecture Principles

### Policy System

- **Validation split**: Syntax validation at policy creation, catalog validation at registration
- **Resolution types**: `REMOVE` filters rows, `KILL` aborts the query
- **Column qualification**: All constraint columns must be qualified with table names
- **Source aggregation**: Source columns must be aggregated when source is present

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

## Important Design Decisions

1. **Policy validation split**: Syntax validation at creation, catalog validation at registration
2. **Aggregation vs scan**: Different transformation strategies (HAVING vs WHERE)
3. **KILL vs REMOVE**: KILL wraps in CASE WHEN to abort query, REMOVE filters rows
4. **Column qualification required**: Simplifies source/sink identification
5. **Source columns must be aggregated**: Ensures constraints work correctly in aggregation context

## When in Doubt

1. Check existing tests for similar patterns
2. Look at how similar functionality is implemented
3. Test with simple cases first, then complex ones
4. Remember: sqlglot expressions are mutable - parse fresh when needed
5. Access clauses via `parsed.args`, not via methods that return SQL strings
