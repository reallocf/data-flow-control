# Weird Passant Bug: Constant Sink Expression False KILL

## Summary

In `oss_experiments`, the 10-row receipt rollup workload works with the current
merchant-based `CASE` expression for `Expenses.biz_use`, but false-kills pass
cases if the query is simplified to use a constant sink expression:

```sql
100.0 AS biz_use
```

The pass case selects only `Office` receipts. The rewritten query computes
`__passant_policy_pass = true` for every selected row, but DuckDB still invokes
`passant_kill()` from the generated KILL wrapper.

## Current Policy

```text
SOURCE Receipts SINK Expenses
CONSTRAINT MIN(Receipts.cat) != 'Meal' OR Expenses.biz_use <= 50.0
ON FAIL KILL
DESCRIPTION meal_deduction_limit
```

## Repro Setup

From:

```bash
cd /Users/charliesummers/code/data-flow-control/oss_experiments
```

Use the current `10_row_queries/q00` generated query. It is a pass case:

```text
selected receipt ids: 2, 12, 22, 32, 42, 52, 62, 72, 82, 92
merchant: Blue Meridian 00-02
cat for all selected rows: Office
```

Confirm the selected rows:

```bash
uv run python - <<'PY'
import duckdb
from oss_experiments.data_setup import QUERY_SET_TEN_ROW, build_query_catalog, reset_dataset

conn = duckdb.connect()
reset_dataset(conn)
q = next(q for q in build_query_catalog() if q.query_set == QUERY_SET_TEN_ROW and q.query_id == "q00")

placeholders = ", ".join("?" for _ in q.input_receipt_ids)
rows = conn.execute(
    f"""
    SELECT id, merchant, cat, cost
    FROM Receipts
    WHERE id IN ({placeholders})
    ORDER BY id
    """,
    list(q.input_receipt_ids),
).fetchall()
print(rows)
print(conn.execute(
    f"SELECT cat, COUNT(*) FROM Receipts WHERE id IN ({placeholders}) GROUP BY cat",
    list(q.input_receipt_ids),
).fetchall())
PY
```

Expected output shape:

```text
[(2, 'Blue Meridian 00-02', 'Office', 100.0), ...]
[('Office', 10)]
```

## Repro Script

This script modifies only the generated SQL string in memory, replacing the
current working `CASE WHEN ... END AS biz_use` with the simplified constant
expression.

```bash
uv run python - <<'PY'
import duckdb
from data_flow_control import dfc
from data_flow_control.adapters.kill import KILL_MESSAGE
from oss_experiments.data_setup import QUERY_SET_TEN_ROW, build_query_catalog, reset_dataset
from oss_experiments.policies import MEAL_DEDUCTION_LIMIT_PGN, register_policy_set

q = next(q for q in build_query_catalog() if q.query_set == QUERY_SET_TEN_ROW and q.query_id == "q00")

sql = q.sql.replace(
    "CASE WHEN merchant IN ('Blue Meridian 00-02') THEN 100.0 ELSE 50.0 END AS biz_use",
    "100.0 AS biz_use",
)

conn = duckdb.connect()
reset_dataset(conn)
dfc_conn = dfc(conn)
register_policy_set(dfc_conn)

rewritten = dfc_conn.transform_query(sql)
print("POLICY:")
print(MEAL_DEDUCTION_LIMIT_PGN)
print("\nREWRITTEN:")
print(rewritten)

print("\nDiagnostic predicate rows:")
print(conn.execute("""
WITH selected_receipts AS (
    SELECT * FROM (VALUES
        (1, 2), (2, 12), (3, 22), (4, 32), (5, 42),
        (6, 52), (7, 62), (8, 72), (9, 82), (10, 92)
    ) AS selected_receipts(selection_position, receipt_id)
)
SELECT
    Receipts.id,
    Receipts.cat,
    Receipts.cat <> 'Meal' OR 100.0 <= 50.0 AS policy_pass
FROM Receipts
JOIN selected_receipts ON selected_receipts.receipt_id = Receipts.id
ORDER BY Receipts.id
""").fetchall())

print("\nRaw original SQL:")
reset_dataset(conn)
conn.execute(sql)
print(conn.execute("SELECT * FROM Expenses").fetchall())

print("\nRewritten SQL:")
reset_dataset(conn)
try:
    conn.execute(rewritten)
    print("rewritten succeeded")
    print(conn.execute("SELECT * FROM Expenses").fetchall())
except Exception as exc:
    print(type(exc).__name__, exc)
    print("is_kill:", KILL_MESSAGE in str(exc))
PY
```

## Observed Behavior

The diagnostic predicate rows are all true:

```text
[(2, 'Office', True), (12, 'Office', True), ..., (92, 'Office', True)]
```

The raw simplified query succeeds:

```text
[(1, 'Blue Meridian 00-02', 1000.0, 100.0, '10_row_queries:q00')]
```

The rewritten query fails:

```text
InvalidInputException ... ValueError: KILLing due to dfc policy violation
is_kill: True
```

## Bad Rewritten Shape

The rewritten query contains:

```sql
Receipts.cat <> 'Meal' OR 100.0 <= 50.0 AS __passant_policy_pass
```

For the q00 selected rows, `Receipts.cat` is always `Office`, so this expression
is true for every row.

The wrapper then uses:

```sql
WHERE
  t1.__passant_policy_pass
  OR CASE
    WHEN NOT t1.__passant_policy_pass THEN passant_kill()
    ELSE true
  END
```

Despite `t1.__passant_policy_pass` being true for all rows, executing the
rewritten query directly in DuckDB invokes `passant_kill()`.

## Working Comparison

If the query uses the current merchant-dependent `CASE` expression instead:

```sql
CASE WHEN merchant IN ('Blue Meridian 00-02') THEN 100.0 ELSE 50.0 END AS biz_use
```

then the 10-row DFC preflight succeeds:

```bash
AWS_EC2_METADATA_DISABLED=true PYTHONUNBUFFERED=1 uv run python -u \
  scripts/run_tax_llm_validation_experiment.py \
  --query-sets 10_row_queries \
  --dfc-only \
  --runs-per-setting 1 \
  --output-filename tax_llm_validation_10row_dfc_preflight_after_case.csv
```

Observed:

```text
Total executions: 10
Successful: 10
Failed: 0
dfc_killed mean: 0.500
dfc_truth_matches_expected: 1.000
```

## Notes

There is a separate aggregate-rewrite issue with
`COUNT(DISTINCT Receipts.cat) = 1`; the rewriter previously transformed that
into `Receipts.cat = 1`. This note is only about the constant sink-expression
false KILL behavior after removing the `COUNT(DISTINCT ...)` conjunct.
