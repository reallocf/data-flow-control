# GProM versus DFC TPC-H rewrites

This probe compares the 13 TPC-H queries supported by the DFC rewrite tests:
Q01, Q03, Q04, Q05, Q06, Q07, Q08, Q09, Q10, Q12, Q14, Q18, and Q19.

The policy used for plan comparison is:

```sql
AVG(lineitem.l_quantity) >= 30
```

For each query, `results/qXX/` contains:

- `gprom_input.sql`: `PROVENANCE OF (...)` immediately consumed by the policy aggregate.
- `gprom_rewritten.sql`: ordinary SQL emitted by GProM.
- `postgres_explain.txt`: PostgreSQL's verbose costed plan for the GProM SQL.
- `internal_logical.sql`: the verified logical/Post-Process baseline from the tests.
- `internal_partial.sql`: the DFC two-phase/Partial-Push rewrite.
- `internal_full.sql`: the standard DFC/Full-Push rewrite.
- `internal_*_explain.txt`: PostgreSQL plans for the three internal rewrites.

## Where the internal rewrites are verified

- `sql_rewriter/test_tpch.py` contains exact expected SQL for the standard fused
  rewrite. Q01 starts at line 57; the suite covers the 13 supported queries.
- `sql_rewriter/test_tpch_two_phase.py` defines the supported query set and exact
  two-phase SQL in `EXPECTED_TWO_PHASE_SQL`. The parametrized test at line 840
  checks both the SQL and multiset equality with standard DFC.
- `vldb_2026_big_paper_experiments/tests/test_tpch_rewriting.py` defines
  `LOGICAL_EXPECTED_SQL` using `AVG(lineitem.l_quantity) >= 30`. Each per-query
  test checks the exact logical SQL and compares its result with DFC. Q18 is
  currently skipped because of a physical-lineage segfault, so the logical Q18
  SQL is specified but not executed by that test.

## Result

GProM is in the same rewrite family as the project's `internal_logical` baseline:
it computes the original result, joins it back to contributing rows, and evaluates
the policy in an outer aggregate. It does not produce either the two-phase summary
join or the fused DFC plan.

The SQL is not textually identical. GProM includes full provenance columns, uses
null-safe group-key joins, and groups the final result by every output value. The
internal logical baseline retains only required source columns, normally joins on
ordinary equality, and uses `MAX(base_query.aggregate_output)` when reconstructing
an output tuple. On TPC-H data these are relationally equivalent because the group
key determines the aggregate outputs and the relevant keys contain no nulls.

The following table counts physical `lineitem` scan nodes in the PostgreSQL plan.
It is a compact indication of repeated work, not a runtime measurement.

| Query | GProM | Logical | Partial | Full | Main GProM difference |
|---|---:|---:|---:|---:|---|
| Q01 | 2 | 2 | 2 | 1 | Null-safe join-back becomes nested loop + materialization |
| Q03 | 3 | 2 | 2 | 1 | LIMIT causes an additional copy of the grouped query |
| Q04 | 3 | 2 | 2 | 1 | Nested existence/group provenance adds another scan |
| Q05 | 2 | 2 | 2 | 1 | Same broad shape as logical baseline |
| Q06 | 2 | 2 | 2 | 1 | Global aggregate joined back to contributors |
| Q07 | 2 | 2 | 2 | 1 | Same broad shape as logical baseline |
| Q08 | 2 | 2 | 2 | 1 | Same broad shape; GProM adds materialization |
| Q09 | 2 | 2 | 2 | 1 | Same broad shape; GProM adds materialization |
| Q10 | 3 | 2 | 2 | 1 | LIMIT causes an additional copy of the grouped query |
| Q12 | 2 | 2 | 2 | 1 | Same broad shape as logical baseline |
| Q14 | 2 | 2 | 2 | 1 | Global aggregate joined back to contributors |
| Q18 | 7 | 5 | 4 | 2 | Nested aggregate + LIMIT compound GProM's join-backs |
| Q19 | 2 | 2 | 2 | 1 | Global aggregate joined back to contributors |

The most pronounced estimated-plan differences are Q03, Q04, and Q18. PostgreSQL
estimates the GProM plans at roughly 1.04 billion, 430 million, and 1.80 billion
cost units, respectively, compared with about 477 thousand, 775 thousand, and
2.07 million for the internal logical plans. These estimates are not elapsed time,
but they expose the cardinality explosion caused by joining aggregate results to
raw contributors before regrouping.

## GProM frontend compatibility

The Docker image reports GProM 1.0.5. Although it targets PostgreSQL, its working
SQL frontend is the Oracle parser/analyzer; the PostgreSQL analyzer reports "not
implemented yet." Semantics-preserving input normalizations were therefore needed:

- `EXTRACT(YEAR FROM x)` was expressed as `date_part('year', x)`.
- PostgreSQL date literals and problematic `BETWEEN`/`IN` forms were expanded.
- Q04's `EXISTS` and Q18's `IN` subquery were expressed as joins against grouped,
  unique subqueries. The original GProM forms fail during parsing or provenance
  rewriting.

These normalized forms are recorded in every `provenance_only_input.sql` and
`gprom_input.sql`; the benchmark query files were not modified.

## Reproduction

The PostgreSQL container is named `gprom-tpch-postgres`, uses port 55432, and is
loaded from the existing SF1 CSV export. To regenerate GProM plans:

```bash
python3 third_party/provenance_engines/gprom_tpch_comparison/run_gprom_tpch.py
```

To regenerate internal plans, run with the experiment virtual environment:

```bash
cd vldb_2026_big_paper_experiments
.venv/bin/python ../third_party/provenance_engines/gprom_tpch_comparison/generate_internal_plans.py
```
