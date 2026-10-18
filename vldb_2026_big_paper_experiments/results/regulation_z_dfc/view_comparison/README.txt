Regulation Z: unchanged policies over a live view
================================================
2026-10-17

Only regz_context changes from a populated table into an ordinary DuckDB view.
Its definition is allocation_facts_sql(), the same relational computations used
in the expanded policies. All 31 original policy constraints are unchanged.
The original UPDATE is unchanged. The table and view approaches produce exactly
the same rewritten SQL (5,421 bytes); DuckDB resolves regz_context differently.
The other original input-query tables remain present in the table/view versions.

Results: one measured pass over the same 100-account tied workload, 8 threads,
DuckDB v1.3.0, no explicit warm-up. Rust policy registration and rewriting are
used in all three approaches. Each approach runs serially in its own connection.

Approach          rewrite seconds   DuckDB seconds   total seconds   ms/update
Materialized           0.486              0.617            1.103         11.0
Live view              0.504             33.284           33.789        337.9
Expanded               5.437             76.664           82.101        821.0

All three produced 100/100 expected decisions: 50 allowed, 50 rejected.
The view is 2.43x faster than expanded and 30.63x slower than materialized.
Compare these same-session numbers, not the earlier 160-second expanded run.
DuckDB seconds include binding, optimization, physical planning and execution.
Evidence preparation, view creation, registration and result readback are excluded;
in particular the table's up-front derivation cost is not included.
This is a single diagnostic pass, not a controlled repeated performance study.

Separately profiled representative query (profiling excluded from timed runs):
                  operators    table scans   optimizer seconds
Materialized            31           8             0.0019
Live view             1874         336             0.2015
Expanded              3560         643             0.5312

The view groups derivations behind one source relation rather than repeating a
separate definition for each policy. It reduces Rust rewrite time and DuckDB plan
size. However, ordinary views are not cached: DuckDB expands their SQL and still
repeats CTE/subquery branches. The view profile spends 0.163 seconds in statistics
propagation alone. This explains why a small dataset can still be slow.
No special MATERIALIZED CTE hints or optimizer overrides are applied here.

Validation and scope
--------------------
All view columns for all 200 proposals were compared with the reference table
before the measured run. Two tests cover tied and non-tied workloads, actual
policy execution, and freshness: changing protected_cents in the underlying
balances immediately changes the decision without rebuilding regz_context.
Ruff passes for src/, tests/, and the new runner.

The view exposes allocation contexts for the v3 workload, not a general
replacement for correction-completion contexts. P19/P20 are inapplicable to
these allocation records, just as in the existing benchmark. Existing trusted
input and legal interpretation assumptions remain. This is not new legal coverage.

The installed DuckDB adapter normally catalogs tables only. A local subclass
includes views in the catalog supplied to Rust (the same pattern as the existing
v2 Rust runner). No Rust planner or core adapter changes were needed.

Reproduce from vldb_2026_big_paper_experiments:
  .venv/bin/python scripts/run_regulation_z_view.py --num-accounts 100
  .venv/bin/python -m pytest tests/test_regulation_z_view.py -q

The script also supports --runs, --warmups, --no-ties and --output.
ExperimentRunner's generic duration_ms retains the inherited expanded duration;
use the explicit materialized_total_ms, view_total_ms and expanded_total_ms
columns for the three-way comparison.

Artifacts: comparison.csv, per-update run-1.decisions.csv, exact view.sql,
rewritten *.example.sql, full compact *.profile.json and verification.json.
Production v3 defaults are unchanged; this is a separate comparison runner.
