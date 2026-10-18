Expanded Regulation Z query-plan diagnosis (2026-10-17)
======================================================

Conclusion
----------
The dominant measured cost is DuckDB optimization of a very large, duplicated
plan. The prior experiment's execute_ms includes binding, optimization, physical
planning and execution; it does not isolate running operators over data. The
previous explanation that execution over data accounted for almost all of the
slowdown was too coarse.

Environment and procedure
-------------------------
Local experiment virtualenv, DuckDB v1.3.0, 8 threads, Rust Passant via the same
Connection/Policy bindings as v3. Generated 100-account tied workload; expanded
preparation deletes the three summary tables. One representative accepted UPDATE
was rewritten once using all 31 policies, then executed repeatedly on the same
in-memory database. Repeating this assignment is idempotent. Timings below are
medians of three non-profiled executions, not rewrite time. Separate detailed
JSON profiles use enable_profiling='json' and profiling_mode='detailed'.
This is a diagnostic experiment, not a controlled performance publication.

Small input, huge plan
---------------------
Input: 200 proposals, 800 proposal lines, 800 balance snapshots, 500 billing
cycles. Original query has 3,560 physical operators, including 899 HASH_JOIN,
643 SEQ_SCAN, 292 HASH_GROUP_BY, 218 LEFT/RIGHT_DELIM_JOIN, and 30 WINDOW.
Largest actual operator output: only 800 rows. No temporary-disk spill.
The profiler reports about 1.93 GB peak buffer memory on this connection; this
is not a per-operator allocation or process RSS measurement.

The SQL contains 31 copies of base, 18 of lot_terms, 10 of lots, and 7 each of
slices/pairs. CTE inlining and repeated references within those CTEs multiply
these into even more branches. Physical scans include:
  regz_proposals: 131
  regz_payments: 131
  regz_proposal_lines: 106
  regz_balances: 80
  regz_cycles: 54
Most proposal scans have no candidate filter and return all 200 proposals.
Total cumulative scanned rows: 228,100 for one target UPDATE. This is repeated
small-table work, not evidence of one huge intermediate join result.

Profiled time breakdown (original.profile.json)
----------------------------------------------
DuckDB latency:                       0.696 seconds
All optimizer passes:                 0.472 seconds (about 68% of latency)
  Statistics propagation alone:       0.398 seconds (about 57% of latency)
Binding/logical planning:              0.039 seconds
Physical planning:                    0.008 seconds
These are nested measurements: statistics time is inside optimizer time, and
binding is inside planner time. Operator CPU timings can overlap across threads
and must not be subtracted from wall-clock latency as if all were serial.

Measured interventions
----------------------
Variant                             median seconds  operators  scans
Original                               0.673          3560      643
MATERIALIZED local CTEs                 0.367          2804      343
Restrict base to selected batch         0.697          3606      643
Disable statistics_propagation          0.310          3417      645
Share identical CTE definitions         0.664          3560      643
Share + MATERIALIZED CTEs               0.199          1827      162

MATERIALIZED here means statement-local intermediate results, not restored
persistent summary tables. Simply sharing definitions does not help because the
optimizer still inlines them. Explicit materialization plus sharing reduces
statistics propagation from 0.398 to 0.052 seconds and all optimization from
0.472 to 0.081 seconds. Scanned rows fall to 66,300. The remaining plan still
contains 430 hash joins, many correlated scalar checks, and the policy bindings.
This proof of concept is therefore not the final optimized design.

The diagnostic sharing transform first verifies that every repeated CTE name
has exactly the same SQL definition, hoists one copy of each in dependency order
to the UPDATE's WITH clause, and sets AS MATERIALIZED. All constraints remain.
All 100 generated updates were then rewritten this way and matched their expected
allow/reject decisions. This checks this workload, not every possible query or
the broader catalog scenario suite. Production runner/policies were not changed.

Disabling statistics propagation is an attribution experiment, not a recommended
global setting. It could hurt other queries. Scoping base to a known selected
batch was also a diagnostic literal specialization, not a general implementation.

Other evidence
--------------
Data-size medians: 2 accounts 0.663 s; 10 accounts 0.714 s; 100 accounts 0.761 s.
A 50x change in data size has little effect compared with the fixed plan cost.
These runs overlapped other local diagnosis work; use them as qualitative support.

Preparing the original query once took 0.528 s; subsequent EXECUTE calls took
0.128, 0.245, 0.159 s. Prepared reuse avoids much of the optimizer cost but leaves
repeated execution branches. This used the exact same literal query; it is not
proof that the existing per-account query strings can automatically share a plan.
One thread did not solve the problem (0.986 s wall versus 0.908 s in the initial
profiled 8-thread probe). Do not interpret these as controlled thread scaling.

Recommended direction
---------------------
Reduce duplicated plan structure: share statement-local evidence computations,
materialize appropriate reused intermediates, and compute related aggregates
jointly rather than as many independent correlated scalar subqueries. Then push
candidate/account scope into those computations and investigate reusable prepared
queries. Revalidate against the full scenario suite before adopting any rewrite.
Adding indexes or tuning thread count is not supported as the primary fix by
these measurements.

Artifacts
---------
query.sql / original.sql: original expanded rewritten UPDATE.
*.sql: exact diagnostic query variants (optimizer-disable variant additionally
requires SET disabled_optimizers='statistics_propagation').
*.profile.json: full detailed profiles, compacted without removing fields.
default.json / one_thread.json: initial profiles.
*.plan.txt: textual EXPLAIN output from initial probe.
plan-summary.json: operator counts, table scan counts, profile timing summaries.
ablations.json / sharing.json / data_size.json: raw sample times and summaries.
