Expanded Regulation Z DFC policies
=================================

Purpose
-------
These 31 policies expand the existing operational constraints: the calculations
previously performed by Python and stored in regz_context are SQL inside each
policy. Each policy contains its own required CTEs, joins, date calculations,
counts, sums, and comparisons. CTEs are local expressions, not installed views.
The original short policies and experiment remain available unchanged.

Files
-----
P*.pgn: readable policy definitions. These are not claimed to have passed the
PGN text parser: its nested-parenthesis parser stalled for several minutes.
policies.json: exact constraints, descriptions, text lengths, and SHA-256 hashes.
correction/P19.pgn and P20.pgn: alternate workflow bindings for correction
completion evidence; these are two bindings of existing checks, not extra rules.
ties/ and no_ties/: paired DFC-only decisions, rewritten SQL, experiment CSV,
and an isolation audit running each of the 31 policies separately.

Execution uses the same Rust-backed path as the real Regulation Z v3 runner:
build_expanded_policies() constructs Policy specifications, Connection registers
them through Rust PyPlanner.register_policy_specs, and transform_query delegates
to Rust PyPlanner.transform_registered. DuckDB executes the rewritten SQL.
Python is the experiment harness and binding layer, not a replacement rewriter.
No core DFC engine code was changed. Only the PGN text parser is bypassed; the
real v3 runner also constructs Policy objects instead of loading PGN text.

Reproduce from vldb_2026_big_paper_experiments
-------------------------------------------
source setup_local_smokedduck.sh
.venv/bin/python scripts/run_regulation_z_expanded.py --num-accounts 100 --runs 1 --warmups 0 --audit-policies
.venv/bin/python scripts/run_regulation_z_expanded.py --num-accounts 100 --runs 1 --warmups 0 --audit-policies --no-ties

The runner uses experiment_harness. It compares original and expanded decisions
against the generated expected result, including exact account IDs. It removes
regz_context, balance_components, and allocation_snapshot_context from the
expanded database before policy registration and execution. The Python context
derivation is skipped. The UPDATE's assignment and candidate selection are
preserved; redundant joins to materialized evidence are removed.

This generator's --no-ties option separates APRs; it retains the balanced valid
and invalid labels. It does not make every proposed UPDATE valid.

Correctness tests
-----------------
tests/test_regulation_z_expanded_differential.py replays 62 catalog scenarios.
For each valid input reaching derivation, it compares every derived field and
each of the 31 policy decisions against the original Python/context calculation.
Malformed-input cases retain the original adapter's rejection tests; these are
not proof that the expanded constraints independently validate malformed inputs.

tests/test_regulation_z_expanded_execution.py executes all 31 policies without
summary tables in both tie modes. It separately checks correction credit,
reversal, recipient, dispatch date and content-reference cases, a changed proposed
amount, and changed raw dispute evidence after preparation (9 tests).

Challenges and boundaries
-------------------------
1. Derived DIMENSION queries parsed but failed registration in the installed
   engine. Inline IN subqueries with local CTEs work in the tested workflows.
2. This DuckDB build rejects certain correlated outer joins. Request calculations
   use explicit prejoined CTEs inside the policy instead.
3. The optimizer incorrectly evaluated the credit join with a LEAST(date,date)
   bound. The expansion uses equivalent separate upper-bound comparisons.
4. Target-column substitution inside an IN subquery can read the old target
   value. The changed allocation amount is checked in an outer scalar equality,
   where the tested rewriter substitutes the proposed expression correctly.
5. Deeply nested PGN text parsing stalled; structured policy registration through
   the Rust Python bindings works, using the same API path as the v3 experiment.
6. Repeating evidence calculations across policies produces large queries and
   considerably higher rewrite/execution costs. Timings are diagnostic: one run,
   no warm-up, with other local work occurring. They exclude evidence generation,
   policy registration, result readback and allocation resets; the materialized
   approach also excludes its up-front derivation cost.

The v3 binding is deliberately specific to its existing single-column UPDATE.
It verifies the proposed cash allocation matches the evaluated proposal, binds
payment/account/date/amounts, and excludes extra positive allocation categories.
It has not been verified for arbitrary UPDATE shapes or multiple assignments.
Policies still have their configured source/sink applicability: this is not a
proof against alternate unmediated write paths.

Input evidence must be authoritative, complete, and valid under the original
adapter's contract. These 31 regulatory predicates do not independently replace
all Python EvidenceError checks (for example missing snapshots, missing programs,
account scope, or malformed evidence). There is no caller-supplied compliance
Boolean, but business facts such as affirmative requests, disputed amounts,
contract terms, and actual dispatch still require trustworthy evidence.

Staged proposals remain relational inputs. Their APR, window, classification,
and allocation claims are checked by the applicable constraints. Correction
checks gate acceptance of a staged completion; they do not schedule an action
or independently prove a real-world dispatch happened.

This expansion preserves the case study's existing disambiguations and scope.
It does not make the P24 classifier equivalent to every external definition, or
resolve 'generally', 'from time to time', feasibility, or other legal questions.
Agreement with the reference establishes tested operational equivalence, not
complete legal coverage or correctness for every possible database/query.

Verified results
-----------------------
Ties: 100/100 decisions match both the reference and expected labels (50 allowed,
50 blocked). Each of 31 policies was independently run on all 100 updates;
only P17 blocked any, and it blocked exactly the 50 expected account IDs.

Tests: the existing catalog/v3/policy-text/top-k suites plus the initial 8 expanded
execution cases passed together (96 passed). The final expanded execution suite,
including sink-value binding and evidence freshness, passed 9/9. The separate
all-policy differential replay passed 62/62. Ruff passed for src/, tests/, and
the new runner. These test counts overlap and should not be added naively.

No ties: 100/100 decisions also match both the reference and expected labels
(50 allowed, 50 blocked). Diagnostic total rewrite + execution times for 100
updates: ties 160.626 seconds expanded / 2.036 seconds materialized; no ties
171.784 seconds expanded / 2.077 seconds materialized. Do not interpret this
single-run comparison as a controlled performance benchmark.

Both isolation audits completed: 31 policies x 100 updates per mode; only P17
blocked writes (exactly 50 expected IDs). All other checks allowed all 100.
