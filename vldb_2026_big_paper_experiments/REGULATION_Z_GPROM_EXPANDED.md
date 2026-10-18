# Expanded Regulation Z policy experiment

The expanded path computes compliance facts from normalized evidence within each
SQL statement. It does not read `regz_context`, `balance_components`, or
`allocation_snapshot_context`; all three tables are omitted from its database.
The existing uncommitted expansion module supplies the allocation calculations.
`regulation_z_expanded_validation.py` adds evidence-contract checks and
`regulation_z_expanded_gprom.py` translates and exports SQLite SQL.

## Reproduce

From `vldb_2026_big_paper_experiments`:

```sh
.venv/bin/python scripts/run_regulation_z_v3_benchmark.py \
  --export-gprom results/regulation_z_gprom/my-expanded-run \
  --expanded-gprom --run-gprom --num-accounts 4 --num-updates 4 --no-ties
```

Omit `--no-ties` for the tied-APR experiment. Use a new output directory; export
refuses to overwrite a database. No model calls are made by this export path.

Each export includes:

- `P01.sql`, etc.: 31 standalone predicate queries, each containing its evidence
  computations. Allocation files return acceptable batch IDs. P19/P20 instead
  evaluate correction-completion evidence, and return passing correction rows.
- `allocation-facts.sql`: all allocation fact derivations for inspection.
- `policy.template.sql`: the complete 40-proposition report with a placeholder
  for the actual GProM-generated SELECT.
- `ALLOC*.sql`: fully composed read-only SQL, with parameters in adjacent JSON.
- `ALLOC*.gprom.sql`, `.generated.sql`, `.gprom.txt`: actual reenactment input,
  generated backend SQL, and engine output.
- `ALLOC*.result.json` and `results.json`: proposition results, decisions,
  database hashes, image identity, and backend policy-evaluation timings.

There are 31 independent rejection predicates, not 40. Supporting definitions,
scope, and permissions remain represented in the full 40-row report. P24 uses
program terms directly. P19/P20 are not applicable to the allocation UPDATE and
are exercised separately against correction evidence.

## Verified runs

`results/regulation_z_gprom/expanded-final-no-ties` and
`results/regulation_z_gprom/expanded-final-ties` contain live GProM experiments.
All eight proposals matched expectations (four ALLOW, four REJECT), and both
database hashes were unchanged. Each run has four UPDATE proposals. Expected outcome is judged against the actual
backend-selected balance, because tied SQL ordering is not portable.

`expanded-final-no-ties/standalone-audit.csv` records exact accepted-ID agreement
for all 29 allocation predicates on eight evidence proposals each (232 comparisons).
The complete composed query is 59,803 bytes / 1,458 lines; P01 is 13,453 bytes
and P17 is 10,558 bytes. Backend evaluation took 5.4–7.0 seconds per proposal
in these eight runs.

The tests replay the catalog scenario suite with field-by-field comparisons
between Python derivations, DuckDB SQL, and SQLite SQL at the allocation adapter
boundary. Evidence errors are also checked against the SQL input validator.
The combined regression run passed 159 tests in 234.48 seconds; the subsequently
added standalone allocation audit test passed separately (1 test). Ruff and
`git diff --check` passed.

Additional tests execute all 29 standalone allocation predicates against the
materialized catalog, exercise both correction predicates (including reversals,
late/misdirected/blank notices and missing items), and run actual GProM with and
without ties. A missing balance must yield INPUT_ERROR in the full report.

```sh
source setup_local_smokedduck.sh
GPROM_TEST_DOCKER=1 .venv/bin/python -m pytest \
  tests/test_regulation_z_expanded.py tests/test_regulation_z_gprom.py \
  tests/test_regulation_z_v3_benchmark.py \
  tests/test_regulation_z_53_full_policy_catalog.py \
  tests/test_regulation_z_payment_allocation_topk.py -q
.venv/bin/python -m ruff check src/ tests/ scripts/run_regulation_z_v3_benchmark.py
```

## Challenges and limits

1. Generic SQL transpilation was insufficient. Nested scalar GREATEST/LEAST,
   date arithmetic, and the VALUES column alias needed explicit SQLite forms.
   An initial AST replacement missed nested min/max calls; translation now
   preserves all operands before dialect serialization.
2. DuckDB rejected a correlated outer join in the input validator. Joining a
   local proposal row before the historical-balance outer join resolved it.
3. Unrestricted CTE expansion made the combined SQLite statement impractically
   slow; exploratory executions were interrupted. `AS MATERIALIZED` computes
   local CTEs once within the statement. This needs a SQLite version supporting
   that syntax; it creates no persistent evidence or compliance tables.
4. Even with those hints, the combined report takes seconds on this small
   fixture. Timings include backend preparation/execution/fetch, exclude Docker
   and GProM rewrite time, and were collected with other work running. They are
   correctness-smoke measurements, not a controlled scalability comparison.
5. GProM still reenacts a frozen single-target UPDATE. Its generated SELECT is
   composed with the expanded SQL and executed by host SQLite. The large report
   is not parsed wholesale by GProM, and provenance of all policy calculations
   or the original ranking/selection is not claimed.
6. Raw evidence means the normalized relational evidence model, not contract
   text or independently proven snapshot completeness. Program terms, dispute
   facts, supplied minimums, calendar records and snapshot completeness markers
   remain trusted inputs. The read-only experiment does not install a production
   write gate or solve concurrency between evaluation and an eventual UPDATE.

The exported SQL uses CTEs to make computations inspectable. There are no
precomputed summary dependencies, but this is not a demand to eliminate every
subquery or intermediate relational expression.
