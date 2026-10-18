# Regulation Z v3 on GProM

The existing v3 data generator can export its complete tables and derived evidence
to SQLite, execute actual GProM reenactment, and evaluate the full 40-proposition
query. This path makes no model calls and never applies an allocation UPDATE.

From this project directory:

```sh
.venv/bin/python scripts/run_regulation_z_v3_benchmark.py \
  --export-gprom results/regulation_z_gprom_new --run-gprom \
  --num-accounts 4 --num-updates 4 --no-ties
```

Use a fresh output directory: the exporter refuses to overwrite an existing
`data.sqlite`. Omit `--run-gprom` to export without Docker. Omit `--no-ties` to
exercise the original tied selection. The runner judges the actual selected
contributor, not the generator's nonportable tie expectation. Use `--gprom-image`
to select another installed image. Docker must have access to the output folder.

## What executes

1. The existing DuckDB generator builds the CCS-shaped tables, both candidate
   proposals, detailed `regz_*` evidence, and trusted context summaries.
2. The exporter copies those tables into a new SQLite database, preserving row
   order, primary keys, non-null constraints, ISO dates, and integer-cent fields.
3. A read-only SQLite SELECT projects the original UPDATE's joins, filter, scalar
   top-1 selection, and right-hand-side value. Its sole candidate is frozen.
4. The runner invokes the actual GProM image with `REENACT WITH PROVENANCE` over
   the frozen target and proposed amount. It saves the engine's returned output.
5. A second GProM invocation with `-Pexecutor sql` obtains the generated SELECT.
   That **actual output**, not a canned handwritten preview, fills the proposed
   state CTE of `regulation_z_gprom.sql`.
6. SQLite executes the combined read-only policy query with the frozen candidate
   parameters. Every case must return exactly P01 through P40 and the expected
   decision for its actual selected balance. Original zero allocations and the
   complete database file hash must remain unchanged.

GProM mounts the fixture read-only, with container networking disabled. The host
connection opens it read-only and sets `query_only`. This is hypothetical
evaluation, not an UPDATE followed by a rollback. Use an isolated fixture with no
other writers: this experiment is not a production concurrency/enforcement layer.

## Output

- `data.sqlite`, `manifest.json`: complete fixture and case metadata.
- `ALLOC*.update.sql`: original UPDATE for inspection, never automatically run.
- `ALLOC*.candidate.sql`: backend candidate projection.
- `ALLOC*.gprom.sql`, `ALLOC*.gprom.txt`: actual reenactment request and output.
- `ALLOC*.generated.sql`: GProM's emitted backend query, including provenance.
- `ALLOC*.sql`, `ALLOC*.parameters.json`: full composed query and value bindings.
- `ALLOC*.result.json`: all 40 proposition results.
- `results.json`: decisions, image ID, and before/after hashes.

For example, replay a saved full query without invoking GProM again:

```python
import json
import sqlite3
from pathlib import Path

folder = Path("results/regulation_z_gprom")
connection = sqlite3.connect(
    (folder / "data.sqlite").resolve().as_uri() + "?mode=ro", uri=True
)
connection.execute("PRAGMA query_only=ON")
rows = connection.execute(
    (folder / "ALLOC0000001.sql").read_text(),
    json.loads((folder / "ALLOC0000001.parameters.json").read_text()),
).fetchall()
print(rows)
connection.close()
```

## Actual run

The saved four distinct-APR cases in `results/regulation_z_gprom/results.json`
returned `ALLOW`, `REJECT`, `REJECT`, `ALLOW`. The two rejected cases selected
disputed cash-advance balances. All proposed visible amounts were $100. Source
database hashes were identical before and after evaluation.

Image ID:
`sha256:e151559f97e830c8748b7573f13c249e7a64e2651e787d65cce0e9303a72e379`.

## Scope and implementation findings

- The original `UPDATE ... AS ... FROM` is not supported by this GProM frontend.
  The runner explicitly specializes its selected target and amount before
  reenactment. The selected balance ID is carried by the trusted adapter. Target
  provenance is real; provenance for why the top-1 candidate won is not claimed.
- The full policy query runs on the **SQLite backend**, around GProM's generated
  SELECT. This does not claim that the full wrapper parses through GProM's frontend.
- This sqlglot version drops the second `LEAST` argument when writing SQLite SQL.
  The candidate adapter uses an explicit CASE/MIN expression to preserve both the
  cap and DuckDB's NULL-ignoring behavior. Regression tests exercise both.
- SQLite NUMERIC affinity uses floating point for non-integral values; it is not
  an exact arbitrary-decimal engine. The benchmark's whole-dollar amounts and
  integer-cent evidence are preserved exactly. No rounding tolerance is added.
- All 40 propositions are represented, but only applicable rejection predicates
  affect the decision. Scope/permission entries are labeled accordingly. P24
  assumes normalized conditional avoidance, waiver, or refund facts. P19/P20 are
  inactive for allocation events. These fixtures do not cover every policy branch.
- The binding adapter intentionally targets the supplied single-cash-lot UPDATE,
  not arbitrary multi-target SQL. Trusted context must describe that exact complete
  proposal. Passing the evaluation does not execute or authorize a later unrelated
  write; production enforcement must preserve the checked state and proposal.

## Tests

```sh
source setup_local_smokedduck.sh
GPROM_TEST_DOCKER=1 .venv/bin/python -m pytest \
  tests/test_regulation_z_gprom.py \
  tests/test_regulation_z_v3_benchmark.py \
  tests/test_regulation_z_53_full_policy_catalog.py \
  tests/test_regulation_z_payment_allocation_topk.py -q
.venv/bin/python -m ruff check src/ tests/ scripts/run_regulation_z_v3_benchmark.py
```

Without `GPROM_TEST_DOCKER=1`, the two live Docker tests are skipped. Live tests
cover tied and distinct-APR workloads, all 40 output IDs, original target values
in GProM provenance, invalid binding rejection, and unchanged source tables.
