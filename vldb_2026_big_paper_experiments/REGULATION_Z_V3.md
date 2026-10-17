# Regulation Z v3: credit-card payment allocation

V3 now registers the full catalog's **31 executable policies**, associated with the
40 propositions in `REGULATION_Z_53_FULL_POLICY_CATALOG.md`. The nine supporting
entries remain scope, permission, and definition support rather than additional
standalone checks. The catalog's stated assumptions and limitations still apply.
Earlier v3 result files used five policies and must not be labeled as 31-policy results.

## The isolated failure

Each account still has two fully outstanding $100 cash-advance transactions at
25% APR. One is disputed and one is not. The same APR-only `ORDER BY ... LIMIT 1`
selects one of them. The physical insertion order determines the tie in the tested
DuckDB build. Both alternatives produce the identical visible $100 cash-advance
allocation, and cases are balanced between the two physical layouts.

The current generator balances all four combinations of selected candidate (A/B)
and compliance (valid/invalid): a 100-case workload contains exactly 25 of each.
Each four-case block contains one of each combination, with its orientation
randomized by the workload seed; each two-case pair still has one valid and one
invalid allocation. This removes the accidental advantage of always assuming the
first-listed A candidate was selected. Earlier result files balanced only validity
and can have unequal A/B selection frequencies; do not mix them with this workload.

Only **P17, Minimize disputed reduction**, rejects the disputed selection. The
other 30 checks pass, either substantively or because their applicability condition
is false. The P18 residual check applies when disputed reduction is unavoidable;
P17 handles avoidable reduction. This removes duplicate rejection of this case
without weakening their combined condition. In particular, a request or correction
rule being registered does not mean the workload exercises that rule's failure path.

The original minimum-payment arithmetic is now an input-integrity check in the
full-catalog derivation, not an extra regulatory policy beyond the 31.

## Optional no-ties control

Tied APRs remain the default. Pass `--no-ties` to assign the intended selected
cash advance 25% APR and the other cash advance 24% APR. Distinct rate buckets
(`cash_advance` and `cash_advance_lower`) carry these rates through daily balances,
component rows, full-catalog snapshots/proposals, and model evidence. Both are still
cash-advance transactions. The SQL is unchanged: APR alone determines the top row.

This mode preserves the balanced allowed/rejected workload: half the accounts have
an undisputed highest-rate lot and half have a disputed highest-rate lot. Only P17
rejects the latter, even though the lower-rate undisputed alternative is available.
The selection is now inferable from the logical evidence regardless of insertion
order. This removes the tie ambiguity for models; it does not guarantee model accuracy.
The result CSV records `no_ties` so the two modes can be distinguished.

For a no-model control run, append `--no-ties` to the DFC-only command below and
use a different output filename. The flag also applies to model runs and to the
per-policy isolation audit.

## Extended data and derivation

The original `statements`, `transactions`, `daily_balances`, and `payment_allocations`
tables remain. `balance_components` and `allocation_snapshot_context` retain their
original materialized data. Additional `regz_*` relations use the runnable full
catalog's schema and computations, implemented in `regulation_z_v3_evidence.py`:

- Explicit consumer open-end, non-home-secured credit-card account facts.
- A $125 payment split into $25 minimum and $100 excess.
- Complete cycle-end and payment-date snapshots with both $100 cash-advance lots.
- A separate $25, 10%-APR lot receiving the entire minimum, with no dispute.
- A $50 deferred-interest lot, contractually 15% but zero for allocation, outside
  its priority window. Its interim-payment requirement does not disqualify it.
- Five contiguous actual billing cycles, the contractual expiration date, and a
  cycle-end reference-day practice with no changes.
- Two complete candidate proposals, each allocating the excess to a different
  cash-advance lot, plus their proposed program classifications.
- Empty request and correction relations: no request exception is elected, and
  no billing-error determination or correction completion is proposed.

The auxiliary minimum and promotional lots are in the detailed `regz_*` ledger;
they do not enter the original cash-advance top-1 candidate selection. All data is
synthetic. `derive_allocation_context` derives each candidate's context from these
facts. Neither the expected selection nor the expected valid/invalid label is used.
Both candidate contexts are materialized, keyed by candidate transaction ID.

The UPDATE joins `regz_context.batch_id` to the transaction actually selected by
the original tied subquery. The 31 catalog predicates are reused verbatim, rebound
from the general runtime's acceptance sink to v3's `payment_allocations` sink.
Every policy is registered separately. There is no additional hidden dispute policy.

This is a fixed-query benchmark adapter: its trusted preparation computes contexts
for the exact generated candidate proposals. Altering allocation amounts, evidence,
or workflow mode requires regenerating those contexts. It is not a general guard
for arbitrary subsequent SQL or mutable/stale context tables. The general catalog's
transaction adapter remains separate.

Model prompts now include all 40 proposition descriptions and the additional
logical evidence, including both candidate contexts. Rows are serialized in stable
logical order, not physical insertion order, so the selected alternative is not
revealed. No oracle labels are included. The DFC-only run makes no model calls.

Each proposition now includes its exact decision condition rendered into plain
language directly from the executable predicate. Supporting propositions are marked
as imposing no separate rejection condition. The shared renderer fails on unknown
predicate syntax rather than silently omitting a condition. Prompt instructions
define context selection, trusted derived facts, exact comparisons, cents versus
dollars, inclusive dates, and the dispute and priority derivations. Tests compare
every supplied context row with the rows used by execution in tied and no-ties
modes, and verify that every predicate input is exposed. Actual selected-contributor
provenance remains the experimental information difference. Older model results
predate these prompt clarifications and are not measurements of the revised prompt.
Conditions use applicability-first wording, preserving the predicate's Boolean logic.
The prompt makes acceptance an if-and-only-if decision and gives the decision
conditions precedence over explanatory descriptions. It explains overlapping ledger
representations, snapshot deduplication, lots and slices, and the distinction between
an active program and its priority window. Models may use the supplied derived facts
without independently reconstructing each regulatory calculation.

## Running and auditing

V3 uses one shared request pool across all selected models, bounded by
`--max-concurrency` (default 50). Submission interleaves models for each case so a
slow model does not prevent other models from starting. SQL execution and progress
writes remain serialized on the coordinator thread. Overall duration counts the
shared pool's wall time once, not the sum of overlapping model wall times.
Model wall time measures completion from the shared pool start, including queueing.
Individual request failures are written to progress and counted as `api_errors`;
they do not discard other in-flight results. Accuracy includes API errors and
unparseable responses as incorrect; parsing failures are separate from false
positives/negatives. Always report `completed_requests` alongside it. The Bedrock client allows a
600-second read timeout and disables automatic SDK retries to avoid silently
repeating paid inference calls. Recovery tooling can pass `skip_model_updates`
(model key to completed update numbers) in strategy config to retry only missing
responses; recovery summaries cover only that invocation, not prior results.
The CLI `--skip-first-updates 10 --num-updates 100` runs cases 11–100 for models,
while evaluating all 100 inexpensive baseline cases. Use
`scripts/merge_regulation_z_v3_progress.py` to combine the original, recovery, and
continuation progress files; it validates identities and complete case coverage,
keeps the first returned response (even if unparseable), and replaces only API
errors with successful recovery responses. Its summary applies current reporting
semantics to older raw rows without modifying the source logs.

Responses request only `{"allow_update": true|false}`, with no explanation field.
Progress CSV rows preserve the exact `prompt` and `raw_response` for auditing.
The October 14 eight-model 100-case run requested a `reason` field; those historical
results are unchanged and are not measurements of this decision-only response format.

Grok 4.7 is selectable with `--models grok_4_7`. Its `us-east-1` application profile
`7nsy832zbewd` copies `us.xai.grok-4.7` and carries `billing-tag1=cgs2161` and
`project=data-flow-control`. It uses Bedrock InvokeModel, provider-default sampling,
and an 8,192-token output budget. A UUID nonce avoids implicit prompt-cache reuse.
Configured US cross-region prices are $2.20/M input, $6.60/M output, and $0.55/M
cached input. It is an additional closed-weight option, not a default model.

MiniMax M2.5 is available with `--models minimax_m2_5` (or alongside other model keys).
It uses the existing `us-east-1` application profile `6ruvu29x7n10`, tagged
`billing-tag1=cgs2161` and `project=data-flow-control`, through Bedrock InvokeModel.
Sampling uses provider defaults, with an 8,192-token output budget for reasoning and
the final decision. Embedded reasoning is excluded from decision parsing but retained
in the raw response and token accounting. No automatic-cache nonce is configured for
MiniMax. Its configured US token prices are $0.30/M input and $1.20/M output.
The model is selectable but is not added to the default closed-model run.

From this project directory:

```sh
source setup_local_smokedduck.sh
.venv/bin/python scripts/run_regulation_z_v3_benchmark.py \
  --dfc-only --audit-policies \
  --num-accounts 100 --num-updates 100 \
  --runs 3 --warmups 1 \
  --output-filename regulation_z_v3_full31_dfc_2026-10-14.csv
```

`--dfc-only` runs no no-policy baseline and creates no model clients or requests.
`--audit-policies` additionally executes every policy alone against the workload,
compares the exact blocked allocation IDs with the expected set, and saves a
`.policy-audit.csv`. It fails if any policy other than P17 rejects a case, or if
P17 rejects the wrong cases. The `.progress.csv` records each measured/warmup decision.

Timed DFC measurements include query rewriting and execution. Synthetic evidence
generation and context materialization occur during database preparation and are
excluded from those timings; the separate policy audit is also untimed. These numbers
must not be described as end-to-end workflow cost or compared directly with old
five-policy timings without noting the changed setup.

Without `--dfc-only`, the existing model and no-policy paths remain available with
the extended evidence and policy descriptions. Model runs incur the usual provider
charges. Tests use a fake provider and make no external requests.

## Validation

Tests execute all 31 policies together and each separately. A separate test reverses
physical transaction order without changing the contexts or oracle labels and verifies
that the actual selected contributor determines acceptance. Tests also cover balanced
labels, full-catalog registration, prompt evidence, the DFC-only path, and the original
full-catalog scenarios. These checks validate this generated workload, not exhaustive
coverage of every regulatory branch.


## Recorded DFC-only result (2026-10-14)

The command above completed one warmup and three measured runs of 100 cases.
Every measured run accepted all 50 undisputed selections and blocked all 50 disputed
selections (100/100 correct). The isolated audit executed 3,100 updates: P17 blocked
exactly the 50 disputed cases; each other policy allowed all 100 cases. No model
requests were made.

Mean measured rewrite-plus-execution time was 880.621 ms per 100 allocations
(8.806 ms per allocation), comprising 434.291 ms rewriting and 446.330 ms execution.
Evidence preparation is excluded. Files:

- `results/regulation_z_v3_full31_dfc_2026-10-14.csv`
- `results/regulation_z_v3_full31_dfc_2026-10-14.progress.csv`
- `results/regulation_z_v3_full31_dfc_2026-10-14.policy-audit.csv`
