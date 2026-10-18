# Full enforcement authoring benchmark

This replaces the predicate-only task as the default generation target. Old prompts,
submissions, and grades remain intact. Saved Astra and Luna batches are under `../runs/`.

## What the model authors

| Target | Artifact | Supplied runtime boundary |
| --- | --- | --- |
| DFC | Any number of complete native PGN policies | Parse/register policies; execute original UPDATE through native DFC |
| GProM | Candidate SELECT, reenactment request, and one complete integrated decision SELECT | Execute supplied selection; substitute frozen values; invoke real GProM; splice engine output; execute supplied decision query |
| ProvSQL | One complete checked transaction with context provenance query | Existing extension/table setup; escaped target substitution; execute script |

The GProM files are required by the actual frontend/backend boundary; they do not
specify how to decompose the rules. Neither SQL target receives regulatory predicates,
a candidate-query body, frozen-choice SQL, gate logic, or a transaction skeleton.
DFC receives no prescribed policy count, list of enforcement IDs, or required grouping.

The common inputs are the full source proposition/assumption snapshot, the UPDATE,
schema, evidence definitions/invariants, and behavioral requirements. The old
policy-by-policy responsibility clarifications are deliberately not included. The
full UPDATE workload is allocation-only: correction execution is not claimed, and
all 40 propositions are supplied so the model must determine applicability.

The evidence service still supplies authoritative regz_context rows and correct
proposal/write preparation. This does not measure generating the evidence service.
Selection and enforcement are now authored/engine responsibilities, not invisible
predicate-grader assumptions. Fixtures should have a unique candidate ordering;
concurrency and ties are outside this initial full-workflow benchmark.

There is a documented output-scope asymmetry: GProM is read-only reenactment
preflight, while DFC/ProvSQL can enforce the actual write. Compare selection,
allow/reject behavior, and proposed/actual state; report this distinction rather
than claiming all three are identical transaction engines. All supplied runtime
code must be frozen and included in the experiment's methods description.

## Exact inputs

- prompts/exact-dfc.txt
- prompts/exact-gprom.txt
- prompts/exact-provsql.txt

The manifest hashes the input files. All prompts use the same task/specification/
evidence/schema prefix. No saved model outputs or baseline SQL templates enter them.
At generation time, each trial now prepends a fresh random UUID nonce to this base
prompt to reduce cross-trial input-prefix cache reuse. The exact transmitted user
prompt is saved in the trial's `prompt.txt`; metadata records `prompt_nonce`,
`base_prompt_sha256`, and `prompt_sha256`. The nonce is not derived from the schedule
seed, so separate batches also receive fresh identifiers. This is cache busting,
not a guarantee of zero cached tokens: shared Codex system/developer prefixes may
still be cached. Continue recording cached input tokens and disclose this protocol
change when comparing with earlier runs, which had no nonce.
The schema is copied from the saved SQLite fixture, with target type conventions
specified separately. Page provenance remains in `../prompts/source.json` relative to this directory;
the snapshot itself is unchanged.

## Current readiness

Prompt generation, structured file submission, output extraction, and envelope
validation are implemented. The container includes both old and new output schemas.
`generate` defaults to `--benchmark workflow`; `--benchmark predicate` reproduces
the earlier output contract. The default settings are Luna (`gpt-6-luna`), default reasoning, five
trials per approach, fifteen workers, and a one-hour per-trial timeout.

**Execution checking is automatic after each workflow trial.** The maintained
`policy_authoring.workflow_execution` module runs eight real-engine integration
cases in a separate process, with fixtures isolated per trial. Generation status
and timing stay separate from execution status and timing. Logs, per-case results,
and `execution-summary.json` are saved under the trial; `metadata.json` also records
the execution summary. Invalid submissions are recorded as `not_executable` and
remain in the success-rate denominator. No feedback or repairs go to the model.

The suite covers two permitted/two prohibited allocations, missing target,
missing candidate, missing selected context, and an unrelated bad context. It
checks application state, GProM hypothetical state, and selected ProvSQL provenance.
It does not establish every regulatory branch or general equivalence. Reference
policies are provisional, not legal ground truth. DuckDB/SQLite queries have a
20-second interrupt deadline; PostgreSQL statements have a 15-second deadline.
Evaluator failures are distinct from artifact failures. Execution can overlap other
trials' generation and consume host resources; generation wall time is not isolated.

To evaluate an existing trial without inference, call
`evaluate_workflow_trial(Path(...))` from `policy_authoring.harness`. The legacy
`score-run` continues to reject workflow artifacts. The broader coverage goals
below remain pending.

## Required execution evaluator

The evaluator must use actual DFC/GProM/ProvSQL runtimes, not just scalar predicates:

1. Load an isolated complete fixture per case/trial. Preserve before-state and
   independent expected original contributor and assignment. Use no shared mutable
   fixture between trial executions. Pin engine images/library versions.
2. DFC: load arbitrary PGN count using the native API, execute the unmodified
   parameterized UPDATE, inspect sink changes and absence of candidate substitution.
   Confirm the engine's rewrite preserves selection; do not add a hidden regulatory
   adapter to make failing native semantics pass.
3. GProM: execute the generated candidate SQL against read-only SQLite, validate its
   output contract, instantiate its reenactment with safely rendered frozen values,
   invoke real GProM, and execute the generated integrated decision query with its
   actual output. Validate both decision and proposed target state independently;
   reject a constant decision or ignored reenactment. Hash fixture before/after.
4. ProvSQL: use the actual extension, enable provenance on context only, execute the
   generated transaction, check before/after state and the provenance result. Confirm
   rejected writes do not persist and allowed writes match the original assignment.
   Distinguish syntax/runtime failure from justified policy rejection; a nonzero
   psql exit is not automatically a correct rejection.
5. Fixtures: include otherwise-valid acceptances and independent violations, source
   account isolation, selected-invalid/alternative-valid candidates, missing context,
   and unchanged non-target fields. Derive coherent contexts through the existing
   evidence builder; preserve legal assumptions and avoid contradictory probes.
6. Save raw artifact, executed rendered SQL, engine output/errors, selected ID,
   predicted/actual before/after values, decision, provenance checks, and disagreements.
   Use the current implementations as provisional comparators; do not expose them to
   the generating model. Recheck disagreements independently.

Primary correctness is complete enforcement behavior, not matching a count of
policy objects. Keep validity, selection, regulatory decision, mutation safety,
and provenance fidelity as separate outcomes. Per-policy attribution is optional,
so the model is not forced back into the old decomposition.
