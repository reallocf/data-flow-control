> **Active authoring task: full workflow.** See [workflow/README.md](workflow/README.md)
> and its exact prompts. `generate` now defaults to `--benchmark workflow`.
> Each workflow trial now automatically runs eight real-engine integration checks
> after generation. Comprehensive regulatory grading remains pending; do not use
> old predicate metrics to score workflow artifacts. The documentation below
> describes the earlier baseline.

# One-shot policy authoring with subscription-authenticated Codex

This is a proposed **policy-expression** experiment, not an end-to-end comparison
of DFC, GProM, and ProvSQL systems. Review the exact prompts before collecting data.
One DFC smoke trial has completed; no comparative batch has been run.

## Scope and supplied inputs

Each independent Astra session receives one complete prompt assembled from:

1. `prompts/task.md`: the task, supported 31 proposition IDs, scope, and JSON format.
2. `prompts/specification.md`: the supplied Page's full 40 propositions, legal links,
   example UPDATE, operational assumptions, and unresolved meanings. `source.json`
   records the Page identity and block hashes. Links are citations, not browsing inputs.
3. `prompts/evidence.md`: all 40 non-null context fields, types, meanings, units, and
   the shared upstream integrity contract.
4. `prompts/clarifications.md`: shared applicability, evidence relationships, timing,
   and the supported domain for correctness scoring (revision 2).
5. `prompts/{dfc,gprom,provsql}.md`: version-appropriate interface explanations and an
   unrelated shipping-policy example. These do not contain the Regulation Z predicates.

The exact user-prompt bytes are in `prompts/exact-dfc.txt`, `exact-gprom.txt`, and
`exact-provsql.txt`. These are user prompts; Codex also supplies its own standard
system/developer instructions. CLI version and configuration are pinned/recorded;
we do not claim that these files are the entire wire-level model request.

**Current defaults:** Luna (`gpt-6-luna`), default reasoning (no effort override), five independent trials
per approach, randomized submission order (seed 20261017), and a 3,600-second
wall timeout per trial. All 15 trials launch concurrently (`--parallelism 15`);
each has its own isolated container and timeout. Concurrent wall times reflect
shared host resources and subscription/service scheduling.
There is one generation per trial, zero feedback/repair rounds, and no warmup model
calls. Token usage is measured, not hard-capped. Report input/cached/output token
usage separately. Never add reasoning tokens to output without checking whether the
reported output total already includes them.

The model receives trusted summaries and an assumed fixed selection/reenactment/transaction
contract. Those adapter stages are not executed by this policy-layer evaluator. It generates 31 standalone policy artifacts:

- DFC: native PGN source/sink declarations with scalar Boolean conditions.
- GProM: SQLite backend policy queries for the supplied reenactment wrapper.
- ProvSQL: PostgreSQL policy queries evaluated with provenance disabled, as in the
  current wrapper; provenance explanation is supplied infrastructure.

The original UPDATE is contextual information, not a second code-generation task.
This experiment deliberately does **not** measure learning the engines, implementing
query adapters, raw-evidence derivation, rewriting, atomic posting, or provenance
circuit quality. In particular, GProM/ProvSQL are backend policy-authoring targets
here, not native provenance programs authored from scratch. A paper must retain
this qualification. This narrow scope may show smaller DFC advantages than a full
implementation experiment; it should not be enlarged silently to favor one approach.

## Isolation and authentication

`container/Dockerfile` installs Codex CLI 0.163.0. The generation container is
non-root, has a read-only root filesystem, capped resources, no host bind mounts,
no Docker socket, and an ephemeral tmpfs for HOME/CODEX_HOME. Only the task and a
ChatGPT auth record are sent through stdin. The auth record is never written to
experiment logs and is deleted at process exit/container removal. No host config,
AGENTS.md, memories, repository, existing policies, skills, or prior sessions are
mounted. The CLI ignores user config, disables memory read/write, skips host skill
discovery, disables plugins, shell, browsing, apps and other tool features, and runs
ephemerally. Any emitted tool activity invalidates a trial; no such trial counts as
a successful one-shot submission. This does not erase pretraining knowledge.

The container is on a Docker **internal network**. A separate CONNECT proxy bridges
to the outside, allowing only chatgpt.com, api.openai.com, and auth.openai.com on
port 443. General web access and direct outbound connections are blocked. These
hosts are for inference/authentication, not a promise of endpoint-level filtering.
No ports are published to the host. Proxy/container/network names are unique and
removed after use. The model never receives an API key, and API-key auth files are
refused. Subscription availability and model entitlement are checked by an actual
inference request when trials run; merely listing a model is not proof of access.

The credential is present in the generation container for the CLI to authenticate.
The model has no permitted tool path to read it; this is not an architecture that
keeps credentials outside the CLI process. Inference is the only intended external
communication. Generation failures and timeouts are retained, with no automatic retry.

## Evaluation and provisional oracle

`prepare-oracle` snapshots the current generated `results/regulation_z_dfc/P*.pgn`,
parses them natively, and evaluates them over a frozen context suite. These PGNs are
the common predicate reference, not a legally validated oracle. The catalog,
derivation code, original GProM template and ProvSQL wrapper are also copied and
hashed for later diagnosis. The concrete fixture database is snapshotted.

The suite includes concrete context records from the existing GProM fixture plus
abstract field/boundary/applicability probes. The latter are **predicate unit tests**;
they are not necessarily jointly realizable financial ledgers. There is no claim
that the finite suite proves semantic equivalence or complete regulatory coverage.

- DFC submissions are parsed by the installed Passant `Policy.from_pgn`; bindings
  are checked, then conditions execute in DuckDB with external access disabled.
- GProM backend queries execute in read-only-authorized in-memory SQLite.
- ProvSQL backend checks execute in a fresh offline PostgreSQL container as a
  select-only role, with a statement timeout. ProvSQL itself is not needed for
  these provenance-disabled checks. Use `--postgres-image` to choose a pinned image.
- SQL artifacts must be single read-only queries, return every case exactly once,
  and contain only Boolean passed values. Syntax/runtime/format errors are separate
  from behavioral disagreements. Full PGN query rewriting is not evaluated.

Reports store every policy result, per-policy and overall decision disagreements,
full differing context, source hashes, and `classification: unresolved`. The metric
is **reference agreement**, not legal correctness. Classification choices for later
review are generated policy, reference policy, specification, fixture, or adapter.
An equal result does not rule out shared mistakes.

**Follow-up required before claiming correctness in the paper:** inspect disagreements
against the frozen specification and sources. Record the cause and rationale; preserve
original reports. If the oracle is corrected, create a new snapshot and regrade a copy
of the saved submissions under a new report version. Do not regenerate the model answer
or feed adjudication back into the original one-shot trial.

## Commands

Run from `sigmod_2027_big_paper_experiments` using its existing venv. Do not run `uv sync`
merely for this harness; it can replace the local DuckDB build.

```sh
.venv/bin/python -m policy_authoring.harness prompts
.venv/bin/python -m policy_authoring.harness build
.venv/bin/python -m policy_authoring.harness smoke
.venv/bin/python -m policy_authoring.harness prepare-oracle policy_authoring/oracles/v1
```

After reviewing prompts, the following starts 15 subscription-consuming model runs:

```sh
.venv/bin/python -m policy_authoring.harness generate policy_authoring/runs/pilot --benchmark predicate --trials 5 --parallelism 15 --timeout 3600
.venv/bin/python -m policy_authoring.harness score-run policy_authoring/runs/pilot policy_authoring/oracles/v1
```

Generation and scoring are separate: the model cannot see the evaluator or feedback.
Every trial stores its exact prompt/hash, image ID, configuration, raw event stream,
final answer, parsed submission, token usage, and elapsed wall time. Wall time includes
Docker/CLI startup and authentication; it excludes separate grading. It is **not** a
provider-only generation-latency measurement. Container build/download and oracle
preparation are outside trial timing. `summary.csv` retains all trials, including
failures; missing usage is unavailable, not zero. Infrastructure evaluation errors
are marked separately and must not be counted as generated-policy defects.

Individual grading is also available:

```sh
.venv/bin/python -m policy_authoring.harness grade dfc submission.json policy_authoring/oracles/v1 report.json
```

Reports and snapshots refuse overwrite. Keep credentials outside all result directories.

## Validation

```sh
.venv/bin/python -m ruff check policy_authoring tests/test_policy_authoring.py
POLICY_AUTHORING_DOCKER=1 .venv/bin/python -m pytest tests/test_policy_authoring.py -q
```

Tests round-trip all reference predicates through the three evaluation backends,
verify that an incorrect P17 becomes an unresolved disagreement, reject missing or
duplicate result rows and unsafe statement shapes, check token-event parsing and
no-tool enforcement, and verify deterministic oracle preparation. The container
smoke probe checks absent host directories/socket/history, blocked direct egress,
blocked arbitrary proxy destinations, allowed inference-host connectivity, and CLI
version. These checks make no model calls.

### Live diagnostics

Generation writes each trial's `events.jsonl` and `stderr.txt` directly as the CLI
emits output, without waiting for completion. Partial logs survive errors and
timeouts. `metadata.json` records `status: running`, the container name, and UTC
start time before launch, then the final status after cleanup. A running status
left after a host crash is not proof the container is still alive.

For example, from the experiment project directory, during a run:

```sh
tail -F policy_authoring/runs/pilot/dfc-001/events.jsonl policy_authoring/runs/pilot/dfc-001/stderr.txt
```

The launch message identifies the container for `docker stats` or `docker inspect`.
Logs only show events the CLI emits; silence during generation is not by itself a
failure. These diagnostics provide no model feedback and do not change the prompts.
Authentication is sent through stdin, never written to these logs by the harness.

A single-approach harness smoke trial can use `--benchmark predicate --approaches dfc --trials 1 --parallelism 1`.
The 2026-10-17 smoke trial completed; see `runs/dfc-smoke-20261017/report.md`.
CLI diagnostic error items are retained in the event log but are not tool activity;
a completed turn, valid submission, and absence of actual tool items are still required.

### Shared specification revision 2

`prompts/clarifications.md` now specifies P04/P05/P18/P35 responsibility and
applicability, evidence relationships, and the independent request-timing check.
It is included verbatim in all three exact prompts. The Page snapshot in
`specification.md` is unchanged; these are separately identified experiment
conventions. The original smoke run retains its original `prompt.txt` and hash.

Revision 2 treats contradictory evidence contexts as diagnostics, not semantic
correctness cases. **The current grader still compares all legacy probes:** its
aggregate agreement numbers do not yet implement that distinction and must not be
reported as revision-2 correctness rates. The oracle/probe classification needs a
separate update before the comparative experiment. No oracle, historical grade,
or generated policy was changed as part of this specification revision.
