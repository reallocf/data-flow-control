# OSS LLM Tax Validation Experiment Plan

## Goal

Create a new `oss_experiments/` project that mirrors the useful structure of
`vldb_2026_big_paper_experiments/`, but evaluates the open source
`data-flow-control` package instead of the local legacy `sql_rewriter` package.
The first experiment should be a tax-domain version of the existing
`llm_validation_strategy`: compare deterministic DFC enforcement against LLM
policy-violation judgments, using a custom dataset aligned with the TaxAgent
examples in `dfc_paper.pdf`.

The experiment should answer:

1. Can GPT 5.5 and Claude Opus 4.8 correctly identify KILL policy violations
   over receipt-processing insert queries?
2. How do result-only vs result-plus-input context, medium vs max reasoning
   effort, and one-row vs 100-row receipt queries affect LLM judgments?
3. How do they compare with DFC enforcement on correctness, runtime, and input
   token/cache behavior?

## Current Baseline To Reuse

`vldb_2026_big_paper_experiments/` already has the relevant harness pattern:

- `pyproject.toml` installs `experiment-harness` as a local editable source.
- Strategies subclass `ExperimentStrategy`, enumerate setting keys, and write
  metrics through `ExperimentResult.custom_metrics`.
- `scripts/run_llm_validation_experiment.py`,
  `scripts/run_llm_validation_grid_experiment.py`, and
  `scripts/run_synthetic_llm_validation_grid_experiment.py` show the runner
  shape.
- `src/vldb_experiments/strategies/llm_validation_common.py` has reusable prompt
  builders, JSON parsing, row formatting, and approach names.
- `SyntheticLLMValidationGridStrategy` is the closest structural template for
  generating deterministic datasets, varying datasets/queries, caching result
  rows locally, and adding a per-run nonce.

Do not copy its package dependencies blindly. It still imports `sql_rewriter`
and SmokedDuck helpers. The OSS project should use standard DuckDB plus
`data_flow_control`.

## Project Scaffold

Create:

```text
oss_experiments/
  pyproject.toml
  README.md
  src/oss_experiments/
    __init__.py
    data_setup.py
    llm_clients.py
    policies.py
    prompts.py
    visualizations.py
    strategies/
      __init__.py
      tax_llm_validation_strategy.py
  scripts/
    run_tax_llm_validation_experiment.py
    generate_tax_llm_validation_visualizations.py
  tests/
    test_tax_data_setup.py
    test_tax_policies.py
    test_tax_llm_validation_strategy.py
    test_prompt_cache_controls.py
```

`pyproject.toml` should:

- Name the project `oss-experiments`.
- Depend on `data-flow-control`, not `sql-rewriter`.
- Keep `experiment-harness` as a local editable dependency from
  `../experiment_harness`.
- Depend on standard `duckdb`, `pandas`, `numpy`, `matplotlib`, `scikit-learn`,
  `openai`, `boto3`, and `python-dotenv`.
- Avoid SmokedDuck setup scripts and any `setup_local_smokedduck.sh` dependency.
- Add dev dependencies for `pytest` and `ruff`.

Use direct OpenAI and Bedrock clients in `llm_clients.py` instead of LangChain
for this experiment. The existing LangChain wrappers hide or normalize response
metadata, but this experiment needs exact cache counters.

## Dataset Design

Generate one deterministic receipt-processing dataset in DuckDB. Use title-case
table names to match paper examples and current parser tests:

```sql
Receipts(
  id INTEGER,
  uid INTEGER,
  batch_id INTEGER,
  merchant VARCHAR,
  item VARCHAR,
  cat VARCHAR,
  cost DOUBLE,
  receipt_date DATE
)

Expenses(
  id INTEGER,
  item VARCHAR,
  cat VARCHAR,
  cost DOUBLE,
  biz_use DOUBLE,
  source_note VARCHAR
)
```

Optionally add `TaxRules(cat, max_biz_use)` later, but start with the literal
meal rule from the paper so policy semantics stay simple.

The generator should create exactly 1,000 `Receipts` input rows split into ten
disjoint batches:

- `batch_id` values `0..9`.
- 100 rows per batch.
- Receipt ids `1..1000`, with batch `n` containing ids
  `n * 100 + 1` through `(n + 1) * 100`.
- Five 100-row batches should produce legal deductions under the meal policy.
- Five 100-row batches should produce at least one meal deduction with
  `biz_use > 50`, so those 100-row queries fail the meal policy.
- Ten distinct single-row query receipts should be designated for the
  `1_row_queries` workload, with five legal rows and five rows that fail the
  meal policy.

Use a deterministic pattern rather than random sampling for the pass/fail split,
for example even-numbered query indexes pass and odd-numbered query indexes
fail. This keeps the expected outcome auditable from the row ids, batch ids, and
query ids.

## Policy Catalog

Implement policies in `policies.py` using `data_flow_control.Policy.from_pgn`.
Use only `ON FAIL KILL` policies for this experiment. KILL produces a crisp
pass/fail signal that is easiest to verify inside `experiment_harness`: either
the query completes or the DFC connection raises the configured policy
exception.

Initial policy set:

```sql
SOURCE REQUIRED Receipts
SINK Expenses
CONSTRAINT Receipts.id = Expenses.id
ON FAIL KILL
```

```sql
SOURCE Receipts
SINK Expenses
CONSTRAINT Expenses.biz_use <= 50 OR Receipts.cat != 'Meal'
ON FAIL KILL
```

Add a description matching the paper concept:

- `meal_deduction_limit`

Later variants can add user/role dimensions, but keep this plan focused on
KILL-only receipt-processing policies.

## Query Catalog

Create two fixed query sets in `data_setup.py` or a dedicated `queries.py`:

- `1_row_queries`: 10 insert queries, each selecting one distinct receipt row.
- `100_row_queries`: 10 insert queries, each selecting one non-overlapping
  100-row receipt batch.

Each query set should use query ids `q00` through `q09`. The unique query key is
`(query_set, query_id)`.

```sql
INSERT INTO Expenses (id, item, cat, cost, biz_use, source_note)
SELECT
    Receipts.id,
    Receipts.item,
    Receipts.cat,
    Receipts.cost,
    <query-specific biz_use expression> AS biz_use,
    '100_row_queries:q00' AS source_note
FROM Receipts
WHERE Receipts.batch_id = 0
ORDER BY Receipts.id
```

For `1_row_queries`, the `WHERE` clause should select exactly one receipt id,
and the 10 selected ids must be distinct. For `100_row_queries`, the `WHERE`
clause should select the full `batch_id`, and the 10 batches must be
non-overlapping.

Workload invariants:

- Each `1_row_queries` query inserts exactly 1 candidate `Expenses` row under
  raw no-policy execution.
- Each `100_row_queries` query inserts exactly 100 candidate `Expenses` rows
  under raw no-policy execution.
- In `1_row_queries`, exactly five queries pass `meal_deduction_limit` and
  exactly five queries fail it.
- In `100_row_queries`, exactly five queries pass `meal_deduction_limit` and
  exactly five queries fail it.

Each query record should define:

- `query_set` (`1_row_queries` or `100_row_queries`)
- `query_id`
- SQL text
- active policy ids
- `batch_id`
- `input_receipt_id_start`
- `input_receipt_id_end`
- `rows_selected` (1 or 100)
- `expected_meal_limit_kill`
- whether it is an `insert_select`
- whether DFC should pass or KILL

## Ground Truth With KILL

The KILL-only experiment should derive truth from deterministic pass/exception
outcomes:

1. Reset the dataset for the query.
2. Run the base query without DFC on a raw DuckDB connection to capture the
   candidate `Expenses` rows for that query: 1 row for `1_row_queries`, 100 rows
   for `100_row_queries`.
3. Reset the dataset again.
4. Wrap a fresh DuckDB connection with `dfc(raw)` from `data_flow_control`.
5. Register the query's policies.
6. Execute the query through the DFC connection.
7. Mark `ground_truth_violation = true` exactly when DFC raises the configured
   KILL exception.
8. Assert the observed KILL/pass outcome matches the query's expected outcome
   before recording the row. A mismatch is a harness/test failure, not an LLM
   result.

Record all of these as separate metrics:

- `query_set`
- `rows_selected`
- `input_row_count` (1 or 100)
- `base_inserted_row_count` (1 or 100)
- `dfc_killed`
- `expected_dfc_kill`
- `expected_meal_limit_kill`
- `dfc_truth_matches_expected`
- `rewritten_sql`
- `dfc_rewrite_time_ms`
- `dfc_exec_time_ms`

This keeps the CSV auditable without needing row-difference semantics. If a
future experiment needs row-level filtering behavior, add it as a separate
strategy after the KILL experiment is stable.

## LLM Baselines

Use `approach = "dfc"` for the deterministic DFC row. Do not carry over older
VLDB approach labels. LLM rows should use `approach = "llm"` and identify the
exact baseline through separate `model_name`, `included_context`, and
`reasoning_effort` columns.

Evaluate the full LLM baseline grid:

- model: `gpt-5.5`, Opus 4.8 via Bedrock
- included context: `query_and_results`, `query_and_results_and_input`
- reasoning effort: `medium`, `max`
- query set: `1_row_queries`, `100_row_queries`

This yields 16 LLM baseline settings across the two query sets, evaluated over
20 total query definitions, plus one `dfc` row per query definition.

Default models:

- OpenAI: `gpt-5.5`
- AWS Bedrock Claude: Opus 4.8 through Bedrock using the application inference
  profile ARN
  `arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/e7ygwg2bquz8`
  as the default `modelId`.

Credential and provider setup:

- GPT 5.5 must use the `OPENAI_API_KEY` value loaded from the local `.env` file
  with `python-dotenv`; do not require users to export it manually for normal
  runs.
- Opus 4.8 must connect through AWS Bedrock using the inference profile ARN
  above by default. Keep `--claude-model` configurable for reruns, but make the
  default the ARN.

Prompt shape:

- Ask for a single JSON judgment: `violates_policy`.
- Include workload-neutral policy descriptions and SQL.
- For `query_and_results`, include only the candidate `Expenses` rows produced
  by the query's raw no-policy execution: 1 row for `1_row_queries`, 100 rows
  for `100_row_queries`.
- For `query_and_results_and_input`, include those same candidate `Expenses`
  rows plus the selected input `Receipts` rows: 1 row for `1_row_queries`, 100
  rows for `100_row_queries`.
- Never include `Expenses` rows produced by earlier queries in either context.
- Include `run_nonce`, `query_set`, `query_id`, `batch_id`, and
  `rows_selected` in the metrics.
- Request `reasoning_effort = medium` or `reasoning_effort = max`. For `max`,
  map to the strongest equivalent effort the provider/model supports at
  implementation time, and record both `requested_reasoning_effort` and
  `effective_reasoning_effort`.
- Keep temperature deterministic if the provider/model supports it. If GPT 5.5
  or Opus 4.8 reject `temperature=0`, use the provider default and document it
  in the CSV as `temperature_effective`.

Prefer OpenAI Structured Outputs if it can be used without hiding usage details;
otherwise keep the current JSON parser and record parse failures explicitly.

## Prompt Cache Avoidance And Verification

This is a correctness requirement for the experiment, not a minor performance
detail.

Provider facts checked on 2026-06-13:

- OpenAI GPT 5.5 prompt caching works automatically for eligible long prompts.
  GPT 5.5 supports extended prompt cache retention, and the docs show `24h` as
  the supported retention option for GPT 5.5. The docs also expose
  `usage.prompt_tokens_details.cached_tokens` and recommend monitoring it.
  Source: https://developers.openai.com/api/docs/guides/prompt-caching
- OpenAI docs say GPT 5.5 works best through the Responses API and recommend
  tracking `usage.prompt_tokens_details.cached_tokens`.
  Source: https://developers.openai.com/api/docs/guides/latest-model
- Claude Opus 4.8 is active on Bedrock, supports Converse, and its Bedrock model
  id is `anthropic.claude-opus-4-8`.
  Source: https://docs.aws.amazon.com/bedrock/latest/userguide/model-card-anthropic-claude-opus-4-8.html
- The planned experiment should invoke Opus 4.8 through the AWS Bedrock
  application inference profile
  `arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/e7ygwg2bquz8`
  rather than a direct model id.
- Bedrock prompt caching for Converse is enabled by adding `cachePoint` content
  blocks; Claude Opus 4.8 supports prompt caching with a 4,096 token checkpoint
  minimum. Source:
  https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html
- Anthropic's own docs support automatic caching only when `cache_control` is
  set, and note that Bedrock does not support Anthropic automatic caching.
  Source: https://docs.anthropic.com/en/docs/build-with-claude/prompt-caching

Implementation rules:

1. Every measured LLM request, for both GPT 5.5 and Opus 4.8, must put a unique
   run nonce in the first user-visible content block and within the first 256
   tokens. Do this uniformly even though OpenAI is the provider where prompt
   caching is the main concern.
2. OpenAI requests must not use a stable `prompt_cache_key`. Either omit it or
   set it to a value derived from the per-run nonce. Prefer the unique value if
   the Responses API client supports it, because the docs say the key is
   combined with the prefix hash for routing.
3. OpenAI requests must log `cached_tokens`, `prompt_tokens`, `output_tokens`,
   model id, and request id where available.
4. Measured OpenAI rows with `cached_tokens > 0` must be flagged as
   `cache_contaminated = true`. The default visualization and summary table
   should exclude contaminated rows and print a warning.
5. Bedrock Converse requests must not include `cachePoint`, `cache_control`,
   prompt-management caching, or fallback-credit beta fields.
6. Bedrock requests must use the same nonce-first prompt format as OpenAI. This
   keeps request construction simple and makes accidental future caching or
   client changes visible.
7. Bedrock responses must log any returned cache counters, including
   `cacheReadInputTokens` and `cacheWriteInputTokens` if present. Treat any
   nonzero value as `cache_contaminated = true`.
8. Do not run measured LLM warmups. If warmups are needed for database work,
   keep them DFC-only or mark LLM warmup rows as unmeasured with unique nonces.
9. Add `tests/test_prompt_cache_controls.py` to assert that both request
   builders place the nonce first and that the Bedrock request builder emits no
   `cachePoint` blocks.

The existing UUID preamble in `llm_validation_common.py` is a good start, but
the nonce should move to the beginning of the actual request payload for both
providers and be paired with cache-counter logging. A nonce at the end of a long
repeated prompt is not enough.

## Strategy Implementation

`TaxLLMValidationStrategy` should:

1. Load/generate the requested dataset specs in `setup`.
2. Build the query and policy catalogs.
3. Create provider clients only when the corresponding baseline is enabled.
4. Enumerate settings as
   `(query_set, query_id, policy_set_id, approach, model_name, included_context, reasoning_effort)`.
5. Use `get_setting_key` for per-setting DFC warmups.
6. Execute DFC first to compute ground truth and timing.
7. Execute LLM baselines with a fresh nonce per measured run.
8. Store full raw responses plus parse status.
9. Store provider usage/cache metadata as first-class CSV columns.

Recommended metrics:

- `dataset_label`
- `query_set`
- `query_id`
- `batch_id`
- `input_receipt_id_start`
- `input_receipt_id_end`
- `rows_selected`
- `query_kind`
- `policy_set_id`
- `policy_ids`
- `approach`
- `provider`
- `model_name`
- `included_context`
- `requested_reasoning_effort`
- `effective_reasoning_effort`
- `runtime_ms`
- `ground_truth_violation`
- `predicted_violation`
- `correct_identification`
- `parse_ok`
- `input_row_count`
- `base_inserted_row_count`
- `result_context_row_count`
- `input_context_row_count`
- `dfc_killed`
- `expected_dfc_kill`
- `expected_meal_limit_kill`
- `dfc_truth_matches_expected`
- `dfc_rewrite_time_ms`
- `dfc_exec_time_ms`
- `input_tokens`
- `output_tokens`
- `cached_input_tokens`
- `cache_write_input_tokens`
- `cache_read_input_tokens`
- `cache_contaminated`
- `prompt_chars`
- `prompt_nonce`
- `request_id`
- `raw_response`

## Runner

`scripts/run_tax_llm_validation_experiment.py` should mirror the existing runner
style:

```bash
cd oss_experiments
uv run python scripts/run_tax_llm_validation_experiment.py \
  --input-row-count 1000 \
  --batch-row-count 100 \
  --query-sets 1_row_queries 100_row_queries \
  --runs-per-setting 5 \
  --gpt-model gpt-5.5 \
  --claude-model arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/e7ygwg2bquz8 \
  --output-filename tax_llm_validation_results.csv
```

The runner should load `OPENAI_API_KEY` from the local `.env` file before
constructing the OpenAI client. The default Opus path should use AWS Bedrock
Converse with the inference profile ARN shown above.

Support switches:

- `--disable-openai`
- `--disable-bedrock`
- `--dfc-only`
- `--query-sets 1_row_queries 100_row_queries`
- `--query-ids`
- `--included-contexts query_and_results query_and_results_and_input`
- `--reasoning-efforts medium max`
- `--input-row-count`
- `--batch-row-count`
- `--runs-per-setting`
- `--warmup-per-setting`
- `--fail-on-cache-hit`

`--dfc-only` should be a cheap smoke path that does not require API keys.

## Visualization

Create `scripts/generate_tax_llm_validation_visualizations.py` and
`src/oss_experiments/visualizations.py`.

Initial outputs:

1. `tax_llm_validation_f1_by_query.png`
   - X axis: query id (`q00` through `q09`), faceted or grouped by
     `query_set`.
   - Bars: model x included-context x reasoning-effort combinations.
   - Y axis: F1 score or accuracy, depending on class balance. Prefer F1 if
     each grouping has both pass and violation examples; otherwise show
     accuracy and label it honestly.
2. `tax_llm_validation_confusion_by_model.png`
   - Small multiples or grouped stacked bars for TP/FP/FN/TN by model,
     included context, and reasoning effort.
3. `tax_llm_validation_runtime_tokens.png`
   - Runtime and input tokens by model, included context, and reasoning effort,
     split by `query_set`, with DFC shown separately.
4. `tax_llm_validation_cache_audit.csv`
   - Summary by provider/model: rows, contaminated rows, max cached tokens, mean
     cached tokens.

All visualizations should exclude `cache_contaminated = true` rows by default
and print the number excluded. Provide `--include-cache-contaminated` only for
diagnostics.

## Tests And Verification

From `oss_experiments/`:

```bash
uv sync
uv run ruff check .
uv run pytest
uv run python scripts/run_tax_llm_validation_experiment.py --dfc-only --runs-per-setting 1
uv run python scripts/generate_tax_llm_validation_visualizations.py --input-csv results/tax_llm_validation_results.csv
```

Before any full LLM run:

```bash
uv run python scripts/run_tax_llm_validation_experiment.py \
  --query-sets 1_row_queries 100_row_queries \
  --query-ids q00 q01 \
  --included-contexts query_and_results query_and_results_and_input \
  --reasoning-efforts medium max \
  --runs-per-setting 1 \
  --fail-on-cache-hit
```

Acceptance gates:

- `pyproject.toml` contains no `sql-rewriter` dependency or `sql_rewriter`
  source.
- `rg -n "sql_rewriter|SQLRewriter" oss_experiments` returns no matches.
- Data setup creates exactly 1,000 `Receipts` rows split into ten 100-row
  batches.
- Query catalog contains exactly 20 query definitions: `q00` through `q09` for
  `1_row_queries`, and `q00` through `q09` for `100_row_queries`.
- The 10 `1_row_queries` definitions select 10 distinct receipt rows.
- The 10 `100_row_queries` definitions select 10 non-overlapping 100-row
  batches.
- DFC-only smoke confirms exactly 5 `1_row_queries` pass `meal_deduction_limit`
  and exactly 5 KILL on it.
- DFC-only smoke confirms exactly 5 `100_row_queries` pass `meal_deduction_limit`
  and exactly 5 KILL on it.
- DFC-only smoke produces expected pass/KILL outcomes for every query.
- OpenAI measured rows have `cached_input_tokens == 0`.
- Bedrock measured rows have no cache read/write counters, or all cache
  counters are zero.
- Visualization scripts regenerate figures from the produced CSV.
- `uv run ruff check .` and `uv run pytest` pass.

## Phased Work

### Phase 1: Scaffold

Create `oss_experiments/`, package metadata, README, empty package modules, and
test skeletons. Verify `uv sync` installs `data-flow-control` from PyPI and
`experiment-harness` from `../experiment_harness`.

### Phase 2: Dataset And DFC Truth

Implement deterministic data generation, PGN policy builders, query catalog, and
DFC truth evaluation without any LLM calls. Add tests for row counts, policy
parsing, exact 1-row query selection, exact 100-row query batches, all-grounded
outcomes, and the 5-pass / 5-KILL meal-policy split within each query set.

### Phase 3: Provider Clients And Cache Audit

Implement direct OpenAI Responses and Bedrock Converse clients. Add request
builder tests for nonce placement and no Bedrock cache checkpoints. Add a tiny
manual smoke run with `--fail-on-cache-hit`.

### Phase 4: Full Strategy

Wire the strategy to `ExperimentRunner`, add all metrics, add resume-safe output
behavior if needed, and run the DFC-only smoke. Then run a 2-query LLM smoke for
GPT 5.5 and Opus 4.8.

### Phase 5: Visualization

Generate the F1/accuracy, confusion, runtime/token, and cache audit artifacts.
Keep visual labels query- and batch-based so the figure is understandable next
to the paper's TaxAgent examples.

### Phase 6: Full Run

Run all 20 query definitions across the full model x context x reasoning-effort
x query-set baseline grid and configured runs per setting. Save the full CSV
and figures under `oss_experiments/results/`, then copy publication-ready outputs to
`oss_experiments/final_results/` only after the cache audit is clean.

## Risks

- OpenAI prompt caching cannot be treated as disabled just because no stable
  cache key is set. The experiment must verify `cached_tokens` per measured row.
- Bedrock Opus 4.8 access may require model enablement, region selection, or a
  geo/global inference id. The runner should make the model id configurable and
  print the effective id.
- This plan intentionally uses KILL-only policies. If row-level filtering or
  valid-column semantics re-enter scope, they should be separate strategies with
  explicit oracles rather than quiet extensions of this harness.
- Required-source and sink semantics must be tested directly for every query so
  the first policy is confirmed to pass before running paid LLM calls.
- If the OpenAI Structured Outputs schema itself creates cached prefix hits,
  fall back to plain JSON text prompts for the measured experiment and keep
  schema validation only in parsing/tests.
