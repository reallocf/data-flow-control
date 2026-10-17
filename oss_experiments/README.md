# OSS LLM Tax Validation Experiments

Experiments comparing deterministic `data-flow-control` KILL enforcement against
GPT 5.5 and Claude Opus 4.8 policy-violation judgments on a tax receipt dataset.
The workload uses 1,000 receipt rows split across 10 report batches.

Two query shapes are supported via `--mode`:

- **`agg`** (default): merchant rollup queries with aggregate DFC policy  
  `MIN(Receipts.cat) != 'Meal' OR Expenses.biz_use <= 50.0`
- **`select`**: row-level `INSERT INTO Expenses ... SELECT ... FROM Receipts WHERE id IN (...)`  
  with the original row-level DFC policy  
  `Expenses.biz_use <= 50 OR Receipts.cat != 'Meal'`

Both modes share the same pass/fail query labels, but DFC enforcement and LLM
policy descriptions follow the mode-specific constraint above.

## Setup

```bash
cd oss_experiments
uv sync
```

Requires `OPENAI_API_KEY` in `.env` for OpenAI runs and AWS credentials for Bedrock.

## DFC-only smoke (no API keys)

```bash
# Aggregation mode (default)
uv run python scripts/run_tax_llm_validation_experiment.py \
  --mode agg --dfc-only --runs-per-setting 1

# Row-level INSERT..SELECT mode
uv run python scripts/run_tax_llm_validation_experiment.py \
  --mode select --dfc-only --runs-per-setting 1
```

## Full experiment

```bash
# Aggregation mode
uv run python scripts/run_tax_llm_validation_experiment.py \
  --mode agg \
  --input-row-count 1000 \
  --batch-row-count 100 \
  --bedrock-max-tokens 8192 \
  --llm-max-attempts 3 \
  --query-sets 1_row_queries 10_row_queries 100_row_queries \
  --runs-per-setting 1 \
  --output-filename tax_llm_validation_agg.csv

# Row-level select mode
uv run python scripts/run_tax_llm_validation_experiment.py \
  --mode select \
  --input-row-count 1000 \
  --batch-row-count 100 \
  --bedrock-max-tokens 8192 \
  --llm-max-attempts 3 \
  --query-sets 1_row_queries 10_row_queries 100_row_queries \
  --runs-per-setting 1 \
  --output-filename tax_llm_validation_select.csv
```

## Visualizations

```bash
uv run python scripts/generate_tax_llm_validation_visualizations.py \
  --input-csv results/tax_llm_validation_results.csv
```

## Tests

```bash
uv run ruff check .
uv run pytest
```

## Acceptance gates

- No `sql_rewriter` imports: `rg -n "sql_rewriter|SQLRewriter" oss_experiments`
- 1,000 `Receipts` rows in 10 batches; 30 query definitions
- DFC-only smoke: 5 pass / 5 KILL per query set for meal policy
- LLM rows: `cached_input_tokens == 0` (OpenAI) and zero Bedrock cache counters
