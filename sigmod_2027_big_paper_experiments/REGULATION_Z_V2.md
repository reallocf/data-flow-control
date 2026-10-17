# Regulation Z v2: credit-card statement reconciliation

## Scope

The benchmark models an agent finalizing a monthly credit-card statement. The agent proposes one
database update that changes a draft statement-publication record to `published`. Before allowing
that update, the system must determine whether the statement agrees with the account ledger,
daily-balance interest calculation, and approved purchase APR.

This is a compact numerical control inspired by the periodic-statement requirements in Regulation
Z §1026.7(b). It is not a complete encoding or legal interpretation of that section. The benchmark
does not evaluate statement layout, mailing, minimum-payment warnings, payment addresses, dispute
notices, or every category that a real statement must disclose.

## Why this case

Periodic statements are a familiar consumer-facing artifact and errors can directly change what a
consumer believes they owe. The task is also difficult enough to distinguish enforcement methods:
the decision depends on joining several operational records, recomputing aggregates, checking a
30-day series, applying two numerical tolerances, and deciding whether to permit a state change.
No single input row reveals the answer.

## Integration architecture

```text
CFPB TCCP workbook
        |
        | select simple product-wide APRs, scale percentages,
        | create source hash, remove issuer/product identity
        v
public_product_terms -----> apr_config
                                 |
accounts ---- products ---------+
    |
    +---- statements <---- transactions
              ^           daily_balances
              |
              +---- statement_reconciliation view
                              |
                              v
                 candidate statement_publications update
```

The public CFPB extract supplies realistic APR values and reproducible provenance. All account
activity, balances, statement amounts, and publication attempts are synthetic because the public
data does not contain consumer ledgers. The public staging table is deliberately absent from the
DFC policy: a survey row is a seed, not proof that a particular statement is correct.

## Operational schema

The experiment uses a natural subset of the supplied credit-card servicing schema:

- `accounts` and `products` identify the active account and card product;
- `apr_config` contains the approved purchase APR;
- `statements` contains the draft statement amounts and disclosed APR;
- `transactions` contains purchases, cash advances, payments, fees, and interest;
- `daily_balances` contains the daily purchase balance and rate used for interest; and
- `statement_publications` is the protected sink.

A `statement_reconciliation` view performs the normal data-integration step. It joins the CCS
entities and derives transaction totals, net activity, daily interest, day count, and minimum and
maximum applied APR. This keeps the DFC policy readable while preserving the relational task.

## DFC policy

Fourteen small DFC policies protect the transition from draft to published. Together they require:

1. The source and sink to identify the same active account, product, and statement.
2. Purchases, cash advances, payments, fees, and interest to match the statement's transactions
   within $0.01.
3. New balance to equal previous balance plus net transaction activity within $0.01.
4. Statement interest to equal the sum computed from every daily purchase balance and daily
   periodic rate within $0.01, with a row for every day in the cycle.
5. The statement APR and every applied daily APR to match the approved purchase APR within
   0.000001.

Each policy expresses one independently explainable constraint: publication linkage, account
status, product status, configuration approval, each of the five transaction totals, new-balance
arithmetic, daily-interest arithmetic, cycle-date completeness, disclosed APR, or applied daily
APR. All applicable policies are conjoined by query rewriting, yielding the same enforcement
condition as a single large policy while making individual controls easier to inspect and report.

Every policy uses `REMOVE`, so failure of any constraint makes the candidate match no row and the
publication remains in draft. The policies do not repair a bad statement.

## Workload

The workload alternates valid and invalid statements. Invalid cases independently introduce:

- incorrect purchase, cash-advance, payment, fee, or interest totals;
- an incorrect new balance;
- an incorrect disclosed purchase APR;
- an unreported purchase that exists in the ledger;
- a different APR in the daily interest calculation; or
- a duplicated balance date that leaves one cycle day missing while preserving the row count and
  interest arithmetic.

Each case uses six or seven transactions and 30 daily-balance rows. All candidate SQL updates are
executable and every statement has source rows, so neither query shape nor row existence reveals
the label. The smallest configuration uses one valid statement for inexpensive infrastructure
testing; a balanced run must use at least two inputs.

## Comparisons and measurements

The approaches are DFC, Astra 6, Sol 6.1, Luna 6, Opus 5.5, Fable 5.1, Haiku 5.5, and No Policy.
All model requests go through project-tagged AWS Bedrock application inference profiles and use
provider-default sampling. OpenAI-family requests use Bedrock InvokeModel with explicit caching mode and a short
checkpoint below Bedrock's cacheable-token minimum, which disables prompt-cache reads and writes;
prompts do not include nonces. Requests
run with up to 50-way concurrency and results are
flushed incrementally.

The runner records accuracy, false positives, false negatives, parse failures, average request
latency, model wall time, SQL execution time, token usage, cache counters, and request IDs.

## Rust DFC timing

The DFC-only Rust comparison uses the upstream Passant PyO3 package and preserves the live
`statement_reconciliation` view used by the Python benchmark. Install the upstream package into the
benchmark environment, then run:

```bash
.venv/bin/python scripts/run_regulation_z_v2_rust_dfc.py --num-statements 100
```

Passant's DuckDB catalog adapter currently lists tables but not views, so the runner supplies a
view-aware adapter for catalog synchronization. Passant accepts scalar source constraints for this
one-row-per-statement reconciliation relation, avoiding the `max(...)` wrappers required by the
legacy Python policy API. Policy registration and catalog synchronization are setup work and are
not included in the timed region. Full DFC time is Rust rewrite time plus DuckDB SQL execution time.

## Current limitations

- Only the numerical core of §1026.7(b) is represented.
- The real public data contributes product APRs, not transaction or statement truth labels.
- Daily balances are synthetic and intentionally simple; a production calculation would also
  model posting rules, grace periods, multiple balance buckets, compounding, and adjustments.
- `statement_reconciliation` is treated as a trusted derived source. A production deployment would
  govern its definition and the upstream ingestion path separately.
- The benchmark measures an `UPDATE` to retain the execution shape of the paper's application
  workload. It preallocates a draft publication row rather than inserting a new row.

## Commands

```bash
source setup_local_smokedduck.sh

# Smallest Bedrock smoke test: six paid requests.
.venv/bin/python scripts/run_regulation_z_v2_benchmark.py --num-statements 1

# Full run used for the paper figures: 100 requests per model.
.venv/bin/python scripts/run_regulation_z_v2_benchmark.py --num-statements 100

.venv/bin/python scripts/generate_regulation_z_v2_benchmark_chart.py
```
