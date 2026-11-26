# DFC development research closeout

This directory consolidates subsequent development
findings for the statutory policy research review.

## Principal findings

| Study | Result |
|---|---|
| Common schema | 41/102 |
| Explicit annual environment | 54/102 |
| Additional typed representation | 38/102 |
| Confidence precision against provisional EXACT labels | 2/36 |
| Actual historical SQL checks | 841 |
| Normalized REMOVE enforcement | 29/29 |
| Selected KILL cases | 10/10 |
| Selected conditional legal decisions | 13/23 |
| Trusted annual boundary | 28/28 conditional cases |
| MCP stdio regression | 4/4 synthetic checks |

The enforcement and legal comparisons have distinct
denominators. Faithful enforcement of a predicate
does not establish the legal correctness of that
predicate.

## Cross-model comparison

| Provision | Qwen3-32B | GPT-OSS-120B |
|---|---:|---:|
| Section 274 | 55/102 | 38/102 |
| Section 195 | 46/60 | 49/60 |

The larger model did not consistently improve
semantic completeness across these two benchmarks.

Both provisions have prior development exposure.

## Partial reference comparison

| Condition | Responses | Score |
|---|---:|---:|
| Target only | 3/3 | 36/102 |
| REJ16 occurrence information | 1/3 | 11/34 |
| LLM occurrence information | 2/3 | 25/68 |

The reference conditions share the selected
dependency passages and differ in archived
occurrence information.

Two requests ended with connection closure.
One request was not attempted.

Server processing and billing for the interrupted
requests remain unknown.

The available responses do not demonstrate a
stable semantic improvement from reference
augmentation. The incomplete design prevents
a reliable comparative conclusion.

The single REJ16 response provides no estimate
of repeated-run agreement.

## Reproducibility

Run:

    python verify_review.py

The verifier checks the distributed file hashes
and selected reported counts against the
distributed assessment and result tables.

It does not recreate the model requests, rerun
Passant or independently validate the legal labels.

## Acceptance boundaries

Completed work consists of development experiments,
provisional assessments and controlled engineering
regressions.

Outstanding work includes independent statutory
review, formal experimental scope confirmation,
the incomplete balanced reference experiment and
production application integration.

The MCP result covers actual stdio communication
with a synthetic lifespan, not production embedding
initialization or the full tax preparation system.
