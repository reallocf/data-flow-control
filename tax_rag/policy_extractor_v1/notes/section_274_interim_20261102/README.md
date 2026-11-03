# Section 274: Interim Research Findings

Research checkpoint: November 2026

## Scope

This study examines statutory reference retrieval and candidate Data Flow Control (DFC) constraints derived from 26 U.S.C. Section 274.

The work concerns statutory context construction, rule-family discovery, and constraint fidelity. It does not establish legal compliance or operational DFC enforcement.

## Retrieval capacity

A deterministic sample of 80 Title 26 sections was assessed separately from the Section 274 development example.

| Budget outcome | Sections |
| --- | ---: |
| Both retrieval approaches reached a budget boundary | 51 |
| Whole-section retrieval only | 4 |
| Structural-unit retrieval only | 0 |
| Neither approach | 25 |
| Total | 80 |

The whole-section threshold was 998,424 local tokens. The structural-unit threshold was 992,498 statutory tokens.

The budget accounting conventions differ. These counts do not establish comparative retrieval performance under identical token budgets.

Thirty-nine starting sections had the same whole-section total of 1,968,118 tokens. A saved dependency-graph snapshot showed an identical closed reachable collection of 1,195 sections for those starting points.

The graph snapshot and exact-token experiment have not been fully matched to the same code revision.

## Section 274 policy experiment

Three input conditions were evaluated in three saved trials each.

| Condition | Trials | Candidates | Mean eligible families found, of 14 | Historical exact candidates |
| --- | ---: | ---: | ---: | ---: |
| Target provision only | 3 | 16 | 4.00 | 1 |
| Direct references | 3 | 17 | 3.33 | 1 |
| Bounded structural context | 3 | 24 | 5.00 | 0 |

The nine trials contain 57 candidate occurrences and 29 distinct complete constraint strings.

The reference inventory contains 17 statutory rule families. Fourteen were eligible for evaluation within the current per-transaction abstraction. Three require cross-transaction aggregation.

Historical candidate classifications were:

| Classification | Candidates |
| --- | ---: |
| EXACT | 2 |
| PARTIAL | 29 |
| INCORRECT | 26 |

These are saved experimental labels, not final legal correctness determinations.

## Findings

Bounded structural context identified more eligible statutory rule families on average, without a corresponding improvement in the historical count of fully accurate constraints.

Preliminary review found errors involving annual gift limits, substantiation conditions, statutory citations, and applicable exceptions.

Rule-family detection and complete constraint semantics therefore require separate evaluation.

## Limitations

The policy experiment centers on one statutory section.

The capacity measurements address retrieval scale rather than legal completeness.

The abstract Receipt and Expense fields have not been fully mapped to the operational DFC schema.

The complete treatment of statutory and regulatory exceptions remains under review. Historical classifications have been retained without modification.

No additional model inference was conducted for this checkpoint.

## Next evaluation

A controlled model comparison requires a fixed statutory snapshot, transaction schema, correctness criteria, code revision, model configuration, and approved research budget.

## Files

- `capacity_summary.csv`: aggregate capacity outcomes for 80 sections.
- `trial_summary.csv`: saved candidate counts and family-level results for nine trials.

## Legal references

- 26 U.S.C. Section 274: https://uscode.house.gov/
- 26 CFR Section 1.132-5: https://www.ecfr.gov/current/title-26/section-1.132-5
- 26 CFR Section 1.274-13: https://www.ecfr.gov/current/title-26/section-1.274-13
