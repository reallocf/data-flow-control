# Section 274 completeness/correctness evaluation v2

Completeness is measured at the statutory rule-family level; correctness is measured at the executable candidate level.

| Condition | Raw | Unique | Detection recall | Usable recall | Exact recall | Exact precision |
|---|---:|---:|---:|---:|---:|---:|
| target_only | 6 | 6 | 0.2857 | 0.2857 | 0.0714 | 0.1667 |
| direct | 6 | 6 | 0.2143 | 0.2143 | 0.0000 | 0.0000 |
| bounded | 8 | 7 | 0.3571 | 0.2143 | 0.0000 | 0.0000 |

## Current interpretation

The bounded condition reaches more eligible statutory families than the smaller-context conditions, but the extra coverage is not converted into exact executable constraints in this pilot. Long-context retrieval should therefore be evaluated as two distinct stages: family discovery and faithful policy encoding.

The result is provisional because each context condition has only one generation run.
