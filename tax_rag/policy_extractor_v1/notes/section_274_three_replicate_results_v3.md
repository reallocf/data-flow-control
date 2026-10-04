# Section 274 three-replicate result

The experiment compares three fixed prompt conditions while holding the model and policy instructions constant:

- target-only: 5,926 prompt tokens
- direct references: 30,260 prompt tokens
- bounded recursive context: 671,210 prompt tokens

The bounded context contains every complete dependency layer through Layer 3; the complete Layer 4 frontier exceeds the statutory-context budget.

| Condition | Family discovery recall | Usable-family recall | Exact-family recall | Exact candidate precision | Exact-string stability | Family stability |
|---|---:|---:|---:|---:|---:|---:|
| target_only | 0.2857 | 0.2381 | 0.0476 | 0.0625 | 0.0000 | 0.5000 |
| direct | 0.2381 | 0.2381 | 0.0238 | 0.0588 | 0.4266 | 0.8333 |
| bounded | 0.3571 | 0.2143 | 0.0000 | 0.0000 | 1.0000 | 1.0000 |

## Result

Bounded recursive context produced the broadest statutory-family discovery: mean eligible-family detection recall was 0.3571, compared with 0.2857 for target-only and 0.2381 for direct context. However, bounded usable-family recall was 0.2143 and no bounded candidate was judged executable-exact.

The stability result is particularly informative. The bounded condition reproduced the same seven unique normalized constraints in all three runs (exact-string mean Jaccard 1.0000), and its detected family set was also identical across all three runs. Thus the observed encoding errors are systematic under this frozen prompt rather than merely generation instability.

This supports a two-stage interpretation for the Section 274 case study: recursive retrieval improves legal-rule discovery breadth, while faithful translation of the retrieved law into executable DFC constraints remains the limiting stage.

The evidence is a Section 274 case study with three generations per condition and should not be generalized to other statutes or models without additional experiments.
