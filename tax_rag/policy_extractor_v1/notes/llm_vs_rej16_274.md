# LLM vs REJ16 reference extraction on section 274

Scope: 26 U.S.C. section 274 only.

Bedrock setup:
- Created a tagged application inference profile.
- Tags:
  - project=data-flow-control
  - billing-tag1=zs2666
- The gpt-oss-120b profile was reachable but returned reasoning content without final JSON for the extraction prompt.
- A Qwen3 32B profile returned usable output.

Files added:
- bedrock_ref_client.py
- llm_reference_audit_274.py
- compare_reference_274.py
- score_reference_274.py
- outputs/llm_reference_audit_274.csv
- outputs/reference_union_gold_274.csv
- outputs/reference_eval_summary_274.csv

Current first-pass result:
| method | tp | fp | fn | precision | recall | f1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| rej16 | 42 | 0 | 42 | 1.0000 | 0.5000 | 0.6667 |
| llm | 54 | 27 | 30 | 0.6667 | 0.6429 | 0.6545 |

Interpretation:
- REJ16 is more precise on this first-pass review.
- The Bedrock extractor has broader candidate coverage but lower precision in the first-pass review.
- The current union file still has duplicate rows caused by target-path formatting differences, such as e.1 versus e(1), so these numbers should be treated as provisional.
- The next improvement should normalize target paths before finalizing the comparison.

Suggested next step:
Normalize target paths, rebuild the union file, and rerun the same §274 scoring before expanding to sections 212 and 162.

## 2026-06-26 target normalization

Normalized §274 target paths before scoring. This merged formatting-only variants such as e.1 and e(1), reducing the union file from the earlier duplicate-heavy version to 95 rows.

Current §274 scores:

| method | tp | fp | fn | precision | recall | f1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| rej16 | 42 | 0 | 26 | 1.0000 | 0.6176 | 0.7636 |
| llm | 53 | 27 | 15 | 0.6625 | 0.7794 | 0.7162 |

In this context, "gold" is the row-level review label in outputs/reference_union_gold_274.csv. That file was first created by integrating candidate rows from outputs/rej16_reference_audit.csv and outputs/llm_reference_audit_274.csv, with the "gold" label left blank. The 1/0 values came from manual review of those candidate rows against the §274 text: "gold"=1 accepts the row as a cross-reference candidate for scoring, and "gold"=0 rejects it.
How to read outputs/reference_union_gold_274.csv:

- surface: extracted phrase from the §274 text.
- target_section: legal section that the phrase points to.
- target_path: normalized path inside the target section, for example e.2.b.i means §274(e)(2)(B)(i).
- ref_class: reference category, for example local_structural means a structural reference inside the same section.
- span_start: starting character offset of surface in the §274 text.
- span_end: ending character offset of surface in the §274 text.
- rej16: 1 if outputs/rej16_reference_audit.csv produced the row; otherwise 0.
- llm: 1 if outputs/llm_reference_audit_274.csv produced the row; otherwise 0.
- "gold": manual review label described above.
- note: brief explanation for special cases, for example no exact surface span.

When span_start and span_end are both -1, the phrase was not anchored to an exact text span in §274.
