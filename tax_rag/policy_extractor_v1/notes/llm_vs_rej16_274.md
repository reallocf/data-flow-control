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
