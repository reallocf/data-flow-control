Scope:
- section: 274
- method: REJ16-style baseline
- output: outputs/rej16_reference_audit.csv

Result:
- 89 references were identified in the main text.
- all 89 rows were marked as valid in outputs/reference_gold.csv.
- precision is 1.0 for this checked set.

Limitation:
- recall and F1 are not measured yet because the AWS Bedrock LLM comparison is unavailable due to account permissions.
- see notes/permission_denied.md.
