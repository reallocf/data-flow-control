# DFC Research Status
Date: November 12, 2026

## Research milestones
- IRC section 274: 57 provisional SQL candidates across 17 rule groups and 33 distinct forms. These are not independent legal determinations.
- Statutory materials: section 274 text from the 2024 and current archived editions, plus 12 external statutory sections per edition. Legal applicability and effective dates await independent review.
- Experiment materials: 68 edition-controlled prompts and 34 revised dependency prompts. Successful Bedrock model responses from this prompt inventory: 0.
- Independent legal worksheets: 16 historical scenarios, 12 date-paired cases, and 17 rule groups. Adjudicated legal outcomes: 0.
- Engineering: E.deduct field units remain unverified. Passant Boolean policy composition remains unresolved. Production status: BLOCKED.

## AWS permission findings
- AWS authentication, model catalog inquiries, and inference-profile inspection succeeded.
- Six initial Converse requests across three model routes and one later Nova Lite canary in us-east-2 returned AccessDeniedException. Successful model responses: 0.
- This evidence establishes denial for the tested identity and routes at the tested times. It does not establish an account-wide denial or identify the specific permission layer.
- Charles or an AWS administrator: please inspect effective bedrock:InvokeModel rights for Nova Lite, Mistral, Claude profile and its destination models; check IAM identity policies, permission boundaries, organization SCPs, endpoint policies, and model prerequisites. Confirm permitted routes and spending approval before another canary.

## Local evidence
- df_58.zip: consolidated research material, independent legal worksheets, and administrator instructions. SHA-256: 4233dcc063b7821259d5d58887ae77dcaf33c70c753af64e458389ccddb597df.
- Cumulative research log: retained locally.
- DFC_history_quarantine_20261112_022851_917348.zip: verified backup of 70 former files.
- This GitHub commit contains this progress note only. Research archives, private AWS details, and credentials remain local.
