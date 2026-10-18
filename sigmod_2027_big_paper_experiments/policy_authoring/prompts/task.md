You are implementing Regulation Z section 1026.53 policies in a one-shot experiment.
Generate the implementation once. You have no tools, internet, repository, earlier
conversation, or opportunity to execute, inspect tests, or repair your answer.
Use only the specification, evidence contract, and interface guide below.

TASK AND BOUNDARY
Implement the 31 independent policy conditions for P01-P08, P13-P20, P23,
P25-P29, and P32-P40. P09-P12, P21-P22, P24, and P30-P31 are supporting
scope, permissions, or definitions; do not invent additional rejection rules.
Use the supplied derived evidence. Do not implement evidence extraction,
natural-language interpretation, database installation, or application query rewriting.
The original UPDATE is included to explain the application, not to be rewritten.
For this policy-only task, assume a fixed application adapter identifies the contributor selected by that UPDATE, freezes it,
and presents its exact proposal context to your policies. It does not filter candidates
before LIMIT, and it does not choose a different proposal after rejection.
Atomic posting is also assumed to be handled by the application.
The evaluator exercises the policy layer directly; it does not test these adapter stages. Your task is policy authoring, not adapter
implementation. This same boundary applies to every implementation target.

Each policy is an independent conditional requirement: return true when it holds
or is inapplicable, and false when it is violated. Acceptance is the conjunction of
all 31 policy results. Do not reject an otherwise permitted allocation. Do not add
new legal interpretations. Use the operational assumptions and common experiment clarifications where they
qualify the proposition prose. The declared uncertainties remain outside this experiment.

OUTPUT
Return only a JSON object of the form:
{"policies": [{"id": "P01", "code": "..."}, ...]}
Include exactly one entry for every independent policy ID above, in numerical order.
The interface guide defines the code string. No Markdown fences or explanatory
text outside the JSON. Do not hard-code example account IDs, dates, or decisions.
