REGULATION Z FULL ENFORCEMENT AUTHORING — WORKFLOW BENCHMARK v1

Implement the supplied application's Regulation Z section 1026.53 enforcement in
the target interface. Work once, without tools, Internet, execution feedback, prior
sessions, or repairs. Use the supplied specification, schema, evidence contract,
and interface documentation. Return a complete implementation, not fragments for
an unseen application to assemble into its enforcement logic.

The example UPDATE is the application operation. Make it reusable for an input
allocation identifier instead of hard-coding ALLOC0000001. Its other SQL semantics
must be preserved. The task concerns allocation UPDATEs; the full legal specification
also discusses corrections, permissions, definitions, and other scope boundaries.
Determine what applies. Do not turn permissions or inapplicable requirements into
additional prohibitions. No prescribed number of checks, policies, CTEs, or
proposition-result rows is required. Organize the implementation yourself.

REQUIRED OBSERVABLE BEHAVIOR
For an in-scope allocation, the implementation must enforce the complete applicable
specification on the contributor selected by the original UPDATE. It must preserve
the selection, assignment amount, and unchanged allocation fields. Rejection must
not cause selection of another contributor. Do not apply policy filtering before
the original top-one choice. The evidence assessed must describe that contributor's
proposal, not another otherwise valid context. Missing/ambiguous required evidence
must not authorize a write. Rejecting a proposal must leave the sink unchanged;
an allowed proposal must have exactly the original UPDATE's effect. The GProM
interface below represents this as a read-only proposed-state decision, since that
workflow is a reenactment preflight rather than a physical UPDATE executor.

SHARED SUPPLIED INPUTS AND TRUST BOUNDARY
regz_context and detailed proposal records already exist. Their evidence preparation
is trusted and outside this authoring task. For a selected supported proposal, the
records describe the complete hypothetical allocation represented by the UPDATE:
the selected cash-advance lot receives its computed excess assignment, all other
allocation fields agree with the proposal, and the source account/payment/snapshot
are consistent. Do not recompute evidence counts from raw balances. This contract
removes evidence derivation equally for all approaches; it does not supply SQL for
selection, reenactment integration, condition composition, or checked writes.

Input fixtures use one target allocation per invocation, a stable database snapshot,
and a unique highest-ranked candidate when a candidate exists. No concurrent writers
or nondeterministic tie ordering are measured in this benchmark. The supplied UPDATE
is already the workload's selection specification; do not invent a replacement order.
Correctness of the native DFC engine is not assumed: rewritten execution must be
checked against the same selected-contributor behavior as the SQL implementations.
This workload does not implement a complete payment-posting or correction service.

OUTPUT
Return JSON only:
{"files": [{"path": "...", "content": "..."}]}
Use the filenames and executable interface described for your target. Do not include
Markdown fences, commentary, a prose plan, unimplemented placeholders, or per-policy
JSON records. The evaluator supplies only the documented parameters/engine outputs;
it will not fill in missing policy expressions, query bodies, or enforcement stages.
