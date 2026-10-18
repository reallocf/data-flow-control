COMMON EXPERIMENT CLARIFICATIONS — revision 2

These are shared operational conventions for this experiment, not additional legal
text or an implementation for any particular approach. They resolve the boundaries
between the supplied propositions, evidence computation, and individual policies.

POLICY RESPONSIBILITY AND APPLICABILITY
Each policy reports its own requirement: true if satisfied or inapplicable, false
if violated. Do not import another policy's requirement merely to make this policy
reject a proposal that the complete policy set would reject anyway. The final
acceptance decision is the conjunction of the individual results.

- P01 and P02 own ordinary APR ordering for default allocations. Their evidence
  already accounts for the stated priority classes and exceptions.
- P04 is the additional outside-window rule for an active qualifying deferred
  program. It applies to default allocations with an active qualifying program
  and no program currently in its priority window. With no active qualifying
  program, P04 is inapplicable; ordinary ordering remains the responsibility of
  P01 and P02. In P04's applicable context, ordinary-priority violations must be
  absent.
- P05 concerns deferred-window priority for default allocations. The
  deferred_violations count already measures breaches of this priority, including
  window eligibility. Without an eligible window program, there cannot be such a
  violation. An explicit applicability guard and relying on that count's meaning
  are equivalent on consistent evidence; do not infer a requirement to reject
  contradictory evidence from this proposition.
- P17 owns avoidance/minimization of protected reduction across the whole payment.
  P18 separately governs an actual positive protected reduction when some
  protected reduction is unavoidable: other capacity must then be exhausted.
  If there is no actual protected reduction, P18 is inapplicable. If unavoidable
  protected reduction is zero, P18 is inapplicable; P17 still governs any actual
  protected reduction. Keep the two responsibilities separate.
- P35 applies when an allocation uses a request method and the request basis is
  a form. It requires affirmative conduct, a completed/submitted request form,
  at least one explicit instruction, and submission of the relevant request by
  the decision date. Default allocations and other request bases are outside
  P35's scope. Form completion alone does not establish timely submission.

EVIDENCE RELATIONSHIPS AND TIMING
These relationships follow from the authoritative evidence computation and the
upstream structural checks. Policy authors may rely on them; they need not add
checks to enforce the relationships themselves.

- window_programs counts a subset of active_programs, so it cannot exceed it.
- A positive deferred_violations count implies a positive window_programs count.
  An early_expiry_violations count cannot exceed deferred_violations.
- ordinary_violations is the sum of highest_violations and remaining_violations.
- With complete, nonnegative allocations of the full payment within balance
  capacities, actual protected_reduction cannot be smaller than
  unavoidable_protected_reduction. This lower bound follows from the amount
  exceeding all undisputed capacity; it is not an additional regulatory rule.
- form_completed records completion/submission of a form as a fact about the
  request evidence. It does not establish submission by the decision date.
  request_present independently records that the relevant request has a submission
  date no later than the decision date. Thus form_completed may be true while
  request_present is false, for example when the form was submitted too late.
  affirmative and instruction_count establish separate facts and do not replace
  the submission-timing check.
- Authoritative evidence may describe a noncompliant allocation or an untimely
  request. "Trusted" means its facts and derived counts are accurate, not that
  the proposed allocation has already passed the regulatory policies.

ABSTRACT PROBES AND THE MEANING OF CORRECTNESS
A synthetic context is suitable for semantic correctness scoring only if it is
consistent with the evidence definitions, their relationships, and upstream
preconditions. Contexts that contradict these facts are outside the supported
input domain. They have no prescribed regulatory pass/fail result in this task.
An evaluator may retain them as diagnostic probes, but disagreement on them alone
must not be interpreted as an incorrectly authored policy. Independent-field
variation does not override the meanings or relationships documented above.
