EVIDENCE CONTRACT
The relation regz_context has one authoritative record per complete proposal.
batch_id is a unique non-null string. Every field below is non-null. Integer
counts are nonnegative; monetary values are exact integer cents. Summary fields
can be checked directly. They are evidence, not precomputed final decisions.

| Field | Type | Meaning |
|---|---|---|
| batch_id | VARCHAR | Identity of the proposal/context; identifies the selected balance-producing transaction in the example UPDATE. |
| kind | VARCHAR | allocation or correction. |
| method | VARCHAR | For allocations: default, deferred_request, or secured_request. Irrelevant for correction events. |
| request_basis | VARCHAR | contact, form, reallocation, agreement_default, application_default, or coupon_default; empty when not used. Unknown forms are handled upstream. |
| highest_violations | BIGINT | Ordinary APR-priority violating pairs involving the highest ordinary APR group. |
| remaining_violations | BIGINT | Ordinary APR-priority violating pairs involving remaining ordinary groups. |
| ordinary_violations | BIGINT | Total ordinary APR-priority violations. |
| deferred_violations | BIGINT | Lower-priority ordinary capacity receives excess while eligible deferred-window capacity remains. |
| snapshot_violations | BIGINT | Later lots receive excess before ordinary historical capacity is exhausted, within the applicable dispute/deferred priority classes. |
| early_expiry_violations | BIGINT | Deferred priority violations in the partial early-expiration cycle. |
| active_programs | BIGINT | Number of qualifying deferred programs active on the credited date. |
| window_programs | BIGINT | Number of active programs currently in their allocation-priority window. |
| unauthorized_changes | BIGINT | Reference-day practice changes lacking the required recorded authorization/effective-date evidence. |
| protected_reduction | BIGINT | Actual protected-balance reduction by the whole proposed payment, including minimum allocations. |
| unavoidable_protected_reduction | BIGINT | Minimum protected reduction forced by payment exceeding all undisputed capacity. |
| other_capacity_remaining | BIGINT | Other remaining capacity after allocation, relevant when protected reduction is unavoidable and positive. |
| effective_apr_mismatches | BIGINT | Proposed effective APRs differ from zero for active qualifying programs or the relevant ordinary snapshot APR. |
| window_start_mismatches | BIGINT | Proposed deferred-window starts differ from the eligible-cycle derivation. |
| request_mismatches | BIGINT | Proposed excess allocations differ from the elected request instructions. |
| unsecured_request_targets | BIGINT | Positive request allocations directed to unsecured balances. |
| request_feasibility_errors | BIGINT | Invalid targets/amounts/capacities, absent instructions, or total not equal to full excess; submission timing is separately represented by request_present. |
| superseding_requests | BIGINT | Submitted replacement requests explicitly superseding the chosen request. |
| instruction_count | BIGINT | Number of explicit allocation instructions. |
| missing_credit_cents | BIGINT | Required correction credits not covered by timely actual posted credits net of reversals. |
| notice_count | BIGINT | Qualifying timely correction notices to the required recipient with content evidence. |
| classification_mismatches | BIGINT | Proposed deferred-program classifications differ from the normalized contractual definition/exclusions. |
| interim_payment_omissions | BIGINT | Qualifying programs omitted because interim payments are required. |
| grace_classifications | BIGINT | Grace periods incorrectly classified as qualifying deferred programs. |
| zero_offer_classifications | BIGINT | Temporary zero offers incorrectly classified without retroactive accrued-interest liability. |
| issuer_elected | BOOLEAN | Issuer elected to honor the particular allocation request. |
| request_present | BOOLEAN | The relevant request was submitted by the decision date. |
| affirmative | BOOLEAN | Affirmative consumer conduct supports the request. |
| contact_channel | BOOLEAN | Contact was oral, electronic, or written. |
| form_completed | BOOLEAN | Request form was completed and submitted; this flag alone does not establish submission by the decision date. |
| prior_default_allocation | BOOLEAN | An earlier accepted default allocation exists and precedes the reallocation request. |
| required_condition | BOOLEAN | Acceptance of the requested allocation was imposed as a condition of services or program use. |
| snapshot_date | DATE | Selected reference date for rates and historical balance identities. |
| cycle_end | DATE | End of preceding billing cycle. |
| credited_date | DATE | Date payment is credited. |
| expected_snapshot_date | DATE | Reference date computed from the latest applicable recorded practice. |

The evidence preparation service validates complete proposal records, account scope,
nonnegative integer-cent amounts, balance capacities, minimum/excess totals, and
supported event/method types. It produces accurate evidence for each proposal. A
proposal may still violate the regulation. Preparing these facts does NOT establish
that the UPDATE selected that proposal, that it proposes the same write, or that
its enforcement succeeded. Those are properties of the target implementation.
Allocation-only fields have non-null placeholders in correction records and vice versa.

Evidence relationships: window_programs <= active_programs; positive
deferred_violations implies window_programs > 0; early_expiry_violations <=
deferred_violations; ordinary_violations = highest_violations + remaining_violations;
protected_reduction >= unavoidable_protected_reduction on complete allocations.
form_completed does not imply request_present: a submitted form can arrive after
the decision date. Other affirmative/conduct facts do not establish timely submission.
Only evidence consistent with its definitions and preparation preconditions is in
scope for correctness scoring. No result is prescribed on contradictory evidence.
