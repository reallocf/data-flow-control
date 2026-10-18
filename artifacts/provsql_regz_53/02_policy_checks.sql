-- Exact predicates translated from the project catalog and linked Page assumptions.
-- Input: frozen_choice(allocation_id, selected_transaction_id, proposed_cash).
-- Run in the same transaction as 01_checked_update.sql, with provsql.active=off.
CREATE TEMP TABLE regz_policy_results ON COMMIT DROP AS
SELECT f.allocation_id, c.batch_id, checks.proposition_id,
       checks.condition IS TRUE AS passed
FROM frozen_choice AS f
JOIN regz_context AS c ON c.batch_id = f.selected_transaction_id
CROSS JOIN LATERAL (VALUES
    ('P01', ((c.kind <> 'allocation' OR (c.method <> 'default' OR (c.highest_violations = 0))))),
    ('P02', ((c.kind <> 'allocation' OR (c.method <> 'default' OR (c.remaining_violations = 0))))),
    ('P03', ((c.kind <> 'allocation' OR (c.classification_mismatches = 0)))),
    ('P04', ((c.kind <> 'allocation' OR (c.method <> 'default' OR (c.active_programs = 0 OR c.window_programs > 0 OR c.ordinary_violations = 0))))),
    ('P05', ((c.kind <> 'allocation' OR (c.method <> 'default' OR (c.deferred_violations = 0))))),
    ('P06', ((c.kind <> 'allocation' OR (c.method <> 'default' OR (c.window_programs = 0 OR c.ordinary_violations = 0))))),
    ('P07', ((c.kind <> 'allocation' OR (c.method <> 'deferred_request' OR (c.active_programs > 0 AND c.issuer_elected AND c.request_mismatches = 0))))),
    ('P08', ((c.kind <> 'allocation' OR (c.method <> 'secured_request' OR (c.unsecured_request_targets = 0 AND c.issuer_elected AND c.request_mismatches = 0))))),
    ('P13', ((c.kind <> 'allocation' OR (c.snapshot_date >= c.cycle_end AND c.snapshot_date <= c.credited_date)))),
    ('P14', ((c.kind <> 'allocation' OR (c.snapshot_date = c.expected_snapshot_date)))),
    ('P15', ((c.kind <> 'allocation' OR (c.unauthorized_changes = 0)))),
    ('P16', ((c.kind <> 'allocation' OR (c.method <> 'default' OR (c.snapshot_violations = 0))))),
    ('P17', ((c.kind <> 'allocation' OR (c.protected_reduction <= c.unavoidable_protected_reduction)))),
    ('P18', ((c.kind <> 'allocation' OR (c.unavoidable_protected_reduction = 0 OR c.protected_reduction = 0 OR c.other_capacity_remaining = 0)))),
    ('P19', ((c.kind <> 'correction' OR c.missing_credit_cents = 0))),
    ('P20', ((c.kind <> 'correction' OR c.notice_count > 0))),
    ('P23', ((c.kind <> 'allocation' OR (c.effective_apr_mismatches = 0)))),
    ('P25', ((c.kind <> 'allocation' OR (c.interim_payment_omissions = 0)))),
    ('P26', ((c.kind <> 'allocation' OR (c.grace_classifications = 0)))),
    ('P27', ((c.kind <> 'allocation' OR (c.zero_offer_classifications = 0)))),
    ('P28', ((c.kind <> 'allocation' OR (c.window_start_mismatches = 0)))),
    ('P29', ((c.kind <> 'allocation' OR (c.method <> 'default' OR (c.early_expiry_violations = 0))))),
    ('P32', ((c.kind <> 'allocation' OR (c.request_present OR c.method = 'default')))),
    ('P33', ((c.kind <> 'allocation' OR (c.method = 'default' OR (c.request_feasibility_errors = 0 AND c.superseding_requests = 0))))),
    ('P34', ((c.kind <> 'allocation' OR (c.method = 'default' OR (c.request_basis <> 'contact' OR (c.affirmative AND c.instruction_count > 0 AND c.contact_channel)))))),
    ('P35', ((c.kind <> 'allocation' OR (c.method = 'default' OR (c.request_basis <> 'form' OR (c.form_completed AND c.request_present AND c.affirmative AND c.instruction_count > 0)))))),
    ('P36', ((c.kind <> 'allocation' OR (c.method = 'default' OR (c.request_basis <> 'reallocation' OR (c.prior_default_allocation AND c.affirmative AND c.contact_channel AND c.instruction_count > 0)))))),
    ('P37', ((c.kind <> 'allocation' OR (c.method = 'default' OR (c.request_basis <> 'agreement_default'))))),
    ('P38', ((c.kind <> 'allocation' OR (c.method = 'default' OR (c.request_basis <> 'application_default'))))),
    ('P39', ((c.kind <> 'allocation' OR (c.method = 'default' OR (c.request_basis <> 'coupon_default'))))),
    ('P40', ((c.kind <> 'allocation' OR (c.method = 'default' OR (NOT c.required_condition)))))
) AS checks(proposition_id, condition);

SELECT allocation_id, batch_id, proposition_id, passed
FROM regz_policy_results
ORDER BY allocation_id, proposition_id;
