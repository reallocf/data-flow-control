-- Fully expanded SQL for the Page's example; stop and roll back on error.
-- PostgreSQL + ProvSQL; tested against PostgreSQL 17.11 / ProvSQL 1.13.0-dev.
-- PRECONDITIONS:
-- 1. payment_allocations is an ordinary PostgreSQL table (NOT add_provenance'd).
-- 2. regz_context is provenance-enabled; its batch_id is unique.
-- 3. Context/evidence is complete, non-null, authoritative, and recomputed for
--    this exact proposed complete allocation. Currency conversion is exact.
-- 4. The supplied proposal agrees with this UPDATE's amount and all unchanged
--    allocation fields. Schema-specific validation must establish that binding.
-- 5. The Page's input-integrity checks (capacity, totals, complete snapshots,
--    supported modes, duplicate submissions, stale replacements) ran already.
-- This is an allocation preflight/check wrapper, not a native UPDATE-provenance
-- implementation or a complete posting/reallocation/correction workflow.
-- One-time setup, if needed (not repeated on already-enabled tables):
-- CREATE EXTENSION provsql CASCADE;
-- SELECT provsql.add_provenance('regz_context');

BEGIN ISOLATION LEVEL SERIALIZABLE;
SET LOCAL search_path TO public, provsql;
SET LOCAL provsql.update_provenance = off;
SET LOCAL provsql.active = off;

-- Lock the sink before capturing the choice. Retry the WHOLE transaction on
-- serialization failure; never reuse a previous attempt's choice or context.
SELECT allocation_id
FROM payment_allocations
WHERE allocation_id = E'ALLOC0000001'
FOR UPDATE;

-- Freeze once BEFORE evaluating policies. Filtering candidates before LIMIT
-- would substitute a different proposal and change the requested semantics.
CREATE TEMP TABLE frozen_choice ON COMMIT DROP AS
SELECT allocation.allocation_id, selected.transaction_id AS selected_transaction_id, LEAST(allocation.total_payment - allocation.minimum_payment_portion, selected.amount) AS proposed_cash
FROM payment_allocations AS allocation, transactions AS selected JOIN regz_context ON regz_context.batch_id = selected.transaction_id JOIN balance_components AS component ON component.component_id = selected.transaction_id JOIN statements AS statement_context ON statement_context.statement_id = selected.statement_id JOIN allocation_snapshot_context AS snapshot ON snapshot.account_id = selected.account_id AND snapshot.statement_id = selected.statement_id
WHERE allocation.allocation_id = 'ALLOC0000001' AND allocation.account_id = selected.account_id AND selected.transaction_id = (SELECT candidate.transaction_id FROM transactions AS candidate JOIN daily_balances AS balance ON balance.account_id = candidate.account_id AND balance.statement_id = candidate.statement_id AND balance.balance_category = candidate.balance_category WHERE candidate.account_id = allocation.account_id AND candidate.statement_id = allocation.statement_id AND candidate.transaction_type = 'cash_advance' AND balance.balance_date = CAST('2027-01-30' AS DATE) ORDER BY balance.apr_applied DESC NULLS LAST LIMIT 1);

DO $$
BEGIN
  IF (SELECT count(*) FROM frozen_choice) <> 1 THEN
    RAISE EXCEPTION 'Expected exactly one selected proposal';
  END IF;
  IF EXISTS (
    SELECT 1 FROM frozen_choice f JOIN regz_context c
      ON c.batch_id = f.selected_transaction_id
    WHERE c.kind IS DISTINCT FROM 'allocation'
       OR c.method IS NULL
       OR c.method NOT IN ('default', 'deferred_request', 'secured_request')
       OR (c.method <> 'default' AND
           (c.request_basis IS NULL OR c.request_basis NOT IN
            ('contact', 'form', 'reallocation', 'agreement_default',
             'application_default', 'coupon_default')))
  ) THEN
    RAISE EXCEPTION 'Unsupported allocation context: review required';
  END IF;
END
$$;

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

-- Explain the selected context through ProvSQL. This is provenance of the
-- supplied context row, not independently reconstructed raw-evidence lineage,
-- nor provenance of ORDER BY/LIMIT or of the physical UPDATE.
SET LOCAL provsql.active = on;
SELECT c.batch_id, provenance() AS context_provenance,
       sr_formula(provenance()) AS context_formula
FROM regz_context AS c
JOIN frozen_choice AS f ON f.selected_transaction_id = c.batch_id;
SET LOCAL provsql.active = off;

DO $$
BEGIN
  IF (SELECT count(*) FROM regz_policy_results) <> 31
     OR EXISTS (SELECT 1 FROM regz_policy_results WHERE NOT passed) THEN
    RAISE EXCEPTION 'Regulation Z proposal rejected; inspect policy results';
  END IF;
END
$$;

-- Use the frozen identity and value; do not rerun the nondeterministic top-one.
UPDATE payment_allocations AS allocation
SET cash_advance_allocation = f.proposed_cash
FROM frozen_choice AS f
WHERE allocation.allocation_id = f.allocation_id
RETURNING allocation.allocation_id, f.selected_transaction_id,
          allocation.cash_advance_allocation;

COMMIT;
