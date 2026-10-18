-- psql template; run with ON_ERROR_STOP=1. Defaults reproduce the Page's query.
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

\set ON_ERROR_STOP on
\if :{?allocation_id}
\else
  \set allocation_id ALLOC0000001
\endif
\if :{?balance_date}
\else
  \set balance_date 2027-01-30
\endif

BEGIN ISOLATION LEVEL SERIALIZABLE;
SET LOCAL search_path TO public, provsql;
SET LOCAL provsql.update_provenance = off;
SET LOCAL provsql.active = off;

-- Lock the sink before capturing the choice. Retry the WHOLE transaction on
-- serialization failure; never reuse a previous attempt's choice or context.
SELECT allocation_id
FROM payment_allocations
WHERE allocation_id = :'allocation_id'
FOR UPDATE;

-- Freeze once BEFORE evaluating policies. Filtering candidates before LIMIT
-- would substitute a different proposal and change the requested semantics.
CREATE TEMP TABLE frozen_choice ON COMMIT DROP AS
SELECT allocation.allocation_id,
       selected.transaction_id AS selected_transaction_id,
       LEAST(allocation.total_payment - allocation.minimum_payment_portion,
             selected.amount) AS proposed_cash
FROM payment_allocations AS allocation
JOIN transactions AS selected ON allocation.account_id = selected.account_id
JOIN regz_context ON regz_context.batch_id = selected.transaction_id
JOIN balance_components AS component
  ON component.component_id = selected.transaction_id
JOIN statements AS statement_context
  ON statement_context.statement_id = selected.statement_id
JOIN allocation_snapshot_context AS snapshot
  ON snapshot.account_id = selected.account_id
 AND snapshot.statement_id = selected.statement_id
WHERE allocation.allocation_id = :'allocation_id'
  AND selected.transaction_id = (
    SELECT candidate.transaction_id
    FROM transactions AS candidate
    JOIN daily_balances AS balance
      ON balance.account_id = candidate.account_id
     AND balance.statement_id = candidate.statement_id
     AND balance.balance_category = candidate.balance_category
    WHERE candidate.account_id = allocation.account_id
      AND candidate.statement_id = allocation.statement_id
      AND candidate.transaction_type = 'cash_advance'
      AND balance.balance_date = :'balance_date'::date
    ORDER BY balance.apr_applied DESC
    LIMIT 1
  );

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

\ir 02_policy_checks.sql

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
