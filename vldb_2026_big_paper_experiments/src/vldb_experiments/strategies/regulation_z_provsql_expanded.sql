-- Each policy contains its raw-evidence calculations and input validation.
-- Only the selected proposal's provenance is recorded, not a circuit of the
-- complete policy computation or the native UPDATE. Structured facts are trusted.
BEGIN ISOLATION LEVEL SERIALIZABLE;
SET LOCAL search_path TO public, provsql;
SET LOCAL jit = off;
SET LOCAL statement_timeout = '90s';
SET LOCAL provsql.active = off;
SET LOCAL provsql.update_provenance = off;

SELECT allocation_id FROM payment_allocations
WHERE allocation_id = {{allocation_id_literal}} FOR UPDATE;
CREATE TEMP TABLE frozen_choice ON COMMIT DROP AS
{{candidate_query}};

DO $$
BEGIN
  IF (SELECT count(*) FROM frozen_choice) <> 1 THEN
    RAISE EXCEPTION 'Expected exactly one selected proposal';
  END IF;
  IF EXISTS (
    SELECT 1 FROM frozen_choice f
    WHERE (f.selected_transaction_id IN ({{valid_input}})) IS NOT TRUE
  ) THEN
    RAISE EXCEPTION 'Invalid raw allocation evidence';
  END IF;
  IF EXISTS (
    SELECT 1 FROM frozen_choice f
    JOIN payment_allocations allocation ON allocation.allocation_id=f.allocation_id
    WHERE ({{update_binding}}) IS NOT TRUE
  ) THEN
    RAISE EXCEPTION 'UPDATE does not match complete raw proposal';
  END IF;
END
$$;

CREATE TEMP TABLE regz_policy_results ON COMMIT DROP AS
SELECT f.allocation_id, f.selected_transaction_id AS batch_id,
       checks.proposition_id, checks.condition IS TRUE AS passed
FROM frozen_choice f
CROSS JOIN LATERAL (VALUES
{{policy_values}}
) AS checks(proposition_id, condition);

SELECT allocation_id, batch_id, proposition_id, passed
FROM regz_policy_results ORDER BY allocation_id, proposition_id;

SET LOCAL provsql.active = on;
SELECT p.batch_id, provenance() AS proposal_provenance,
       sr_formula(provenance()) AS proposal_formula
FROM regz_proposals p JOIN frozen_choice f ON f.selected_transaction_id=p.batch_id;
SET LOCAL provsql.active = off;

DO $$
BEGIN
  IF (SELECT count(*) FROM regz_policy_results) <> {{policy_count}}
     OR EXISTS (SELECT 1 FROM regz_policy_results WHERE NOT passed) THEN
    RAISE EXCEPTION 'Regulation Z proposal rejected; inspect policy results';
  END IF;
END
$$;

UPDATE payment_allocations allocation SET cash_advance_allocation=f.proposed_cash
FROM frozen_choice f WHERE allocation.allocation_id=f.allocation_id
RETURNING allocation.allocation_id, f.selected_transaction_id,
          allocation.cash_advance_allocation;
COMMIT;
