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
WHERE allocation_id = E'ALLOC0000017' FOR UPDATE;
CREATE TEMP TABLE frozen_choice ON COMMIT DROP AS
SELECT allocation.allocation_id, selected.transaction_id AS selected_transaction_id, LEAST(allocation.total_payment - allocation.minimum_payment_portion, selected.amount) AS proposed_cash
FROM payment_allocations AS allocation, transactions AS selected
WHERE allocation.allocation_id = 'ALLOC0000017' AND allocation.account_id = selected.account_id AND selected.transaction_id = (SELECT candidate.transaction_id FROM transactions AS candidate JOIN daily_balances AS balance ON balance.account_id = candidate.account_id AND balance.statement_id = candidate.statement_id AND balance.balance_category = candidate.balance_category WHERE candidate.account_id = allocation.account_id AND candidate.statement_id = allocation.statement_id AND candidate.transaction_type = 'cash_advance' AND balance.balance_date = CAST('2028-06-09' AS DATE) ORDER BY balance.apr_applied DESC NULLS LAST LIMIT 1);

DO $$
BEGIN
  IF (SELECT count(*) FROM frozen_choice) <> 1 THEN
    RAISE EXCEPTION 'Expected exactly one selected proposal';
  END IF;
  IF EXISTS (
    SELECT 1 FROM frozen_choice f
    WHERE (f.selected_transaction_id IN (SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default'))))) IS NOT TRUE
  ) THEN
    RAISE EXCEPTION 'Invalid raw allocation evidence';
  END IF;
  IF EXISTS (
    SELECT 1 FROM frozen_choice f
    JOIN payment_allocations allocation ON allocation.allocation_id=f.allocation_id
    WHERE ((
 f.proposed_cash*100 = (
   SELECT line.excess_cents FROM regz_proposal_lines line
   WHERE line.batch_id=f.selected_transaction_id AND line.balance_id=f.selected_transaction_id
 ) AND EXISTS (
   SELECT 1 FROM regz_proposals bp JOIN regz_payments pay ON pay.payment_id=bp.payment_id
   WHERE bp.batch_id=f.selected_transaction_id AND pay.payment_id=allocation.transaction_id
     AND pay.account_id=allocation.account_id AND pay.credited_date=allocation.payment_date
     AND pay.total_cents=allocation.total_payment*100
     AND pay.minimum_cents=allocation.minimum_payment_portion*100
     AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines other_line
       WHERE other_line.batch_id=bp.batch_id AND other_line.balance_id<>bp.batch_id
         AND other_line.excess_cents<>0)
 ) AND allocation.purchase_allocation=0 AND allocation.balance_transfer_allocation=0
   AND allocation.promo_allocation=0
)) IS NOT TRUE
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
('P01', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
program_terms AS (
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
),
lot_terms AS (
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
),
lots AS (
SELECT t.*, CASE WHEN t.active THEN 0 ELSE t.reference_apr END AS effective_apr,
 CASE WHEN t.active THEN (
   SELECT MIN(last_two.start_date) FROM (
     SELECT c.start_date FROM regz_cycles c
     WHERE c.account_id = t.account_id AND c.start_date <= t.expiration_date
       AND c.due_date <= t.expiration_date
     ORDER BY c.start_date DESC LIMIT 2
   ) last_two
 ) ELSE NULL END AS window_start,
 CASE WHEN t.active THEN (
   SELECT c.start_date FROM regz_cycles c WHERE c.account_id = t.account_id
     AND t.expiration_date BETWEEN c.start_date AND c.end_date
     AND t.expiration_date < c.due_date
 ) ELSE NULL END AS early_start
FROM lot_terms t
),
slices AS (
SELECT l.batch_id, l.balance_id, part.disputed, l.phase, l.effective_apr,
 COALESCE(l.active AND l.credited_date BETWEEN l.window_start
          AND l.expiration_date, false) AS in_window,
 COALESCE(l.active AND l.credited_date BETWEEN l.window_start AND l.expiration_date
          AND l.credited_date >= l.early_start, false) AS early,
 CASE WHEN part.disputed THEN
   l.protected_cents - GREATEST(0, l.minimum_cents - (l.amount_cents-l.protected_cents))
 ELSE GREATEST(0, l.amount_cents-l.protected_cents-l.minimum_cents) END AS capacity,
 CASE WHEN part.disputed THEN
   GREATEST(0, l.excess_cents-GREATEST(0,l.amount_cents-l.protected_cents-l.minimum_cents))
 ELSE LEAST(l.excess_cents,GREATEST(0,l.amount_cents-l.protected_cents-l.minimum_cents))
 END AS allocated
FROM lots l CROSS JOIN (VALUES (false), (true)) part(disputed)
),
pairs AS (
SELECT h.batch_id,
 h.in_window AND NOT lo.in_window AS deferred,
 h.in_window AND NOT lo.in_window AND h.early AS early,
 NOT h.in_window AND NOT lo.in_window AND h.phase < lo.phase AS historical,
 NOT h.in_window AND NOT lo.in_window AND h.phase = lo.phase
   AND h.effective_apr > lo.effective_apr AS ordinary,
 h.effective_apr = (
   SELECT MAX(peer.effective_apr) FROM slices peer
   WHERE peer.batch_id = h.batch_id AND peer.disputed = h.disputed
     AND peer.phase = h.phase AND NOT peer.in_window AND peer.capacity > 0
 ) AS highest
FROM slices h JOIN slices lo ON lo.batch_id = h.batch_id AND lo.disputed = h.disputed
WHERE h.capacity > 0 AND lo.capacity > 0 AND h.allocated < h.capacity AND lo.allocated > 0
)
SELECT b.batch_id, (SELECT COUNT(*) FROM pairs x WHERE x.batch_id = b.batch_id AND (x.ordinary AND x.highest)) AS highest_violations,
       'allocation' AS kind,
       b.method AS method
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.method <> 'default' OR (e.highest_violations = 0)))
))),
('P02', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
program_terms AS (
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
),
lot_terms AS (
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
),
lots AS (
SELECT t.*, CASE WHEN t.active THEN 0 ELSE t.reference_apr END AS effective_apr,
 CASE WHEN t.active THEN (
   SELECT MIN(last_two.start_date) FROM (
     SELECT c.start_date FROM regz_cycles c
     WHERE c.account_id = t.account_id AND c.start_date <= t.expiration_date
       AND c.due_date <= t.expiration_date
     ORDER BY c.start_date DESC LIMIT 2
   ) last_two
 ) ELSE NULL END AS window_start,
 CASE WHEN t.active THEN (
   SELECT c.start_date FROM regz_cycles c WHERE c.account_id = t.account_id
     AND t.expiration_date BETWEEN c.start_date AND c.end_date
     AND t.expiration_date < c.due_date
 ) ELSE NULL END AS early_start
FROM lot_terms t
),
slices AS (
SELECT l.batch_id, l.balance_id, part.disputed, l.phase, l.effective_apr,
 COALESCE(l.active AND l.credited_date BETWEEN l.window_start
          AND l.expiration_date, false) AS in_window,
 COALESCE(l.active AND l.credited_date BETWEEN l.window_start AND l.expiration_date
          AND l.credited_date >= l.early_start, false) AS early,
 CASE WHEN part.disputed THEN
   l.protected_cents - GREATEST(0, l.minimum_cents - (l.amount_cents-l.protected_cents))
 ELSE GREATEST(0, l.amount_cents-l.protected_cents-l.minimum_cents) END AS capacity,
 CASE WHEN part.disputed THEN
   GREATEST(0, l.excess_cents-GREATEST(0,l.amount_cents-l.protected_cents-l.minimum_cents))
 ELSE LEAST(l.excess_cents,GREATEST(0,l.amount_cents-l.protected_cents-l.minimum_cents))
 END AS allocated
FROM lots l CROSS JOIN (VALUES (false), (true)) part(disputed)
),
pairs AS (
SELECT h.batch_id,
 h.in_window AND NOT lo.in_window AS deferred,
 h.in_window AND NOT lo.in_window AND h.early AS early,
 NOT h.in_window AND NOT lo.in_window AND h.phase < lo.phase AS historical,
 NOT h.in_window AND NOT lo.in_window AND h.phase = lo.phase
   AND h.effective_apr > lo.effective_apr AS ordinary,
 h.effective_apr = (
   SELECT MAX(peer.effective_apr) FROM slices peer
   WHERE peer.batch_id = h.batch_id AND peer.disputed = h.disputed
     AND peer.phase = h.phase AND NOT peer.in_window AND peer.capacity > 0
 ) AS highest
FROM slices h JOIN slices lo ON lo.batch_id = h.batch_id AND lo.disputed = h.disputed
WHERE h.capacity > 0 AND lo.capacity > 0 AND h.allocated < h.capacity AND lo.allocated > 0
)
SELECT b.batch_id, 'allocation' AS kind,
       b.method AS method,
       (SELECT COUNT(*) FROM pairs x WHERE x.batch_id = b.batch_id AND (x.ordinary AND NOT x.highest)) AS remaining_violations
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.method <> 'default' OR (e.remaining_violations = 0)))
))),
('P03', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
program_terms AS (
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
),
lot_terms AS (
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
)
SELECT b.batch_id, (SELECT COUNT(*) FROM lot_terms x WHERE x.batch_id = b.batch_id AND (x.program_id IS NOT NULL AND x.declared IS DISTINCT FROM x.qualifies)) AS classification_mismatches,
       'allocation' AS kind
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.classification_mismatches = 0))
))),
('P04', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
program_terms AS (
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
),
lot_terms AS (
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
),
lots AS (
SELECT t.*, CASE WHEN t.active THEN 0 ELSE t.reference_apr END AS effective_apr,
 CASE WHEN t.active THEN (
   SELECT MIN(last_two.start_date) FROM (
     SELECT c.start_date FROM regz_cycles c
     WHERE c.account_id = t.account_id AND c.start_date <= t.expiration_date
       AND c.due_date <= t.expiration_date
     ORDER BY c.start_date DESC LIMIT 2
   ) last_two
 ) ELSE NULL END AS window_start,
 CASE WHEN t.active THEN (
   SELECT c.start_date FROM regz_cycles c WHERE c.account_id = t.account_id
     AND t.expiration_date BETWEEN c.start_date AND c.end_date
     AND t.expiration_date < c.due_date
 ) ELSE NULL END AS early_start
FROM lot_terms t
),
slices AS (
SELECT l.batch_id, l.balance_id, part.disputed, l.phase, l.effective_apr,
 COALESCE(l.active AND l.credited_date BETWEEN l.window_start
          AND l.expiration_date, false) AS in_window,
 COALESCE(l.active AND l.credited_date BETWEEN l.window_start AND l.expiration_date
          AND l.credited_date >= l.early_start, false) AS early,
 CASE WHEN part.disputed THEN
   l.protected_cents - GREATEST(0, l.minimum_cents - (l.amount_cents-l.protected_cents))
 ELSE GREATEST(0, l.amount_cents-l.protected_cents-l.minimum_cents) END AS capacity,
 CASE WHEN part.disputed THEN
   GREATEST(0, l.excess_cents-GREATEST(0,l.amount_cents-l.protected_cents-l.minimum_cents))
 ELSE LEAST(l.excess_cents,GREATEST(0,l.amount_cents-l.protected_cents-l.minimum_cents))
 END AS allocated
FROM lots l CROSS JOIN (VALUES (false), (true)) part(disputed)
),
pairs AS (
SELECT h.batch_id,
 h.in_window AND NOT lo.in_window AS deferred,
 h.in_window AND NOT lo.in_window AND h.early AS early,
 NOT h.in_window AND NOT lo.in_window AND h.phase < lo.phase AS historical,
 NOT h.in_window AND NOT lo.in_window AND h.phase = lo.phase
   AND h.effective_apr > lo.effective_apr AS ordinary,
 h.effective_apr = (
   SELECT MAX(peer.effective_apr) FROM slices peer
   WHERE peer.batch_id = h.batch_id AND peer.disputed = h.disputed
     AND peer.phase = h.phase AND NOT peer.in_window AND peer.capacity > 0
 ) AS highest
FROM slices h JOIN slices lo ON lo.batch_id = h.batch_id AND lo.disputed = h.disputed
WHERE h.capacity > 0 AND lo.capacity > 0 AND h.allocated < h.capacity AND lo.allocated > 0
)
SELECT b.batch_id, (SELECT COUNT(DISTINCT x.program_id) FROM lots x WHERE x.batch_id=b.batch_id AND x.active) AS active_programs,
       'allocation' AS kind,
       b.method AS method,
       (SELECT COUNT(*) FROM pairs x WHERE x.batch_id = b.batch_id AND (x.ordinary)) AS ordinary_violations,
       (SELECT COUNT(DISTINCT x.program_id) FROM lots x WHERE x.batch_id=b.batch_id AND x.active AND x.credited_date BETWEEN x.window_start AND x.expiration_date) AS window_programs
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.method <> 'default' OR (e.active_programs = 0 OR e.window_programs > 0 OR e.ordinary_violations = 0)))
))),
('P05', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
program_terms AS (
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
),
lot_terms AS (
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
),
lots AS (
SELECT t.*, CASE WHEN t.active THEN 0 ELSE t.reference_apr END AS effective_apr,
 CASE WHEN t.active THEN (
   SELECT MIN(last_two.start_date) FROM (
     SELECT c.start_date FROM regz_cycles c
     WHERE c.account_id = t.account_id AND c.start_date <= t.expiration_date
       AND c.due_date <= t.expiration_date
     ORDER BY c.start_date DESC LIMIT 2
   ) last_two
 ) ELSE NULL END AS window_start,
 CASE WHEN t.active THEN (
   SELECT c.start_date FROM regz_cycles c WHERE c.account_id = t.account_id
     AND t.expiration_date BETWEEN c.start_date AND c.end_date
     AND t.expiration_date < c.due_date
 ) ELSE NULL END AS early_start
FROM lot_terms t
),
slices AS (
SELECT l.batch_id, l.balance_id, part.disputed, l.phase, l.effective_apr,
 COALESCE(l.active AND l.credited_date BETWEEN l.window_start
          AND l.expiration_date, false) AS in_window,
 COALESCE(l.active AND l.credited_date BETWEEN l.window_start AND l.expiration_date
          AND l.credited_date >= l.early_start, false) AS early,
 CASE WHEN part.disputed THEN
   l.protected_cents - GREATEST(0, l.minimum_cents - (l.amount_cents-l.protected_cents))
 ELSE GREATEST(0, l.amount_cents-l.protected_cents-l.minimum_cents) END AS capacity,
 CASE WHEN part.disputed THEN
   GREATEST(0, l.excess_cents-GREATEST(0,l.amount_cents-l.protected_cents-l.minimum_cents))
 ELSE LEAST(l.excess_cents,GREATEST(0,l.amount_cents-l.protected_cents-l.minimum_cents))
 END AS allocated
FROM lots l CROSS JOIN (VALUES (false), (true)) part(disputed)
),
pairs AS (
SELECT h.batch_id,
 h.in_window AND NOT lo.in_window AS deferred,
 h.in_window AND NOT lo.in_window AND h.early AS early,
 NOT h.in_window AND NOT lo.in_window AND h.phase < lo.phase AS historical,
 NOT h.in_window AND NOT lo.in_window AND h.phase = lo.phase
   AND h.effective_apr > lo.effective_apr AS ordinary,
 h.effective_apr = (
   SELECT MAX(peer.effective_apr) FROM slices peer
   WHERE peer.batch_id = h.batch_id AND peer.disputed = h.disputed
     AND peer.phase = h.phase AND NOT peer.in_window AND peer.capacity > 0
 ) AS highest
FROM slices h JOIN slices lo ON lo.batch_id = h.batch_id AND lo.disputed = h.disputed
WHERE h.capacity > 0 AND lo.capacity > 0 AND h.allocated < h.capacity AND lo.allocated > 0
)
SELECT b.batch_id, (SELECT COUNT(*) FROM pairs x WHERE x.batch_id = b.batch_id AND (x.deferred)) AS deferred_violations,
       'allocation' AS kind,
       b.method AS method
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.method <> 'default' OR (e.deferred_violations = 0)))
))),
('P06', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
program_terms AS (
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
),
lot_terms AS (
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
),
lots AS (
SELECT t.*, CASE WHEN t.active THEN 0 ELSE t.reference_apr END AS effective_apr,
 CASE WHEN t.active THEN (
   SELECT MIN(last_two.start_date) FROM (
     SELECT c.start_date FROM regz_cycles c
     WHERE c.account_id = t.account_id AND c.start_date <= t.expiration_date
       AND c.due_date <= t.expiration_date
     ORDER BY c.start_date DESC LIMIT 2
   ) last_two
 ) ELSE NULL END AS window_start,
 CASE WHEN t.active THEN (
   SELECT c.start_date FROM regz_cycles c WHERE c.account_id = t.account_id
     AND t.expiration_date BETWEEN c.start_date AND c.end_date
     AND t.expiration_date < c.due_date
 ) ELSE NULL END AS early_start
FROM lot_terms t
),
slices AS (
SELECT l.batch_id, l.balance_id, part.disputed, l.phase, l.effective_apr,
 COALESCE(l.active AND l.credited_date BETWEEN l.window_start
          AND l.expiration_date, false) AS in_window,
 COALESCE(l.active AND l.credited_date BETWEEN l.window_start AND l.expiration_date
          AND l.credited_date >= l.early_start, false) AS early,
 CASE WHEN part.disputed THEN
   l.protected_cents - GREATEST(0, l.minimum_cents - (l.amount_cents-l.protected_cents))
 ELSE GREATEST(0, l.amount_cents-l.protected_cents-l.minimum_cents) END AS capacity,
 CASE WHEN part.disputed THEN
   GREATEST(0, l.excess_cents-GREATEST(0,l.amount_cents-l.protected_cents-l.minimum_cents))
 ELSE LEAST(l.excess_cents,GREATEST(0,l.amount_cents-l.protected_cents-l.minimum_cents))
 END AS allocated
FROM lots l CROSS JOIN (VALUES (false), (true)) part(disputed)
),
pairs AS (
SELECT h.batch_id,
 h.in_window AND NOT lo.in_window AS deferred,
 h.in_window AND NOT lo.in_window AND h.early AS early,
 NOT h.in_window AND NOT lo.in_window AND h.phase < lo.phase AS historical,
 NOT h.in_window AND NOT lo.in_window AND h.phase = lo.phase
   AND h.effective_apr > lo.effective_apr AS ordinary,
 h.effective_apr = (
   SELECT MAX(peer.effective_apr) FROM slices peer
   WHERE peer.batch_id = h.batch_id AND peer.disputed = h.disputed
     AND peer.phase = h.phase AND NOT peer.in_window AND peer.capacity > 0
 ) AS highest
FROM slices h JOIN slices lo ON lo.batch_id = h.batch_id AND lo.disputed = h.disputed
WHERE h.capacity > 0 AND lo.capacity > 0 AND h.allocated < h.capacity AND lo.allocated > 0
)
SELECT b.batch_id, 'allocation' AS kind,
       b.method AS method,
       (SELECT COUNT(*) FROM pairs x WHERE x.batch_id = b.batch_id AND (x.ordinary)) AS ordinary_violations,
       (SELECT COUNT(DISTINCT x.program_id) FROM lots x WHERE x.batch_id=b.batch_id AND x.active AND x.credited_date BETWEEN x.window_start AND x.expiration_date) AS window_programs
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.method <> 'default' OR (e.window_programs = 0 OR e.ordinary_violations = 0)))
))),
('P07', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
program_terms AS (
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
),
lot_terms AS (
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
),
lots AS (
SELECT t.*, CASE WHEN t.active THEN 0 ELSE t.reference_apr END AS effective_apr,
 CASE WHEN t.active THEN (
   SELECT MIN(last_two.start_date) FROM (
     SELECT c.start_date FROM regz_cycles c
     WHERE c.account_id = t.account_id AND c.start_date <= t.expiration_date
       AND c.due_date <= t.expiration_date
     ORDER BY c.start_date DESC LIMIT 2
   ) last_two
 ) ELSE NULL END AS window_start,
 CASE WHEN t.active THEN (
   SELECT c.start_date FROM regz_cycles c WHERE c.account_id = t.account_id
     AND t.expiration_date BETWEEN c.start_date AND c.end_date
     AND t.expiration_date < c.due_date
 ) ELSE NULL END AS early_start
FROM lot_terms t
),
request AS (
SELECT b.batch_id, r.* FROM base b JOIN regz_requests r ON r.request_id = b.request_id
WHERE b.method <> 'default'
),
request_targets AS (
SELECT r.batch_id, i.*, l.balance_id AS existing_balance,
       l.amount_cents AS capacity, l.minimum_cents, l.secured
FROM request r JOIN regz_request_lines i ON i.request_id=r.request_id
LEFT JOIN lot_terms l ON l.batch_id=r.batch_id AND l.balance_id=i.balance_id
),
request_allocations AS (
SELECT r.batch_id, l.balance_id, l.excess_cents, COALESCE(i.amount_cents,0) AS requested
FROM request r JOIN regz_proposal_lines l ON l.batch_id=r.batch_id
LEFT JOIN regz_request_lines i ON i.request_id=r.request_id AND i.balance_id=l.balance_id
)
SELECT b.batch_id, (SELECT COUNT(DISTINCT x.program_id) FROM lots x WHERE x.batch_id=b.batch_id AND x.active) AS active_programs,
       COALESCE((SELECT r.issuer_elected FROM request r WHERE r.batch_id=b.batch_id), false) AS issuer_elected,
       'allocation' AS kind,
       b.method AS method,
       (SELECT COUNT(*) FROM request_allocations x
      WHERE x.batch_id=b.batch_id AND x.excess_cents <> x.requested)
      + (SELECT COUNT(*) FROM request_targets x WHERE x.batch_id=b.batch_id
         AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
             WHERE l.batch_id=x.batch_id AND l.balance_id=x.balance_id)) AS request_mismatches
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.method <> 'deferred_request' OR (e.active_programs > 0 AND e.issuer_elected AND e.request_mismatches = 0)))
))),
('P08', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
program_terms AS (
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
),
lot_terms AS (
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
),
request AS (
SELECT b.batch_id, r.* FROM base b JOIN regz_requests r ON r.request_id = b.request_id
WHERE b.method <> 'default'
),
request_targets AS (
SELECT r.batch_id, i.*, l.balance_id AS existing_balance,
       l.amount_cents AS capacity, l.minimum_cents, l.secured
FROM request r JOIN regz_request_lines i ON i.request_id=r.request_id
LEFT JOIN lot_terms l ON l.batch_id=r.batch_id AND l.balance_id=i.balance_id
),
request_allocations AS (
SELECT r.batch_id, l.balance_id, l.excess_cents, COALESCE(i.amount_cents,0) AS requested
FROM request r JOIN regz_proposal_lines l ON l.batch_id=r.batch_id
LEFT JOIN regz_request_lines i ON i.request_id=r.request_id AND i.balance_id=l.balance_id
)
SELECT b.batch_id, COALESCE((SELECT r.issuer_elected FROM request r WHERE r.batch_id=b.batch_id), false) AS issuer_elected,
       'allocation' AS kind,
       b.method AS method,
       (SELECT COUNT(*) FROM request_allocations x
      WHERE x.batch_id=b.batch_id AND x.excess_cents <> x.requested)
      + (SELECT COUNT(*) FROM request_targets x WHERE x.batch_id=b.batch_id
         AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
             WHERE l.batch_id=x.batch_id AND l.balance_id=x.balance_id)) AS request_mismatches,
       (SELECT COUNT(*) FROM request r
      JOIN regz_request_lines i ON i.request_id=r.request_id
      JOIN lot_terms l ON l.batch_id=b.batch_id AND l.balance_id=i.balance_id
      WHERE r.batch_id=b.batch_id AND i.amount_cents > 0 AND NOT l.secured) AS unsecured_request_targets
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.method <> 'secured_request' OR (e.unsecured_request_targets = 0 AND e.issuer_elected AND e.request_mismatches = 0)))
))),
('P13', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
)
SELECT b.batch_id, b.credited_date AS credited_date,
       b.cycle_end AS cycle_end,
       'allocation' AS kind,
       b.snapshot_date AS snapshot_date
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.snapshot_date >= e.cycle_end AND e.snapshot_date <= e.credited_date))
))),
('P14', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
)
SELECT b.batch_id, (SELECT CASE p.convention WHEN 'credit_date' THEN b.credited_date WHEN 'offset' THEN b.cycle_end + CAST(p.offset_days AS INTEGER) ELSE b.cycle_end END FROM regz_practices p WHERE p.account_id=b.account_id AND p.effective_date <= b.credited_date ORDER BY p.effective_date DESC LIMIT 1) AS expected_snapshot_date,
       'allocation' AS kind,
       b.snapshot_date AS snapshot_date
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.snapshot_date = e.expected_snapshot_date))
))),
('P15', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
practice_history AS (
SELECT p.*, LAG(p.convention) OVER w AS previous_convention,
 LAG(p.offset_days) OVER w AS previous_offset
FROM regz_practices p
WINDOW w AS (PARTITION BY p.account_id ORDER BY p.effective_date)
)
SELECT b.batch_id, 'allocation' AS kind,
       (SELECT COUNT(*) FROM practice_history p WHERE p.account_id=b.account_id AND p.effective_date <= b.credited_date AND p.previous_convention IS NOT NULL AND (p.convention <> p.previous_convention OR p.offset_days <> p.previous_offset) AND (p.authorization_evidence IS NULL OR p.authorization_evidence='')) AS unauthorized_changes
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.unauthorized_changes = 0))
))),
('P16', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
program_terms AS (
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
),
lot_terms AS (
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
),
lots AS (
SELECT t.*, CASE WHEN t.active THEN 0 ELSE t.reference_apr END AS effective_apr,
 CASE WHEN t.active THEN (
   SELECT MIN(last_two.start_date) FROM (
     SELECT c.start_date FROM regz_cycles c
     WHERE c.account_id = t.account_id AND c.start_date <= t.expiration_date
       AND c.due_date <= t.expiration_date
     ORDER BY c.start_date DESC LIMIT 2
   ) last_two
 ) ELSE NULL END AS window_start,
 CASE WHEN t.active THEN (
   SELECT c.start_date FROM regz_cycles c WHERE c.account_id = t.account_id
     AND t.expiration_date BETWEEN c.start_date AND c.end_date
     AND t.expiration_date < c.due_date
 ) ELSE NULL END AS early_start
FROM lot_terms t
),
slices AS (
SELECT l.batch_id, l.balance_id, part.disputed, l.phase, l.effective_apr,
 COALESCE(l.active AND l.credited_date BETWEEN l.window_start
          AND l.expiration_date, false) AS in_window,
 COALESCE(l.active AND l.credited_date BETWEEN l.window_start AND l.expiration_date
          AND l.credited_date >= l.early_start, false) AS early,
 CASE WHEN part.disputed THEN
   l.protected_cents - GREATEST(0, l.minimum_cents - (l.amount_cents-l.protected_cents))
 ELSE GREATEST(0, l.amount_cents-l.protected_cents-l.minimum_cents) END AS capacity,
 CASE WHEN part.disputed THEN
   GREATEST(0, l.excess_cents-GREATEST(0,l.amount_cents-l.protected_cents-l.minimum_cents))
 ELSE LEAST(l.excess_cents,GREATEST(0,l.amount_cents-l.protected_cents-l.minimum_cents))
 END AS allocated
FROM lots l CROSS JOIN (VALUES (false), (true)) part(disputed)
),
pairs AS (
SELECT h.batch_id,
 h.in_window AND NOT lo.in_window AS deferred,
 h.in_window AND NOT lo.in_window AND h.early AS early,
 NOT h.in_window AND NOT lo.in_window AND h.phase < lo.phase AS historical,
 NOT h.in_window AND NOT lo.in_window AND h.phase = lo.phase
   AND h.effective_apr > lo.effective_apr AS ordinary,
 h.effective_apr = (
   SELECT MAX(peer.effective_apr) FROM slices peer
   WHERE peer.batch_id = h.batch_id AND peer.disputed = h.disputed
     AND peer.phase = h.phase AND NOT peer.in_window AND peer.capacity > 0
 ) AS highest
FROM slices h JOIN slices lo ON lo.batch_id = h.batch_id AND lo.disputed = h.disputed
WHERE h.capacity > 0 AND lo.capacity > 0 AND h.allocated < h.capacity AND lo.allocated > 0
)
SELECT b.batch_id, 'allocation' AS kind,
       b.method AS method,
       (SELECT COUNT(*) FROM pairs x WHERE x.batch_id = b.batch_id AND (x.historical)) AS snapshot_violations
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.method <> 'default' OR (e.snapshot_violations = 0)))
))),
('P17', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
program_terms AS (
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
),
lot_terms AS (
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
)
SELECT b.batch_id, 'allocation' AS kind,
       (SELECT COALESCE(SUM(GREATEST(0, x.minimum_cents+x.excess_cents-(x.amount_cents-x.protected_cents))),0) FROM lot_terms x WHERE x.batch_id=b.batch_id) AS protected_reduction,
       GREATEST(0,b.total_cents-(SELECT COALESCE(SUM(x.amount_cents-x.protected_cents),0) FROM lot_terms x WHERE x.batch_id=b.batch_id)) AS unavoidable_protected_reduction
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.protected_reduction <= e.unavoidable_protected_reduction))
))),
('P18', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
program_terms AS (
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
),
lot_terms AS (
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
)
SELECT b.batch_id, 'allocation' AS kind,
       (SELECT COALESCE(SUM(GREATEST(0,x.amount_cents-x.protected_cents-x.minimum_cents-x.excess_cents)),0) FROM lot_terms x WHERE x.batch_id=b.batch_id) AS other_capacity_remaining,
       (SELECT COALESCE(SUM(GREATEST(0, x.minimum_cents+x.excess_cents-(x.amount_cents-x.protected_cents))),0) FROM lot_terms x WHERE x.batch_id=b.batch_id) AS protected_reduction,
       GREATEST(0,b.total_cents-(SELECT COALESCE(SUM(x.amount_cents-x.protected_cents),0) FROM lot_terms x WHERE x.batch_id=b.batch_id)) AS unavoidable_protected_reduction
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.unavoidable_protected_reduction = 0 OR e.protected_reduction = 0 OR e.other_capacity_remaining = 0))
))),
('P19', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
)
SELECT b.batch_id, 'allocation' AS kind,
       0 AS missing_credit_cents
FROM base b
) e WHERE (e.kind <> 'correction' OR e.missing_credit_cents = 0)
UNION ALL
SELECT c.batch_id FROM (
WITH item_credits AS (
 SELECT k.error_id, i.item_id, i.required_cents,
        COALESCE(SUM(c.amount_cents),0) AS credited_cents
 FROM regz_correction_completions k
 JOIN regz_error_obligations o ON o.error_id=k.error_id
 JOIN regz_error_items i ON i.error_id=k.error_id
 LEFT JOIN regz_credits c ON c.error_id=i.error_id AND c.item_id=i.item_id
   AND c.posted_date >= o.determination_date
   AND c.posted_date <= k.checkpoint_date AND c.posted_date <= o.deadline
 GROUP BY k.error_id, i.item_id, i.required_cents
)
SELECT 'correction:' || o.error_id AS batch_id,
 (SELECT SUM(GREATEST(0, i.required_cents-i.credited_cents))
  FROM item_credits i WHERE i.error_id=o.error_id) AS missing_credit_cents,
 (SELECT COUNT(*) FROM regz_dispatches d
  WHERE d.error_id=o.error_id AND d.recipient=o.recipient AND d.notice_type='correction'
    AND d.sent_date BETWEEN o.determination_date AND LEAST(k.checkpoint_date,o.deadline)
    AND LENGTH(TRIM(d.content_evidence)) > 0) AS notice_count
FROM regz_correction_completions k JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE k.checkpoint_date >= o.determination_date
) c WHERE c.missing_credit_cents = 0
))),
('P20', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
)
SELECT b.batch_id, 'allocation' AS kind,
       0 AS notice_count
FROM base b
) e WHERE (e.kind <> 'correction' OR e.notice_count > 0)
UNION ALL
SELECT c.batch_id FROM (
WITH item_credits AS (
 SELECT k.error_id, i.item_id, i.required_cents,
        COALESCE(SUM(c.amount_cents),0) AS credited_cents
 FROM regz_correction_completions k
 JOIN regz_error_obligations o ON o.error_id=k.error_id
 JOIN regz_error_items i ON i.error_id=k.error_id
 LEFT JOIN regz_credits c ON c.error_id=i.error_id AND c.item_id=i.item_id
   AND c.posted_date >= o.determination_date
   AND c.posted_date <= k.checkpoint_date AND c.posted_date <= o.deadline
 GROUP BY k.error_id, i.item_id, i.required_cents
)
SELECT 'correction:' || o.error_id AS batch_id,
 (SELECT SUM(GREATEST(0, i.required_cents-i.credited_cents))
  FROM item_credits i WHERE i.error_id=o.error_id) AS missing_credit_cents,
 (SELECT COUNT(*) FROM regz_dispatches d
  WHERE d.error_id=o.error_id AND d.recipient=o.recipient AND d.notice_type='correction'
    AND d.sent_date BETWEEN o.determination_date AND LEAST(k.checkpoint_date,o.deadline)
    AND LENGTH(TRIM(d.content_evidence)) > 0) AS notice_count
FROM regz_correction_completions k JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE k.checkpoint_date >= o.determination_date
) c WHERE c.notice_count > 0
))),
('P23', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
program_terms AS (
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
),
lot_terms AS (
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
),
lots AS (
SELECT t.*, CASE WHEN t.active THEN 0 ELSE t.reference_apr END AS effective_apr,
 CASE WHEN t.active THEN (
   SELECT MIN(last_two.start_date) FROM (
     SELECT c.start_date FROM regz_cycles c
     WHERE c.account_id = t.account_id AND c.start_date <= t.expiration_date
       AND c.due_date <= t.expiration_date
     ORDER BY c.start_date DESC LIMIT 2
   ) last_two
 ) ELSE NULL END AS window_start,
 CASE WHEN t.active THEN (
   SELECT c.start_date FROM regz_cycles c WHERE c.account_id = t.account_id
     AND t.expiration_date BETWEEN c.start_date AND c.end_date
     AND t.expiration_date < c.due_date
 ) ELSE NULL END AS early_start
FROM lot_terms t
)
SELECT b.batch_id, (SELECT COUNT(*) FROM lots x WHERE x.batch_id = b.batch_id AND (x.proposed_apr IS DISTINCT FROM x.effective_apr)) AS effective_apr_mismatches,
       'allocation' AS kind
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.effective_apr_mismatches = 0))
))),
('P25', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
program_terms AS (
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
),
lot_terms AS (
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
)
SELECT b.batch_id, (SELECT COUNT(*) FROM lot_terms x WHERE x.batch_id = b.batch_id AND (x.interim_payment_required AND x.qualifies AND NOT x.declared)) AS interim_payment_omissions,
       'allocation' AS kind
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.interim_payment_omissions = 0))
))),
('P26', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
program_terms AS (
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
),
lot_terms AS (
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
)
SELECT b.batch_id, (SELECT COUNT(*) FROM lot_terms x WHERE x.batch_id = b.batch_id AND (x.grace_period AND x.declared)) AS grace_classifications,
       'allocation' AS kind
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.grace_classifications = 0))
))),
('P27', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
program_terms AS (
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
),
lot_terms AS (
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
)
SELECT b.batch_id, 'allocation' AS kind,
       (SELECT COUNT(*) FROM lot_terms x WHERE x.batch_id = b.batch_id AND (x.temporary_zero AND NOT x.retroactive_interest AND x.declared)) AS zero_offer_classifications
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.zero_offer_classifications = 0))
))),
('P28', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
program_terms AS (
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
),
lot_terms AS (
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
),
lots AS (
SELECT t.*, CASE WHEN t.active THEN 0 ELSE t.reference_apr END AS effective_apr,
 CASE WHEN t.active THEN (
   SELECT MIN(last_two.start_date) FROM (
     SELECT c.start_date FROM regz_cycles c
     WHERE c.account_id = t.account_id AND c.start_date <= t.expiration_date
       AND c.due_date <= t.expiration_date
     ORDER BY c.start_date DESC LIMIT 2
   ) last_two
 ) ELSE NULL END AS window_start,
 CASE WHEN t.active THEN (
   SELECT c.start_date FROM regz_cycles c WHERE c.account_id = t.account_id
     AND t.expiration_date BETWEEN c.start_date AND c.end_date
     AND t.expiration_date < c.due_date
 ) ELSE NULL END AS early_start
FROM lot_terms t
)
SELECT b.batch_id, 'allocation' AS kind,
       (SELECT COUNT(*) FROM lots x WHERE x.batch_id = b.batch_id AND (x.proposed_window IS DISTINCT FROM x.window_start)) AS window_start_mismatches
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.window_start_mismatches = 0))
))),
('P29', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
program_terms AS (
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
),
lot_terms AS (
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
),
lots AS (
SELECT t.*, CASE WHEN t.active THEN 0 ELSE t.reference_apr END AS effective_apr,
 CASE WHEN t.active THEN (
   SELECT MIN(last_two.start_date) FROM (
     SELECT c.start_date FROM regz_cycles c
     WHERE c.account_id = t.account_id AND c.start_date <= t.expiration_date
       AND c.due_date <= t.expiration_date
     ORDER BY c.start_date DESC LIMIT 2
   ) last_two
 ) ELSE NULL END AS window_start,
 CASE WHEN t.active THEN (
   SELECT c.start_date FROM regz_cycles c WHERE c.account_id = t.account_id
     AND t.expiration_date BETWEEN c.start_date AND c.end_date
     AND t.expiration_date < c.due_date
 ) ELSE NULL END AS early_start
FROM lot_terms t
),
slices AS (
SELECT l.batch_id, l.balance_id, part.disputed, l.phase, l.effective_apr,
 COALESCE(l.active AND l.credited_date BETWEEN l.window_start
          AND l.expiration_date, false) AS in_window,
 COALESCE(l.active AND l.credited_date BETWEEN l.window_start AND l.expiration_date
          AND l.credited_date >= l.early_start, false) AS early,
 CASE WHEN part.disputed THEN
   l.protected_cents - GREATEST(0, l.minimum_cents - (l.amount_cents-l.protected_cents))
 ELSE GREATEST(0, l.amount_cents-l.protected_cents-l.minimum_cents) END AS capacity,
 CASE WHEN part.disputed THEN
   GREATEST(0, l.excess_cents-GREATEST(0,l.amount_cents-l.protected_cents-l.minimum_cents))
 ELSE LEAST(l.excess_cents,GREATEST(0,l.amount_cents-l.protected_cents-l.minimum_cents))
 END AS allocated
FROM lots l CROSS JOIN (VALUES (false), (true)) part(disputed)
),
pairs AS (
SELECT h.batch_id,
 h.in_window AND NOT lo.in_window AS deferred,
 h.in_window AND NOT lo.in_window AND h.early AS early,
 NOT h.in_window AND NOT lo.in_window AND h.phase < lo.phase AS historical,
 NOT h.in_window AND NOT lo.in_window AND h.phase = lo.phase
   AND h.effective_apr > lo.effective_apr AS ordinary,
 h.effective_apr = (
   SELECT MAX(peer.effective_apr) FROM slices peer
   WHERE peer.batch_id = h.batch_id AND peer.disputed = h.disputed
     AND peer.phase = h.phase AND NOT peer.in_window AND peer.capacity > 0
 ) AS highest
FROM slices h JOIN slices lo ON lo.batch_id = h.batch_id AND lo.disputed = h.disputed
WHERE h.capacity > 0 AND lo.capacity > 0 AND h.allocated < h.capacity AND lo.allocated > 0
)
SELECT b.batch_id, (SELECT COUNT(*) FROM pairs x WHERE x.batch_id = b.batch_id AND (x.early)) AS early_expiry_violations,
       'allocation' AS kind,
       b.method AS method
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.method <> 'default' OR (e.early_expiry_violations = 0)))
))),
('P32', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
request AS (
SELECT b.batch_id, r.* FROM base b JOIN regz_requests r ON r.request_id = b.request_id
WHERE b.method <> 'default'
)
SELECT b.batch_id, 'allocation' AS kind,
       b.method AS method,
       EXISTS (SELECT 1 FROM request r WHERE r.batch_id=b.batch_id AND r.submitted_date <= b.decision_date) AS request_present
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.request_present OR e.method = 'default'))
))),
('P33', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
program_terms AS (
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
),
lot_terms AS (
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
),
request AS (
SELECT b.batch_id, r.* FROM base b JOIN regz_requests r ON r.request_id = b.request_id
WHERE b.method <> 'default'
),
request_targets AS (
SELECT r.batch_id, i.*, l.balance_id AS existing_balance,
       l.amount_cents AS capacity, l.minimum_cents, l.secured
FROM request r JOIN regz_request_lines i ON i.request_id=r.request_id
LEFT JOIN lot_terms l ON l.batch_id=r.batch_id AND l.balance_id=i.balance_id
)
SELECT b.batch_id, 'allocation' AS kind,
       b.method AS method,
       COALESCE((SELECT
      CAST(NOT EXISTS (SELECT 1 FROM regz_request_lines i WHERE i.request_id=r.request_id) AS INTEGER)
      + CAST((SELECT COALESCE(SUM(i.amount_cents),0) FROM regz_request_lines i
              WHERE i.request_id=r.request_id) <> b.total_cents-b.minimum_cents AS INTEGER)
      + (SELECT COUNT(*) FROM request_targets x
         WHERE x.batch_id=b.batch_id AND (x.existing_balance IS NULL OR x.amount_cents < 0
            OR x.amount_cents > x.capacity-x.minimum_cents))
      FROM request r WHERE r.batch_id=b.batch_id),0) AS request_feasibility_errors,
       (SELECT COUNT(*) FROM request r JOIN regz_requests replacement ON replacement.supersedes_id=r.request_id AND replacement.payment_id=b.payment_id WHERE r.batch_id=b.batch_id AND replacement.submitted_date <= b.decision_date) AS superseding_requests
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.method = 'default' OR (e.request_feasibility_errors = 0 AND e.superseding_requests = 0)))
))),
('P34', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
request AS (
SELECT b.batch_id, r.* FROM base b JOIN regz_requests r ON r.request_id = b.request_id
WHERE b.method <> 'default'
)
SELECT b.batch_id, COALESCE((SELECT r.affirmative FROM request r WHERE r.batch_id=b.batch_id), false) AS affirmative,
       COALESCE((SELECT r.channel IN ('oral','electronic','written') FROM request r WHERE r.batch_id=b.batch_id), false) AS contact_channel,
       (SELECT COUNT(*) FROM request r JOIN regz_request_lines i ON i.request_id=r.request_id WHERE r.batch_id=b.batch_id) AS instruction_count,
       'allocation' AS kind,
       b.method AS method,
       COALESCE((SELECT r.basis FROM request r WHERE r.batch_id=b.batch_id), 'none') AS request_basis
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.method = 'default' OR (e.request_basis <> 'contact' OR (e.affirmative AND e.instruction_count > 0 AND e.contact_channel))))
))),
('P35', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
request AS (
SELECT b.batch_id, r.* FROM base b JOIN regz_requests r ON r.request_id = b.request_id
WHERE b.method <> 'default'
)
SELECT b.batch_id, COALESCE((SELECT r.affirmative FROM request r WHERE r.batch_id=b.batch_id), false) AS affirmative,
       COALESCE((SELECT r.form_completed FROM request r WHERE r.batch_id=b.batch_id), false) AS form_completed,
       (SELECT COUNT(*) FROM request r JOIN regz_request_lines i ON i.request_id=r.request_id WHERE r.batch_id=b.batch_id) AS instruction_count,
       'allocation' AS kind,
       b.method AS method,
       COALESCE((SELECT r.basis FROM request r WHERE r.batch_id=b.batch_id), 'none') AS request_basis,
       EXISTS (SELECT 1 FROM request r WHERE r.batch_id=b.batch_id AND r.submitted_date <= b.decision_date) AS request_present
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.method = 'default' OR (e.request_basis <> 'form' OR (e.form_completed AND e.request_present AND e.affirmative AND e.instruction_count > 0))))
))),
('P36', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
request AS (
SELECT b.batch_id, r.* FROM base b JOIN regz_requests r ON r.request_id = b.request_id
WHERE b.method <> 'default'
)
SELECT b.batch_id, COALESCE((SELECT r.affirmative FROM request r WHERE r.batch_id=b.batch_id), false) AS affirmative,
       COALESCE((SELECT r.channel IN ('oral','electronic','written') FROM request r WHERE r.batch_id=b.batch_id), false) AS contact_channel,
       (SELECT COUNT(*) FROM request r JOIN regz_request_lines i ON i.request_id=r.request_id WHERE r.batch_id=b.batch_id) AS instruction_count,
       'allocation' AS kind,
       b.method AS method,
       EXISTS (SELECT 1 FROM request r WHERE r.batch_id=b.batch_id AND r.submitted_date IS NOT NULL AND EXISTS (SELECT 1 FROM regz_posted_allocations p WHERE p.batch_id=b.replaces_batch_id AND p.payment_id=b.payment_id) AND NOT EXISTS (SELECT 1 FROM regz_posted_allocations p WHERE p.batch_id=b.replaces_batch_id AND p.payment_id=b.payment_id AND (p.method <> 'default' OR p.decision_date > r.submitted_date))) AS prior_default_allocation,
       COALESCE((SELECT r.basis FROM request r WHERE r.batch_id=b.batch_id), 'none') AS request_basis
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.method = 'default' OR (e.request_basis <> 'reallocation' OR (e.prior_default_allocation AND e.affirmative AND e.contact_channel AND e.instruction_count > 0))))
))),
('P37', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
request AS (
SELECT b.batch_id, r.* FROM base b JOIN regz_requests r ON r.request_id = b.request_id
WHERE b.method <> 'default'
)
SELECT b.batch_id, 'allocation' AS kind,
       b.method AS method,
       COALESCE((SELECT r.basis FROM request r WHERE r.batch_id=b.batch_id), 'none') AS request_basis
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.method = 'default' OR (e.request_basis <> 'agreement_default')))
))),
('P38', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
request AS (
SELECT b.batch_id, r.* FROM base b JOIN regz_requests r ON r.request_id = b.request_id
WHERE b.method <> 'default'
)
SELECT b.batch_id, 'allocation' AS kind,
       b.method AS method,
       COALESCE((SELECT r.basis FROM request r WHERE r.batch_id=b.batch_id), 'none') AS request_basis
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.method = 'default' OR (e.request_basis <> 'application_default')))
))),
('P39', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
request AS (
SELECT b.batch_id, r.* FROM base b JOIN regz_requests r ON r.request_id = b.request_id
WHERE b.method <> 'default'
)
SELECT b.batch_id, 'allocation' AS kind,
       b.method AS method,
       COALESCE((SELECT r.basis FROM request r WHERE r.batch_id=b.batch_id), 'none') AS request_basis
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.method = 'default' OR (e.request_basis <> 'coupon_default')))
))),
('P40', (f.selected_transaction_id IN (
SELECT b.batch_id FROM regz_proposals b
JOIN regz_payments pay ON pay.payment_id=b.payment_id
JOIN regz_accounts a ON a.account_id=pay.account_id
WHERE a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured
 AND b.method IN ('default','deferred_request','secured_request')
 AND b.decision_date >= pay.credited_date
 AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=b.snapshot_date)
 AND EXISTS (SELECT 1 FROM regz_snapshot_sets s WHERE s.account_id=pay.account_id AND s.snapshot_date=pay.credited_date)
 AND EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date)
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id AND l.balance_id=c.balance_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.balance_id=l.balance_id))
 AND (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.minimum_cents
 AND (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)=pay.total_cents-pay.minimum_cents
 AND ((b.replaces_batch_id IS NULL AND NOT EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id))
      OR EXISTS (SELECT 1 FROM regz_payment_heads h WHERE h.payment_id=pay.payment_id AND h.batch_id=b.replaces_batch_id))
 AND EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date)
 AND NOT EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=pay.account_id AND p.effective_date<=pay.credited_date AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c
   JOIN regz_proposal_lines l ON l.balance_id=c.balance_id
   JOIN regz_proposals local_proposal ON local_proposal.batch_id=l.batch_id
   LEFT JOIN regz_balances old ON old.account_id=c.account_id AND old.balance_id=c.balance_id AND old.snapshot_date=local_proposal.snapshot_date
   WHERE l.batch_id=b.batch_id AND c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date
   AND (c.protected_cents<0 OR c.protected_cents>c.amount_cents
     OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>c.amount_cents
     OR (old.balance_id IS NOT NULL AND c.amount_cents>old.amount_cents)
     OR COALESCE(old.apr,c.apr)<0))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id IS NOT NULL
   AND (NOT EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id AND d.program_id=c.program_id)
     OR NOT EXISTS (SELECT 1 FROM regz_programs g WHERE g.program_id=c.program_id AND g.account_id=pay.account_id AND g.start_date<=g.expiration_date)))
 AND NOT EXISTS (
   SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances c WHERE c.account_id=pay.account_id AND c.snapshot_date=pay.credited_date AND c.program_id=d.program_id))
 AND NOT EXISTS (
   SELECT 1 FROM regz_balances bal JOIN regz_programs g ON g.program_id=bal.program_id
   WHERE bal.account_id=pay.account_id AND bal.snapshot_date=pay.credited_date
     AND g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
     AND (NOT g.temporary_zero OR g.retroactive_interest)
     AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
     AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
       OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=pay.account_id AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
       OR EXISTS (SELECT 1 FROM regz_cycles c WHERE c.account_id=pay.account_id
          AND EXISTS (SELECT 1 FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date)
          AND c.end_date + 1 <> (SELECT MIN(n.start_date) FROM regz_cycles n WHERE n.account_id=c.account_id AND n.start_date>c.start_date))))
 AND (b.method='default' OR b.request_id IS NULL OR EXISTS (
   SELECT 1 FROM regz_requests r WHERE r.request_id=b.request_id AND r.payment_id=pay.payment_id
    AND r.basis IN ('contact','form','reallocation','agreement_default','application_default','coupon_default')))
) AND f.selected_transaction_id IN (
SELECT e.batch_id FROM (
WITH base AS (
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
),
request AS (
SELECT b.batch_id, r.* FROM base b JOIN regz_requests r ON r.request_id = b.request_id
WHERE b.method <> 'default'
)
SELECT b.batch_id, 'allocation' AS kind,
       b.method AS method,
       COALESCE((SELECT r.required_condition FROM request r WHERE r.batch_id=b.batch_id), false) AS required_condition
FROM base b
) e WHERE (e.kind <> 'allocation' OR (e.method = 'default' OR (NOT e.required_condition)))
)))
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
  IF (SELECT count(*) FROM regz_policy_results) <> 31
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
