-- Fully expanded checked UPDATE: no reads of stored compliance summaries.
-- Each policy independently contains its computations AND input validation.
-- regz_* base records are authoritative structured evidence, not raw documents.
-- ProvSQL circuit rewriting is off, as in the previously tested enforcement wrapper.
-- SQL semantics and raw evidence are evaluated here; no end-to-end lineage claim.
-- Run as one transaction and roll back on any error; retry serialization failures.
BEGIN ISOLATION LEVEL SERIALIZABLE;
-- Avoid disproportionate JIT compilation for this small expanded fixture.
SET LOCAL jit = off;
SET LOCAL provsql.active=off;
SET LOCAL provsql.update_provenance=off;
SELECT allocation_id FROM payment_allocations
 WHERE allocation_id='ALLOC0000001' FOR UPDATE;
CREATE TEMP TABLE frozen_choice ON COMMIT DROP AS
WITH balance_components AS (
 SELECT t.transaction_id AS component_id, t.account_id, t.statement_id,
        t.amount AS outstanding_amount, d.apr_applied AS effective_apr
 FROM transactions t JOIN statements s ON s.statement_id=t.statement_id
 JOIN daily_balances d ON d.account_id=t.account_id
   AND d.statement_id=t.statement_id AND d.balance_category=t.balance_category
   AND d.balance_date=s.cycle_end
 WHERE t.amount>0
), allocation_snapshot_context AS (
 SELECT s.account_id, s.statement_id, d.balance_date AS snapshot_date,
        MAX(d.apr_applied) AS highest_effective_apr
 FROM statements s JOIN daily_balances d ON d.account_id=s.account_id
   AND d.statement_id=s.statement_id AND d.balance_date=s.cycle_end
 GROUP BY s.account_id,s.statement_id,s.cycle_end,d.balance_date
)
SELECT allocation.allocation_id, selected.transaction_id AS selected_transaction_id,
 LEAST(allocation.total_payment-allocation.minimum_payment_portion,selected.amount) AS proposed_cash
FROM payment_allocations allocation JOIN transactions selected
 ON selected.account_id=allocation.account_id
JOIN regz_proposals proposal ON proposal.batch_id=selected.transaction_id
JOIN balance_components component ON component.component_id=selected.transaction_id
JOIN statements statement_context ON statement_context.statement_id=selected.statement_id
JOIN allocation_snapshot_context snapshot ON snapshot.account_id=selected.account_id
 AND snapshot.statement_id=selected.statement_id
WHERE allocation.allocation_id='ALLOC0000001'
 AND selected.transaction_id=(
  SELECT candidate.transaction_id FROM transactions candidate
  JOIN daily_balances balance ON balance.account_id=candidate.account_id
   AND balance.statement_id=candidate.statement_id
   AND balance.balance_category=candidate.balance_category
  WHERE candidate.account_id=allocation.account_id
   AND candidate.statement_id=allocation.statement_id
   AND candidate.transaction_type='cash_advance'
   AND balance.balance_date=DATE '2027-01-30'
  ORDER BY balance.apr_applied DESC LIMIT 1
 );
DO $$ BEGIN
 IF (SELECT COUNT(*) FROM frozen_choice)<>1 THEN
  RAISE EXCEPTION 'Missing or ambiguous selected proposal';
 END IF;
 -- The example's one-cash-lot encoding must match the entire excess allocation.
 IF NOT EXISTS (
  SELECT 1 FROM frozen_choice f JOIN payment_allocations a ON a.allocation_id=f.allocation_id
  JOIN regz_proposals p ON p.batch_id=f.selected_transaction_id
  JOIN regz_payments pay ON pay.payment_id=p.payment_id
  JOIN regz_proposal_lines l ON l.batch_id=p.batch_id AND l.balance_id=f.selected_transaction_id
  WHERE a.transaction_id=pay.payment_id AND a.account_id=pay.account_id
   AND a.payment_date=pay.credited_date AND a.total_payment*100=pay.total_cents
   AND a.minimum_payment_portion*100=pay.minimum_cents
   AND a.excess_payment_amount*100=pay.total_cents-pay.minimum_cents
   AND f.proposed_cash*100=l.excess_cents
   AND f.proposed_cash*100=pay.total_cents-pay.minimum_cents
   AND a.purchase_allocation=0 AND a.balance_transfer_allocation=0 AND a.promo_allocation=0
   AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines other
    WHERE other.batch_id=p.batch_id AND other.balance_id<>f.selected_transaction_id
      AND other.excess_cents<>0)
 ) THEN RAISE EXCEPTION 'UPDATE and complete proposal do not match'; END IF;
END $$;
CREATE TEMP TABLE raw_policy_results ON COMMIT DROP AS
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P01' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P02' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P03' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P04' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P05' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P06' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P07' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P08' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P13' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P14' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P15' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P16' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P17' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P18' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
SELECT 'correction:' || o.error_id AS batch_id,
 (SELECT SUM(GREATEST(0, i.required_cents - COALESCE((
    SELECT SUM(c.amount_cents) FROM regz_credits c
    WHERE c.error_id=i.error_id AND c.item_id=i.item_id
      AND c.posted_date BETWEEN o.determination_date AND LEAST(k.checkpoint_date,o.deadline)
 ),0))) FROM regz_error_items i WHERE i.error_id=o.error_id) AS missing_credit_cents,
 (SELECT COUNT(*) FROM regz_dispatches d
  WHERE d.error_id=o.error_id AND d.recipient=o.recipient AND d.notice_type='correction'
    AND d.sent_date BETWEEN o.determination_date AND LEAST(k.checkpoint_date,o.deadline)
    AND LENGTH(TRIM(d.content_evidence)) > 0) AS notice_count
FROM regz_correction_completions k JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE k.checkpoint_date >= o.determination_date
) c WHERE c.missing_credit_cents = 0
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P19' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT 'correction:' || error_id FROM regz_correction_completions)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
SELECT 'correction:' || o.error_id AS batch_id,
 (SELECT SUM(GREATEST(0, i.required_cents - COALESCE((
    SELECT SUM(c.amount_cents) FROM regz_credits c
    WHERE c.error_id=i.error_id AND c.item_id=i.item_id
      AND c.posted_date BETWEEN o.determination_date AND LEAST(k.checkpoint_date,o.deadline)
 ),0))) FROM regz_error_items i WHERE i.error_id=o.error_id) AS missing_credit_cents,
 (SELECT COUNT(*) FROM regz_dispatches d
  WHERE d.error_id=o.error_id AND d.recipient=o.recipient AND d.notice_type='correction'
    AND d.sent_date BETWEEN o.determination_date AND LEAST(k.checkpoint_date,o.deadline)
    AND LENGTH(TRIM(d.content_evidence)) > 0) AS notice_count
FROM regz_correction_completions k JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE k.checkpoint_date >= o.determination_date
) c WHERE c.notice_count > 0
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P20' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT 'correction:' || error_id FROM regz_correction_completions)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P23' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P25' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P26' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P27' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P28' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P29' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P32' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P33' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P34' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P35' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P36' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P37' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P38' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P39' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id
UNION ALL
SELECT r.* FROM (
WITH input_errors AS (
-- Explicit input contract from derive_allocation_context; these are not
-- additional regulatory propositions. One row means INPUT_ERROR, not FAIL.
WITH base_input AS (
 SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
        pay.total_cents, pay.minimum_cents
 FROM regz_proposals p LEFT JOIN regz_payments pay ON pay.payment_id=p.payment_id
), input_errors AS (
 SELECT b.batch_id, 'Missing payment or out-of-scope/missing account' AS reason
 FROM base_input b LEFT JOIN regz_accounts a ON a.account_id=b.account_id
 WHERE b.account_id IS NULL OR a.account_id IS NULL
    OR NOT (a.credit_card AND a.consumer_plan AND a.open_end AND NOT a.home_secured)
 UNION ALL
 SELECT b.batch_id, 'Unsupported mode, decision date, or minimum amount'
 FROM base_input b WHERE b.method NOT IN ('default','deferred_request','secured_request')
    OR b.decision_date < b.credited_date OR b.minimum_cents < 0
    OR b.minimum_cents > b.total_cents
 UNION ALL
 SELECT b.batch_id, 'Missing authoritative snapshot'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.snapshot_date)
 OR NOT EXISTS (SELECT 1 FROM regz_snapshot_sets s
   WHERE s.account_id=b.account_id AND s.snapshot_date=b.credited_date)
 UNION ALL
 SELECT b.batch_id, 'Incomplete proposal lines or empty balance set'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_balances x
   WHERE x.account_id=b.account_id AND x.snapshot_date=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
     WHERE l.batch_id=b.batch_id AND l.balance_id=x.balance_id))
 OR EXISTS (SELECT 1 FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
     AND x.snapshot_date=b.credited_date AND x.balance_id=l.balance_id))
 UNION ALL
 SELECT b.batch_id, 'Minimum or excess allocation does not conserve payment'
 FROM base_input b WHERE
   (SELECT SUM(l.minimum_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.minimum_cents
 OR (SELECT SUM(l.excess_cents) FROM regz_proposal_lines l WHERE l.batch_id=b.batch_id)
     IS DISTINCT FROM b.total_cents-b.minimum_cents
 UNION ALL
 SELECT b.batch_id, 'Duplicate payment or stale replacement'
 FROM base_input b LEFT JOIN regz_payment_heads h ON h.payment_id=b.payment_id
 WHERE (h.payment_id IS NULL AND b.replaces_batch_id IS NOT NULL)
    OR (h.payment_id IS NOT NULL AND h.batch_id IS DISTINCT FROM b.replaces_batch_id)
 UNION ALL
 SELECT b.batch_id, 'Missing or unsupported effective snapshot practice'
 FROM base_input b WHERE NOT EXISTS (SELECT 1 FROM regz_practices p
   WHERE p.account_id=b.account_id AND p.effective_date<=b.credited_date)
 OR EXISTS (SELECT 1 FROM regz_practices p WHERE p.account_id=b.account_id
   AND p.effective_date<=b.credited_date
   AND p.convention NOT IN ('cycle_end','credit_date','offset'))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Invalid amounts, growing lot, or negative reference APR'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_proposal_lines l ON l.batch_id=b.batch_id AND l.balance_id=x.balance_id
 LEFT JOIN regz_balances old ON old.account_id=b.account_id
   AND old.balance_id=x.balance_id AND old.snapshot_date=b.snapshot_date
 WHERE x.protected_cents<0 OR x.protected_cents>x.amount_cents
   OR l.minimum_cents<0 OR l.excess_cents<0 OR l.minimum_cents+l.excess_cents>x.amount_cents
   OR (old.balance_id IS NOT NULL AND x.amount_cents>old.amount_cents)
   OR COALESCE(old.apr,x.apr)<0
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/extra program classification'
 FROM base_input b WHERE EXISTS (
   SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM regz_program_classifications d
      WHERE d.batch_id=b.batch_id AND d.program_id=x.program_id))
 OR EXISTS (SELECT 1 FROM regz_program_classifications d WHERE d.batch_id=b.batch_id
   AND NOT EXISTS (SELECT 1 FROM regz_balances x WHERE x.account_id=b.account_id
    AND x.snapshot_date=b.credited_date AND x.program_id=d.program_id))
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing program, wrong account, or invalid program dates'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 LEFT JOIN regz_programs g ON g.program_id=x.program_id
 WHERE x.program_id IS NOT NULL AND (g.program_id IS NULL
   OR g.account_id<>b.account_id OR g.start_date>g.expiration_date)
 UNION ALL
 SELECT DISTINCT b.batch_id, 'Missing/noncontiguous cycles or insufficient expiration history'
 FROM base_input b JOIN regz_balances x ON x.account_id=b.account_id
   AND x.snapshot_date=b.credited_date
 JOIN regz_programs g ON g.program_id=x.program_id
 WHERE g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
   AND (NOT g.temporary_zero OR g.retroactive_interest)
   AND b.credited_date BETWEEN g.start_date AND g.expiration_date
   AND ((SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND g.expiration_date BETWEEN c.start_date AND c.end_date)<>1
    OR (SELECT COUNT(*) FROM regz_cycles c WHERE c.account_id=b.account_id
     AND c.start_date<=g.expiration_date AND c.due_date<=g.expiration_date)<2
    OR EXISTS (SELECT 1 FROM (
       SELECT c.start_date, LAG(c.end_date) OVER (ORDER BY c.start_date) AS previous_end
       FROM regz_cycles c WHERE c.account_id=b.account_id
     ) history WHERE previous_end IS NOT NULL AND start_date<>previous_end+1))
 UNION ALL
 SELECT b.batch_id, 'Missing, wrong-payment, or unsupported request evidence'
 FROM base_input b LEFT JOIN regz_requests r ON r.request_id=b.request_id
 WHERE b.method<>'default' AND b.request_id IS NOT NULL
   AND (r.request_id IS NULL OR r.payment_id<>b.payment_id
     OR r.basis NOT IN ('contact','form','reallocation','agreement_default',
                       'application_default','coupon_default'))
)
SELECT batch_id, reason FROM input_errors
), correction_errors AS (
SELECT 'correction:' || k.error_id AS batch_id
FROM regz_correction_completions k
LEFT JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE o.error_id IS NULL OR k.checkpoint_date<o.determination_date
 OR NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=k.error_id)
 OR EXISTS (SELECT 1 FROM regz_error_items i
            WHERE i.error_id=k.error_id AND i.required_cents<0)
), eligible AS (
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
), targets AS (
 SELECT batch_id FROM regz_proposals
 UNION ALL
 SELECT 'correction:' || error_id FROM regz_correction_completions
)
SELECT t.batch_id, 'P40' AS proposition_id,
 CASE WHEN EXISTS (SELECT 1 FROM input_errors i WHERE i.batch_id=t.batch_id)
        OR EXISTS (SELECT 1 FROM correction_errors i WHERE i.batch_id=t.batch_id)
      THEN 'INPUT_ERROR'
      WHEN t.batch_id NOT IN (SELECT batch_id FROM regz_proposals)
      THEN 'NOT_APPLICABLE'
      WHEN EXISTS (SELECT 1 FROM eligible e WHERE e.batch_id=t.batch_id)
      THEN 'PASS' ELSE 'FAIL' END AS status
FROM targets t
ORDER BY t.batch_id
) r JOIN frozen_choice f ON f.selected_transaction_id=r.batch_id;
SELECT * FROM raw_policy_results ORDER BY proposition_id;
DO $$ BEGIN
 IF (SELECT COUNT(*) FROM raw_policy_results)<>31
 OR EXISTS (SELECT 1 FROM raw_policy_results WHERE status NOT IN ('PASS','NOT_APPLICABLE'))
 THEN RAISE EXCEPTION 'Raw-evidence policy rejection or input error'; END IF;
END $$;
UPDATE payment_allocations a SET cash_advance_allocation=f.proposed_cash
FROM frozen_choice f WHERE a.allocation_id=f.allocation_id
RETURNING a.allocation_id, f.selected_transaction_id, a.cash_advance_allocation;
COMMIT;
