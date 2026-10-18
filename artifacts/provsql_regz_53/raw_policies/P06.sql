-- P06: Remainder after deferred priority
-- Raw-evidence operational translation; no precomputed compliance summaries.
-- Reports every supplied proposal/checkpoint. Bind a selected UPDATE by its
-- actual contributor batch_id; do not require all alternative proposals to pass.
-- NOT_APPLICABLE distinguishes allocation from correction; other conditional
-- permissions still pass vacuously, as in the existing catalog.
-- Preconditions enforced by schema: declared primary keys and NOT NULL fields.
-- External inputs remain trusted: completeness attestations, contract terms,
-- qualifying disputes, request conduct, and required credits/notice content.
-- These are PostgreSQL policy computations on ProvSQL, not provenance rewriting
-- of the entire computation. No persistent views/functions/tables are created.
BEGIN ISOLATION LEVEL REPEATABLE READ;
-- Avoid disproportionate JIT compilation for this small expanded fixture.
SET LOCAL jit = off;
SET LOCAL provsql.active = off;
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
ORDER BY t.batch_id;
ROLLBACK;
