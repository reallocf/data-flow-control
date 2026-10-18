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
