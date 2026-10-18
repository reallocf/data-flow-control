"""Input-contract checks evaluated from evidence, before regulatory verdicts."""

VALID_ALLOCATION_SQL = """
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
""".strip()

VALID_CORRECTION_SQL = """
SELECT 'correction:' || o.error_id AS batch_id
FROM regz_correction_completions k JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE k.checkpoint_date >= o.determination_date
 AND EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=o.error_id)
 AND NOT EXISTS (SELECT 1 FROM regz_error_items i WHERE i.error_id=o.error_id AND i.required_cents<0)
""".strip()
