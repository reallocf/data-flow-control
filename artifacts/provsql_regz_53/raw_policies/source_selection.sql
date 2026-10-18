-- Recompute both former helper tables locally. Raw proposals replace context membership.
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
