CREATE VIEW regz_context AS
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
),
practice_history AS (
SELECT p.*, LAG(p.convention) OVER w AS previous_convention,
 LAG(p.offset_days) OVER w AS previous_offset
FROM regz_practices p
WINDOW w AS (PARTITION BY p.account_id ORDER BY p.effective_date)
)
SELECT b.batch_id, 'allocation' AS kind,
       b.method AS method,
       b.snapshot_date AS snapshot_date,
       b.cycle_end AS cycle_end,
       b.credited_date AS credited_date,
       COALESCE((SELECT r.basis FROM request r WHERE r.batch_id=b.batch_id), 'none') AS request_basis,
       COALESCE((SELECT r.issuer_elected FROM request r WHERE r.batch_id=b.batch_id), false) AS issuer_elected,
       COALESCE((SELECT r.affirmative FROM request r WHERE r.batch_id=b.batch_id), false) AS affirmative,
       COALESCE((SELECT r.form_completed FROM request r WHERE r.batch_id=b.batch_id), false) AS form_completed,
       COALESCE((SELECT r.required_condition FROM request r WHERE r.batch_id=b.batch_id), false) AS required_condition,
       COALESCE((SELECT r.channel IN ('oral','electronic','written') FROM request r WHERE r.batch_id=b.batch_id), false) AS contact_channel,
       EXISTS (SELECT 1 FROM request r WHERE r.batch_id=b.batch_id AND r.submitted_date <= b.decision_date) AS request_present,
       (SELECT COUNT(*) FROM request r JOIN regz_request_lines i ON i.request_id=r.request_id WHERE r.batch_id=b.batch_id) AS instruction_count,
       (SELECT COUNT(*) FROM request r JOIN regz_requests replacement ON replacement.supersedes_id=r.request_id AND replacement.payment_id=b.payment_id WHERE r.batch_id=b.batch_id AND replacement.submitted_date <= b.decision_date) AS superseding_requests,
       EXISTS (SELECT 1 FROM request r WHERE r.batch_id=b.batch_id AND r.submitted_date IS NOT NULL AND EXISTS (SELECT 1 FROM regz_posted_allocations p WHERE p.batch_id=b.replaces_batch_id AND p.payment_id=b.payment_id) AND NOT EXISTS (SELECT 1 FROM regz_posted_allocations p WHERE p.batch_id=b.replaces_batch_id AND p.payment_id=b.payment_id AND (p.method <> 'default' OR p.decision_date > r.submitted_date))) AS prior_default_allocation,
       (SELECT COUNT(DISTINCT x.program_id) FROM lots x WHERE x.batch_id=b.batch_id AND x.active) AS active_programs,
       (SELECT COUNT(DISTINCT x.program_id) FROM lots x WHERE x.batch_id=b.batch_id AND x.active AND x.credited_date BETWEEN x.window_start AND x.expiration_date) AS window_programs,
       (SELECT COUNT(*) FROM lot_terms x WHERE x.batch_id = b.batch_id AND (x.program_id IS NOT NULL AND x.declared IS DISTINCT FROM x.qualifies)) AS classification_mismatches,
       (SELECT COUNT(*) FROM lot_terms x WHERE x.batch_id = b.batch_id AND (x.interim_payment_required AND x.qualifies AND NOT x.declared)) AS interim_payment_omissions,
       (SELECT COUNT(*) FROM lot_terms x WHERE x.batch_id = b.batch_id AND (x.grace_period AND x.declared)) AS grace_classifications,
       (SELECT COUNT(*) FROM lot_terms x WHERE x.batch_id = b.batch_id AND (x.temporary_zero AND NOT x.retroactive_interest AND x.declared)) AS zero_offer_classifications,
       (SELECT COUNT(*) FROM lots x WHERE x.batch_id = b.batch_id AND (x.proposed_apr IS DISTINCT FROM x.effective_apr)) AS effective_apr_mismatches,
       (SELECT COUNT(*) FROM lots x WHERE x.batch_id = b.batch_id AND (x.proposed_window IS DISTINCT FROM x.window_start)) AS window_start_mismatches,
       (SELECT COUNT(*) FROM pairs x WHERE x.batch_id = b.batch_id AND (x.ordinary AND x.highest)) AS highest_violations,
       (SELECT COUNT(*) FROM pairs x WHERE x.batch_id = b.batch_id AND (x.ordinary AND NOT x.highest)) AS remaining_violations,
       (SELECT COUNT(*) FROM pairs x WHERE x.batch_id = b.batch_id AND (x.ordinary)) AS ordinary_violations,
       (SELECT COUNT(*) FROM pairs x WHERE x.batch_id = b.batch_id AND (x.deferred)) AS deferred_violations,
       (SELECT COUNT(*) FROM pairs x WHERE x.batch_id = b.batch_id AND (x.historical)) AS snapshot_violations,
       (SELECT COUNT(*) FROM pairs x WHERE x.batch_id = b.batch_id AND (x.early)) AS early_expiry_violations,
       (SELECT COALESCE(SUM(GREATEST(0, x.minimum_cents+x.excess_cents-(x.amount_cents-x.protected_cents))),0) FROM lot_terms x WHERE x.batch_id=b.batch_id) AS protected_reduction,
       GREATEST(0,b.total_cents-(SELECT COALESCE(SUM(x.amount_cents-x.protected_cents),0) FROM lot_terms x WHERE x.batch_id=b.batch_id)) AS unavoidable_protected_reduction,
       (SELECT COALESCE(SUM(GREATEST(0,x.amount_cents-x.protected_cents-x.minimum_cents-x.excess_cents)),0) FROM lot_terms x WHERE x.batch_id=b.batch_id) AS other_capacity_remaining,
       (SELECT CASE p.convention WHEN 'credit_date' THEN b.credited_date WHEN 'offset' THEN b.cycle_end + CAST(p.offset_days AS INTEGER) ELSE b.cycle_end END FROM regz_practices p WHERE p.account_id=b.account_id AND p.effective_date <= b.credited_date ORDER BY p.effective_date DESC LIMIT 1) AS expected_snapshot_date,
       (SELECT COUNT(*) FROM practice_history p WHERE p.account_id=b.account_id AND p.effective_date <= b.credited_date AND p.previous_convention IS NOT NULL AND (p.convention <> p.previous_convention OR p.offset_days <> p.previous_offset) AND (p.authorization_evidence IS NULL OR p.authorization_evidence='')) AS unauthorized_changes,
       0 AS missing_credit_cents,
       0 AS notice_count,
       (SELECT COUNT(*) FROM request_allocations x
      WHERE x.batch_id=b.batch_id AND x.excess_cents <> x.requested)
      + (SELECT COUNT(*) FROM request_targets x WHERE x.batch_id=b.batch_id
         AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
             WHERE l.batch_id=x.batch_id AND l.balance_id=x.balance_id)) AS request_mismatches,
       (SELECT COUNT(*) FROM request r
      JOIN regz_request_lines i ON i.request_id=r.request_id
      JOIN lot_terms l ON l.batch_id=b.batch_id AND l.balance_id=i.balance_id
      WHERE r.batch_id=b.batch_id AND i.amount_cents > 0 AND NOT l.secured) AS unsecured_request_targets,
       COALESCE((SELECT
      CAST(NOT EXISTS (SELECT 1 FROM regz_request_lines i WHERE i.request_id=r.request_id) AS INTEGER)
      + CAST((SELECT COALESCE(SUM(i.amount_cents),0) FROM regz_request_lines i
              WHERE i.request_id=r.request_id) <> b.total_cents-b.minimum_cents AS INTEGER)
      + (SELECT COUNT(*) FROM request_targets x
         WHERE x.batch_id=b.batch_id AND (x.existing_balance IS NULL OR x.amount_cents < 0
            OR x.amount_cents > x.capacity-x.minimum_cents))
      FROM request r WHERE r.batch_id=b.batch_id),0) AS request_feasibility_errors
FROM base b;
