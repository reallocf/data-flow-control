SELECT
  p.batch_id
FROM (
  SELECT
    e.batch_id
  FROM (
    WITH base AS MATERIALIZED (
      SELECT
        p.*,
        pay.account_id,
        pay.credited_date,
        pay.cycle_end,
        pay.total_cents,
        pay.minimum_cents
      FROM regz_proposals AS p
      JOIN regz_payments AS pay
        ON pay.payment_id = p.payment_id
    ), program_terms AS MATERIALIZED (
      SELECT
        g.*,
        (
          g.interest_accrues
          AND g.waived_on_payoff
          AND NOT g.grace_period
          AND (
            NOT g.temporary_zero OR g.retroactive_interest
          )
        ) AS qualifies
      FROM regz_programs AS g
    ), lot_terms AS MATERIALIZED (
      SELECT
        b.batch_id,
        b.account_id,
        b.credited_date,
        b.snapshot_date,
        bal.balance_id,
        bal.amount_cents,
        bal.protected_cents,
        bal.secured,
        bal.program_id,
        l.minimum_cents,
        l.excess_cents,
        l.effective_apr AS proposed_apr,
        l.window_start AS proposed_window,
        CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
        COALESCE(old.apr, bal.apr) AS reference_apr,
        COALESCE(g.qualifies, FALSE) AS qualifies,
        COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date AND g.expiration_date, FALSE) AS active,
        g.expiration_date,
        g.grace_period,
        g.temporary_zero,
        g.retroactive_interest,
        g.interim_payment_required,
        d.qualifies AS declared
      FROM base AS b
      JOIN regz_balances AS bal
        ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
      JOIN regz_proposal_lines AS l
        ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
      LEFT JOIN regz_balances AS old
        ON old.account_id = b.account_id
        AND old.balance_id = bal.balance_id
        AND old.snapshot_date = b.snapshot_date
      LEFT JOIN program_terms AS g
        ON g.program_id = bal.program_id
      LEFT JOIN regz_program_classifications AS d
        ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
    )
    SELECT
      b.batch_id,
      'allocation' AS kind,
      (
        SELECT
          COUNT(*)
        FROM lot_terms AS x
        WHERE
          x.batch_id = b.batch_id
          AND (
            x.temporary_zero AND NOT x.retroactive_interest AND x.declared
          )
      ) AS zero_offer_classifications
    FROM base AS b
  ) AS e
  WHERE
    (
      e.kind <> 'allocation' OR (
        e.zero_offer_classifications = 0
      )
    )
) AS p
WHERE
  p.batch_id IN (
    SELECT
      b.batch_id
    FROM regz_proposals AS b
    JOIN regz_payments AS pay
      ON pay.payment_id = b.payment_id
    JOIN regz_accounts AS a
      ON a.account_id = pay.account_id
    WHERE
      a.credit_card
      AND a.consumer_plan
      AND a.open_end
      AND NOT a.home_secured
      AND b.method IN ('default', 'deferred_request', 'secured_request')
      AND b.decision_date >= pay.credited_date
      AND pay.minimum_cents BETWEEN 0 AND pay.total_cents
      AND EXISTS(
        SELECT
          1
        FROM regz_snapshot_sets AS s
        WHERE
          s.account_id = pay.account_id AND s.snapshot_date = b.snapshot_date
      )
      AND EXISTS(
        SELECT
          1
        FROM regz_snapshot_sets AS s
        WHERE
          s.account_id = pay.account_id AND s.snapshot_date = pay.credited_date
      )
      AND EXISTS(
        SELECT
          1
        FROM regz_balances AS c
        WHERE
          c.account_id = pay.account_id AND c.snapshot_date = pay.credited_date
      )
      AND NOT EXISTS(
        SELECT
          1
        FROM regz_balances AS c
        WHERE
          c.account_id = pay.account_id
          AND c.snapshot_date = pay.credited_date
          AND NOT EXISTS(
            SELECT
              1
            FROM regz_proposal_lines AS l
            WHERE
              l.batch_id = b.batch_id AND l.balance_id = c.balance_id
          )
      )
      AND NOT EXISTS(
        SELECT
          1
        FROM regz_proposal_lines AS l
        WHERE
          l.batch_id = b.batch_id
          AND NOT EXISTS(
            SELECT
              1
            FROM regz_balances AS c
            WHERE
              c.account_id = pay.account_id
              AND c.snapshot_date = pay.credited_date
              AND c.balance_id = l.balance_id
          )
      )
      AND (
        SELECT
          SUM(l.minimum_cents)
        FROM regz_proposal_lines AS l
        WHERE
          l.batch_id = b.batch_id
      ) = pay.minimum_cents
      AND (
        SELECT
          SUM(l.excess_cents)
        FROM regz_proposal_lines AS l
        WHERE
          l.batch_id = b.batch_id
      ) = pay.total_cents - pay.minimum_cents
      AND (
        (
          b.replaces_batch_id IS NULL
          AND NOT EXISTS(
            SELECT
              1
            FROM regz_payment_heads AS h
            WHERE
              h.payment_id = pay.payment_id
          )
        )
        OR EXISTS(
          SELECT
            1
          FROM regz_payment_heads AS h
          WHERE
            h.payment_id = pay.payment_id AND h.batch_id = b.replaces_batch_id
        )
      )
      AND EXISTS(
        SELECT
          1
        FROM regz_practices AS p
        WHERE
          p.account_id = pay.account_id AND p.effective_date <= pay.credited_date
      )
      AND NOT EXISTS(
        SELECT
          1
        FROM regz_practices AS p
        WHERE
          p.account_id = pay.account_id
          AND p.effective_date <= pay.credited_date
          AND NOT p.convention IN ('cycle_end', 'credit_date', 'offset')
      )
      AND NOT EXISTS(
        SELECT
          1
        FROM regz_balances AS c
        JOIN regz_proposal_lines AS l
          ON l.balance_id = c.balance_id
        JOIN regz_proposals AS local_proposal
          ON local_proposal.batch_id = l.batch_id
        LEFT JOIN regz_balances AS old
          ON old.account_id = c.account_id
          AND old.balance_id = c.balance_id
          AND old.snapshot_date = local_proposal.snapshot_date
        WHERE
          l.batch_id = b.batch_id
          AND c.account_id = pay.account_id
          AND c.snapshot_date = pay.credited_date
          AND (
            c.protected_cents < 0
            OR c.protected_cents > c.amount_cents
            OR l.minimum_cents < 0
            OR l.excess_cents < 0
            OR l.minimum_cents + l.excess_cents > c.amount_cents
            OR (
              NOT old.balance_id IS NULL AND c.amount_cents > old.amount_cents
            )
            OR COALESCE(old.apr, c.apr) < 0
          )
      )
      AND NOT EXISTS(
        SELECT
          1
        FROM regz_balances AS c
        WHERE
          c.account_id = pay.account_id
          AND c.snapshot_date = pay.credited_date
          AND NOT c.program_id IS NULL
          AND (
            NOT EXISTS(
              SELECT
                1
              FROM regz_program_classifications AS d
              WHERE
                d.batch_id = b.batch_id AND d.program_id = c.program_id
            )
            OR NOT EXISTS(
              SELECT
                1
              FROM regz_programs AS g
              WHERE
                g.program_id = c.program_id
                AND g.account_id = pay.account_id
                AND g.start_date <= g.expiration_date
            )
          )
      )
      AND NOT EXISTS(
        SELECT
          1
        FROM regz_program_classifications AS d
        WHERE
          d.batch_id = b.batch_id
          AND NOT EXISTS(
            SELECT
              1
            FROM regz_balances AS c
            WHERE
              c.account_id = pay.account_id
              AND c.snapshot_date = pay.credited_date
              AND c.program_id = d.program_id
          )
      )
      AND NOT EXISTS(
        SELECT
          1
        FROM regz_balances AS bal
        JOIN regz_programs AS g
          ON g.program_id = bal.program_id
        WHERE
          bal.account_id = pay.account_id
          AND bal.snapshot_date = pay.credited_date
          AND g.interest_accrues
          AND g.waived_on_payoff
          AND NOT g.grace_period
          AND (
            NOT g.temporary_zero OR g.retroactive_interest
          )
          AND pay.credited_date BETWEEN g.start_date AND g.expiration_date
          AND (
            (
              SELECT
                COUNT(*)
              FROM regz_cycles AS c
              WHERE
                c.account_id = pay.account_id
                AND g.expiration_date BETWEEN c.start_date AND c.end_date
            ) <> 1
            OR (
              SELECT
                COUNT(*)
              FROM regz_cycles AS c
              WHERE
                c.account_id = pay.account_id
                AND c.start_date <= g.expiration_date
                AND c.due_date <= g.expiration_date
            ) < 2
            OR EXISTS(
              SELECT
                1
              FROM regz_cycles AS c
              WHERE
                c.account_id = pay.account_id
                AND EXISTS(
                  SELECT
                    1
                  FROM regz_cycles AS n
                  WHERE
                    n.account_id = c.account_id AND n.start_date > c.start_date
                )
                AND DATE(c.end_date, '+1 day') <> (
                  SELECT
                    MIN(n.start_date)
                  FROM regz_cycles AS n
                  WHERE
                    n.account_id = c.account_id AND n.start_date > c.start_date
                )
            )
          )
      )
      AND (
        b.method = 'default'
        OR b.request_id IS NULL
        OR EXISTS(
          SELECT
            1
          FROM regz_requests AS r
          WHERE
            r.request_id = b.request_id
            AND r.payment_id = pay.payment_id
            AND r.basis IN (
              'contact',
              'form',
              'reallocation',
              'agreement_default',
              'application_default',
              'coupon_default'
            )
        )
      )
  );
