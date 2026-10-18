SELECT
  *
FROM (
  WITH item_credits AS MATERIALIZED (
    SELECT
      k.error_id,
      i.item_id,
      i.required_cents,
      COALESCE(SUM(c.amount_cents), 0) AS credited_cents
    FROM regz_correction_completions AS k
    JOIN regz_error_obligations AS o
      ON o.error_id = k.error_id
    JOIN regz_error_items AS i
      ON i.error_id = k.error_id
    LEFT JOIN regz_credits AS c
      ON c.error_id = i.error_id
      AND c.item_id = i.item_id
      AND c.posted_date >= o.determination_date
      AND c.posted_date <= k.checkpoint_date
      AND c.posted_date <= o.deadline
    GROUP BY
      k.error_id,
      i.item_id,
      i.required_cents
  )
  SELECT
    'correction:' || o.error_id AS batch_id,
    (
      SELECT
        SUM(MAX(0, i.required_cents - i.credited_cents))
      FROM item_credits AS i
      WHERE
        i.error_id = o.error_id
    ) AS missing_credit_cents,
    (
      SELECT
        COUNT(*)
      FROM regz_dispatches AS d
      WHERE
        d.error_id = o.error_id
        AND d.recipient = o.recipient
        AND d.notice_type = 'correction'
        AND d.sent_date BETWEEN o.determination_date AND MIN(k.checkpoint_date, o.deadline)
        AND LENGTH(TRIM(d.content_evidence)) > 0
    ) AS notice_count
  FROM regz_correction_completions AS k
  JOIN regz_error_obligations AS o
    ON o.error_id = k.error_id
  WHERE
    k.checkpoint_date >= o.determination_date
) AS correction
WHERE
  notice_count > 0
  AND batch_id IN (
    SELECT
      'correction:' || o.error_id AS batch_id
    FROM regz_correction_completions AS k
    JOIN regz_error_obligations AS o
      ON o.error_id = k.error_id
    WHERE
      k.checkpoint_date >= o.determination_date
      AND EXISTS(
        SELECT
          1
        FROM regz_error_items AS i
        WHERE
          i.error_id = o.error_id
      )
      AND NOT EXISTS(
        SELECT
          1
        FROM regz_error_items AS i
        WHERE
          i.error_id = o.error_id AND i.required_cents < 0
      )
  );
