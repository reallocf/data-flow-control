-- Base query: Q = γ_sum(y)(T ⋈ S)
SELECT
  t_id,
  SUM(dfc_s.y) AS sum_y
FROM dfc_t
JOIN dfc_s ON tid = t_id
GROUP BY t_id
ORDER BY t_id;

-- Post-process-shaped enforcement.
-- This simulates what a provenance-expanded result makes easy: aggregate over
-- the annotated contributing S tuples after producing output/provenance rows.
WITH annotated_output AS (
  SELECT
    t_id,
    SUM(y) OVER (PARTITION BY t_id) AS sum_y,
    s_id AS prov_s_id,
    y AS prov_s_y
  FROM dfc_t
  JOIN dfc_s ON tid = t_id
),
policy_eval AS (
  SELECT
    t_id,
    sum_y,
    MAX(prov_s_y) AS policy_max_s_y
  FROM annotated_output
  GROUP BY t_id, sum_y
)
SELECT
  t_id,
  sum_y
FROM policy_eval
WHERE policy_max_s_y > 3
ORDER BY t_id;

-- Partial-Push-shaped enforcement.
-- Push only the policy aggregate below the final policy-evaluation join.
WITH base_q AS (
  SELECT
    t_id,
    SUM(y) AS sum_y
  FROM dfc_t
  JOIN dfc_s ON tid = t_id
  GROUP BY t_id
),
s_policy AS (
  SELECT
    tid,
    MAX(y) AS policy_max_s_y
  FROM dfc_s
  GROUP BY tid
)
SELECT
  base_q.t_id,
  base_q.sum_y
FROM base_q
JOIN s_policy ON s_policy.tid = base_q.t_id
WHERE s_policy.policy_max_s_y > 3
ORDER BY base_q.t_id;

-- Full-Push-shaped enforcement.
-- Compute the base aggregate and policy aggregate in one grouped pass.
SELECT
  t_id,
  SUM(y) AS sum_y
FROM dfc_t
JOIN dfc_s ON tid = t_id
GROUP BY t_id
HAVING MAX(y) > 3
ORDER BY t_id;
