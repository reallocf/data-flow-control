CREATE EXTENSION IF NOT EXISTS provsql CASCADE;
SET search_path TO public, provsql;

\i /workspace/third_party/provenance_engines/dfc_pushdown_probe/setup_postgres.sql

SELECT add_provenance('dfc_t');
SELECT add_provenance('dfc_s');

\echo 'Base query'
SELECT
  t_id,
  SUM(y) AS sum_y
FROM dfc_t
JOIN dfc_s ON tid = t_id
GROUP BY t_id
ORDER BY t_id;

\echo 'ProvSQL provenance-token query'
SELECT
  t_id,
  SUM(y) AS sum_y,
  provenance() AS prov_token
FROM dfc_t
JOIN dfc_s ON tid = t_id
GROUP BY t_id
ORDER BY t_id;

\echo 'ProvSQL plan for provenance-token query'
EXPLAIN (VERBOSE, COSTS OFF)
SELECT
  t_id,
  SUM(y) AS sum_y,
  provenance() AS prov_token
FROM dfc_t
JOIN dfc_s ON tid = t_id
GROUP BY t_id
ORDER BY t_id;

\echo 'Post-process-shaped policy plan'
EXPLAIN (VERBOSE, COSTS OFF)
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
    plain(sum_y) AS sum_y,
    MAX(prov_s_y) AS policy_max_s_y
  FROM annotated_output
  GROUP BY t_id, plain(sum_y)
)
SELECT
  t_id,
  sum_y
FROM policy_eval
WHERE policy_max_s_y > 3
ORDER BY t_id;

\echo 'Partial-Push-shaped policy plan'
EXPLAIN (VERBOSE, COSTS OFF)
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

\echo 'Full-Push-shaped policy plan'
EXPLAIN (VERBOSE, COSTS OFF)
SELECT
  t_id,
  SUM(y) AS sum_y
FROM dfc_t
JOIN dfc_s ON tid = t_id
GROUP BY t_id
HAVING MAX(y) > 3
ORDER BY t_id;
