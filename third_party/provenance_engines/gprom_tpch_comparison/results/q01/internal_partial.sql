WITH base_query AS (
  SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS sum_qty,
    SUM(l_extendedprice) AS sum_base_price,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS sum_disc_price,
    SUM(l_extendedprice * (
      1 - l_discount
    ) * (
      1 + l_tax
    )) AS sum_charge,
    AVG(l_quantity) AS avg_qty,
    AVG(l_extendedprice) AS avg_price,
    AVG(l_discount) AS avg_disc,
    COUNT(*) AS count_order
  FROM lineitem
  WHERE
    l_shipdate <= CAST('1998-09-02' AS DATE)
  GROUP BY
    l_returnflag,
    l_linestatus
  ORDER BY
    l_returnflag,
    l_linestatus
), policy_eval AS (
  SELECT
    l_returnflag AS l_returnflag,
    l_linestatus AS l_linestatus
  FROM lineitem
  WHERE
    l_shipdate <= CAST('1998-09-02' AS DATE)
  GROUP BY
    l_returnflag,
    l_linestatus
  HAVING
    (
      AVG(lineitem.l_quantity) >= 30
    )
)
SELECT
  base_query.*
FROM base_query
JOIN policy_eval
  ON base_query.l_returnflag = policy_eval.l_returnflag
  AND base_query.l_linestatus = policy_eval.l_linestatus;
