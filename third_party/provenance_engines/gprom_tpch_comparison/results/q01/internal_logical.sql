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
)
SELECT
  base_query.l_returnflag,
  base_query.l_linestatus,
  MAX(base_query.sum_qty) AS sum_qty,
  MAX(base_query.sum_base_price) AS sum_base_price,
  MAX(base_query.sum_disc_price) AS sum_disc_price,
  MAX(base_query.sum_charge) AS sum_charge,
  MAX(base_query.avg_qty) AS avg_qty,
  MAX(base_query.avg_price) AS avg_price,
  MAX(base_query.avg_disc) AS avg_disc,
  MAX(base_query.count_order) AS count_order
FROM base_query, lineitem
WHERE
  lineitem.l_shipdate <= CAST('1998-09-02' AS DATE)
  AND base_query.l_returnflag = lineitem.l_returnflag
  AND base_query.l_linestatus = lineitem.l_linestatus
GROUP BY
  base_query.l_returnflag,
  base_query.l_linestatus
HAVING
  AVG(lineitem.l_quantity) >= 30
ORDER BY
  base_query.l_returnflag,
  base_query.l_linestatus;
