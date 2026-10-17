WITH base_query AS (
  SELECT
    n1.n_name AS supp_nation,
    n2.n_name AS cust_nation,
    EXTRACT(YEAR FROM l_shipdate) AS l_year,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS revenue
  FROM supplier, lineitem, orders, customer, nation AS n1, nation AS n2
  WHERE
    s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND (
      (
        n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY'
      )
      OR (
        n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'
      )
    )
    AND l_shipdate BETWEEN CAST('1995-01-01' AS DATE) AND CAST('1996-12-31' AS DATE)
  GROUP BY
    n1.n_name,
    n2.n_name,
    EXTRACT(YEAR FROM l_shipdate)
)
SELECT
  base_query.supp_nation,
  base_query.cust_nation,
  base_query.l_year,
  MAX(base_query.revenue) AS revenue
FROM base_query, supplier, lineitem, orders, customer, nation AS n1, nation AS n2
WHERE
  supplier.s_suppkey = lineitem.l_suppkey
  AND orders.o_orderkey = lineitem.l_orderkey
  AND customer.c_custkey = orders.o_custkey
  AND supplier.s_nationkey = n1.n_nationkey
  AND customer.c_nationkey = n2.n_nationkey
  AND (
    (
      n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY'
    )
    OR (
      n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'
    )
  )
  AND lineitem.l_shipdate BETWEEN CAST('1995-01-01' AS DATE) AND CAST('1996-12-31' AS DATE)
  AND base_query.supp_nation = n1.n_name
  AND base_query.cust_nation = n2.n_name
  AND base_query.l_year = EXTRACT(YEAR FROM lineitem.l_shipdate)
GROUP BY
  base_query.supp_nation,
  base_query.cust_nation,
  base_query.l_year
HAVING
  AVG(lineitem.l_quantity) >= 30
ORDER BY
  base_query.supp_nation,
  base_query.cust_nation,
  base_query.l_year;
