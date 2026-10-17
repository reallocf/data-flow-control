WITH base_query AS (
  SELECT
    supp_nation,
    cust_nation,
    l_year,
    SUM(volume) AS revenue
  FROM (
    SELECT
      n1.n_name AS supp_nation,
      n2.n_name AS cust_nation,
      EXTRACT(YEAR FROM l_shipdate) AS l_year,
      l_extendedprice * (
        1 - l_discount
      ) AS volume
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
  ) AS shipping
  GROUP BY
    supp_nation,
    cust_nation,
    l_year
  ORDER BY
    supp_nation,
    cust_nation,
    l_year
), policy_eval AS (
  SELECT
    supp_nation AS supp_nation,
    cust_nation AS cust_nation,
    l_year AS l_year
  FROM (
    SELECT
      n1.n_name AS supp_nation,
      n2.n_name AS cust_nation,
      EXTRACT(YEAR FROM l_shipdate) AS l_year,
      l_extendedprice * (
        1 - l_discount
      ) AS volume,
      lineitem.l_quantity
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
  ) AS shipping
  GROUP BY
    supp_nation,
    cust_nation,
    l_year
  HAVING
    (
      AVG(shipping.l_quantity) >= 30
    )
)
SELECT
  base_query.*
FROM base_query
JOIN policy_eval
  ON base_query.supp_nation = policy_eval.supp_nation
  AND base_query.cust_nation = policy_eval.cust_nation
  AND base_query.l_year = policy_eval.l_year;
