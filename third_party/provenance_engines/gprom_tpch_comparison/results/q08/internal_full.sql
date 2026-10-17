SELECT
  o_year,
  CAST(SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) AS DOUBLE PRECISION) / NULLIF(SUM(volume), 0) AS mkt_share
FROM (
  SELECT
    EXTRACT(YEAR FROM o_orderdate) AS o_year,
    l_extendedprice * (
      1 - l_discount
    ) AS volume,
    n2.n_name AS nation,
    lineitem.l_quantity
  FROM part, supplier, lineitem, orders, customer, nation AS n1, nation AS n2, region
  WHERE
    p_partkey = l_partkey
    AND s_suppkey = l_suppkey
    AND l_orderkey = o_orderkey
    AND o_custkey = c_custkey
    AND c_nationkey = n1.n_nationkey
    AND n1.n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND s_nationkey = n2.n_nationkey
    AND o_orderdate BETWEEN CAST('1995-01-01' AS DATE) AND CAST('1996-12-31' AS DATE)
    AND p_type = 'ECONOMY ANODIZED STEEL'
) AS all_nations
GROUP BY
  o_year
HAVING
  (
    AVG(all_nations.l_quantity) >= 30
  )
ORDER BY
  o_year;
