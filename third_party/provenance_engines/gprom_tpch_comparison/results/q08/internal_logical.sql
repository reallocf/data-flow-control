WITH base_query AS (
  SELECT
    EXTRACT(YEAR FROM o_orderdate) AS o_year,
    CAST(SUM(
      CASE
        WHEN n2.n_name = 'BRAZIL'
        THEN l_extendedprice * (
          1 - l_discount
        )
        ELSE 0
      END
    ) AS DOUBLE PRECISION) / NULLIF(SUM(l_extendedprice * (
      1 - l_discount
    )), 0) AS mkt_share
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
  GROUP BY
    EXTRACT(YEAR FROM o_orderdate)
)
SELECT
  base_query.o_year,
  MAX(base_query.mkt_share) AS mkt_share
FROM base_query, part, supplier, lineitem, orders, customer, nation AS n1, nation AS n2, region
WHERE
  part.p_partkey = lineitem.l_partkey
  AND supplier.s_suppkey = lineitem.l_suppkey
  AND lineitem.l_orderkey = orders.o_orderkey
  AND orders.o_custkey = customer.c_custkey
  AND customer.c_nationkey = n1.n_nationkey
  AND n1.n_regionkey = region.r_regionkey
  AND region.r_name = 'AMERICA'
  AND supplier.s_nationkey = n2.n_nationkey
  AND orders.o_orderdate BETWEEN CAST('1995-01-01' AS DATE) AND CAST('1996-12-31' AS DATE)
  AND part.p_type = 'ECONOMY ANODIZED STEEL'
  AND base_query.o_year = EXTRACT(YEAR FROM orders.o_orderdate)
GROUP BY
  base_query.o_year
HAVING
  AVG(lineitem.l_quantity) >= 30
ORDER BY
  base_query.o_year;
