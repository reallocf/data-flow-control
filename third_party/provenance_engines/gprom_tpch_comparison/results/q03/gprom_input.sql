WITH provenance_result AS (
  PROVENANCE OF (
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS date)
    AND l_shipdate > CAST('1995-03-15' AS date)
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10
  )
)
SELECT l_orderkey, revenue, o_orderdate, o_shippriority
FROM provenance_result
GROUP BY l_orderkey, revenue, o_orderdate, o_shippriority
HAVING AVG(prov_lineitem_l__quantity) >= 30;
