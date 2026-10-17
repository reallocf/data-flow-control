WITH provenance_result AS (
  PROVENANCE OF (
SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(outer_lineitem.l_quantity) AS sum_l_quantity
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem AS outer_lineitem ON o_orderkey = outer_lineitem.l_orderkey
JOIN (
    SELECT l_orderkey
    FROM lineitem
    GROUP BY l_orderkey
    HAVING sum(l_quantity) > 300
) AS large_orders ON o_orderkey = large_orders.l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100
  )
)
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum_l_quantity
FROM provenance_result
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum_l_quantity
HAVING AVG(prov_lineitem_1_l__quantity) >= 30 AND AVG(prov_lineitem_l__quantity) >= 30;
