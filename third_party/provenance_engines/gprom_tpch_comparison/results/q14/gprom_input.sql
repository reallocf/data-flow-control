WITH provenance_result AS (
  PROVENANCE OF (
SELECT
    100.00 * sum(
        CASE WHEN p_type LIKE 'PROMO%' THEN
            l_extendedprice * (1 - l_discount)
        ELSE
            0
        END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= CAST('1995-09-01' AS date)
    AND l_shipdate < CAST('1995-10-01' AS date)
  )
)
SELECT promo_revenue
FROM provenance_result
GROUP BY promo_revenue
HAVING AVG(prov_lineitem_l__quantity) >= 30;
