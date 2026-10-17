WITH base_query AS (
  SELECT
    CAST(100.00 * SUM(
      CASE
        WHEN p_type LIKE 'PROMO%'
        THEN l_extendedprice * (
          1 - l_discount
        )
        ELSE 0
      END
    ) AS DOUBLE PRECISION) / NULLIF(SUM(l_extendedprice * (
      1 - l_discount
    )), 0) AS promo_revenue
  FROM lineitem, part
  WHERE
    l_partkey = p_partkey
    AND l_shipdate >= CAST('1995-09-01' AS DATE)
    AND l_shipdate < CAST('1995-10-01' AS DATE)
)
SELECT
  MAX(base_query.promo_revenue) AS promo_revenue
FROM base_query, lineitem, part
WHERE
  lineitem.l_partkey = part.p_partkey
  AND lineitem.l_shipdate >= CAST('1995-09-01' AS DATE)
  AND lineitem.l_shipdate < CAST('1995-10-01' AS DATE)
HAVING
  AVG(lineitem.l_quantity) >= 30;
