WITH base_query AS (
  SELECT
    SUM(l_extendedprice * l_discount) AS revenue
  FROM lineitem
  WHERE
    l_shipdate >= CAST('1994-01-01' AS DATE)
    AND l_shipdate < CAST('1995-01-01' AS DATE)
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24
)
SELECT
  MAX(base_query.revenue) AS revenue
FROM base_query, lineitem
WHERE
  lineitem.l_shipdate >= CAST('1994-01-01' AS DATE)
  AND lineitem.l_shipdate < CAST('1995-01-01' AS DATE)
  AND lineitem.l_discount BETWEEN 0.05 AND 0.07
  AND lineitem.l_quantity < 24
HAVING
  AVG(lineitem.l_quantity) >= 30;
