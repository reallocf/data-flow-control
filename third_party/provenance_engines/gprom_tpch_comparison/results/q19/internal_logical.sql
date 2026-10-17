WITH base_query AS (
  SELECT
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS revenue
  FROM lineitem, part
  WHERE
    (
      p_partkey = l_partkey
      AND p_brand = 'Brand#12'
      AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
      AND l_quantity >= 1
      AND l_quantity <= 1 + 10
      AND p_size BETWEEN 1 AND 5
      AND l_shipmode IN ('AIR', 'AIR REG')
      AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR (
      p_partkey = l_partkey
      AND p_brand = 'Brand#23'
      AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
      AND l_quantity >= 10
      AND l_quantity <= 10 + 10
      AND p_size BETWEEN 1 AND 10
      AND l_shipmode IN ('AIR', 'AIR REG')
      AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR (
      p_partkey = l_partkey
      AND p_brand = 'Brand#34'
      AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
      AND l_quantity >= 20
      AND l_quantity <= 20 + 10
      AND p_size BETWEEN 1 AND 15
      AND l_shipmode IN ('AIR', 'AIR REG')
      AND l_shipinstruct = 'DELIVER IN PERSON'
    )
)
SELECT
  MAX(base_query.revenue) AS revenue
FROM base_query, lineitem, part
WHERE
  (
    part.p_partkey = lineitem.l_partkey
    AND part.p_brand = 'Brand#12'
    AND part.p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    AND lineitem.l_quantity >= 1
    AND lineitem.l_quantity <= 1 + 10
    AND part.p_size BETWEEN 1 AND 5
    AND lineitem.l_shipmode IN ('AIR', 'AIR REG')
    AND lineitem.l_shipinstruct = 'DELIVER IN PERSON'
  )
  OR (
    part.p_partkey = lineitem.l_partkey
    AND part.p_brand = 'Brand#23'
    AND part.p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    AND lineitem.l_quantity >= 10
    AND lineitem.l_quantity <= 10 + 10
    AND part.p_size BETWEEN 1 AND 10
    AND lineitem.l_shipmode IN ('AIR', 'AIR REG')
    AND lineitem.l_shipinstruct = 'DELIVER IN PERSON'
  )
  OR (
    part.p_partkey = lineitem.l_partkey
    AND part.p_brand = 'Brand#34'
    AND part.p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    AND lineitem.l_quantity >= 20
    AND lineitem.l_quantity <= 20 + 10
    AND part.p_size BETWEEN 1 AND 15
    AND lineitem.l_shipmode IN ('AIR', 'AIR REG')
    AND lineitem.l_shipinstruct = 'DELIVER IN PERSON'
  )
HAVING
  AVG(lineitem.l_quantity) >= 30;
