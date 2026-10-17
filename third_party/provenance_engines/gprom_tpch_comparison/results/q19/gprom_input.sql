WITH provenance_result AS (
  PROVENANCE OF (
SELECT
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    lineitem,
    part
WHERE (p_partkey = l_partkey
    AND p_brand = 'Brand#12'
    AND (p_container = 'SM CASE' OR p_container = 'SM BOX' OR p_container = 'SM PACK' OR p_container = 'SM PKG')
    AND l_quantity >= 1
    AND l_quantity <= 1 + 10
    AND p_size >= 1 AND p_size <= 5
    AND (l_shipmode = 'AIR' OR l_shipmode = 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON')
    OR (p_partkey = l_partkey
        AND p_brand = 'Brand#23'
        AND (p_container = 'MED BAG' OR p_container = 'MED BOX' OR p_container = 'MED PKG' OR p_container = 'MED PACK')
        AND l_quantity >= 10
        AND l_quantity <= 10 + 10
        AND p_size >= 1 AND p_size <= 10
        AND (l_shipmode = 'AIR' OR l_shipmode = 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON')
    OR (p_partkey = l_partkey
        AND p_brand = 'Brand#34'
        AND (p_container = 'LG CASE' OR p_container = 'LG BOX' OR p_container = 'LG PACK' OR p_container = 'LG PKG')
        AND l_quantity >= 20
        AND l_quantity <= 20 + 10
        AND p_size >= 1 AND p_size <= 15
        AND (l_shipmode = 'AIR' OR l_shipmode = 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON')
  )
)
SELECT revenue
FROM provenance_result
GROUP BY revenue
HAVING AVG(prov_lineitem_l__quantity) >= 30;
