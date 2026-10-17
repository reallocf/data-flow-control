PROVENANCE OF (SELECT
    sum(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= CAST('1994-01-01' AS date)
    AND l_shipdate < CAST('1995-01-01' AS date)
    AND l_discount * 100 >= 5
    AND l_discount * 100 <= 7
    AND l_quantity < 24);
