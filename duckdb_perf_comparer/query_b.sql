WITH base_query AS (
    SELECT
        nation,
        o_year,
        SUM(amount) AS sum_profit
    FROM (
        SELECT
            n_name AS nation,
            EXTRACT(YEAR FROM o_orderdate) AS o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount,
            lineitem.l_quantity
        FROM part, supplier, lineitem, partsupp, orders, nation
        WHERE s_suppkey = l_suppkey
            AND ps_suppkey = l_suppkey
            AND ps_partkey = l_partkey
            AND p_partkey = l_partkey
            AND o_orderkey = l_orderkey
            AND s_nationkey = n_nationkey
            AND p_name LIKE '%green%'
    ) AS profit
    GROUP BY nation, o_year
)
SELECT
    base_query.nation,
    base_query.o_year,
    MAX(base_query.sum_profit) AS sum_profit
FROM base_query
JOIN (
    SELECT
        nation,
        o_year,
        l_quantity
    FROM (
        SELECT
            n_name AS nation,
            EXTRACT(YEAR FROM o_orderdate) AS o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount,
            lineitem.l_quantity
        FROM part, supplier, lineitem, partsupp, orders, nation
        WHERE s_suppkey = l_suppkey
            AND ps_suppkey = l_suppkey
            AND ps_partkey = l_partkey
            AND p_partkey = l_partkey
            AND o_orderkey = l_orderkey
            AND s_nationkey = n_nationkey
            AND p_name LIKE '%green%'
    ) AS profit
) AS rescan
    ON base_query.nation = rescan.nation
    AND base_query.o_year = rescan.o_year
GROUP BY base_query.nation, base_query.o_year
HAVING count(*) > 1500
ORDER BY base_query.nation, base_query.o_year DESC

-- • Yes—based on the join diagnostics, Query A is slower because its hottest join is sitting above a big, expensive join
--   subtree and is repeatedly re‑joining the base aggregation back to multiple base tables (nation → supplier → lineitem →
--   orders → part) with the %green% filter applied late. That creates a large intermediate join graph and blows up the hash
--   join cost.

--   Key signals:

--   - Query A’s slowest join (l_orderkey = o_orderkey) takes ~4.3s and has a HASH_JOIN child that already costs ~186 ms,
--     indicating a large, expensive join subtree feeding into another join.
--   - Query B’s top joins are ~270 ms or less and are fed by lightweight PROJECTION children, i.e., the rescan path is
--     narrower and cheaper.
--   - Query B keeps the “rescan” as a small, focused join on just (nation, o_year, l_quantity), rather than re‑joining to
--     multiple tables and re‑applying the %green% predicate after aggregation.

--   So the main reason is join shape: Query A re‑expands the result by joining to many base tables after aggregation, while
--   Query B keeps a smaller rescan scope. That makes A’s hash joins far more expensive.

--   If you want, I can extend the tool to show estimated cardinality deltas and per‑join input sizes to quantify the blow‑up
--   more explicitly.
