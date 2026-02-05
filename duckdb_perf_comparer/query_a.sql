WITH
rewrite AS (
    WITH
    base AS (
        SELECT
            nation,
            o_year,
            SUM(amount) AS sum_profit
        FROM (
            SELECT
                n_name AS nation,
                EXTRACT(YEAR FROM o_orderdate) AS o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
            FROM
                part,
                supplier,
                lineitem,
                partsupp,
                orders,
                nation
            WHERE
                s_suppkey = l_suppkey
                AND ps_suppkey = l_suppkey
                AND ps_partkey = l_partkey
                AND p_partkey = l_partkey
                AND o_orderkey = l_orderkey
                AND s_nationkey = n_nationkey
                AND p_name LIKE '%green%'
        ) AS profit
        GROUP BY
            nation,
            o_year
        ORDER BY
            nation,
            o_year DESC
    )

    SELECT
        base.nation,
        base.o_year,
        base.sum_profit,
        lineitem.l_quantity
    FROM base
    JOIN nation ON base.nation = nation.n_name
    JOIN supplier ON nation.n_nationkey = supplier.s_nationkey
    JOIN lineitem ON supplier.s_suppkey = lineitem.l_suppkey
    JOIN orders ON lineitem.l_orderkey = orders.o_orderkey AND EXTRACT(YEAR FROM orders.o_orderdate) = base.o_year
    JOIN part ON lineitem.l_partkey = part.p_partkey
    WHERE part.p_name LIKE '%green%'
)

SELECT
    nation,
    o_year,
    sum_profit
FROM rewrite
GROUP BY nation, o_year, sum_profit
HAVING count(*) > 1500
-- HAVING case when count(*) > 1500 then 1 else kill() end
ORDER BY nation, o_year DESC
