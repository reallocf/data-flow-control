"""Tests for TPC-H query rewriting correctness.

These tests verify that logical rewriting produces the same results
as DFC (SQLRewriter) for each TPC-H query. DFC is the source of truth.

Each test:
1. Loads TPC-H data (sf=0.1)
2. Loads the query
3. Runs DFC approach (source of truth)
4. Runs logical approach
5. Compares results using compare_results()
6. Asserts all match

These tests enable fast iteration when fixing rewriting bugs.
"""

import contextlib
import re

import duckdb
import pytest
from sql_rewriter import DFCPolicy, Resolution, SQLRewriter
import sqlglot

from vldb_experiments.baselines.logical_baseline import execute_query_logical, rewrite_query_logical
from vldb_experiments.correctness import compare_results
from vldb_experiments.strategies.tpch_strategy import load_tpch_query

# Policies used in test_tpch.py
lineitem_policy = DFCPolicy(
    source="lineitem",
    constraint="avg(lineitem.l_quantity) >= 30",
    on_fail=Resolution.REMOVE,
)


LOGICAL_EXPECTED_SQL = {
    1: """
        WITH base_query AS (
            SELECT
                l_returnflag,
                l_linestatus,
                sum(l_quantity) AS sum_qty,
                sum(l_extendedprice) AS sum_base_price,
                sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                avg(l_quantity) AS avg_qty,
                avg(l_extendedprice) AS avg_price,
                avg(l_discount) AS avg_disc,
                count(*) AS count_order
            FROM lineitem
            WHERE l_shipdate <= CAST('1998-09-02' AS DATE)
            GROUP BY l_returnflag, l_linestatus
        )
        SELECT
            base_query.l_returnflag,
            base_query.l_linestatus,
            max(base_query.sum_qty) AS sum_qty,
            max(base_query.sum_base_price) AS sum_base_price,
            max(base_query.sum_disc_price) AS sum_disc_price,
            max(base_query.sum_charge) AS sum_charge,
            max(base_query.avg_qty) AS avg_qty,
            max(base_query.avg_price) AS avg_price,
            max(base_query.avg_disc) AS avg_disc,
            max(base_query.count_order) AS count_order
        FROM base_query
        JOIN (
            SELECT
                l_returnflag,
                l_linestatus,
                lineitem.l_quantity
            FROM lineitem
            WHERE l_shipdate <= CAST('1998-09-02' AS DATE)
        ) AS rescan
            ON base_query.l_returnflag = rescan.l_returnflag
            AND base_query.l_linestatus = rescan.l_linestatus
        GROUP BY base_query.l_returnflag, base_query.l_linestatus
        HAVING avg(rescan.l_quantity) >= 30
        ORDER BY base_query.l_returnflag, base_query.l_linestatus
    """,
    3: """
        WITH base_query AS (
            SELECT
                l_orderkey,
                SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                o_orderdate,
                o_shippriority
            FROM customer, orders, lineitem
            WHERE c_mktsegment = 'BUILDING'
                AND c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND o_orderdate < CAST('1995-03-15' AS DATE)
                AND l_shipdate > CAST('1995-03-15' AS DATE)
            GROUP BY l_orderkey, o_orderdate, o_shippriority
            ORDER BY revenue DESC, o_orderdate
            LIMIT 10
        )
        SELECT
            base_query.l_orderkey,
            max(base_query.revenue) AS revenue,
            base_query.o_orderdate,
            base_query.o_shippriority
        FROM base_query
        JOIN (
            SELECT
                l_orderkey,
                lineitem.l_quantity
            FROM customer, orders, lineitem
            WHERE c_mktsegment = 'BUILDING'
                AND c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND o_orderdate < CAST('1995-03-15' AS DATE)
                AND l_shipdate > CAST('1995-03-15' AS DATE)
        ) AS rescan
            ON base_query.l_orderkey = rescan.l_orderkey
        GROUP BY base_query.l_orderkey, base_query.o_orderdate, base_query.o_shippriority
        HAVING avg(rescan.l_quantity) >= 30
        ORDER BY revenue DESC, base_query.o_orderdate
        LIMIT 10
    """,
    4: """
        WITH rewrite AS (
            WITH base_query AS (
                SELECT
                    o_orderpriority,
                    count(*) AS order_count
                FROM orders
                WHERE o_orderdate >= CAST('1993-07-01' AS DATE)
                    AND o_orderdate < CAST('1993-10-01' AS DATE)
                    AND EXISTS (
                        SELECT *
                        FROM lineitem
                        WHERE l_orderkey = o_orderkey
                            AND l_commitdate < l_receiptdate
                    )
                GROUP BY o_orderpriority
                ORDER BY o_orderpriority
            )
            SELECT
                base_query.o_orderpriority,
                base_query.order_count,
                orders.o_orderkey,
                avg(lineitem.l_quantity) AS policy_1
            FROM base_query
            JOIN orders
                ON base_query.o_orderpriority = orders.o_orderpriority
            JOIN lineitem
                ON l_orderkey = o_orderkey
            WHERE l_commitdate < l_receiptdate
            GROUP BY
                base_query.o_orderpriority,
                base_query.order_count,
                orders.o_orderkey
        )
        SELECT
            o_orderpriority,
            order_count
        FROM rewrite
        GROUP BY o_orderpriority, order_count
        HAVING max(rewrite.policy_1) >= 30
        ORDER BY o_orderpriority
    """,
    5: """
        WITH base_query AS (
            SELECT
                n_name,
                SUM(l_extendedprice * (1 - l_discount)) AS revenue
            FROM customer, orders, lineitem, supplier, nation, region
            WHERE c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND l_suppkey = s_suppkey
                AND c_nationkey = s_nationkey
                AND s_nationkey = n_nationkey
                AND n_regionkey = r_regionkey
                AND r_name = 'ASIA'
                AND o_orderdate >= CAST('1994-01-01' AS DATE)
                AND o_orderdate < CAST('1995-01-01' AS DATE)
            GROUP BY n_name
        )
        SELECT
            base_query.n_name,
            max(base_query.revenue) AS revenue
        FROM base_query
        JOIN (
            SELECT
                n_name,
                lineitem.l_quantity
            FROM customer, orders, lineitem, supplier, nation, region
            WHERE c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND l_suppkey = s_suppkey
                AND c_nationkey = s_nationkey
                AND s_nationkey = n_nationkey
                AND n_regionkey = r_regionkey
                AND r_name = 'ASIA'
                AND o_orderdate >= CAST('1994-01-01' AS DATE)
                AND o_orderdate < CAST('1995-01-01' AS DATE)
        ) AS rescan
            ON base_query.n_name = rescan.n_name
        GROUP BY base_query.n_name
        HAVING avg(rescan.l_quantity) >= 30
        ORDER BY revenue DESC
    """,
    6: """
        WITH base_query AS (
            SELECT
                sum(l_extendedprice * l_discount) AS revenue
            FROM lineitem
            WHERE l_shipdate >= CAST('1994-01-01' AS DATE)
                AND l_shipdate < CAST('1995-01-01' AS DATE)
                AND l_discount BETWEEN 0.05 AND 0.07
                AND l_quantity < 24
        )
        SELECT max(base_query.revenue) AS revenue
        FROM base_query
        JOIN (
            SELECT
                lineitem.l_quantity
            FROM lineitem
            WHERE l_shipdate >= CAST('1994-01-01' AS DATE)
                AND l_shipdate < CAST('1995-01-01' AS DATE)
                AND l_discount BETWEEN 0.05 AND 0.07
                AND l_quantity < 24
        ) AS rescan
            ON 1=1
        HAVING avg(rescan.l_quantity) >= 30
    """,
    7: """
        WITH base_query AS (
            SELECT
                supp_nation,
                cust_nation,
                l_year,
                SUM(volume) AS revenue
            FROM (
                SELECT
                    n1.n_name AS supp_nation,
                    n2.n_name AS cust_nation,
                    EXTRACT(YEAR FROM l_shipdate) AS l_year,
                    l_extendedprice * (1 - l_discount) AS volume,
                    lineitem.l_quantity
                FROM supplier, lineitem, orders, customer, nation AS n1, nation AS n2
                WHERE s_suppkey = l_suppkey
                    AND o_orderkey = l_orderkey
                    AND c_custkey = o_custkey
                    AND s_nationkey = n1.n_nationkey
                    AND c_nationkey = n2.n_nationkey
                    AND (
                        (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                        OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
                    )
                    AND l_shipdate BETWEEN CAST('1995-01-01' AS DATE)
                        AND CAST('1996-12-31' AS DATE)
            ) AS shipping
            GROUP BY supp_nation, cust_nation, l_year
        )
        SELECT
            base_query.supp_nation,
            base_query.cust_nation,
            base_query.l_year,
            MAX(base_query.revenue) AS revenue
        FROM base_query
        JOIN (
            SELECT
                supp_nation,
                cust_nation,
                l_year,
                l_quantity
            FROM (
                SELECT
                    n1.n_name AS supp_nation,
                    n2.n_name AS cust_nation,
                    EXTRACT(YEAR FROM l_shipdate) AS l_year,
                    l_extendedprice * (1 - l_discount) AS volume,
                    lineitem.l_quantity
                FROM supplier, lineitem, orders, customer, nation AS n1, nation AS n2
                WHERE s_suppkey = l_suppkey
                    AND o_orderkey = l_orderkey
                    AND c_custkey = o_custkey
                    AND s_nationkey = n1.n_nationkey
                    AND c_nationkey = n2.n_nationkey
                    AND (
                        (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                        OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
                    )
                    AND l_shipdate BETWEEN CAST('1995-01-01' AS DATE)
                        AND CAST('1996-12-31' AS DATE)
            ) AS shipping
        ) AS rescan
            ON base_query.supp_nation = rescan.supp_nation
            AND base_query.cust_nation = rescan.cust_nation
            AND base_query.l_year = rescan.l_year
        GROUP BY base_query.supp_nation, base_query.cust_nation, base_query.l_year
        HAVING avg(rescan.l_quantity) >= 30
        ORDER BY base_query.supp_nation, base_query.cust_nation, base_query.l_year
    """,
    8: """
        WITH base_query AS (
            SELECT
                o_year,
                SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END)
                    / SUM(volume) AS mkt_share
            FROM (
                SELECT
                    EXTRACT(YEAR FROM o_orderdate) AS o_year,
                    l_extendedprice * (1 - l_discount) AS volume,
                    n2.n_name AS nation,
                    lineitem.l_quantity
                FROM part, supplier, lineitem, orders, customer, nation AS n1, nation AS n2, region
                WHERE p_partkey = l_partkey
                    AND s_suppkey = l_suppkey
                    AND l_orderkey = o_orderkey
                    AND o_custkey = c_custkey
                    AND c_nationkey = n1.n_nationkey
                    AND n1.n_regionkey = r_regionkey
                    AND r_name = 'AMERICA'
                    AND s_nationkey = n2.n_nationkey
                    AND o_orderdate BETWEEN CAST('1995-01-01' AS DATE)
                        AND CAST('1996-12-31' AS DATE)
                    AND p_type = 'ECONOMY ANODIZED STEEL'
            ) AS all_nations
            GROUP BY o_year
        )
        SELECT
            base_query.o_year,
            max(base_query.mkt_share) AS mkt_share
        FROM base_query
        JOIN (
            SELECT
                o_year,
                l_quantity
            FROM (
                SELECT
                    EXTRACT(YEAR FROM o_orderdate) AS o_year,
                    l_extendedprice * (1 - l_discount) AS volume,
                    n2.n_name AS nation,
                    lineitem.l_quantity
                FROM part, supplier, lineitem, orders, customer, nation AS n1, nation AS n2, region
                WHERE p_partkey = l_partkey
                    AND s_suppkey = l_suppkey
                    AND l_orderkey = o_orderkey
                    AND o_custkey = c_custkey
                    AND c_nationkey = n1.n_nationkey
                    AND n1.n_regionkey = r_regionkey
                    AND r_name = 'AMERICA'
                    AND s_nationkey = n2.n_nationkey
                    AND o_orderdate BETWEEN CAST('1995-01-01' AS DATE)
                        AND CAST('1996-12-31' AS DATE)
                    AND p_type = 'ECONOMY ANODIZED STEEL'
            ) AS all_nations
        ) AS rescan
            ON base_query.o_year = rescan.o_year
        GROUP BY base_query.o_year
        HAVING avg(rescan.l_quantity) >= 30
        ORDER BY base_query.o_year
    """,
    9: """
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
        HAVING avg(rescan.l_quantity) >= 30
        ORDER BY base_query.nation, base_query.o_year DESC
    """,
    10: """
        WITH base_query AS (
            SELECT
                c_custkey,
                c_name,
                SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                c_acctbal,
                n_name,
                c_address,
                c_phone,
                c_comment
            FROM customer, orders, lineitem, nation
            WHERE c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND o_orderdate >= CAST('1993-10-01' AS DATE)
                AND o_orderdate < CAST('1994-01-01' AS DATE)
                AND l_returnflag = 'R'
                AND c_nationkey = n_nationkey
            GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
            ORDER BY revenue DESC
            LIMIT 20
        )
        SELECT
            base_query.c_custkey,
            base_query.c_name,
            max(base_query.revenue) AS revenue,
            base_query.c_acctbal,
            base_query.n_name,
            base_query.c_address,
            base_query.c_phone,
            base_query.c_comment
        FROM base_query
        JOIN (
            SELECT
                c_custkey,
                c_name,
                c_acctbal,
                c_phone,
                n_name,
                c_address,
                c_comment,
                lineitem.l_quantity
            FROM customer, orders, lineitem, nation
            WHERE c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND o_orderdate >= CAST('1993-10-01' AS DATE)
                AND o_orderdate < CAST('1994-01-01' AS DATE)
                AND l_returnflag = 'R'
                AND c_nationkey = n_nationkey
        ) AS rescan
            ON base_query.c_custkey = rescan.c_custkey
            AND base_query.c_name = rescan.c_name
            AND base_query.c_acctbal = rescan.c_acctbal
            AND base_query.c_phone = rescan.c_phone
            AND base_query.n_name = rescan.n_name
            AND base_query.c_address = rescan.c_address
            AND base_query.c_comment = rescan.c_comment
        GROUP BY base_query.c_custkey, base_query.c_name, base_query.c_acctbal, base_query.c_phone,
            base_query.n_name, base_query.c_address, base_query.c_comment
        HAVING avg(rescan.l_quantity) >= 30
        ORDER BY revenue DESC
        LIMIT 20
    """,
    12: """
        WITH base_query AS (
            SELECT
                l_shipmode,
                SUM(
                    CASE
                        WHEN o_orderpriority = '1-URGENT'
                            OR o_orderpriority = '2-HIGH'
                            THEN 1
                        ELSE 0
                    END
                ) AS high_line_count,
                SUM(
                    CASE
                        WHEN o_orderpriority <> '1-URGENT'
                            AND o_orderpriority <> '2-HIGH'
                            THEN 1
                        ELSE 0
                    END
                ) AS low_line_count
            FROM orders, lineitem
            WHERE o_orderkey = l_orderkey
                AND l_shipmode IN ('MAIL', 'SHIP')
                AND l_commitdate < l_receiptdate
                AND l_shipdate < l_commitdate
                AND l_receiptdate >= CAST('1994-01-01' AS DATE)
                AND l_receiptdate < CAST('1995-01-01' AS DATE)
            GROUP BY l_shipmode
        )
        SELECT
            base_query.l_shipmode,
            max(base_query.high_line_count) AS high_line_count,
            max(base_query.low_line_count) AS low_line_count
        FROM base_query
        JOIN (
            SELECT
                l_shipmode,
                lineitem.l_quantity
            FROM orders, lineitem
            WHERE o_orderkey = l_orderkey
                AND l_shipmode IN ('MAIL', 'SHIP')
                AND l_commitdate < l_receiptdate
                AND l_shipdate < l_commitdate
                AND l_receiptdate >= CAST('1994-01-01' AS DATE)
                AND l_receiptdate < CAST('1995-01-01' AS DATE)
        ) AS rescan
            ON base_query.l_shipmode = rescan.l_shipmode
        GROUP BY base_query.l_shipmode
        HAVING avg(rescan.l_quantity) >= 30
        ORDER BY base_query.l_shipmode
    """,
    14: """
        WITH base_query AS (
            SELECT
                100.00 * SUM(
                    CASE
                        WHEN p_type LIKE 'PROMO%'
                            THEN l_extendedprice * (1 - l_discount)
                        ELSE 0
                    END
                ) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
            FROM lineitem, part
            WHERE l_partkey = p_partkey
                AND l_shipdate >= CAST('1995-09-01' AS DATE)
                AND l_shipdate < CAST('1995-10-01' AS DATE)
        )
        SELECT max(base_query.promo_revenue) AS promo_revenue
        FROM base_query
        JOIN (
            SELECT
                lineitem.l_quantity
            FROM lineitem, part
            WHERE l_partkey = p_partkey
                AND l_shipdate >= CAST('1995-09-01' AS DATE)
                AND l_shipdate < CAST('1995-10-01' AS DATE)
        ) AS rescan
            ON 1=1
        HAVING avg(rescan.l_quantity) >= 30
    """,
    18: """
        WITH rewrite AS (
            WITH base_query AS (
                SELECT
                    c_name,
                    c_custkey,
                    o_orderkey,
                    o_orderdate,
                    o_totalprice,
                    SUM(l_quantity) AS sum_l_quantity
                FROM customer, orders, lineitem
                WHERE o_orderkey IN (
                    SELECT l_orderkey
                    FROM lineitem
                    GROUP BY l_orderkey
                    HAVING SUM(l_quantity) > 300
                )
                    AND c_custkey = o_custkey
                    AND o_orderkey = l_orderkey
                GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
                ORDER BY o_totalprice DESC, o_orderdate
                LIMIT 100
            )
            SELECT
                base_query.c_name,
                base_query.c_custkey,
                base_query.o_orderkey,
                base_query.o_orderdate,
                base_query.o_totalprice,
                max(base_query.sum_l_quantity) AS sum_l_quantity,
                avg(lineitem.l_quantity) AS policy_1,
                avg(inner_lineitem.l_quantity) AS policy_2
            FROM base_query
            JOIN lineitem
                ON base_query.o_orderkey = lineitem.l_orderkey
            JOIN (
                SELECT l_orderkey
                FROM lineitem
                GROUP BY l_orderkey
                HAVING SUM(l_quantity) > 300
            ) AS in_subquery
                ON base_query.o_orderkey = in_subquery.l_orderkey
            JOIN lineitem AS inner_lineitem
                ON in_subquery.l_orderkey = inner_lineitem.l_orderkey
            GROUP BY base_query.c_name, base_query.c_custkey, base_query.o_orderkey,
                base_query.o_orderdate, base_query.o_totalprice
        )
        SELECT
            c_name,
            c_custkey,
            o_orderkey,
            o_orderdate,
            o_totalprice,
            max(sum_l_quantity) AS sum_l_quantity
        FROM rewrite
        GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
        HAVING max(rewrite.policy_1) >= 30 AND max(rewrite.policy_2) >= 30
        ORDER BY o_totalprice DESC, o_orderdate
    """,
    19: """
        WITH base_query AS (
            SELECT
                sum(l_extendedprice * (1 - l_discount)) AS revenue
            FROM lineitem, part
            WHERE (
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
        SELECT max(base_query.revenue) AS revenue
        FROM base_query
        JOIN (
            SELECT
                lineitem.l_quantity
            FROM lineitem, part
            WHERE (
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
        ) AS rescan
            ON 1=1
        HAVING avg(rescan.l_quantity) >= 30
    """,
}

def _normalize_sql(sql: str) -> str:
    safe_sql = sql.replace("{temp_table_name}", "temp_table_name")
    safe_sql = re.sub(r"query_results_[a-f0-9]{8}", "temp_table_name", safe_sql)
    safe_sql = re.sub(r"LINEAGE_\d+_", "LINEAGE_1_", safe_sql)
    safe_sql = re.sub(r"CAST\((LINEAGE_[^\s)]+) AS VARCHAR\)", r"\1", safe_sql)
    return sqlglot.parse_one(safe_sql, read="duckdb").sql(dialect="duckdb")


@pytest.fixture
def tpch_connections():
    """Create connections with TPC-H data loaded."""
    # Regular DuckDB connections
    dfc_conn = duckdb.connect(":memory:")
    logical_conn = duckdb.connect(":memory:")

    # Set up TPC-H data in each connection
    for conn in [dfc_conn, logical_conn]:
        with contextlib.suppress(Exception):
            conn.execute("INSTALL tpch")
        conn.execute("LOAD tpch")
        conn.execute("CALL dbgen(sf=0.1)")

    yield {
        "dfc": dfc_conn,
        "logical": logical_conn,
    }

    # Cleanup
    for conn in [dfc_conn, logical_conn]:
        with contextlib.suppress(Exception):
            conn.close()


def test_tpch_q01(tpch_connections):
    """Test TPC-H Q1: Pricing Summary Report Query."""
    query = load_tpch_query(1)
    policy = lineitem_policy

    # DFC approach (source of truth)
    dfc_rewriter = SQLRewriter(conn=tpch_connections["dfc"])
    dfc_rewriter.register_policy(policy)
    dfc_results = dfc_rewriter.execute(query).fetchall()
    dfc_rewriter.close()

    # Logical approach
    logical_sql = rewrite_query_logical(query, policy)
    assert _normalize_sql(logical_sql) == _normalize_sql(LOGICAL_EXPECTED_SQL[1])
    logical_results, _ = execute_query_logical(tpch_connections["logical"], query, policy)
    # Compare results
    match, error = compare_results(dfc_results, logical_results)
    assert match, f"Results don't match: {error}"


def test_tpch_q03(tpch_connections):
    """Test TPC-H Q3: Shipping Priority Query."""
    query = load_tpch_query(3)
    policy = lineitem_policy

    # DFC approach (source of truth)
    dfc_rewriter = SQLRewriter(conn=tpch_connections["dfc"])
    dfc_rewriter.register_policy(policy)
    dfc_results = dfc_rewriter.execute(query).fetchall()
    dfc_rewriter.close()

    # Logical approach
    logical_sql = rewrite_query_logical(query, policy)
    assert _normalize_sql(logical_sql) == _normalize_sql(LOGICAL_EXPECTED_SQL[3])
    logical_results, _ = execute_query_logical(tpch_connections["logical"], query, policy)
    # Compare results
    match, error = compare_results(dfc_results, logical_results)
    assert match, f"Results don't match: {error}"


def test_tpch_q04(tpch_connections):
    """Test TPC-H Q4: Order Priority Checking Query."""
    query = load_tpch_query(4)
    policy = lineitem_policy

    # DFC approach (source of truth)
    dfc_rewriter = SQLRewriter(conn=tpch_connections["dfc"])
    dfc_rewriter.register_policy(policy)
    dfc_results = dfc_rewriter.execute(query).fetchall()
    dfc_rewriter.close()

    # Logical approach
    logical_sql = rewrite_query_logical(query, policy)
    assert _normalize_sql(logical_sql) == _normalize_sql(LOGICAL_EXPECTED_SQL[4])
    logical_results, _ = execute_query_logical(tpch_connections["logical"], query, policy)
    # Compare results
    match, error = compare_results(dfc_results, logical_results)
    assert match, f"Results don't match: {error}"


def test_tpch_q05(tpch_connections):
    """Test TPC-H Q5: Local Supplier Volume Query."""
    query = load_tpch_query(5)
    policy = lineitem_policy

    # DFC approach (source of truth)
    dfc_rewriter = SQLRewriter(conn=tpch_connections["dfc"])
    dfc_rewriter.register_policy(policy)
    dfc_results = dfc_rewriter.execute(query).fetchall()
    dfc_rewriter.close()

    # Logical approach
    logical_sql = rewrite_query_logical(query, policy)
    assert _normalize_sql(logical_sql) == _normalize_sql(LOGICAL_EXPECTED_SQL[5])
    logical_results, _ = execute_query_logical(tpch_connections["logical"], query, policy)
    # Compare results
    match, error = compare_results(dfc_results, logical_results)
    assert match, f"Results don't match: {error}"


def test_tpch_q06(tpch_connections):
    """Test TPC-H Q6: Forecasting Revenue Change Query."""
    query = load_tpch_query(6)
    policy = lineitem_policy

    # DFC approach (source of truth)
    dfc_rewriter = SQLRewriter(conn=tpch_connections["dfc"])
    dfc_rewriter.register_policy(policy)
    dfc_results = dfc_rewriter.execute(query).fetchall()
    dfc_rewriter.close()

    # Logical approach
    logical_sql = rewrite_query_logical(query, policy)
    assert _normalize_sql(logical_sql) == _normalize_sql(LOGICAL_EXPECTED_SQL[6])
    logical_results, _ = execute_query_logical(tpch_connections["logical"], query, policy)
    # Compare results
    match, error = compare_results(dfc_results, logical_results)
    assert match, f"Results don't match: {error}"


def test_tpch_q07(tpch_connections):
    """Test TPC-H Q7: Volume Shipping Query."""
    query = load_tpch_query(7)
    policy = lineitem_policy

    # DFC approach (source of truth)
    dfc_rewriter = SQLRewriter(conn=tpch_connections["dfc"])
    dfc_rewriter.register_policy(policy)
    dfc_results = dfc_rewriter.execute(query).fetchall()
    dfc_rewriter.close()

    # Logical approach
    logical_sql = rewrite_query_logical(query, policy)
    assert _normalize_sql(logical_sql) == _normalize_sql(LOGICAL_EXPECTED_SQL[7])
    logical_results, _ = execute_query_logical(tpch_connections["logical"], query, policy)
    # Compare results
    match, error = compare_results(dfc_results, logical_results)
    assert match, f"Results don't match: {error}"


def test_tpch_q08(tpch_connections):
    """Test TPC-H Q8: National Market Share Query."""
    query = load_tpch_query(8)
    policy = lineitem_policy

    # DFC approach (source of truth)
    dfc_rewriter = SQLRewriter(conn=tpch_connections["dfc"])
    dfc_rewriter.register_policy(policy)
    dfc_results = dfc_rewriter.execute(query).fetchall()
    dfc_rewriter.close()

    # Logical approach
    logical_sql = rewrite_query_logical(query, policy)
    assert _normalize_sql(logical_sql) == _normalize_sql(LOGICAL_EXPECTED_SQL[8])
    logical_results, _ = execute_query_logical(tpch_connections["logical"], query, policy)
    # Compare results
    match, error = compare_results(dfc_results, logical_results)
    assert match, f"Results don't match: {error}"


def test_tpch_q09(tpch_connections):
    """Test TPC-H Q9: Product Type Profit Measure Query."""
    query = load_tpch_query(9)
    policy = lineitem_policy

    # DFC approach (source of truth)
    dfc_rewriter = SQLRewriter(conn=tpch_connections["dfc"])
    dfc_rewriter.register_policy(policy)
    dfc_results = dfc_rewriter.execute(query).fetchall()
    dfc_rewriter.close()

    # Logical approach
    logical_sql = rewrite_query_logical(query, policy)
    assert _normalize_sql(logical_sql) == _normalize_sql(LOGICAL_EXPECTED_SQL[9])
    logical_results, _ = execute_query_logical(tpch_connections["logical"], query, policy)
    # Compare results
    match, error = compare_results(dfc_results, logical_results)
    assert match, f"Results don't match: {error}"


def test_tpch_q10(tpch_connections):
    """Test TPC-H Q10: Returned Item Reporting Query."""
    query = load_tpch_query(10)
    policy = lineitem_policy

    # DFC approach (source of truth)
    dfc_rewriter = SQLRewriter(conn=tpch_connections["dfc"])
    dfc_rewriter.register_policy(policy)
    dfc_results = dfc_rewriter.execute(query).fetchall()
    dfc_rewriter.close()

    # Logical approach
    logical_sql = rewrite_query_logical(query, policy)
    assert _normalize_sql(logical_sql) == _normalize_sql(LOGICAL_EXPECTED_SQL[10])
    logical_results, _ = execute_query_logical(tpch_connections["logical"], query, policy)
    # Compare results
    match, error = compare_results(dfc_results, logical_results)
    assert match, f"Results don't match: {error}"


def test_tpch_q12(tpch_connections):
    """Test TPC-H Q12: Shipping Modes and Order Priority Query."""
    query = load_tpch_query(12)
    policy = lineitem_policy

    # DFC approach (source of truth)
    dfc_rewriter = SQLRewriter(conn=tpch_connections["dfc"])
    dfc_rewriter.register_policy(policy)
    dfc_results = dfc_rewriter.execute(query).fetchall()
    dfc_rewriter.close()

    # Logical approach
    logical_sql = rewrite_query_logical(query, policy)
    assert _normalize_sql(logical_sql) == _normalize_sql(LOGICAL_EXPECTED_SQL[12])
    logical_results, _ = execute_query_logical(tpch_connections["logical"], query, policy)
    # Compare results
    match, error = compare_results(dfc_results, logical_results)
    assert match, f"Results don't match: {error}"


def test_tpch_q14(tpch_connections):
    """Test TPC-H Q14: Promotion Effect Query."""
    query = load_tpch_query(14)
    policy = lineitem_policy

    # DFC approach (source of truth)
    dfc_rewriter = SQLRewriter(conn=tpch_connections["dfc"])
    dfc_rewriter.register_policy(policy)
    dfc_results = dfc_rewriter.execute(query).fetchall()
    dfc_rewriter.close()

    # Logical approach
    logical_sql = rewrite_query_logical(query, policy)
    assert _normalize_sql(logical_sql) == _normalize_sql(LOGICAL_EXPECTED_SQL[14])
    logical_results, _ = execute_query_logical(tpch_connections["logical"], query, policy)
    # Compare results
    match, error = compare_results(dfc_results, logical_results)
    assert match, f"Results don't match: {error}"


def test_tpch_q18(tpch_connections):
    """Test TPC-H Q18: Large Volume Customer Query."""
    query = load_tpch_query(18)
    policy = lineitem_policy

    # DFC approach (source of truth)
    dfc_rewriter = SQLRewriter(conn=tpch_connections["dfc"])
    dfc_rewriter.register_policy(policy)
    dfc_results = dfc_rewriter.execute(query).fetchall()
    dfc_rewriter.close()

    # Logical approach
    logical_sql = rewrite_query_logical(query, policy)
    assert _normalize_sql(logical_sql) == _normalize_sql(LOGICAL_EXPECTED_SQL[18])
    logical_results, _ = execute_query_logical(tpch_connections["logical"], query, policy)
    # Compare results
    match, error = compare_results(dfc_results, logical_results)
    assert match, f"Results don't match: {error}"


def test_tpch_q19(tpch_connections):
    """Test TPC-H Q19: Discounted Revenue Query."""
    query = load_tpch_query(19)
    policy = lineitem_policy

    # DFC approach (source of truth)
    dfc_rewriter = SQLRewriter(conn=tpch_connections["dfc"])
    dfc_rewriter.register_policy(policy)
    dfc_results = dfc_rewriter.execute(query).fetchall()
    dfc_rewriter.close()

    # Logical approach
    logical_sql = rewrite_query_logical(query, policy)
    assert _normalize_sql(logical_sql) == _normalize_sql(LOGICAL_EXPECTED_SQL[19])
    logical_results, _ = execute_query_logical(tpch_connections["logical"], query, policy)
    # Compare results
    match, error = compare_results(dfc_results, logical_results)
    assert match, f"Results don't match: {error}"
