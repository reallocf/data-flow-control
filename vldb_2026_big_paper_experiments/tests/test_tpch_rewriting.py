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
from vldb_experiments.correctness import compare_results_exact
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
            SUM(l_quantity) AS sum_qty,
            SUM(l_extendedprice) AS sum_base_price,
            SUM(l_extendedprice * (
              1 - l_discount
            )) AS sum_disc_price,
            SUM(l_extendedprice * (
              1 - l_discount
            ) * (
              1 + l_tax
            )) AS sum_charge,
            AVG(l_quantity) AS avg_qty,
            AVG(l_extendedprice) AS avg_price,
            AVG(l_discount) AS avg_disc,
            COUNT(*) AS count_order
          FROM lineitem
          WHERE
            l_shipdate <= CAST('1998-09-02' AS DATE)
          GROUP BY
            l_returnflag,
            l_linestatus
        )
        SELECT
          base_query.l_returnflag,
          base_query.l_linestatus,
          MAX(base_query.sum_qty) AS sum_qty,
          MAX(base_query.sum_base_price) AS sum_base_price,
          MAX(base_query.sum_disc_price) AS sum_disc_price,
          MAX(base_query.sum_charge) AS sum_charge,
          MAX(base_query.avg_qty) AS avg_qty,
          MAX(base_query.avg_price) AS avg_price,
          MAX(base_query.avg_disc) AS avg_disc,
          MAX(base_query.count_order) AS count_order
        FROM base_query, lineitem
        WHERE
          lineitem.l_shipdate <= CAST('1998-09-02' AS DATE)
          AND base_query.l_returnflag = lineitem.l_returnflag
          AND base_query.l_linestatus = lineitem.l_linestatus
        GROUP BY
          base_query.l_returnflag,
          base_query.l_linestatus
        HAVING
          AVG(lineitem.l_quantity) >= 30
        ORDER BY
          base_query.l_returnflag,
          base_query.l_linestatus
    """,
    3: """
        WITH base_query AS (
          SELECT
            l_orderkey,
            SUM(l_extendedprice * (
              1 - l_discount
            )) AS revenue,
            o_orderdate,
            o_shippriority
          FROM customer, orders, lineitem
          WHERE
            c_mktsegment = 'BUILDING'
            AND c_custkey = o_custkey
            AND l_orderkey = o_orderkey
            AND o_orderdate < CAST('1995-03-15' AS DATE)
            AND l_shipdate > CAST('1995-03-15' AS DATE)
          GROUP BY
            l_orderkey,
            o_orderdate,
            o_shippriority
          ORDER BY
            revenue DESC,
            o_orderdate
          LIMIT 10
        )
        SELECT
          base_query.l_orderkey,
          MAX(base_query.revenue) AS revenue,
          base_query.o_orderdate,
          base_query.o_shippriority
        FROM base_query, customer, orders, lineitem
        WHERE
          customer.c_mktsegment = 'BUILDING'
          AND customer.c_custkey = orders.o_custkey
          AND lineitem.l_orderkey = orders.o_orderkey
          AND orders.o_orderdate < CAST('1995-03-15' AS DATE)
          AND lineitem.l_shipdate > CAST('1995-03-15' AS DATE)
          AND base_query.l_orderkey = lineitem.l_orderkey
          AND base_query.o_orderdate = orders.o_orderdate
          AND base_query.o_shippriority = orders.o_shippriority
        GROUP BY
          base_query.l_orderkey,
          base_query.o_orderdate,
          base_query.o_shippriority
        HAVING
          AVG(lineitem.l_quantity) >= 30
    """,
    4: """
        WITH base_query AS (
          SELECT
            o_orderpriority,
            COUNT(*) AS order_count
          FROM orders
          WHERE
            o_orderdate >= CAST('1993-07-01' AS DATE)
            AND o_orderdate < CAST('1993-10-01' AS DATE)
            AND EXISTS(
              SELECT
                *
              FROM lineitem
              WHERE
                l_orderkey = o_orderkey AND l_commitdate < l_receiptdate
            )
          GROUP BY
            o_orderpriority
          ORDER BY
            o_orderpriority
        ), rewrite AS (
          SELECT
            base_query.o_orderpriority AS o_orderpriority,
            base_query.order_count AS order_count,
            orders.o_orderkey,
            AVG(lineitem.l_quantity) AS policy_1
          FROM base_query
          JOIN orders
            ON base_query.o_orderpriority = orders.o_orderpriority
          JOIN lineitem
            ON l_orderkey = o_orderkey
          WHERE
            l_commitdate < l_receiptdate
          GROUP BY
            base_query.o_orderpriority,
            base_query.order_count,
            orders.o_orderkey
        )
        SELECT
          rewrite.o_orderpriority AS o_orderpriority,
          rewrite.order_count AS order_count
        FROM rewrite
        GROUP BY
          rewrite.o_orderpriority,
          rewrite.order_count
        HAVING
          MAX(rewrite.policy_1) >= 30
        ORDER BY
          o_orderpriority
    """,
    5: """
        WITH base_query AS (
          SELECT
            n_name,
            SUM(l_extendedprice * (
              1 - l_discount
            )) AS revenue
          FROM customer, orders, lineitem, supplier, nation, region
          WHERE
            c_custkey = o_custkey
            AND l_orderkey = o_orderkey
            AND l_suppkey = s_suppkey
            AND c_nationkey = s_nationkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'ASIA'
            AND o_orderdate >= CAST('1994-01-01' AS DATE)
            AND o_orderdate < CAST('1995-01-01' AS DATE)
          GROUP BY
            n_name
        )
        SELECT
          base_query.n_name,
          MAX(base_query.revenue) AS revenue
        FROM base_query, customer, orders, lineitem, supplier, nation, region
        WHERE
          customer.c_custkey = orders.o_custkey
          AND lineitem.l_orderkey = orders.o_orderkey
          AND lineitem.l_suppkey = supplier.s_suppkey
          AND customer.c_nationkey = supplier.s_nationkey
          AND supplier.s_nationkey = nation.n_nationkey
          AND nation.n_regionkey = region.r_regionkey
          AND region.r_name = 'ASIA'
          AND orders.o_orderdate >= CAST('1994-01-01' AS DATE)
          AND orders.o_orderdate < CAST('1995-01-01' AS DATE)
          AND base_query.n_name = nation.n_name
        GROUP BY
          base_query.n_name
        HAVING
          AVG(lineitem.l_quantity) >= 30
        ORDER BY
          revenue DESC
    """,
    6: """
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
          AVG(lineitem.l_quantity) >= 30
    """,
    7: """
        WITH base_query AS (
          SELECT
            n1.n_name AS supp_nation,
            n2.n_name AS cust_nation,
            EXTRACT(YEAR FROM l_shipdate) AS l_year,
            SUM(l_extendedprice * (
              1 - l_discount
            )) AS revenue
          FROM supplier, lineitem, orders, customer, nation AS n1, nation AS n2
          WHERE
            s_suppkey = l_suppkey
            AND o_orderkey = l_orderkey
            AND c_custkey = o_custkey
            AND s_nationkey = n1.n_nationkey
            AND c_nationkey = n2.n_nationkey
            AND (
              (
                n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY'
              )
              OR (
                n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'
              )
            )
            AND l_shipdate BETWEEN CAST('1995-01-01' AS DATE) AND CAST('1996-12-31' AS DATE)
          GROUP BY
            n1.n_name,
            n2.n_name,
            EXTRACT(YEAR FROM l_shipdate)
        )
        SELECT
          base_query.supp_nation,
          base_query.cust_nation,
          base_query.l_year,
          MAX(base_query.revenue) AS revenue
        FROM base_query, supplier, lineitem, orders, customer, nation AS n1, nation AS n2
        WHERE
          supplier.s_suppkey = lineitem.l_suppkey
          AND orders.o_orderkey = lineitem.l_orderkey
          AND customer.c_custkey = orders.o_custkey
          AND supplier.s_nationkey = n1.n_nationkey
          AND customer.c_nationkey = n2.n_nationkey
          AND (
            (
              n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY'
            )
            OR (
              n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'
            )
          )
          AND lineitem.l_shipdate BETWEEN CAST('1995-01-01' AS DATE) AND CAST('1996-12-31' AS DATE)
          AND base_query.supp_nation = n1.n_name
          AND base_query.cust_nation = n2.n_name
          AND base_query.l_year = EXTRACT(YEAR FROM lineitem.l_shipdate)
        GROUP BY
          base_query.supp_nation,
          base_query.cust_nation,
          base_query.l_year
        HAVING
          AVG(lineitem.l_quantity) >= 30
        ORDER BY
          base_query.supp_nation,
          base_query.cust_nation,
          base_query.l_year
    """,
    8: """
        WITH base_query AS (
          SELECT
            EXTRACT(YEAR FROM o_orderdate) AS o_year,
            SUM(
              CASE
                WHEN n2.n_name = 'BRAZIL'
                THEN l_extendedprice * (
                  1 - l_discount
                )
                ELSE 0
              END
            ) / SUM(l_extendedprice * (
              1 - l_discount
            )) AS mkt_share
          FROM part, supplier, lineitem, orders, customer, nation AS n1, nation AS n2, region
          WHERE
            p_partkey = l_partkey
            AND s_suppkey = l_suppkey
            AND l_orderkey = o_orderkey
            AND o_custkey = c_custkey
            AND c_nationkey = n1.n_nationkey
            AND n1.n_regionkey = r_regionkey
            AND r_name = 'AMERICA'
            AND s_nationkey = n2.n_nationkey
            AND o_orderdate BETWEEN CAST('1995-01-01' AS DATE) AND CAST('1996-12-31' AS DATE)
            AND p_type = 'ECONOMY ANODIZED STEEL'
          GROUP BY
            EXTRACT(YEAR FROM o_orderdate)
        )
        SELECT
          base_query.o_year,
          MAX(base_query.mkt_share) AS mkt_share
        FROM base_query, part, supplier, lineitem, orders, customer, nation AS n1, nation AS n2, region
        WHERE
          part.p_partkey = lineitem.l_partkey
          AND supplier.s_suppkey = lineitem.l_suppkey
          AND lineitem.l_orderkey = orders.o_orderkey
          AND orders.o_custkey = customer.c_custkey
          AND customer.c_nationkey = n1.n_nationkey
          AND n1.n_regionkey = region.r_regionkey
          AND region.r_name = 'AMERICA'
          AND supplier.s_nationkey = n2.n_nationkey
          AND orders.o_orderdate BETWEEN CAST('1995-01-01' AS DATE) AND CAST('1996-12-31' AS DATE)
          AND part.p_type = 'ECONOMY ANODIZED STEEL'
          AND base_query.o_year = EXTRACT(YEAR FROM orders.o_orderdate)
        GROUP BY
          base_query.o_year
        HAVING
          AVG(lineitem.l_quantity) >= 30
        ORDER BY
          base_query.o_year
    """,
    9: """
        WITH base_query AS (
          SELECT
            n_name AS nation,
            EXTRACT(YEAR FROM o_orderdate) AS o_year,
            SUM(l_extendedprice * (
              1 - l_discount
            ) - ps_supplycost * l_quantity) AS sum_profit
          FROM part, supplier, lineitem, partsupp, orders, nation
          WHERE
            s_suppkey = l_suppkey
            AND ps_suppkey = l_suppkey
            AND ps_partkey = l_partkey
            AND p_partkey = l_partkey
            AND o_orderkey = l_orderkey
            AND s_nationkey = n_nationkey
            AND p_name LIKE '%green%'
          GROUP BY
            n_name,
            EXTRACT(YEAR FROM o_orderdate)
        )
        SELECT
          base_query.nation,
          base_query.o_year,
          MAX(base_query.sum_profit) AS sum_profit
        FROM base_query, part, supplier, lineitem, partsupp, orders, nation
        WHERE
          supplier.s_suppkey = lineitem.l_suppkey
          AND partsupp.ps_suppkey = lineitem.l_suppkey
          AND partsupp.ps_partkey = lineitem.l_partkey
          AND part.p_partkey = lineitem.l_partkey
          AND orders.o_orderkey = lineitem.l_orderkey
          AND supplier.s_nationkey = nation.n_nationkey
          AND part.p_name LIKE '%green%'
          AND base_query.nation = nation.n_name
          AND base_query.o_year = EXTRACT(YEAR FROM orders.o_orderdate)
        GROUP BY
          base_query.nation,
          base_query.o_year
        HAVING
          AVG(lineitem.l_quantity) >= 30
        ORDER BY
          base_query.nation,
          base_query.o_year DESC
    """,
    10: """
        WITH base_query AS (
          SELECT
            c_custkey,
            c_name,
            SUM(l_extendedprice * (
              1 - l_discount
            )) AS revenue,
            c_acctbal,
            n_name,
            c_address,
            c_phone,
            c_comment
          FROM customer, orders, lineitem, nation
          WHERE
            c_custkey = o_custkey
            AND l_orderkey = o_orderkey
            AND o_orderdate >= CAST('1993-10-01' AS DATE)
            AND o_orderdate < CAST('1994-01-01' AS DATE)
            AND l_returnflag = 'R'
            AND c_nationkey = n_nationkey
          GROUP BY
            c_custkey,
            c_name,
            c_acctbal,
            c_phone,
            n_name,
            c_address,
            c_comment
          ORDER BY
            revenue DESC
          LIMIT 20
        )
        SELECT
          base_query.c_custkey,
          base_query.c_name,
          MAX(base_query.revenue) AS revenue,
          base_query.c_acctbal,
          base_query.n_name,
          base_query.c_address,
          base_query.c_phone,
          base_query.c_comment
        FROM base_query, customer, orders, lineitem, nation
        WHERE
          customer.c_custkey = orders.o_custkey
          AND lineitem.l_orderkey = orders.o_orderkey
          AND orders.o_orderdate >= CAST('1993-10-01' AS DATE)
          AND orders.o_orderdate < CAST('1994-01-01' AS DATE)
          AND lineitem.l_returnflag = 'R'
          AND customer.c_nationkey = nation.n_nationkey
          AND base_query.c_custkey = customer.c_custkey
          AND base_query.c_name = customer.c_name
          AND base_query.c_acctbal = customer.c_acctbal
          AND base_query.c_phone = customer.c_phone
          AND base_query.n_name = nation.n_name
          AND base_query.c_address = customer.c_address
          AND base_query.c_comment = customer.c_comment
        GROUP BY
          base_query.c_custkey,
          base_query.c_name,
          base_query.c_acctbal,
          base_query.c_phone,
          base_query.n_name,
          base_query.c_address,
          base_query.c_comment
        HAVING
          AVG(lineitem.l_quantity) >= 30
    """,
    12: """
        WITH base_query AS (
          SELECT
            l_shipmode,
            SUM(
              CASE
                WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
                THEN 1
                ELSE 0
              END
            ) AS high_line_count,
            SUM(
              CASE
                WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
                THEN 1
                ELSE 0
              END
            ) AS low_line_count
          FROM orders, lineitem
          WHERE
            o_orderkey = l_orderkey
            AND l_shipmode IN ('MAIL', 'SHIP')
            AND l_commitdate < l_receiptdate
            AND l_shipdate < l_commitdate
            AND l_receiptdate >= CAST('1994-01-01' AS DATE)
            AND l_receiptdate < CAST('1995-01-01' AS DATE)
          GROUP BY
            l_shipmode
        )
        SELECT
          base_query.l_shipmode,
          MAX(base_query.high_line_count) AS high_line_count,
          MAX(base_query.low_line_count) AS low_line_count
        FROM base_query, orders, lineitem
        WHERE
          orders.o_orderkey = lineitem.l_orderkey
          AND lineitem.l_shipmode IN ('MAIL', 'SHIP')
          AND lineitem.l_commitdate < lineitem.l_receiptdate
          AND lineitem.l_shipdate < lineitem.l_commitdate
          AND lineitem.l_receiptdate >= CAST('1994-01-01' AS DATE)
          AND lineitem.l_receiptdate < CAST('1995-01-01' AS DATE)
          AND base_query.l_shipmode = lineitem.l_shipmode
        GROUP BY
          base_query.l_shipmode
        HAVING
          AVG(lineitem.l_quantity) >= 30
        ORDER BY
          base_query.l_shipmode
    """,
    14: """
        WITH base_query AS (
          SELECT
            100.00 * SUM(
              CASE
                WHEN p_type LIKE 'PROMO%'
                THEN l_extendedprice * (
                  1 - l_discount
                )
                ELSE 0
              END
            ) / SUM(l_extendedprice * (
              1 - l_discount
            )) AS promo_revenue
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
          AVG(lineitem.l_quantity) >= 30
    """,
    18: """
        WITH base_query AS (
          SELECT
            c_name,
            c_custkey,
            o_orderkey,
            o_orderdate,
            o_totalprice,
            SUM(l_quantity) AS sum_l_quantity
          FROM customer, orders, lineitem
          WHERE
            o_orderkey IN (
              SELECT
                l_orderkey
              FROM lineitem
              GROUP BY
                l_orderkey
              HAVING
                SUM(l_quantity) > 300
            )
            AND c_custkey = o_custkey
            AND o_orderkey = l_orderkey
          GROUP BY
            c_name,
            c_custkey,
            o_orderkey,
            o_orderdate,
            o_totalprice
          ORDER BY
            o_totalprice DESC,
            o_orderdate
          LIMIT 100
        ), rewrite AS (
          SELECT
            base_query.c_name AS c_name,
            base_query.c_custkey AS c_custkey,
            base_query.o_orderkey AS o_orderkey,
            base_query.o_orderdate AS o_orderdate,
            base_query.o_totalprice AS o_totalprice,
            MAX(base_query.sum_l_quantity) AS sum_l_quantity,
            AVG(lineitem.l_quantity) AS policy_1,
            AVG(inner_lineitem.l_quantity) AS policy_2
          FROM base_query
          JOIN lineitem
            ON base_query.o_orderkey = lineitem.l_orderkey
          JOIN (
            SELECT
              l_orderkey
            FROM lineitem
            GROUP BY
              l_orderkey
            HAVING
              SUM(l_quantity) > 300
          ) AS in_subquery
            ON base_query.o_orderkey = in_subquery.l_orderkey
          JOIN lineitem AS inner_lineitem
            ON in_subquery.l_orderkey = inner_lineitem.l_orderkey
          GROUP BY
            base_query.c_name,
            base_query.c_custkey,
            base_query.o_orderkey,
            base_query.o_orderdate,
            base_query.o_totalprice
        )
        SELECT
          rewrite.c_name AS c_name,
          rewrite.c_custkey AS c_custkey,
          rewrite.o_orderkey AS o_orderkey,
          rewrite.o_orderdate AS o_orderdate,
          rewrite.o_totalprice AS o_totalprice,
          MAX(rewrite.sum_l_quantity) AS sum_l_quantity
        FROM rewrite
        GROUP BY
          rewrite.c_name,
          rewrite.c_custkey,
          rewrite.o_orderkey,
          rewrite.o_orderdate,
          rewrite.o_totalprice
        HAVING
          (
            MAX(rewrite.policy_1) >= 30
          ) AND (
            MAX(rewrite.policy_2) >= 30
          )
        ORDER BY
          o_totalprice DESC,
          o_orderdate
    """,
    19: """
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
          AVG(lineitem.l_quantity) >= 30
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
    match, error = compare_results_exact(dfc_results, logical_results)
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
    match, error = compare_results_exact(dfc_results, logical_results)
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
    match, error = compare_results_exact(dfc_results, logical_results)
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
    match, error = compare_results_exact(dfc_results, logical_results)
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
    match, error = compare_results_exact(dfc_results, logical_results)
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
    match, error = compare_results_exact(dfc_results, logical_results)
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
    match, error = compare_results_exact(dfc_results, logical_results)
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
    match, error = compare_results_exact(dfc_results, logical_results)
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
    match, error = compare_results_exact(dfc_results, logical_results)
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
    match, error = compare_results_exact(dfc_results, logical_results)
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
    match, error = compare_results_exact(dfc_results, logical_results)
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
    match, error = compare_results_exact(dfc_results, logical_results)
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
    match, error = compare_results_exact(dfc_results, logical_results)
    assert match, f"Results don't match: {error}"
