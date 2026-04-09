"""Two-phase SQL rewriter tests for TPC-H benchmark queries.

Excluded queries (non-monotonic):
- Q02, Q11, Q13, Q15, Q16, Q17, Q20, Q21, Q22
"""

from collections import Counter
import pathlib

import pytest

from sql_rewriter import DFCPolicy, Resolution, SQLRewriter

LINEITEM_POLICY = DFCPolicy(
    sources=["lineitem"],
    constraint="max(lineitem.l_quantity) >= 1",
    on_fail=Resolution.REMOVE,
)

TPCH_QUERIES = [1, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 18, 19]


EXPECTED_TWO_PHASE_SQL: dict[int, str] = {
    1: """WITH base_query AS (
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
  ORDER BY
    l_returnflag,
    l_linestatus
), policy_eval AS (
  SELECT
    l_returnflag AS l_returnflag,
    l_linestatus AS l_linestatus
  FROM lineitem
  WHERE
    l_shipdate <= CAST('1998-09-02' AS DATE)
  GROUP BY
    l_returnflag,
    l_linestatus
  HAVING
    (
      MAX(lineitem.l_quantity) >= 1
    )
)
SELECT
  base_query.*
FROM base_query
JOIN policy_eval
  ON base_query.l_returnflag = policy_eval.l_returnflag
  AND base_query.l_linestatus = policy_eval.l_linestatus""",
    3: """WITH base_query AS (
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
), policy_eval AS (
  SELECT
    l_orderkey AS l_orderkey,
    o_orderdate AS o_orderdate,
    o_shippriority AS o_shippriority,
    MAX(lineitem.l_quantity) AS dfc
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
), cte AS (
  SELECT
    base_query.*,
    policy_eval.dfc AS dfc
  FROM base_query
  JOIN policy_eval
    ON base_query.l_orderkey = policy_eval.l_orderkey
    AND base_query.o_orderdate = policy_eval.o_orderdate
    AND base_query.o_shippriority = policy_eval.o_shippriority
  ORDER BY
    revenue DESC,
    base_query.o_orderdate
  LIMIT 10
)
SELECT
  l_orderkey,
  revenue,
  o_orderdate,
  o_shippriority
FROM cte
WHERE
  dfc >= 1""",
    4: """WITH base_query AS (
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
), policy_eval AS (
  SELECT
    o_orderpriority AS o_orderpriority
  FROM orders
  INNER JOIN (
    SELECT
      l_orderkey,
      MAX(l_quantity) AS agg_0
    FROM lineitem
    WHERE
      l_commitdate < l_receiptdate
    GROUP BY
      l_orderkey
  ) AS exists_subquery
    ON o_orderkey = exists_subquery.l_orderkey
  WHERE
    o_orderdate >= CAST('1993-07-01' AS DATE)
    AND o_orderdate < CAST('1993-10-01' AS DATE)
  GROUP BY
    o_orderpriority
  HAVING
    (
      MAX(exists_subquery.agg_0) >= 1
    )
)
SELECT
  base_query.*
FROM base_query
JOIN policy_eval
  ON base_query.o_orderpriority = policy_eval.o_orderpriority""",
    5: """WITH base_query AS (
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
  ORDER BY
    revenue DESC
), policy_eval AS (
  SELECT
    n_name AS n_name
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
  HAVING
    (
      MAX(lineitem.l_quantity) >= 1
    )
)
SELECT
  base_query.*
FROM base_query
JOIN policy_eval
  ON base_query.n_name = policy_eval.n_name""",
    6: """WITH base_query AS (
  SELECT
    SUM(l_extendedprice * l_discount) AS revenue
  FROM lineitem
  WHERE
    l_shipdate >= CAST('1994-01-01' AS DATE)
    AND l_shipdate < CAST('1995-01-01' AS DATE)
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24
), policy_eval AS (
  SELECT
    1 AS __dfc_two_phase_key
  FROM lineitem
  WHERE
    l_shipdate >= CAST('1994-01-01' AS DATE)
    AND l_shipdate < CAST('1995-01-01' AS DATE)
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24
  HAVING
    (
      MAX(lineitem.l_quantity) >= 1
    )
)
SELECT
  base_query.*
FROM base_query
CROSS JOIN policy_eval""",
    7: """WITH base_query AS (
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
      l_extendedprice * (
        1 - l_discount
      ) AS volume
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
  ) AS shipping
  GROUP BY
    supp_nation,
    cust_nation,
    l_year
  ORDER BY
    supp_nation,
    cust_nation,
    l_year
), policy_eval AS (
  SELECT
    supp_nation AS supp_nation,
    cust_nation AS cust_nation,
    l_year AS l_year
  FROM (
    SELECT
      n1.n_name AS supp_nation,
      n2.n_name AS cust_nation,
      EXTRACT(YEAR FROM l_shipdate) AS l_year,
      l_extendedprice * (
        1 - l_discount
      ) AS volume,
      lineitem.l_quantity
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
  ) AS shipping
  GROUP BY
    supp_nation,
    cust_nation,
    l_year
  HAVING
    (
      MAX(shipping.l_quantity) >= 1
    )
)
SELECT
  base_query.*
FROM base_query
JOIN policy_eval
  ON base_query.supp_nation = policy_eval.supp_nation
  AND base_query.cust_nation = policy_eval.cust_nation
  AND base_query.l_year = policy_eval.l_year""",
    8: """WITH base_query AS (
  SELECT
    o_year,
    SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share
  FROM (
    SELECT
      EXTRACT(YEAR FROM o_orderdate) AS o_year,
      l_extendedprice * (
        1 - l_discount
      ) AS volume,
      n2.n_name AS nation
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
  ) AS all_nations
  GROUP BY
    o_year
  ORDER BY
    o_year
), policy_eval AS (
  SELECT
    o_year AS o_year
  FROM (
    SELECT
      EXTRACT(YEAR FROM o_orderdate) AS o_year,
      l_extendedprice * (
        1 - l_discount
      ) AS volume,
      n2.n_name AS nation,
      lineitem.l_quantity
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
  ) AS all_nations
  GROUP BY
    o_year
  HAVING
    (
      MAX(all_nations.l_quantity) >= 1
    )
)
SELECT
  base_query.*
FROM base_query
JOIN policy_eval
  ON base_query.o_year = policy_eval.o_year""",
    9: """WITH base_query AS (
  SELECT
    nation,
    o_year,
    SUM(amount) AS sum_profit
  FROM (
    SELECT
      n_name AS nation,
      EXTRACT(YEAR FROM o_orderdate) AS o_year,
      l_extendedprice * (
        1 - l_discount
      ) - ps_supplycost * l_quantity AS amount
    FROM part, supplier, lineitem, partsupp, orders, nation
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
), policy_eval AS (
  SELECT
    nation AS nation,
    o_year AS o_year
  FROM (
    SELECT
      n_name AS nation,
      EXTRACT(YEAR FROM o_orderdate) AS o_year,
      l_extendedprice * (
        1 - l_discount
      ) - ps_supplycost * l_quantity AS amount,
      lineitem.l_quantity
    FROM part, supplier, lineitem, partsupp, orders, nation
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
  HAVING
    (
      MAX(profit.l_quantity) >= 1
    )
)
SELECT
  base_query.*
FROM base_query
JOIN policy_eval
  ON base_query.nation = policy_eval.nation AND base_query.o_year = policy_eval.o_year""",
    10: """WITH base_query AS (
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
), policy_eval AS (
  SELECT
    c_custkey AS c_custkey,
    c_name AS c_name,
    c_acctbal AS c_acctbal,
    c_phone AS c_phone,
    n_name AS n_name,
    c_address AS c_address,
    c_comment AS c_comment,
    MAX(lineitem.l_quantity) AS dfc
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
), cte AS (
  SELECT
    base_query.*,
    policy_eval.dfc AS dfc
  FROM base_query
  JOIN policy_eval
    ON base_query.c_custkey = policy_eval.c_custkey
    AND base_query.c_name = policy_eval.c_name
    AND base_query.c_acctbal = policy_eval.c_acctbal
    AND base_query.c_phone = policy_eval.c_phone
    AND base_query.n_name = policy_eval.n_name
    AND base_query.c_address = policy_eval.c_address
    AND base_query.c_comment = policy_eval.c_comment
  ORDER BY
    revenue DESC
  LIMIT 20
)
SELECT
  c_custkey,
  c_name,
  revenue,
  c_acctbal,
  n_name,
  c_address,
  c_phone,
  c_comment
FROM cte
WHERE
  dfc >= 1""",
    12: """WITH base_query AS (
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
  ORDER BY
    l_shipmode
), policy_eval AS (
  SELECT
    l_shipmode AS l_shipmode
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
  HAVING
    (
      MAX(lineitem.l_quantity) >= 1
    )
)
SELECT
  base_query.*
FROM base_query
JOIN policy_eval
  ON base_query.l_shipmode = policy_eval.l_shipmode""",
    14: """WITH base_query AS (
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
), policy_eval AS (
  SELECT
    1 AS __dfc_two_phase_key
  FROM lineitem, part
  WHERE
    l_partkey = p_partkey
    AND l_shipdate >= CAST('1995-09-01' AS DATE)
    AND l_shipdate < CAST('1995-10-01' AS DATE)
  HAVING
    (
      MAX(lineitem.l_quantity) >= 1
    )
)
SELECT
  base_query.*
FROM base_query
CROSS JOIN policy_eval""",
    18: """WITH base_query AS (
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
), policy_eval AS (
  SELECT
    c_name AS c_name,
    c_custkey AS c_custkey,
    o_orderkey AS o_orderkey,
    o_orderdate AS o_orderdate,
    o_totalprice AS o_totalprice,
    MAX(lineitem.l_quantity) AS dfc,
    MAX(in_subquery.dfc2) AS dfc2
  FROM customer
  INNER JOIN orders
    ON customer.c_custkey = orders.o_custkey
  INNER JOIN lineitem
    ON orders.o_orderkey = lineitem.l_orderkey
  INNER JOIN (
    SELECT
      l_orderkey,
      MAX(l_quantity) AS dfc2
    FROM lineitem
    GROUP BY
      l_orderkey
    HAVING
      SUM(l_quantity) > 300
  ) AS in_subquery
    ON o_orderkey = in_subquery.l_orderkey
  GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
), cte AS (
  SELECT
    base_query.*,
    policy_eval.dfc AS dfc,
    policy_eval.dfc2 AS dfc2
  FROM base_query
  JOIN policy_eval
    ON base_query.c_name = policy_eval.c_name
    AND base_query.c_custkey = policy_eval.c_custkey
    AND base_query.o_orderkey = policy_eval.o_orderkey
    AND base_query.o_orderdate = policy_eval.o_orderdate
    AND base_query.o_totalprice = policy_eval.o_totalprice
  ORDER BY
    base_query.o_totalprice DESC,
    base_query.o_orderdate
  LIMIT 100
)
SELECT
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  sum_l_quantity
FROM cte
WHERE
  dfc >= 1 AND dfc2 >= 1""",
    19: """WITH base_query AS (
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
), policy_eval AS (
  SELECT
    1 AS __dfc_two_phase_key
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
  HAVING
    (
      MAX(lineitem.l_quantity) >= 1
    )
)
SELECT
  base_query.*
FROM base_query
CROSS JOIN policy_eval""",
}


class TwoPhaseTPCHHarness:
    def __init__(self) -> None:
        self.two_phase = SQLRewriter()
        self.standard = SQLRewriter()
        self._setup(self.two_phase)
        self._setup(self.standard)

    def _setup(self, rewriter: SQLRewriter) -> None:
        rewriter.execute("INSTALL tpch")
        rewriter.execute("LOAD tpch")
        rewriter.execute("CALL dbgen(sf=0.1)")
        rewriter.register_policy(LINEITEM_POLICY)

    def close(self) -> None:
        self.two_phase.close()
        self.standard.close()


@pytest.fixture
def tpch_harness():
    harness = TwoPhaseTPCHHarness()
    yield harness
    harness.close()


def load_tpch_query(query_num: int) -> str:
    benchmarks_dir = pathlib.Path(__file__).parent.parent / "benchmarks" / "tpch" / "queries"
    query_file = benchmarks_dir / f"q{query_num:02d}.sql"
    return query_file.read_text()


def _multiset(rows: list[tuple]) -> Counter[str]:
    return Counter(repr(row) for row in rows)


@pytest.mark.parametrize("query_num", TPCH_QUERIES)
def test_tpch_two_phase_matches_standard_dfc(tpch_harness, query_num: int):
    query = load_tpch_query(query_num)

    two_phase_sql = tpch_harness.two_phase.transform_query(query, use_two_phase=True)
    standard_sql = tpch_harness.standard.transform_query(query, use_two_phase=False)

    assert two_phase_sql == EXPECTED_TWO_PHASE_SQL[query_num], (
        f"Unexpected two-phase rewrite for Q{query_num:02d}.\n\n"
        f"Expected:\n{EXPECTED_TWO_PHASE_SQL[query_num]}\n\n"
        f"Actual:\n{two_phase_sql}"
    )

    two_phase_rows = tpch_harness.two_phase.conn.execute(two_phase_sql).fetchall()
    standard_rows = tpch_harness.standard.conn.execute(standard_sql).fetchall()

    assert _multiset(two_phase_rows) == _multiset(standard_rows), (
        f"Two-phase output diverged from standard DFC for Q{query_num:02d}.\n\n"
        f"Two-phase SQL:\n{two_phase_sql}\n\n"
        f"Standard SQL:\n{standard_sql}\n\n"
        f"Two-phase rows: {two_phase_rows}\n"
        f"Standard rows: {standard_rows}"
    )
