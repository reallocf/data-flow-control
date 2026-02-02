"""Tests for SQL rewriter with TPC-H benchmark queries.

Excluded queries (non-monotonic):
- Q02: Minimum Cost Supplier Query
- Q11: Important Stock Identification Query
- Q13: Customer Distribution Query
- Q15: Top Supplier Query
- Q16: Parts/Supplier Relationship Query
- Q17: Small-Quantity-Order Revenue Query
- Q20: Potential Part Promotion Query
- Q21: Suppliers Who Kept Orders Waiting Query
- Q22: Global Sales Opportunity Query

These queries are excluded because they contain non-monotonic operations that are
not compatible with Data Flow Control policies (for now).
"""

import pathlib

import pytest
from sqlglot import parse_one

from sql_rewriter import DFCPolicy, Resolution, SQLRewriter

lineitem_policy = DFCPolicy(
    source="lineitem",
    constraint="max(lineitem.l_quantity) >= 1",
    on_fail=Resolution.REMOVE,
)

@pytest.fixture
def tpch_rewriter():
    """Create a SQLRewriter instance with TPC-H data loaded."""
    rewriter = SQLRewriter()

    rewriter.execute("INSTALL tpch")
    rewriter.execute("LOAD tpch")
    rewriter.execute("CALL dbgen(sf=0.1)")

    yield rewriter

    rewriter.close()


def load_tpch_query(query_num: int) -> str:
    """Load a TPC-H query from the benchmarks directory."""
    benchmarks_dir = pathlib.Path(__file__).parent.parent / "benchmarks" / "tpch" / "queries"
    query_file = benchmarks_dir / f"q{query_num:02d}.sql"

    if not query_file.exists():
        raise FileNotFoundError(f"TPC-H query {query_num} not found at {query_file}")

    return query_file.read_text()


def test_tpch_q01(tpch_rewriter):
    """Test TPC-H Q1: Pricing Summary Report Query."""
    query = load_tpch_query(1)
    tpch_rewriter.register_policy(lineitem_policy)
    transformed = tpch_rewriter.transform_query(query)
    expected = """SELECT
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
HAVING
  (
    MAX(lineitem.l_quantity) >= 1
  )
ORDER BY
  l_returnflag,
  l_linestatus"""
    assert transformed == expected
    result = tpch_rewriter.conn.execute(transformed).fetchall()
    assert result is not None


def test_tpch_q03(tpch_rewriter):
    """Test TPC-H Q3: Shipping Priority Query."""
    query = load_tpch_query(3)
    tpch_rewriter.register_policy(lineitem_policy)
    transformed = tpch_rewriter.transform_query(query)
    expected = """WITH cte AS (
  SELECT
    l_orderkey,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS revenue,
    o_orderdate,
    o_shippriority,
    MAX(l_quantity) AS dfc
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
  l_orderkey,
  revenue,
  o_orderdate,
  o_shippriority
FROM cte
WHERE
  dfc >= 1"""
    expected_normalized = parse_one(expected, read="duckdb").sql(pretty=True, dialect="duckdb")
    transformed_normalized = parse_one(transformed, read="duckdb").sql(pretty=True, dialect="duckdb")

    assert transformed_normalized == expected_normalized, (
        f"Transformed query does not match expected.\n"
        f"Expected:\n{expected_normalized}\n\n"
        f"Actual:\n{transformed_normalized}"
    )

    # Execute the transformed query directly (don't transform again via execute())
    # Use conn.execute directly since the query is already transformed
    result = tpch_rewriter.conn.execute(transformed).fetchall()
    assert result is not None


def test_tpch_q04(tpch_rewriter):
    """Test TPC-H Q4: Order Priority Checking Query."""
    query = load_tpch_query(4)
    tpch_rewriter.register_policy(lineitem_policy)
    transformed = tpch_rewriter.transform_query(query)
    expected = """SELECT
  o_orderpriority,
  COUNT(*) AS order_count
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
ORDER BY
  o_orderpriority"""
    # Normalize both queries for comparison (handles formatting differences)
    expected_normalized = parse_one(expected, read="duckdb").sql(pretty=True, dialect="duckdb")
    transformed_normalized = parse_one(transformed, read="duckdb").sql(pretty=True, dialect="duckdb")
    assert transformed_normalized == expected_normalized, (
        f"Transformed query does not match expected.\n"
        f"Expected:\n{expected_normalized}\n\n"
        f"Actual:\n{transformed_normalized}"
    )


def test_tpch_q05(tpch_rewriter):
    """Test TPC-H Q5: Local Supplier Volume Query."""
    query = load_tpch_query(5)
    tpch_rewriter.register_policy(lineitem_policy)
    transformed = tpch_rewriter.transform_query(query)
    expected = """SELECT
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
HAVING
  (
    MAX(lineitem.l_quantity) >= 1
  )
ORDER BY
  revenue DESC"""
    assert transformed == expected
    result = tpch_rewriter.conn.execute(transformed).fetchall()
    assert result is not None


def test_tpch_q06(tpch_rewriter):
    """Test TPC-H Q6: Forecasting Revenue Change Query."""
    query = load_tpch_query(6)
    tpch_rewriter.register_policy(lineitem_policy)
    transformed = tpch_rewriter.transform_query(query)
    expected = """SELECT
  SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE
  l_shipdate >= CAST('1994-01-01' AS DATE)
  AND l_shipdate < CAST('1995-01-01' AS DATE)
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24
HAVING
  (
    MAX(lineitem.l_quantity) >= 1
  )"""
    assert transformed == expected
    result = tpch_rewriter.conn.execute(transformed).fetchall()
    assert result is not None


def test_tpch_q07(tpch_rewriter):
    """Test TPC-H Q7: Volume Shipping Query."""
    query = load_tpch_query(7)
    tpch_rewriter.register_policy(lineitem_policy)
    transformed = tpch_rewriter.transform_query(query)
    expected = """SELECT
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
ORDER BY
  supp_nation,
  cust_nation,
  l_year"""
    assert transformed == expected
    result = tpch_rewriter.conn.execute(transformed).fetchall()
    assert result is not None


def test_tpch_q08(tpch_rewriter):
    """Test TPC-H Q8: National Market Share Query."""
    query = load_tpch_query(8)
    tpch_rewriter.register_policy(lineitem_policy)
    transformed = tpch_rewriter.transform_query(query)
    expected = """SELECT
  o_year,
  SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share
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
ORDER BY
  o_year"""
    assert transformed == expected
    result = tpch_rewriter.conn.execute(transformed).fetchall()
    assert result is not None


def test_tpch_q09(tpch_rewriter):
    """Test TPC-H Q9: Product Type Profit Measure Query."""
    query = load_tpch_query(9)
    tpch_rewriter.register_policy(lineitem_policy)
    transformed = tpch_rewriter.transform_query(query)
    expected = """SELECT
  nation,
  o_year,
  SUM(amount) AS sum_profit
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
ORDER BY
  nation,
  o_year DESC"""
    assert transformed == expected
    result = tpch_rewriter.conn.execute(transformed).fetchall()
    assert result is not None


def test_tpch_q10(tpch_rewriter):
    """Test TPC-H Q10: Returned Item Reporting Query."""
    query = load_tpch_query(10)
    tpch_rewriter.register_policy(lineitem_policy)
    transformed = tpch_rewriter.transform_query(query)
    expected = """WITH cte AS (
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
    c_comment,
    MAX(l_quantity) AS dfc
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
  dfc >= 1"""
    expected_normalized = parse_one(expected, read="duckdb").sql(pretty=True, dialect="duckdb")
    transformed_normalized = parse_one(transformed, read="duckdb").sql(pretty=True, dialect="duckdb")

    assert transformed_normalized == expected_normalized, (
        f"Transformed query does not match expected.\n"
        f"Expected:\n{expected_normalized}\n\n"
        f"Actual:\n{transformed_normalized}"
    )

    # Execute the transformed query directly (don't transform again via execute())
    # Use conn.execute directly since the query is already transformed
    result = tpch_rewriter.conn.execute(transformed).fetchall()
    assert result is not None


def test_tpch_q12(tpch_rewriter):
    """Test TPC-H Q12: Shipping Modes and Order Priority Query."""
    query = load_tpch_query(12)
    tpch_rewriter.register_policy(lineitem_policy)
    transformed = tpch_rewriter.transform_query(query)
    expected = """SELECT
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
HAVING
  (
    MAX(lineitem.l_quantity) >= 1
  )
ORDER BY
  l_shipmode"""
    assert transformed == expected
    result = tpch_rewriter.conn.execute(transformed).fetchall()
    assert result is not None


def test_tpch_q14(tpch_rewriter):
    """Test TPC-H Q14: Promotion Effect Query."""
    query = load_tpch_query(14)
    tpch_rewriter.register_policy(lineitem_policy)
    transformed = tpch_rewriter.transform_query(query)
    expected = """SELECT
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
HAVING
  (
    MAX(lineitem.l_quantity) >= 1
  )"""
    assert transformed == expected
    result = tpch_rewriter.conn.execute(transformed).fetchall()
    assert result is not None


def test_tpch_q18(tpch_rewriter):
    """Test TPC-H Q18: Large Volume Customer Query."""
    query = load_tpch_query(18)
    tpch_rewriter.register_policy(lineitem_policy)
    transformed = tpch_rewriter.transform_query(query)
    expected = """WITH cte AS (
  SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    SUM(l_quantity) AS sum_l_quantity,
    MAX(l_quantity) AS dfc,
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
  ORDER BY
    o_totalprice DESC,
    o_orderdate
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
  dfc >= 1 AND dfc2 >= 1"""
    expected_normalized = parse_one(expected, read="duckdb").sql(pretty=True, dialect="duckdb")
    transformed_normalized = parse_one(transformed, read="duckdb").sql(pretty=True, dialect="duckdb")
    assert transformed_normalized == expected_normalized, (
        f"Transformed query does not match expected.\n"
        f"Expected:\n{expected_normalized}\n\n"
        f"Actual:\n{transformed_normalized}"
    )
    # Execute the transformed query directly (don't transform again via execute())
    # Use conn.execute directly since the query is already transformed
    result = tpch_rewriter.conn.execute(transformed).fetchall()
    assert result is not None


def test_tpch_q19(tpch_rewriter):
    """Test TPC-H Q19: Discounted Revenue Query."""
    query = load_tpch_query(19)
    tpch_rewriter.register_policy(lineitem_policy)
    transformed = tpch_rewriter.transform_query(query)
    expected = """SELECT
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
HAVING
  (
    MAX(lineitem.l_quantity) >= 1
  )"""
    assert transformed == expected
    result = tpch_rewriter.conn.execute(transformed).fetchall()
    assert result is not None
