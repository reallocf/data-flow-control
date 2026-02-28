"""Exact SQL tests for the TPC-H self-join alias-policy experiment."""

from vldb_experiments.strategies.tpch_self_join_policy_queries import (
    get_cached_tpch_q01_self_join_1phase_optimized_query,
    get_cached_tpch_q01_self_join_1phase_query,
)

EXPECTED_1PHASE_1 = """SELECT
  l1.l_returnflag,
  l1.l_linestatus,
  SUM(l1.l_quantity) AS sum_qty,
  SUM(l1.l_extendedprice) AS sum_base_price,
  SUM(l1.l_extendedprice * (
    1 - l1.l_discount
  )) AS sum_disc_price,
  SUM(l1.l_extendedprice * (
    1 - l1.l_discount
  ) * (
    1 + l1.l_tax
  )) AS sum_charge,
  AVG(l1.l_quantity) AS avg_qty,
  AVG(l1.l_extendedprice) AS avg_price,
  AVG(l1.l_discount) AS avg_disc,
  COUNT(*) AS count_order
FROM
  lineitem l1,
  lineitem l2
WHERE
  l1.l_shipdate <= CAST('1998-09-02' AS DATE)
  AND l1.rowid = l2.rowid
GROUP BY
  l1.l_returnflag,
  l1.l_linestatus
HAVING
  (
    MAX(l1.l_shipdate) = MAX(l2.l_shipdate)
  )
  AND (
    MAX(l2.l_shipdate) = MAX(l1.l_shipdate)
  )
ORDER BY
  l1.l_returnflag,
  l1.l_linestatus"""

EXPECTED_1PHASE_OPTIMIZED_1 = """SELECT
  l1.l_returnflag,
  l1.l_linestatus,
  SUM(l1.l_quantity) AS sum_qty,
  SUM(l1.l_extendedprice) AS sum_base_price,
  SUM(l1.l_extendedprice * (
    1 - l1.l_discount
  )) AS sum_disc_price,
  SUM(l1.l_extendedprice * (
    1 - l1.l_discount
  ) * (
    1 + l1.l_tax
  )) AS sum_charge,
  AVG(l1.l_quantity) AS avg_qty,
  AVG(l1.l_extendedprice) AS avg_price,
  AVG(l1.l_discount) AS avg_disc,
  COUNT(*) AS count_order
FROM
  lineitem l1,
  lineitem l2
WHERE
  l1.l_shipdate <= CAST('1998-09-02' AS DATE)
  AND l1.rowid = l2.rowid
GROUP BY
  l1.l_returnflag,
  l1.l_linestatus
HAVING
  (
    MAX(l1.l_shipdate) = MAX(l2.l_shipdate)
  )
ORDER BY
  l1.l_returnflag,
  l1.l_linestatus"""

EXPECTED_1PHASE_10 = """SELECT
  l1.l_returnflag,
  l1.l_linestatus,
  SUM(l1.l_quantity) AS sum_qty,
  SUM(l1.l_extendedprice) AS sum_base_price,
  SUM(l1.l_extendedprice * (
    1 - l1.l_discount
  )) AS sum_disc_price,
  SUM(l1.l_extendedprice * (
    1 - l1.l_discount
  ) * (
    1 + l1.l_tax
  )) AS sum_charge,
  AVG(l1.l_quantity) AS avg_qty,
  AVG(l1.l_extendedprice) AS avg_price,
  AVG(l1.l_discount) AS avg_disc,
  COUNT(*) AS count_order
FROM
  lineitem l1,
  lineitem l2,
  lineitem l3,
  lineitem l4,
  lineitem l5,
  lineitem l6,
  lineitem l7,
  lineitem l8,
  lineitem l9,
  lineitem l10,
  lineitem l11
WHERE
  l1.l_shipdate <= CAST('1998-09-02' AS DATE)
  AND l1.rowid = l2.rowid
  AND l1.rowid = l3.rowid
  AND l1.rowid = l4.rowid
  AND l1.rowid = l5.rowid
  AND l1.rowid = l6.rowid
  AND l1.rowid = l7.rowid
  AND l1.rowid = l8.rowid
  AND l1.rowid = l9.rowid
  AND l1.rowid = l10.rowid
  AND l1.rowid = l11.rowid
GROUP BY
  l1.l_returnflag,
  l1.l_linestatus
HAVING
  (
    MAX(l1.l_shipdate) = MAX(l2.l_shipdate)
  )
  AND (
    MAX(l1.l_shipdate) = MAX(l3.l_shipdate)
  )
  AND (
    MAX(l1.l_shipdate) = MAX(l4.l_shipdate)
  )
  AND (
    MAX(l1.l_shipdate) = MAX(l5.l_shipdate)
  )
  AND (
    MAX(l1.l_shipdate) = MAX(l6.l_shipdate)
  )
  AND (
    MAX(l1.l_shipdate) = MAX(l7.l_shipdate)
  )
  AND (
    MAX(l1.l_shipdate) = MAX(l8.l_shipdate)
  )
  AND (
    MAX(l1.l_shipdate) = MAX(l9.l_shipdate)
  )
  AND (
    MAX(l1.l_shipdate) = MAX(l10.l_shipdate)
  )
  AND (
    MAX(l1.l_shipdate) = MAX(l11.l_shipdate)
  )
  AND (
    MAX(l2.l_shipdate) = MAX(l1.l_shipdate)
  )
  AND (
    MAX(l2.l_shipdate) = MAX(l3.l_shipdate)
  )
  AND (
    MAX(l2.l_shipdate) = MAX(l4.l_shipdate)
  )
  AND (
    MAX(l2.l_shipdate) = MAX(l5.l_shipdate)
  )
  AND (
    MAX(l2.l_shipdate) = MAX(l6.l_shipdate)
  )
  AND (
    MAX(l2.l_shipdate) = MAX(l7.l_shipdate)
  )
  AND (
    MAX(l2.l_shipdate) = MAX(l8.l_shipdate)
  )
  AND (
    MAX(l2.l_shipdate) = MAX(l9.l_shipdate)
  )
  AND (
    MAX(l2.l_shipdate) = MAX(l10.l_shipdate)
  )
  AND (
    MAX(l2.l_shipdate) = MAX(l11.l_shipdate)
  )
  AND (
    MAX(l3.l_shipdate) = MAX(l1.l_shipdate)
  )
  AND (
    MAX(l3.l_shipdate) = MAX(l2.l_shipdate)
  )
  AND (
    MAX(l3.l_shipdate) = MAX(l4.l_shipdate)
  )
  AND (
    MAX(l3.l_shipdate) = MAX(l5.l_shipdate)
  )
  AND (
    MAX(l3.l_shipdate) = MAX(l6.l_shipdate)
  )
  AND (
    MAX(l3.l_shipdate) = MAX(l7.l_shipdate)
  )
  AND (
    MAX(l3.l_shipdate) = MAX(l8.l_shipdate)
  )
  AND (
    MAX(l3.l_shipdate) = MAX(l9.l_shipdate)
  )
  AND (
    MAX(l3.l_shipdate) = MAX(l10.l_shipdate)
  )
  AND (
    MAX(l3.l_shipdate) = MAX(l11.l_shipdate)
  )
  AND (
    MAX(l4.l_shipdate) = MAX(l1.l_shipdate)
  )
  AND (
    MAX(l4.l_shipdate) = MAX(l2.l_shipdate)
  )
  AND (
    MAX(l4.l_shipdate) = MAX(l3.l_shipdate)
  )
  AND (
    MAX(l4.l_shipdate) = MAX(l5.l_shipdate)
  )
  AND (
    MAX(l4.l_shipdate) = MAX(l6.l_shipdate)
  )
  AND (
    MAX(l4.l_shipdate) = MAX(l7.l_shipdate)
  )
  AND (
    MAX(l4.l_shipdate) = MAX(l8.l_shipdate)
  )
  AND (
    MAX(l4.l_shipdate) = MAX(l9.l_shipdate)
  )
  AND (
    MAX(l4.l_shipdate) = MAX(l10.l_shipdate)
  )
  AND (
    MAX(l4.l_shipdate) = MAX(l11.l_shipdate)
  )
  AND (
    MAX(l5.l_shipdate) = MAX(l1.l_shipdate)
  )
  AND (
    MAX(l5.l_shipdate) = MAX(l2.l_shipdate)
  )
  AND (
    MAX(l5.l_shipdate) = MAX(l3.l_shipdate)
  )
  AND (
    MAX(l5.l_shipdate) = MAX(l4.l_shipdate)
  )
  AND (
    MAX(l5.l_shipdate) = MAX(l6.l_shipdate)
  )
  AND (
    MAX(l5.l_shipdate) = MAX(l7.l_shipdate)
  )
  AND (
    MAX(l5.l_shipdate) = MAX(l8.l_shipdate)
  )
  AND (
    MAX(l5.l_shipdate) = MAX(l9.l_shipdate)
  )
  AND (
    MAX(l5.l_shipdate) = MAX(l10.l_shipdate)
  )
  AND (
    MAX(l5.l_shipdate) = MAX(l11.l_shipdate)
  )
  AND (
    MAX(l6.l_shipdate) = MAX(l1.l_shipdate)
  )
  AND (
    MAX(l6.l_shipdate) = MAX(l2.l_shipdate)
  )
  AND (
    MAX(l6.l_shipdate) = MAX(l3.l_shipdate)
  )
  AND (
    MAX(l6.l_shipdate) = MAX(l4.l_shipdate)
  )
  AND (
    MAX(l6.l_shipdate) = MAX(l5.l_shipdate)
  )
  AND (
    MAX(l6.l_shipdate) = MAX(l7.l_shipdate)
  )
  AND (
    MAX(l6.l_shipdate) = MAX(l8.l_shipdate)
  )
  AND (
    MAX(l6.l_shipdate) = MAX(l9.l_shipdate)
  )
  AND (
    MAX(l6.l_shipdate) = MAX(l10.l_shipdate)
  )
  AND (
    MAX(l6.l_shipdate) = MAX(l11.l_shipdate)
  )
  AND (
    MAX(l7.l_shipdate) = MAX(l1.l_shipdate)
  )
  AND (
    MAX(l7.l_shipdate) = MAX(l2.l_shipdate)
  )
  AND (
    MAX(l7.l_shipdate) = MAX(l3.l_shipdate)
  )
  AND (
    MAX(l7.l_shipdate) = MAX(l4.l_shipdate)
  )
  AND (
    MAX(l7.l_shipdate) = MAX(l5.l_shipdate)
  )
  AND (
    MAX(l7.l_shipdate) = MAX(l6.l_shipdate)
  )
  AND (
    MAX(l7.l_shipdate) = MAX(l8.l_shipdate)
  )
  AND (
    MAX(l7.l_shipdate) = MAX(l9.l_shipdate)
  )
  AND (
    MAX(l7.l_shipdate) = MAX(l10.l_shipdate)
  )
  AND (
    MAX(l7.l_shipdate) = MAX(l11.l_shipdate)
  )
  AND (
    MAX(l8.l_shipdate) = MAX(l1.l_shipdate)
  )
  AND (
    MAX(l8.l_shipdate) = MAX(l2.l_shipdate)
  )
  AND (
    MAX(l8.l_shipdate) = MAX(l3.l_shipdate)
  )
  AND (
    MAX(l8.l_shipdate) = MAX(l4.l_shipdate)
  )
  AND (
    MAX(l8.l_shipdate) = MAX(l5.l_shipdate)
  )
  AND (
    MAX(l8.l_shipdate) = MAX(l6.l_shipdate)
  )
  AND (
    MAX(l8.l_shipdate) = MAX(l7.l_shipdate)
  )
  AND (
    MAX(l8.l_shipdate) = MAX(l9.l_shipdate)
  )
  AND (
    MAX(l8.l_shipdate) = MAX(l10.l_shipdate)
  )
  AND (
    MAX(l8.l_shipdate) = MAX(l11.l_shipdate)
  )
  AND (
    MAX(l9.l_shipdate) = MAX(l1.l_shipdate)
  )
  AND (
    MAX(l9.l_shipdate) = MAX(l2.l_shipdate)
  )
  AND (
    MAX(l9.l_shipdate) = MAX(l3.l_shipdate)
  )
  AND (
    MAX(l9.l_shipdate) = MAX(l4.l_shipdate)
  )
  AND (
    MAX(l9.l_shipdate) = MAX(l5.l_shipdate)
  )
  AND (
    MAX(l9.l_shipdate) = MAX(l6.l_shipdate)
  )
  AND (
    MAX(l9.l_shipdate) = MAX(l7.l_shipdate)
  )
  AND (
    MAX(l9.l_shipdate) = MAX(l8.l_shipdate)
  )
  AND (
    MAX(l9.l_shipdate) = MAX(l10.l_shipdate)
  )
  AND (
    MAX(l9.l_shipdate) = MAX(l11.l_shipdate)
  )
  AND (
    MAX(l10.l_shipdate) = MAX(l1.l_shipdate)
  )
  AND (
    MAX(l10.l_shipdate) = MAX(l2.l_shipdate)
  )
  AND (
    MAX(l10.l_shipdate) = MAX(l3.l_shipdate)
  )
  AND (
    MAX(l10.l_shipdate) = MAX(l4.l_shipdate)
  )
  AND (
    MAX(l10.l_shipdate) = MAX(l5.l_shipdate)
  )
  AND (
    MAX(l10.l_shipdate) = MAX(l6.l_shipdate)
  )
  AND (
    MAX(l10.l_shipdate) = MAX(l7.l_shipdate)
  )
  AND (
    MAX(l10.l_shipdate) = MAX(l8.l_shipdate)
  )
  AND (
    MAX(l10.l_shipdate) = MAX(l9.l_shipdate)
  )
  AND (
    MAX(l10.l_shipdate) = MAX(l11.l_shipdate)
  )
  AND (
    MAX(l11.l_shipdate) = MAX(l1.l_shipdate)
  )
  AND (
    MAX(l11.l_shipdate) = MAX(l2.l_shipdate)
  )
  AND (
    MAX(l11.l_shipdate) = MAX(l3.l_shipdate)
  )
  AND (
    MAX(l11.l_shipdate) = MAX(l4.l_shipdate)
  )
  AND (
    MAX(l11.l_shipdate) = MAX(l5.l_shipdate)
  )
  AND (
    MAX(l11.l_shipdate) = MAX(l6.l_shipdate)
  )
  AND (
    MAX(l11.l_shipdate) = MAX(l7.l_shipdate)
  )
  AND (
    MAX(l11.l_shipdate) = MAX(l8.l_shipdate)
  )
  AND (
    MAX(l11.l_shipdate) = MAX(l9.l_shipdate)
  )
  AND (
    MAX(l11.l_shipdate) = MAX(l10.l_shipdate)
  )
ORDER BY
  l1.l_returnflag,
  l1.l_linestatus"""

EXPECTED_1PHASE_OPTIMIZED_10 = """SELECT
  l1.l_returnflag,
  l1.l_linestatus,
  SUM(l1.l_quantity) AS sum_qty,
  SUM(l1.l_extendedprice) AS sum_base_price,
  SUM(l1.l_extendedprice * (
    1 - l1.l_discount
  )) AS sum_disc_price,
  SUM(l1.l_extendedprice * (
    1 - l1.l_discount
  ) * (
    1 + l1.l_tax
  )) AS sum_charge,
  AVG(l1.l_quantity) AS avg_qty,
  AVG(l1.l_extendedprice) AS avg_price,
  AVG(l1.l_discount) AS avg_disc,
  COUNT(*) AS count_order
FROM
  lineitem l1,
  lineitem l2,
  lineitem l3,
  lineitem l4,
  lineitem l5,
  lineitem l6,
  lineitem l7,
  lineitem l8,
  lineitem l9,
  lineitem l10,
  lineitem l11
WHERE
  l1.l_shipdate <= CAST('1998-09-02' AS DATE)
  AND l1.rowid = l2.rowid
  AND l1.rowid = l3.rowid
  AND l1.rowid = l4.rowid
  AND l1.rowid = l5.rowid
  AND l1.rowid = l6.rowid
  AND l1.rowid = l7.rowid
  AND l1.rowid = l8.rowid
  AND l1.rowid = l9.rowid
  AND l1.rowid = l10.rowid
  AND l1.rowid = l11.rowid
GROUP BY
  l1.l_returnflag,
  l1.l_linestatus
HAVING
  (
    MAX(l1.l_shipdate) = MAX(l2.l_shipdate)
  )
  AND (
    MAX(l1.l_shipdate) = MAX(l3.l_shipdate)
  )
  AND (
    MAX(l1.l_shipdate) = MAX(l4.l_shipdate)
  )
  AND (
    MAX(l1.l_shipdate) = MAX(l5.l_shipdate)
  )
  AND (
    MAX(l1.l_shipdate) = MAX(l6.l_shipdate)
  )
  AND (
    MAX(l1.l_shipdate) = MAX(l7.l_shipdate)
  )
  AND (
    MAX(l1.l_shipdate) = MAX(l8.l_shipdate)
  )
  AND (
    MAX(l1.l_shipdate) = MAX(l9.l_shipdate)
  )
  AND (
    MAX(l1.l_shipdate) = MAX(l10.l_shipdate)
  )
  AND (
    MAX(l1.l_shipdate) = MAX(l11.l_shipdate)
  )
ORDER BY
  l1.l_returnflag,
  l1.l_linestatus"""


def test_tpch_q01_self_join_1phase_query_one_self_join():
    assert get_cached_tpch_q01_self_join_1phase_query(1) == EXPECTED_1PHASE_1


def test_tpch_q01_self_join_1phase_optimized_query_one_self_join():
    assert (
        get_cached_tpch_q01_self_join_1phase_optimized_query(1)
        == EXPECTED_1PHASE_OPTIMIZED_1
    )


def test_tpch_q01_self_join_1phase_query_ten_self_joins():
    assert get_cached_tpch_q01_self_join_1phase_query(10) == EXPECTED_1PHASE_10


def test_tpch_q01_self_join_1phase_optimized_query_ten_self_joins():
    assert (
        get_cached_tpch_q01_self_join_1phase_optimized_query(10)
        == EXPECTED_1PHASE_OPTIMIZED_10
    )
