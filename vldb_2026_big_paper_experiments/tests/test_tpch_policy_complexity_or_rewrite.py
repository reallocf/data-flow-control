"""Tests for TPC-H policy complexity / OR rewrite outputs."""

import contextlib

import duckdb
from sql_rewriter import SQLRewriter

from vldb_experiments.baselines.logical_baseline import rewrite_query_logical_multi
from vldb_experiments.strategies.tpch_policy_complexity_strategy import (
    build_tpch_q01_complexity_policy,
)
from vldb_experiments.strategies.tpch_policy_many_ors_strategy import (
    build_tpch_q01_or_policy,
)
from vldb_experiments.strategies.tpch_strategy import load_tpch_query


def _build_tpch_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    with contextlib.suppress(Exception):
        conn.execute("INSTALL tpch")
    conn.execute("LOAD tpch")
    conn.execute("CALL dbgen(sf=0.1)")
    return conn


COMPLEXITY_100_DFC_SQL = """SELECT
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
    MAX(
      (
        (
          (
            (
              (
                (
                  (
                    (
                      lineitem.l_quantity
                    ) + (
                      lineitem.l_extendedprice
                    )
                  ) + (
                    (
                      lineitem.l_discount
                    ) + (
                      lineitem.l_tax
                    )
                  )
                ) + (
                  (
                    (
                      lineitem.l_linenumber
                    ) + (
                      lineitem.l_orderkey
                    )
                  ) + (
                    (
                      lineitem.l_partkey
                    ) + (
                      lineitem.l_suppkey
                    )
                  )
                )
              ) + (
                (
                  (
                    (
                      lineitem.l_quantity
                    ) + (
                      lineitem.l_extendedprice
                    )
                  ) + (
                    (
                      lineitem.l_discount
                    ) + (
                      lineitem.l_tax
                    )
                  )
                ) + (
                  (
                    (
                      lineitem.l_linenumber
                    ) + (
                      lineitem.l_orderkey
                    )
                  ) + (
                    (
                      lineitem.l_partkey
                    ) + (
                      lineitem.l_suppkey
                    )
                  )
                )
              )
            ) + (
              (
                (
                  (
                    (
                      lineitem.l_quantity
                    ) + (
                      lineitem.l_extendedprice
                    )
                  ) + (
                    (
                      lineitem.l_discount
                    ) + (
                      lineitem.l_tax
                    )
                  )
                ) + (
                  (
                    (
                      lineitem.l_linenumber
                    ) + (
                      lineitem.l_orderkey
                    )
                  ) + (
                    (
                      lineitem.l_partkey
                    ) + (
                      lineitem.l_suppkey
                    )
                  )
                )
              ) + (
                (
                  (
                    (
                      lineitem.l_quantity
                    ) + (
                      lineitem.l_extendedprice
                    )
                  ) + (
                    (
                      lineitem.l_discount
                    ) + (
                      lineitem.l_tax
                    )
                  )
                ) + (
                  (
                    (
                      lineitem.l_linenumber
                    ) + (
                      lineitem.l_orderkey
                    )
                  ) + (
                    (
                      lineitem.l_partkey
                    ) + (
                      lineitem.l_suppkey
                    )
                  )
                )
              )
            )
          ) + (
            (
              (
                (
                  (
                    (
                      lineitem.l_quantity
                    ) + (
                      lineitem.l_extendedprice
                    )
                  ) + (
                    (
                      lineitem.l_discount
                    ) + (
                      lineitem.l_tax
                    )
                  )
                ) + (
                  (
                    (
                      lineitem.l_linenumber
                    ) + (
                      lineitem.l_orderkey
                    )
                  ) + (
                    (
                      lineitem.l_partkey
                    ) + (
                      lineitem.l_suppkey
                    )
                  )
                )
              ) + (
                (
                  (
                    (
                      lineitem.l_quantity
                    ) + (
                      lineitem.l_extendedprice
                    )
                  ) + (
                    (
                      lineitem.l_discount
                    ) + (
                      lineitem.l_tax
                    )
                  )
                ) + (
                  (
                    (
                      lineitem.l_linenumber
                    ) + (
                      lineitem.l_orderkey
                    )
                  ) + (
                    (
                      lineitem.l_partkey
                    ) + (
                      lineitem.l_suppkey
                    )
                  )
                )
              )
            ) + (
              (
                (
                  (
                    (
                      lineitem.l_quantity
                    ) + (
                      lineitem.l_extendedprice
                    )
                  ) + (
                    (
                      lineitem.l_discount
                    ) + (
                      lineitem.l_tax
                    )
                  )
                ) + (
                  (
                    (
                      lineitem.l_linenumber
                    ) + (
                      lineitem.l_orderkey
                    )
                  ) + (
                    (
                      lineitem.l_partkey
                    ) + (
                      lineitem.l_suppkey
                    )
                  )
                )
              ) + (
                (
                  (
                    (
                      lineitem.l_quantity
                    ) + (
                      lineitem.l_extendedprice
                    )
                  ) + (
                    (
                      lineitem.l_discount
                    ) + (
                      lineitem.l_tax
                    )
                  )
                ) + (
                  (
                    (
                      lineitem.l_linenumber
                    ) + (
                      lineitem.l_orderkey
                    )
                  ) + (
                    (
                      lineitem.l_partkey
                    ) + (
                      lineitem.l_suppkey
                    )
                  )
                )
              )
            )
          )
        ) + (
          (
            (
              (
                (
                  (
                    (
                      lineitem.l_quantity
                    ) + (
                      lineitem.l_extendedprice
                    )
                  ) + (
                    (
                      lineitem.l_discount
                    ) + (
                      lineitem.l_tax
                    )
                  )
                ) + (
                  (
                    (
                      lineitem.l_linenumber
                    ) + (
                      lineitem.l_orderkey
                    )
                  ) + (
                    (
                      lineitem.l_partkey
                    ) + (
                      lineitem.l_suppkey
                    )
                  )
                )
              ) + (
                (
                  (
                    (
                      lineitem.l_quantity
                    ) + (
                      lineitem.l_extendedprice
                    )
                  ) + (
                    (
                      lineitem.l_discount
                    ) + (
                      lineitem.l_tax
                    )
                  )
                ) + (
                  (
                    (
                      lineitem.l_linenumber
                    ) + (
                      lineitem.l_orderkey
                    )
                  ) + (
                    (
                      lineitem.l_partkey
                    ) + (
                      lineitem.l_suppkey
                    )
                  )
                )
              )
            ) + (
              (
                (
                  (
                    (
                      lineitem.l_quantity
                    ) + (
                      lineitem.l_extendedprice
                    )
                  ) + (
                    (
                      lineitem.l_discount
                    ) + (
                      lineitem.l_tax
                    )
                  )
                ) + (
                  (
                    (
                      lineitem.l_linenumber
                    ) + (
                      lineitem.l_orderkey
                    )
                  ) + (
                    (
                      lineitem.l_partkey
                    ) + (
                      lineitem.l_suppkey
                    )
                  )
                )
              ) + (
                (
                  (
                    (
                      lineitem.l_quantity
                    ) + (
                      lineitem.l_extendedprice
                    )
                  ) + (
                    (
                      lineitem.l_discount
                    ) + (
                      lineitem.l_tax
                    )
                  )
                ) + (
                  (
                    (
                      lineitem.l_linenumber
                    ) + (
                      lineitem.l_orderkey
                    )
                  ) + (
                    (
                      lineitem.l_partkey
                    ) + (
                      lineitem.l_suppkey
                    )
                  )
                )
              )
            )
          ) + (
            (
              (
                lineitem.l_quantity
              ) + (
                lineitem.l_extendedprice
              )
            ) + (
              (
                lineitem.l_discount
              ) + (
                lineitem.l_tax
              )
            )
          )
        )
      )
    ) >= 0
  )
ORDER BY
  l_returnflag,
  l_linestatus"""


COMPLEXITY_100_LOGICAL_SQL = """WITH base_query AS (SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE l_shipdate <= CAST('1998-09-02' AS DATE) GROUP BY l_returnflag, l_linestatus) SELECT base_query.l_returnflag, base_query.l_linestatus, MAX(base_query.sum_qty) AS sum_qty, MAX(base_query.sum_base_price) AS sum_base_price, MAX(base_query.sum_disc_price) AS sum_disc_price, MAX(base_query.sum_charge) AS sum_charge, MAX(base_query.avg_qty) AS avg_qty, MAX(base_query.avg_price) AS avg_price, MAX(base_query.avg_disc) AS avg_disc, MAX(base_query.count_order) AS count_order FROM base_query, lineitem WHERE lineitem.l_shipdate <= CAST('1998-09-02' AS DATE) AND base_query.l_returnflag = lineitem.l_returnflag AND base_query.l_linestatus = lineitem.l_linestatus GROUP BY base_query.l_returnflag, base_query.l_linestatus HAVING (MAX(((((((((lineitem.l_quantity) + (lineitem.l_extendedprice)) + ((lineitem.l_discount) + (lineitem.l_tax))) + (((lineitem.l_linenumber) + (lineitem.l_orderkey)) + ((lineitem.l_partkey) + (lineitem.l_suppkey)))) + ((((lineitem.l_quantity) + (lineitem.l_extendedprice)) + ((lineitem.l_discount) + (lineitem.l_tax))) + (((lineitem.l_linenumber) + (lineitem.l_orderkey)) + ((lineitem.l_partkey) + (lineitem.l_suppkey))))) + (((((lineitem.l_quantity) + (lineitem.l_extendedprice)) + ((lineitem.l_discount) + (lineitem.l_tax))) + (((lineitem.l_linenumber) + (lineitem.l_orderkey)) + ((lineitem.l_partkey) + (lineitem.l_suppkey)))) + ((((lineitem.l_quantity) + (lineitem.l_extendedprice)) + ((lineitem.l_discount) + (lineitem.l_tax))) + (((lineitem.l_linenumber) + (lineitem.l_orderkey)) + ((lineitem.l_partkey) + (lineitem.l_suppkey)))))) + ((((((lineitem.l_quantity) + (lineitem.l_extendedprice)) + ((lineitem.l_discount) + (lineitem.l_tax))) + (((lineitem.l_linenumber) + (lineitem.l_orderkey)) + ((lineitem.l_partkey) + (lineitem.l_suppkey)))) + ((((lineitem.l_quantity) + (lineitem.l_extendedprice)) + ((lineitem.l_discount) + (lineitem.l_tax))) + (((lineitem.l_linenumber) + (lineitem.l_orderkey)) + ((lineitem.l_partkey) + (lineitem.l_suppkey))))) + (((((lineitem.l_quantity) + (lineitem.l_extendedprice)) + ((lineitem.l_discount) + (lineitem.l_tax))) + (((lineitem.l_linenumber) + (lineitem.l_orderkey)) + ((lineitem.l_partkey) + (lineitem.l_suppkey)))) + ((((lineitem.l_quantity) + (lineitem.l_extendedprice)) + ((lineitem.l_discount) + (lineitem.l_tax))) + (((lineitem.l_linenumber) + (lineitem.l_orderkey)) + ((lineitem.l_partkey) + (lineitem.l_suppkey))))))) + (((((((lineitem.l_quantity) + (lineitem.l_extendedprice)) + ((lineitem.l_discount) + (lineitem.l_tax))) + (((lineitem.l_linenumber) + (lineitem.l_orderkey)) + ((lineitem.l_partkey) + (lineitem.l_suppkey)))) + ((((lineitem.l_quantity) + (lineitem.l_extendedprice)) + ((lineitem.l_discount) + (lineitem.l_tax))) + (((lineitem.l_linenumber) + (lineitem.l_orderkey)) + ((lineitem.l_partkey) + (lineitem.l_suppkey))))) + (((((lineitem.l_quantity) + (lineitem.l_extendedprice)) + ((lineitem.l_discount) + (lineitem.l_tax))) + (((lineitem.l_linenumber) + (lineitem.l_orderkey)) + ((lineitem.l_partkey) + (lineitem.l_suppkey)))) + ((((lineitem.l_quantity) + (lineitem.l_extendedprice)) + ((lineitem.l_discount) + (lineitem.l_tax))) + (((lineitem.l_linenumber) + (lineitem.l_orderkey)) + ((lineitem.l_partkey) + (lineitem.l_suppkey)))))) + (((lineitem.l_quantity) + (lineitem.l_extendedprice)) + ((lineitem.l_discount) + (lineitem.l_tax)))))) >= 0) ORDER BY base_query.l_returnflag, base_query.l_linestatus"""


OR_100_DFC_SQL = """SELECT
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
    MAX(lineitem.l_quantity) >= 0
    OR MAX(lineitem.l_quantity + 1) >= 0
    OR MAX(lineitem.l_extendedprice + 2) >= 0
    OR MAX(lineitem.l_discount + 3) >= 0
    OR MAX(lineitem.l_tax + 4) >= 0
    OR MAX(lineitem.l_linenumber + 5) >= 0
    OR MAX(lineitem.l_orderkey + 6) >= 0
    OR MAX(lineitem.l_partkey + 7) >= 0
    OR MAX(lineitem.l_suppkey + 8) >= 0
    OR MAX(lineitem.l_quantity + 9) >= 0
    OR MAX(lineitem.l_extendedprice + 10) >= 0
    OR MAX(lineitem.l_discount + 11) >= 0
    OR MAX(lineitem.l_tax + 12) >= 0
    OR MAX(lineitem.l_linenumber + 13) >= 0
    OR MAX(lineitem.l_orderkey + 14) >= 0
    OR MAX(lineitem.l_partkey + 15) >= 0
    OR MAX(lineitem.l_suppkey + 16) >= 0
    OR MAX(lineitem.l_quantity + 17) >= 0
    OR MAX(lineitem.l_extendedprice + 18) >= 0
    OR MAX(lineitem.l_discount + 19) >= 0
    OR MAX(lineitem.l_tax + 20) >= 0
    OR MAX(lineitem.l_linenumber + 21) >= 0
    OR MAX(lineitem.l_orderkey + 22) >= 0
    OR MAX(lineitem.l_partkey + 23) >= 0
    OR MAX(lineitem.l_suppkey + 24) >= 0
    OR MAX(lineitem.l_quantity + 25) >= 0
    OR MAX(lineitem.l_extendedprice + 26) >= 0
    OR MAX(lineitem.l_discount + 27) >= 0
    OR MAX(lineitem.l_tax + 28) >= 0
    OR MAX(lineitem.l_linenumber + 29) >= 0
    OR MAX(lineitem.l_orderkey + 30) >= 0
    OR MAX(lineitem.l_partkey + 31) >= 0
    OR MAX(lineitem.l_suppkey + 32) >= 0
    OR MAX(lineitem.l_quantity + 33) >= 0
    OR MAX(lineitem.l_extendedprice + 34) >= 0
    OR MAX(lineitem.l_discount + 35) >= 0
    OR MAX(lineitem.l_tax + 36) >= 0
    OR MAX(lineitem.l_linenumber + 37) >= 0
    OR MAX(lineitem.l_orderkey + 38) >= 0
    OR MAX(lineitem.l_partkey + 39) >= 0
    OR MAX(lineitem.l_suppkey + 40) >= 0
    OR MAX(lineitem.l_quantity + 41) >= 0
    OR MAX(lineitem.l_extendedprice + 42) >= 0
    OR MAX(lineitem.l_discount + 43) >= 0
    OR MAX(lineitem.l_tax + 44) >= 0
    OR MAX(lineitem.l_linenumber + 45) >= 0
    OR MAX(lineitem.l_orderkey + 46) >= 0
    OR MAX(lineitem.l_partkey + 47) >= 0
    OR MAX(lineitem.l_suppkey + 48) >= 0
    OR MAX(lineitem.l_quantity + 49) >= 0
    OR MAX(lineitem.l_extendedprice + 50) >= 0
    OR MAX(lineitem.l_discount + 51) >= 0
    OR MAX(lineitem.l_tax + 52) >= 0
    OR MAX(lineitem.l_linenumber + 53) >= 0
    OR MAX(lineitem.l_orderkey + 54) >= 0
    OR MAX(lineitem.l_partkey + 55) >= 0
    OR MAX(lineitem.l_suppkey + 56) >= 0
    OR MAX(lineitem.l_quantity + 57) >= 0
    OR MAX(lineitem.l_extendedprice + 58) >= 0
    OR MAX(lineitem.l_discount + 59) >= 0
    OR MAX(lineitem.l_tax + 60) >= 0
    OR MAX(lineitem.l_linenumber + 61) >= 0
    OR MAX(lineitem.l_orderkey + 62) >= 0
    OR MAX(lineitem.l_partkey + 63) >= 0
    OR MAX(lineitem.l_suppkey + 64) >= 0
    OR MAX(lineitem.l_quantity + 65) >= 0
    OR MAX(lineitem.l_extendedprice + 66) >= 0
    OR MAX(lineitem.l_discount + 67) >= 0
    OR MAX(lineitem.l_tax + 68) >= 0
    OR MAX(lineitem.l_linenumber + 69) >= 0
    OR MAX(lineitem.l_orderkey + 70) >= 0
    OR MAX(lineitem.l_partkey + 71) >= 0
    OR MAX(lineitem.l_suppkey + 72) >= 0
    OR MAX(lineitem.l_quantity + 73) >= 0
    OR MAX(lineitem.l_extendedprice + 74) >= 0
    OR MAX(lineitem.l_discount + 75) >= 0
    OR MAX(lineitem.l_tax + 76) >= 0
    OR MAX(lineitem.l_linenumber + 77) >= 0
    OR MAX(lineitem.l_orderkey + 78) >= 0
    OR MAX(lineitem.l_partkey + 79) >= 0
    OR MAX(lineitem.l_suppkey + 80) >= 0
    OR MAX(lineitem.l_quantity + 81) >= 0
    OR MAX(lineitem.l_extendedprice + 82) >= 0
    OR MAX(lineitem.l_discount + 83) >= 0
    OR MAX(lineitem.l_tax + 84) >= 0
    OR MAX(lineitem.l_linenumber + 85) >= 0
    OR MAX(lineitem.l_orderkey + 86) >= 0
    OR MAX(lineitem.l_partkey + 87) >= 0
    OR MAX(lineitem.l_suppkey + 88) >= 0
    OR MAX(lineitem.l_quantity + 89) >= 0
    OR MAX(lineitem.l_extendedprice + 90) >= 0
    OR MAX(lineitem.l_discount + 91) >= 0
    OR MAX(lineitem.l_tax + 92) >= 0
    OR MAX(lineitem.l_linenumber + 93) >= 0
    OR MAX(lineitem.l_orderkey + 94) >= 0
    OR MAX(lineitem.l_partkey + 95) >= 0
    OR MAX(lineitem.l_suppkey + 96) >= 0
    OR MAX(lineitem.l_quantity + 97) >= 0
    OR MAX(lineitem.l_extendedprice + 98) >= 0
    OR MAX(lineitem.l_discount + 99) >= 0
    OR MAX(lineitem.l_tax + 100) >= 0
  )
ORDER BY
  l_returnflag,
  l_linestatus"""


OR_100_LOGICAL_SQL = """WITH base_query AS (SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE l_shipdate <= CAST('1998-09-02' AS DATE) GROUP BY l_returnflag, l_linestatus) SELECT base_query.l_returnflag, base_query.l_linestatus, MAX(base_query.sum_qty) AS sum_qty, MAX(base_query.sum_base_price) AS sum_base_price, MAX(base_query.sum_disc_price) AS sum_disc_price, MAX(base_query.sum_charge) AS sum_charge, MAX(base_query.avg_qty) AS avg_qty, MAX(base_query.avg_price) AS avg_price, MAX(base_query.avg_disc) AS avg_disc, MAX(base_query.count_order) AS count_order FROM base_query, lineitem WHERE lineitem.l_shipdate <= CAST('1998-09-02' AS DATE) AND base_query.l_returnflag = lineitem.l_returnflag AND base_query.l_linestatus = lineitem.l_linestatus GROUP BY base_query.l_returnflag, base_query.l_linestatus HAVING (MAX(lineitem.l_quantity) >= 0 OR MAX(lineitem.l_quantity + 1) >= 0 OR MAX(lineitem.l_extendedprice + 2) >= 0 OR MAX(lineitem.l_discount + 3) >= 0 OR MAX(lineitem.l_tax + 4) >= 0 OR MAX(lineitem.l_linenumber + 5) >= 0 OR MAX(lineitem.l_orderkey + 6) >= 0 OR MAX(lineitem.l_partkey + 7) >= 0 OR MAX(lineitem.l_suppkey + 8) >= 0 OR MAX(lineitem.l_quantity + 9) >= 0 OR MAX(lineitem.l_extendedprice + 10) >= 0 OR MAX(lineitem.l_discount + 11) >= 0 OR MAX(lineitem.l_tax + 12) >= 0 OR MAX(lineitem.l_linenumber + 13) >= 0 OR MAX(lineitem.l_orderkey + 14) >= 0 OR MAX(lineitem.l_partkey + 15) >= 0 OR MAX(lineitem.l_suppkey + 16) >= 0 OR MAX(lineitem.l_quantity + 17) >= 0 OR MAX(lineitem.l_extendedprice + 18) >= 0 OR MAX(lineitem.l_discount + 19) >= 0 OR MAX(lineitem.l_tax + 20) >= 0 OR MAX(lineitem.l_linenumber + 21) >= 0 OR MAX(lineitem.l_orderkey + 22) >= 0 OR MAX(lineitem.l_partkey + 23) >= 0 OR MAX(lineitem.l_suppkey + 24) >= 0 OR MAX(lineitem.l_quantity + 25) >= 0 OR MAX(lineitem.l_extendedprice + 26) >= 0 OR MAX(lineitem.l_discount + 27) >= 0 OR MAX(lineitem.l_tax + 28) >= 0 OR MAX(lineitem.l_linenumber + 29) >= 0 OR MAX(lineitem.l_orderkey + 30) >= 0 OR MAX(lineitem.l_partkey + 31) >= 0 OR MAX(lineitem.l_suppkey + 32) >= 0 OR MAX(lineitem.l_quantity + 33) >= 0 OR MAX(lineitem.l_extendedprice + 34) >= 0 OR MAX(lineitem.l_discount + 35) >= 0 OR MAX(lineitem.l_tax + 36) >= 0 OR MAX(lineitem.l_linenumber + 37) >= 0 OR MAX(lineitem.l_orderkey + 38) >= 0 OR MAX(lineitem.l_partkey + 39) >= 0 OR MAX(lineitem.l_suppkey + 40) >= 0 OR MAX(lineitem.l_quantity + 41) >= 0 OR MAX(lineitem.l_extendedprice + 42) >= 0 OR MAX(lineitem.l_discount + 43) >= 0 OR MAX(lineitem.l_tax + 44) >= 0 OR MAX(lineitem.l_linenumber + 45) >= 0 OR MAX(lineitem.l_orderkey + 46) >= 0 OR MAX(lineitem.l_partkey + 47) >= 0 OR MAX(lineitem.l_suppkey + 48) >= 0 OR MAX(lineitem.l_quantity + 49) >= 0 OR MAX(lineitem.l_extendedprice + 50) >= 0 OR MAX(lineitem.l_discount + 51) >= 0 OR MAX(lineitem.l_tax + 52) >= 0 OR MAX(lineitem.l_linenumber + 53) >= 0 OR MAX(lineitem.l_orderkey + 54) >= 0 OR MAX(lineitem.l_partkey + 55) >= 0 OR MAX(lineitem.l_suppkey + 56) >= 0 OR MAX(lineitem.l_quantity + 57) >= 0 OR MAX(lineitem.l_extendedprice + 58) >= 0 OR MAX(lineitem.l_discount + 59) >= 0 OR MAX(lineitem.l_tax + 60) >= 0 OR MAX(lineitem.l_linenumber + 61) >= 0 OR MAX(lineitem.l_orderkey + 62) >= 0 OR MAX(lineitem.l_partkey + 63) >= 0 OR MAX(lineitem.l_suppkey + 64) >= 0 OR MAX(lineitem.l_quantity + 65) >= 0 OR MAX(lineitem.l_extendedprice + 66) >= 0 OR MAX(lineitem.l_discount + 67) >= 0 OR MAX(lineitem.l_tax + 68) >= 0 OR MAX(lineitem.l_linenumber + 69) >= 0 OR MAX(lineitem.l_orderkey + 70) >= 0 OR MAX(lineitem.l_partkey + 71) >= 0 OR MAX(lineitem.l_suppkey + 72) >= 0 OR MAX(lineitem.l_quantity + 73) >= 0 OR MAX(lineitem.l_extendedprice + 74) >= 0 OR MAX(lineitem.l_discount + 75) >= 0 OR MAX(lineitem.l_tax + 76) >= 0 OR MAX(lineitem.l_linenumber + 77) >= 0 OR MAX(lineitem.l_orderkey + 78) >= 0 OR MAX(lineitem.l_partkey + 79) >= 0 OR MAX(lineitem.l_suppkey + 80) >= 0 OR MAX(lineitem.l_quantity + 81) >= 0 OR MAX(lineitem.l_extendedprice + 82) >= 0 OR MAX(lineitem.l_discount + 83) >= 0 OR MAX(lineitem.l_tax + 84) >= 0 OR MAX(lineitem.l_linenumber + 85) >= 0 OR MAX(lineitem.l_orderkey + 86) >= 0 OR MAX(lineitem.l_partkey + 87) >= 0 OR MAX(lineitem.l_suppkey + 88) >= 0 OR MAX(lineitem.l_quantity + 89) >= 0 OR MAX(lineitem.l_extendedprice + 90) >= 0 OR MAX(lineitem.l_discount + 91) >= 0 OR MAX(lineitem.l_tax + 92) >= 0 OR MAX(lineitem.l_linenumber + 93) >= 0 OR MAX(lineitem.l_orderkey + 94) >= 0 OR MAX(lineitem.l_partkey + 95) >= 0 OR MAX(lineitem.l_suppkey + 96) >= 0 OR MAX(lineitem.l_quantity + 97) >= 0 OR MAX(lineitem.l_extendedprice + 98) >= 0 OR MAX(lineitem.l_discount + 99) >= 0 OR MAX(lineitem.l_tax + 100) >= 0) ORDER BY base_query.l_returnflag, base_query.l_linestatus"""


def test_tpch_q01_policy_complexity_100_dfc_sql_matches():
    conn = _build_tpch_conn()
    try:
        query = load_tpch_query(1)
        policy = build_tpch_q01_complexity_policy(100)
        rewriter = SQLRewriter(conn=conn)
        rewriter.register_policy(policy)
        rewritten = rewriter.transform_query(query)
    finally:
        conn.close()

    assert rewritten == COMPLEXITY_100_DFC_SQL, (
        "DFC SQL does not match expected for complexity=100.\n"
        f"Expected SQL:\n{COMPLEXITY_100_DFC_SQL}\n\n"
        f"Actual SQL:\n{rewritten}"
    )


def test_tpch_q01_policy_complexity_100_logical_sql_matches():
    conn = _build_tpch_conn()
    try:
        query = load_tpch_query(1)
        policy = build_tpch_q01_complexity_policy(100)
        rewritten = rewrite_query_logical_multi(query, [policy])
    finally:
        conn.close()

    assert rewritten == COMPLEXITY_100_LOGICAL_SQL, (
        "Logical SQL does not match expected for complexity=100.\n"
        f"Expected SQL:\n{COMPLEXITY_100_LOGICAL_SQL}\n\n"
        f"Actual SQL:\n{rewritten}"
    )


def test_tpch_q01_policy_or_100_dfc_sql_matches():
    conn = _build_tpch_conn()
    try:
        query = load_tpch_query(1)
        policy = build_tpch_q01_or_policy(100)
        rewriter = SQLRewriter(conn=conn)
        rewriter.register_policy(policy)
        rewritten = rewriter.transform_query(query)
    finally:
        conn.close()

    assert rewritten == OR_100_DFC_SQL, (
        "DFC SQL does not match expected for or_count=100.\n"
        f"Expected SQL:\n{OR_100_DFC_SQL}\n\n"
        f"Actual SQL:\n{rewritten}"
    )


def test_tpch_q01_policy_or_100_logical_sql_matches():
    conn = _build_tpch_conn()
    try:
        query = load_tpch_query(1)
        policy = build_tpch_q01_or_policy(100)
        rewritten = rewrite_query_logical_multi(query, [policy])
    finally:
        conn.close()

    assert rewritten == OR_100_LOGICAL_SQL, (
        "Logical SQL does not match expected for or_count=100.\n"
        f"Expected SQL:\n{OR_100_LOGICAL_SQL}\n\n"
        f"Actual SQL:\n{rewritten}"
    )
