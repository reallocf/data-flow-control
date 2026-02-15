"""Tests for multi-source experiment query rewriting."""

import duckdb
from sql_rewriter import DFCPolicy, Resolution, SQLRewriter

from vldb_experiments.strategies.multi_source_strategy import _build_chain_query
from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck


def _setup_chain_schema(conn: duckdb.DuckDBPyConnection, join_count: int) -> None:
    table_count = join_count + 1
    for idx in range(1, table_count + 1):
        if idx == 1:
            conn.execute("CREATE TABLE t1 (id INTEGER, payload INTEGER)")
        else:
            prev = idx - 1
            conn.execute(
                f"CREATE TABLE t{idx} (id INTEGER, t{prev}_id INTEGER, payload INTEGER)"
            )

def _populate_chain_data(
    conn: duckdb.DuckDBPyConnection,
    join_count: int,
    num_rows: int,
) -> None:
    table_count = join_count + 1
    conn.execute(
        """
        INSERT INTO t1
        SELECT
          i AS id,
          i * 10 AS payload
        FROM range(1, ?) AS tbl(i)
        """,
        [num_rows + 1],
    )
    for idx in range(2, table_count + 1):
        prev = idx - 1
        conn.execute(
            f"""
            INSERT INTO t{idx}
            SELECT
              i AS id,
              i AS t{prev}_id,
              i * 10 AS payload
            FROM range(1, ?) AS tbl(i)
            """,
            [num_rows + 1],
        )


def test_multi_source_32_join_32_sources_rewrite() -> None:
    join_count = 32
    source_count = 32
    num_rows = 10_000

    conn = _ensure_smokedduck().connect(":memory:")
    _setup_chain_schema(conn, join_count)
    _populate_chain_data(conn, join_count, num_rows)

    sources = [f"t{i}" for i in range(1, source_count + 1)]
    constraint = " AND ".join(
        [f"max(t{i}.id) >= 1" for i in range(1, source_count + 1)]
    )
    policy = DFCPolicy(sources=sources, constraint=constraint, on_fail=Resolution.REMOVE)

    rewriter = SQLRewriter(conn=conn)
    rewriter.register_policy(policy)

    query = _build_chain_query(join_count)
    transformed = rewriter.transform_query(query)

    expected = """SELECT
  t1.id % 10 AS bucket,
  SUM(
    t1.payload + t2.payload + t3.payload + t4.payload + t5.payload + t6.payload + t7.payload + t8.payload + t9.payload + t10.payload + t11.payload + t12.payload + t13.payload + t14.payload + t15.payload + t16.payload + t17.payload + t18.payload + t19.payload + t20.payload + t21.payload + t22.payload + t23.payload + t24.payload + t25.payload + t26.payload + t27.payload + t28.payload + t29.payload + t30.payload + t31.payload + t32.payload + t33.payload
  ) AS total_payload
FROM t1
JOIN t2
  ON t1.id = t2.t1_id
JOIN t3
  ON t2.id = t3.t2_id
JOIN t4
  ON t3.id = t4.t3_id
JOIN t5
  ON t4.id = t5.t4_id
JOIN t6
  ON t5.id = t6.t5_id
JOIN t7
  ON t6.id = t7.t6_id
JOIN t8
  ON t7.id = t8.t7_id
JOIN t9
  ON t8.id = t9.t8_id
JOIN t10
  ON t9.id = t10.t9_id
JOIN t11
  ON t10.id = t11.t10_id
JOIN t12
  ON t11.id = t12.t11_id
JOIN t13
  ON t12.id = t13.t12_id
JOIN t14
  ON t13.id = t14.t13_id
JOIN t15
  ON t14.id = t15.t14_id
JOIN t16
  ON t15.id = t16.t15_id
JOIN t17
  ON t16.id = t17.t16_id
JOIN t18
  ON t17.id = t18.t17_id
JOIN t19
  ON t18.id = t19.t18_id
JOIN t20
  ON t19.id = t20.t19_id
JOIN t21
  ON t20.id = t21.t20_id
JOIN t22
  ON t21.id = t22.t21_id
JOIN t23
  ON t22.id = t23.t22_id
JOIN t24
  ON t23.id = t24.t23_id
JOIN t25
  ON t24.id = t25.t24_id
JOIN t26
  ON t25.id = t26.t25_id
JOIN t27
  ON t26.id = t27.t26_id
JOIN t28
  ON t27.id = t28.t27_id
JOIN t29
  ON t28.id = t29.t28_id
JOIN t30
  ON t29.id = t30.t29_id
JOIN t31
  ON t30.id = t31.t30_id
JOIN t32
  ON t31.id = t32.t31_id
JOIN t33
  ON t32.id = t33.t32_id
GROUP BY
  bucket
HAVING
  (
    MAX(t1.id) >= 1
    AND MAX(t2.id) >= 1
    AND MAX(t3.id) >= 1
    AND MAX(t4.id) >= 1
    AND MAX(t5.id) >= 1
    AND MAX(t6.id) >= 1
    AND MAX(t7.id) >= 1
    AND MAX(t8.id) >= 1
    AND MAX(t9.id) >= 1
    AND MAX(t10.id) >= 1
    AND MAX(t11.id) >= 1
    AND MAX(t12.id) >= 1
    AND MAX(t13.id) >= 1
    AND MAX(t14.id) >= 1
    AND MAX(t15.id) >= 1
    AND MAX(t16.id) >= 1
    AND MAX(t17.id) >= 1
    AND MAX(t18.id) >= 1
    AND MAX(t19.id) >= 1
    AND MAX(t20.id) >= 1
    AND MAX(t21.id) >= 1
    AND MAX(t22.id) >= 1
    AND MAX(t23.id) >= 1
    AND MAX(t24.id) >= 1
    AND MAX(t25.id) >= 1
    AND MAX(t26.id) >= 1
    AND MAX(t27.id) >= 1
    AND MAX(t28.id) >= 1
    AND MAX(t29.id) >= 1
    AND MAX(t30.id) >= 1
    AND MAX(t31.id) >= 1
    AND MAX(t32.id) >= 1
  )"""

    assert transformed == expected, (
        "Transformed SQL does not match expected.\n"
        f"Expected SQL:\n{expected}\n\n"
        f"Actual SQL:\n{transformed}"
    )

    expected_results = conn.execute(query).fetchall()
    transformed_results = conn.execute(transformed).fetchall()
    assert sorted(transformed_results) == sorted(expected_results)
