"""Precomputed query builders for the optimized TPC-H Q01 policy-count experiment."""

from __future__ import annotations

from functools import cache

from shared_sql_utils import combine_constraints_balanced

SUPPORTED_POLICY_COUNTS = (
    1,
    10,
    100,
    256,
    512,
    1000,
    1024,
    2048,
    10000,
    100000,
    1000000,
)

_OUTPUT_COLUMNS = [
    "l_returnflag",
    "l_linestatus",
    "sum_qty",
    "sum_base_price",
    "sum_disc_price",
    "sum_charge",
    "avg_qty",
    "avg_price",
    "avg_disc",
    "count_order",
]

_BASE_QUERY_NO_ORDER = """SELECT
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
  l_linestatus"""

_BASE_QUERY_WITH_ORDER = f"""{_BASE_QUERY_NO_ORDER}
ORDER BY
  l_returnflag,
  l_linestatus"""

_PHYSICAL_BASE_QUERY = """SELECT
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
FROM
    lineitem
WHERE
    l_shipdate <= CAST('1998-09-02' AS date)
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus"""


def _validate_policy_count(policy_count: int) -> None:
    if policy_count not in SUPPORTED_POLICY_COUNTS:
        msg = f"Unsupported policy count for cached optimized queries: {policy_count}"
        raise ValueError(msg)


def _policy_constraints(policy_count: int, column_ref: str) -> list[str]:
    return [f"MAX({column_ref}) > {-i}" for i in range(policy_count)]


def _having_chain(policy_count: int, column_ref: str) -> str:
    clauses = [f"(\n    {constraint}\n  )" for constraint in _policy_constraints(policy_count, column_ref)]
    return "\n  AND ".join(clauses)


def _logical_constraint(policy_count: int) -> str:
    return combine_constraints_balanced(_policy_constraints(policy_count, "lineitem.l_quantity"))


def _logical_projection() -> str:
    return ", ".join(
        [f"MAX(base_query.{column}) AS {column}" for column in _OUTPUT_COLUMNS[2:]]
    )


@cache
def get_cached_dfc_1phase_query(policy_count: int) -> str:
    _validate_policy_count(policy_count)
    dfc_having = _having_chain(policy_count, "lineitem.l_quantity")
    return f"""{_BASE_QUERY_NO_ORDER}
HAVING
  {dfc_having}
ORDER BY
  l_returnflag,
  l_linestatus"""


@cache
def get_cached_dfc_1phase_optimized_query(policy_count: int) -> str:
    _validate_policy_count(policy_count)
    return f"""{_BASE_QUERY_NO_ORDER}
HAVING
  (
    MAX(lineitem.l_quantity) > 0
  )
ORDER BY
  l_returnflag,
  l_linestatus"""


@cache
def get_cached_dfc_2phase_query(policy_count: int) -> str:
    _validate_policy_count(policy_count)
    dfc_having = _having_chain(policy_count, "lineitem.l_quantity")
    return f"""WITH base_query AS (
  {_BASE_QUERY_WITH_ORDER}
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
    {dfc_having}
)
SELECT
  base_query.*
FROM base_query
JOIN policy_eval
  USING (l_returnflag, l_linestatus)"""


@cache
def get_cached_logical_query(policy_count: int) -> str:
    _validate_policy_count(policy_count)
    logical_constraint = _logical_constraint(policy_count)
    return (
        "WITH base_query AS (SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, "
        "SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) "
        "AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) "
        "AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, "
        "AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE "
        "l_shipdate <= CAST('1998-09-02' AS DATE) GROUP BY l_returnflag, l_linestatus) "
        "SELECT base_query.l_returnflag, base_query.l_linestatus, "
        f"{_logical_projection()} FROM base_query, lineitem WHERE "
        "lineitem.l_shipdate <= CAST('1998-09-02' AS DATE) AND "
        "base_query.l_returnflag = lineitem.l_returnflag AND "
        "base_query.l_linestatus = lineitem.l_linestatus GROUP BY "
        "base_query.l_returnflag, base_query.l_linestatus HAVING "
        f"{logical_constraint} ORDER BY base_query.l_returnflag, base_query.l_linestatus"
    )


@cache
def get_cached_physical_base_query(policy_count: int) -> str:
    _validate_policy_count(policy_count)
    return _PHYSICAL_BASE_QUERY


@cache
def get_cached_physical_filter_template(policy_count: int) -> str:
    _validate_policy_count(policy_count)
    physical_having = " AND ".join(_policy_constraints(policy_count, "lineitem.l_quantity"))
    return f"""WITH lineage AS (
{{lineage_query}}
)
SELECT
    generated_table."l_returnflag", generated_table."l_linestatus", generated_table."sum_qty", generated_table."sum_base_price", generated_table."sum_disc_price", generated_table."sum_charge", generated_table."avg_qty", generated_table."avg_price", generated_table."avg_disc", generated_table."count_order"
FROM {{temp_table_name}} AS generated_table
JOIN lineage
    ON generated_table.rowid::bigint = lineage.out_index::bigint
JOIN lineitem
    ON lineitem.rowid::bigint = lineage.lineitem::bigint
GROUP BY generated_table.rowid, generated_table."l_returnflag", generated_table."l_linestatus", generated_table."sum_qty", generated_table."sum_base_price", generated_table."sum_disc_price", generated_table."sum_charge", generated_table."avg_qty", generated_table."avg_price", generated_table."avg_disc", generated_table."count_order"
HAVING {physical_having}
ORDER BY generated_table.l_returnflag, generated_table.l_linestatus"""


@cache
def get_cached_tpch_q01_optimized_queries(policy_count: int) -> dict[str, str]:
    _validate_policy_count(policy_count)
    return {
        "dfc_1phase": get_cached_dfc_1phase_query(policy_count),
        "dfc_1phase_optimized": get_cached_dfc_1phase_optimized_query(policy_count),
        "dfc_2phase": get_cached_dfc_2phase_query(policy_count),
        "logical": get_cached_logical_query(policy_count),
        "physical_base": get_cached_physical_base_query(policy_count),
        "physical_filter_template": get_cached_physical_filter_template(policy_count),
    }


def prime_cached_tpch_q01_optimized_queries(
    policy_counts: list[int],
    query_types: tuple[str, ...] = ("dfc_1phase", "dfc_1phase_optimized"),
) -> None:
    for policy_count in policy_counts:
        for query_type in query_types:
            if query_type == "dfc_1phase":
                get_cached_dfc_1phase_query(policy_count)
            elif query_type == "dfc_1phase_optimized":
                get_cached_dfc_1phase_optimized_query(policy_count)
            elif query_type == "dfc_2phase":
                get_cached_dfc_2phase_query(policy_count)
            elif query_type == "logical":
                get_cached_logical_query(policy_count)
            elif query_type == "physical_base":
                get_cached_physical_base_query(policy_count)
            elif query_type == "physical_filter_template":
                get_cached_physical_filter_template(policy_count)
            else:
                msg = f"Unsupported cached query type: {query_type}"
                raise ValueError(msg)
