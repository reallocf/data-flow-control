"""Cached query builders for the TPC-H Q01 self-join alias-policy experiment."""

from __future__ import annotations

from functools import cache
from itertools import islice

DEFAULT_SELF_JOIN_COUNTS = [1, 10, 100, 1000]
CHUNKED_SELF_JOIN_THRESHOLD = 100
ALIAS_CHUNK_SIZE = 32


def _alias_names(self_join_count: int) -> list[str]:
    if self_join_count < 1:
        msg = "self_join_count must be at least 1"
        raise ValueError(msg)
    return [f"l{i}" for i in range(1, self_join_count + 2)]


def _from_clause(self_join_count: int) -> str:
    aliases = _alias_names(self_join_count)
    tables = [f"lineitem {alias}" for alias in aliases]
    return "FROM\n  " + ",\n  ".join(tables)


def _group_by_clause() -> str:
    return "GROUP BY\n  l1.l_returnflag,\n  l1.l_linestatus"


def _order_by_clause() -> str:
    return "ORDER BY\n  l1.l_returnflag,\n  l1.l_linestatus"


def _base_query_select() -> str:
    return """SELECT
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
  COUNT(*) AS count_order"""


def _join_predicates(self_join_count: int) -> list[str]:
    aliases = _alias_names(self_join_count)
    return [f"l1.rowid = {alias}.rowid" for alias in aliases[1:]]


def _base_query_where(self_join_count: int) -> str:
    predicates = [
        "l1.l_shipdate <= CAST('1998-09-02' AS DATE)",
        *_join_predicates(self_join_count),
    ]
    return "WHERE\n  " + "\n  AND ".join(predicates)


def _chunked(iterable: list[str], chunk_size: int) -> list[list[str]]:
    iterator = iter(iterable)
    return [list(chunk) for chunk in iter(lambda: list(islice(iterator, chunk_size)), [])]


def _chunk_column_name(alias: str) -> str:
    return f"{alias}_shipdate"


def _chunk_subquery(chunk_aliases: list[str], chunk_index: int) -> tuple[str, dict[str, str]]:
    inner_aliases = [f"c{chunk_index}_{idx}" for idx in range(1, len(chunk_aliases) + 1)]
    alias_map = dict(zip(chunk_aliases, inner_aliases, strict=True))
    select_lines = [f"    {inner_aliases[0]}.rowid AS base_rowid"]
    outer_refs: dict[str, str] = {}
    for outer_alias, inner_alias in alias_map.items():
        column_name = _chunk_column_name(outer_alias)
        select_lines.append(f"    {inner_alias}.l_shipdate AS {column_name}")
        outer_refs[outer_alias] = f"chunk_{chunk_index}.{column_name}"

    tables = ",\n      ".join(f"lineitem {inner_alias}" for inner_alias in inner_aliases)
    predicates = " AND ".join(
        f"{inner_aliases[0]}.rowid = {inner_alias}.rowid" for inner_alias in inner_aliases[1:]
    )
    where_clause = f"\n    WHERE {predicates}" if predicates else ""
    subquery = (
        "JOIN (\n"
        "  SELECT\n"
        + ",\n".join(select_lines)
        + "\n"
        + "    FROM\n"
        + f"      {tables}"
        + where_clause
        + f"\n) chunk_{chunk_index}\n"
        + f"  ON l1.rowid = chunk_{chunk_index}.base_rowid"
    )
    return subquery, outer_refs


def _alias_expression_map(self_join_count: int) -> dict[str, str]:
    aliases = _alias_names(self_join_count)
    if self_join_count <= CHUNKED_SELF_JOIN_THRESHOLD:
        return {alias: f"{alias}.l_shipdate" for alias in aliases}

    expression_map = {"l1": "l1.l_shipdate"}
    for chunk_index, chunk_aliases in enumerate(
        _chunked(aliases[1:], ALIAS_CHUNK_SIZE),
        start=1,
    ):
        _, outer_refs = _chunk_subquery(chunk_aliases, chunk_index)
        expression_map.update(outer_refs)
    return expression_map


def _chunked_from_clause(self_join_count: int) -> str:
    aliases = _alias_names(self_join_count)
    lines = ["FROM lineitem l1"]
    for chunk_index, chunk_aliases in enumerate(
        _chunked(aliases[1:], ALIAS_CHUNK_SIZE),
        start=1,
    ):
        subquery, _ = _chunk_subquery(chunk_aliases, chunk_index)
        lines.append(subquery)
    return "\n".join(lines)


def _outer_where_clause(self_join_count: int) -> str:
    if self_join_count <= CHUNKED_SELF_JOIN_THRESHOLD:
        return _base_query_where(self_join_count)
    return """WHERE
  l1.l_shipdate <= CAST('1998-09-02' AS DATE)"""


def _ordered_pair_constraints(self_join_count: int) -> list[str]:
    aliases = _alias_names(self_join_count)
    expr_map = _alias_expression_map(self_join_count)
    constraints = []
    for left in aliases:
        for right in aliases:
            if left == right:
                continue
            constraints.append(f"MAX({expr_map[left]}) = MAX({expr_map[right]})")
    return constraints


def _star_constraints(self_join_count: int) -> list[str]:
    aliases = _alias_names(self_join_count)
    expr_map = _alias_expression_map(self_join_count)
    return [f"MAX({expr_map['l1']}) = MAX({expr_map[alias]})" for alias in aliases[1:]]


def _having_clause(constraints: list[str]) -> str:
    clauses = [f"(\n    {constraint}\n  )" for constraint in constraints]
    return "HAVING\n  " + "\n  AND ".join(clauses)


@cache
def get_cached_tpch_q01_self_join_no_policy_query(self_join_count: int) -> str:
    from_clause = (
        _from_clause(self_join_count)
        if self_join_count <= CHUNKED_SELF_JOIN_THRESHOLD
        else _chunked_from_clause(self_join_count)
    )
    return f"""{_base_query_select()}
{from_clause}
{_outer_where_clause(self_join_count)}
{_group_by_clause()}
{_order_by_clause()}"""


@cache
def get_cached_tpch_q01_self_join_1phase_query(self_join_count: int) -> str:
    from_clause = (
        _from_clause(self_join_count)
        if self_join_count <= CHUNKED_SELF_JOIN_THRESHOLD
        else _chunked_from_clause(self_join_count)
    )
    return f"""{_base_query_select()}
{from_clause}
{_outer_where_clause(self_join_count)}
{_group_by_clause()}
{_having_clause(_ordered_pair_constraints(self_join_count))}
{_order_by_clause()}"""


@cache
def get_cached_tpch_q01_self_join_1phase_optimized_query(self_join_count: int) -> str:
    from_clause = (
        _from_clause(self_join_count)
        if self_join_count <= CHUNKED_SELF_JOIN_THRESHOLD
        else _chunked_from_clause(self_join_count)
    )
    return f"""{_base_query_select()}
{from_clause}
{_outer_where_clause(self_join_count)}
{_group_by_clause()}
{_having_clause(_star_constraints(self_join_count))}
{_order_by_clause()}"""


def prime_cached_tpch_q01_self_join_queries(self_join_counts: list[int]) -> None:
    for self_join_count in self_join_counts:
        get_cached_tpch_q01_self_join_no_policy_query(self_join_count)
        get_cached_tpch_q01_self_join_1phase_query(self_join_count)
        get_cached_tpch_q01_self_join_1phase_optimized_query(self_join_count)
