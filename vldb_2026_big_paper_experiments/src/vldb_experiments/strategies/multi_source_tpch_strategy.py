"""TPC-H Q01 multi-source strategy with valid additional joins."""

from __future__ import annotations

import contextlib
import time

from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import DFCPolicy, Resolution, SQLRewriter

from vldb_experiments.correctness import compare_results_approx
from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck

DEFAULT_TPCH_SF = 1.0
DEFAULT_SOURCE_COUNTS = [1, 2, 3, 4, 5, 6, 7, 8]
DEFAULT_JOIN_COUNTS = [0, 1, 2, 3, 4, 5, 6, 7]
DEFAULT_WARMUP_PER_SETTING = 1
DEFAULT_RUNS_PER_SETTING = 5

JOIN_PATTERN = [
    ("orders", "lineitem.l_orderkey = {alias}.o_orderkey"),
    ("customer", "orders.o_custkey = {alias}.c_custkey"),
    ("nation", "customer.c_nationkey = {alias}.n_nationkey"),
    ("region", "nation.n_regionkey = {alias}.r_regionkey"),
    ("supplier", "lineitem.l_suppkey = {alias}.s_suppkey"),
    (
        "partsupp",
        "lineitem.l_partkey = {alias}.ps_partkey AND lineitem.l_suppkey = {alias}.ps_suppkey",
    ),
    ("part", "lineitem.l_partkey = {alias}.p_partkey"),
]

TABLE_TO_POLICY_COLUMN = {
    "lineitem": "lineitem.l_orderkey",
    "orders": "orders.o_orderkey",
    "customer": "customer.c_custkey",
    "nation": "nation.n_nationkey",
    "region": "region.r_regionkey",
    "supplier": "supplier.s_suppkey",
    "partsupp": "partsupp.ps_partkey",
    "part": "part.p_partkey",
}


def _build_q01_with_extra_joins(join_count: int) -> str:
    if join_count < 0:
        raise ValueError("join_count must be >= 0")

    counters: dict[str, int] = {}
    join_lines: list[str] = []
    for join_idx in range(join_count):
        table_name, condition_template = JOIN_PATTERN[join_idx % len(JOIN_PATTERN)]
        counters[table_name] = counters.get(table_name, 0) + 1
        alias = table_name if counters[table_name] == 1 else f"{table_name}_{counters[table_name]}"
        join_condition = condition_template.format(alias=alias)
        if alias == table_name:
            join_lines.append(f"JOIN {table_name} ON {join_condition}")
        else:
            join_lines.append(f"JOIN {table_name} {alias} ON {join_condition}")

    joins_sql = "\n".join(join_lines)
    return f"""
SELECT
  lineitem.l_returnflag AS returnflag,
  lineitem.l_linestatus AS linestatus,
  SUM(lineitem.l_quantity) AS sum_qty,
  SUM(lineitem.l_extendedprice) AS sum_base_price,
  SUM(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS sum_disc_price,
  SUM(lineitem.l_extendedprice * (1 + lineitem.l_tax) * (1 - lineitem.l_discount)) AS sum_charge,
  AVG(lineitem.l_quantity) AS avg_qty,
  AVG(lineitem.l_extendedprice) AS avg_price,
  AVG(lineitem.l_discount) AS avg_disc,
  COUNT(*) AS count_order
FROM lineitem
{joins_sql}
WHERE
  lineitem.l_shipdate <= DATE '1998-12-01' - INTERVAL 90 DAY
GROUP BY
  lineitem.l_returnflag,
  lineitem.l_linestatus
ORDER BY
  lineitem.l_returnflag,
  lineitem.l_linestatus
"""


def _distinct_tables_for_join_count(join_count: int) -> list[str]:
    tables: list[str] = ["lineitem"]
    for join_idx in range(join_count):
        table_name = JOIN_PATTERN[join_idx % len(JOIN_PATTERN)][0]
        if table_name not in tables:
            tables.append(table_name)
    return tables


def _build_policy(source_count: int, join_count: int) -> DFCPolicy:
    available_tables = _distinct_tables_for_join_count(join_count)
    if source_count > len(available_tables):
        raise ValueError(
            f"source_count={source_count} exceeds available tables={len(available_tables)} "
            f"for join_count={join_count}"
        )
    sources = available_tables[:source_count]
    constraints = [f"MAX({TABLE_TO_POLICY_COLUMN[table_name]}) >= 1" for table_name in sources]
    return DFCPolicy(
        sources=sources,
        constraint=" AND ".join(constraints),
        on_fail=Resolution.REMOVE,
        description=f"tpch_multi_source_sources{source_count}_joins{join_count}",
    )


class MultiSourceTPCHStrategy(ExperimentStrategy):
    """Compare No Policy vs 1Phase vs 2Phase on TPC-H Q01 with extra joins."""

    def setup(self, context: ExperimentContext) -> None:
        self.tpch_sf = float(context.strategy_config.get("tpch_sf", DEFAULT_TPCH_SF))
        source_counts = context.strategy_config.get("source_counts", DEFAULT_SOURCE_COUNTS)
        join_counts = context.strategy_config.get("join_counts", DEFAULT_JOIN_COUNTS)
        self.source_counts = [int(count) for count in source_counts]
        self.join_counts = [int(count) for count in join_counts]
        self.warmup_per_setting = int(
            context.strategy_config.get("warmup_per_setting", DEFAULT_WARMUP_PER_SETTING)
        )
        self.runs_per_setting = int(
            context.strategy_config.get("runs_per_setting", DEFAULT_RUNS_PER_SETTING)
        )

        self.local_duckdb = _ensure_smokedduck()
        db_path = context.strategy_config.get(
            "tpch_db_path",
            f"./results/multi_source_tpch_sf{self.tpch_sf}.db",
        )
        self.no_policy_conn = self.local_duckdb.connect(db_path)
        self.dfc_conn = self.local_duckdb.connect(db_path)

        for conn in [self.no_policy_conn, self.dfc_conn]:
            with contextlib.suppress(Exception):
                conn.execute("INSTALL tpch")
            conn.execute("LOAD tpch")
            table_exists = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'lineitem'"
            ).fetchone()[0]
            if table_exists == 0:
                conn.execute(f"CALL dbgen(sf={self.tpch_sf})")

        self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)

        self.valid_pairs = []
        for join_count in self.join_counts:
            available_sources = len(_distinct_tables_for_join_count(join_count))
            for source_count in self.source_counts:
                if source_count <= available_sources:
                    self.valid_pairs.append((join_count, source_count))
        if not self.valid_pairs:
            raise ValueError("No valid (join_count, source_count) settings for MultiSourceTPCHStrategy.")

        context.shared_state["tpch_sf"] = self.tpch_sf
        context.shared_state["source_counts"] = self.source_counts
        context.shared_state["join_counts"] = self.join_counts
        context.shared_state["warmup_per_setting"] = self.warmup_per_setting
        context.shared_state["runs_per_setting"] = self.runs_per_setting
        context.shared_state["valid_pairs"] = self.valid_pairs

    def _setting_and_run_for_execution(self, execution_number: int) -> tuple[int, int, int]:
        setting_index = (execution_number - 1) // self.runs_per_setting
        run_num = ((execution_number - 1) % self.runs_per_setting) + 1
        join_count, source_count = self.valid_pairs[setting_index]
        return join_count, source_count, run_num

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        join_count, source_count, run_num = self._setting_and_run_for_execution(context.execution_number)
        phase_label = "warmup" if context.is_warmup else f"run {run_num}"
        print(
            f"[Execution {context.execution_number}] "
            f"multi-source-tpch sf={self.tpch_sf} sources={source_count} joins={join_count} ({phase_label})"
        )

        query = _build_q01_with_extra_joins(join_count)
        policy = _build_policy(source_count, join_count)

        try:
            for old_policy in self.dfc_rewriter.get_dfc_policies():
                self.dfc_rewriter.delete_policy(
                    sources=old_policy.sources,
                    constraint=old_policy.constraint,
                    on_fail=old_policy.on_fail,
                )
            self.dfc_rewriter.register_policy(policy)
        except Exception:
            self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)
            self.dfc_rewriter.register_policy(policy)

        total_start = time.perf_counter()

        try:
            no_policy_start = time.perf_counter()
            no_policy_results = self.no_policy_conn.execute(query).fetchall()
            no_policy_exec_time = (time.perf_counter() - no_policy_start) * 1000.0
            no_policy_rows = len(no_policy_results)
            no_policy_error = None
        except Exception as exc:
            no_policy_exec_time = 0.0
            no_policy_results = []
            no_policy_rows = 0
            no_policy_error = str(exc)

        try:
            dfc_1phase_rewrite_start = time.perf_counter()
            dfc_1phase_transformed = self.dfc_rewriter.transform_query(query)
            dfc_1phase_rewrite_time = (time.perf_counter() - dfc_1phase_rewrite_start) * 1000.0
            dfc_1phase_exec_start = time.perf_counter()
            dfc_1phase_results = self.dfc_conn.execute(dfc_1phase_transformed).fetchall()
            dfc_1phase_exec_time = (time.perf_counter() - dfc_1phase_exec_start) * 1000.0
            dfc_1phase_rows = len(dfc_1phase_results)
            dfc_1phase_time = dfc_1phase_rewrite_time + dfc_1phase_exec_time
            dfc_1phase_error = None
        except Exception as exc:
            dfc_1phase_rewrite_time = 0.0
            dfc_1phase_exec_time = 0.0
            dfc_1phase_time = 0.0
            dfc_1phase_results = []
            dfc_1phase_rows = 0
            dfc_1phase_error = str(exc)

        try:
            dfc_2phase_rewrite_start = time.perf_counter()
            dfc_2phase_transformed = self.dfc_rewriter.transform_query(query, use_two_phase=True)
            dfc_2phase_rewrite_time = (time.perf_counter() - dfc_2phase_rewrite_start) * 1000.0
            dfc_2phase_exec_start = time.perf_counter()
            dfc_2phase_results = self.dfc_conn.execute(dfc_2phase_transformed).fetchall()
            dfc_2phase_exec_time = (time.perf_counter() - dfc_2phase_exec_start) * 1000.0
            dfc_2phase_rows = len(dfc_2phase_results)
            dfc_2phase_time = dfc_2phase_rewrite_time + dfc_2phase_exec_time
            dfc_2phase_error = None
        except Exception as exc:
            dfc_2phase_rewrite_time = 0.0
            dfc_2phase_exec_time = 0.0
            dfc_2phase_time = 0.0
            dfc_2phase_results = []
            dfc_2phase_rows = 0
            dfc_2phase_error = str(exc)

        total_time = (time.perf_counter() - total_start) * 1000.0

        correctness_1phase, correctness_error_1phase = compare_results_approx(
            no_policy_results,
            dfc_1phase_results,
            precision=5,
        )
        correctness_2phase, correctness_error_2phase = compare_results_approx(
            dfc_1phase_results,
            dfc_2phase_results,
            precision=5,
        )

        custom_metrics = {
            "tpch_sf": self.tpch_sf,
            "source_count": source_count,
            "join_count": join_count,
            "run_num": run_num or 0,
            "no_policy_exec_time_ms": no_policy_exec_time,
            "dfc_1phase_time_ms": dfc_1phase_time,
            "dfc_1phase_rewrite_time_ms": dfc_1phase_rewrite_time,
            "dfc_1phase_exec_time_ms": dfc_1phase_exec_time,
            "dfc_2phase_time_ms": dfc_2phase_time,
            "dfc_2phase_rewrite_time_ms": dfc_2phase_rewrite_time,
            "dfc_2phase_exec_time_ms": dfc_2phase_exec_time,
            "no_policy_rows": no_policy_rows,
            "dfc_1phase_rows": dfc_1phase_rows,
            "dfc_2phase_rows": dfc_2phase_rows,
            "no_policy_error": no_policy_error or "",
            "dfc_1phase_error": dfc_1phase_error or "",
            "dfc_2phase_error": dfc_2phase_error or "",
            "correctness_1phase_match": 1 if correctness_1phase else 0,
            "correctness_1phase_error": correctness_error_1phase or "",
            "correctness_2phase_match": 1 if correctness_2phase else 0,
            "correctness_2phase_error": correctness_error_2phase or "",
        }

        return ExperimentResult(duration_ms=total_time, custom_metrics=custom_metrics)

    def teardown(self, _context: ExperimentContext) -> None:
        if hasattr(self, "dfc_rewriter"):
            self.dfc_rewriter.close()
        for conn_name in ["no_policy_conn", "dfc_conn"]:
            if hasattr(self, conn_name):
                with contextlib.suppress(Exception):
                    getattr(self, conn_name).close()

    def get_metrics(self) -> list:
        return [
            "tpch_sf",
            "source_count",
            "join_count",
            "run_num",
            "no_policy_exec_time_ms",
            "dfc_1phase_time_ms",
            "dfc_1phase_rewrite_time_ms",
            "dfc_1phase_exec_time_ms",
            "dfc_2phase_time_ms",
            "dfc_2phase_rewrite_time_ms",
            "dfc_2phase_exec_time_ms",
            "no_policy_rows",
            "dfc_1phase_rows",
            "dfc_2phase_rows",
            "no_policy_error",
            "dfc_1phase_error",
            "dfc_2phase_error",
            "correctness_1phase_match",
            "correctness_1phase_error",
            "correctness_2phase_match",
            "correctness_2phase_error",
        ]

    def get_setting_key(self, context: ExperimentContext) -> tuple[int, int]:
        join_count, source_count, _ = self._setting_and_run_for_execution(context.execution_number)
        return (join_count, source_count)
