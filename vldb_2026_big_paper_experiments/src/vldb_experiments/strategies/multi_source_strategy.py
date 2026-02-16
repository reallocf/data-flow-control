"""Multi-source join chain strategy for measuring DFC overhead."""

import contextlib
import time

import duckdb
from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import DFCPolicy, Resolution, SQLRewriter

from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck

DEFAULT_SOURCE_COUNTS = [2, 4, 8, 16, 32]
DEFAULT_JOIN_COUNTS = [2, 4, 8, 16, 32]
DEFAULT_NUM_ROWS = 10_000
DEFAULT_WARMUP_PER_SETTING = 1
DEFAULT_RUNS_PER_SETTING = 5


def _setup_chain_tables(
    conn: duckdb.DuckDBPyConnection,
    table_count: int,
    num_rows: int,
) -> None:
    for table_idx in range(1, table_count + 1):
        conn.execute(f"DROP TABLE IF EXISTS t{table_idx}")

    for table_idx in range(1, table_count + 1):
        if table_idx == 1:
            conn.execute(
                """
                CREATE TABLE t1 AS
                SELECT
                  i AS id,
                  i * 10 AS payload
                FROM range(1, ?) AS tbl(i)
                """,
                [num_rows + 1],
            )
            continue

        prev_idx = table_idx - 1
        conn.execute(
            f"""
            CREATE TABLE t{table_idx} AS
            SELECT
              i AS id,
              i AS t{prev_idx}_id,
              i * 10 AS payload
            FROM range(1, ?) AS tbl(i)
            """,
            [num_rows + 1],
        )


def _build_chain_query(join_count: int) -> str:
    if join_count < 1:
        raise ValueError("join_count must be >= 1")

    table_count = join_count + 1

    payload_sum = " + ".join([f"t{idx}.payload" for idx in range(1, table_count + 1)])
    join_lines = []
    for idx in range(2, table_count + 1):
        prev_idx = idx - 1
        join_lines.append(f"JOIN t{idx} ON t{prev_idx}.id = t{idx}.t{prev_idx}_id")

    join_sql = "\n".join(join_lines)

    return f"""
SELECT
  t1.id % 10 AS bucket,
  SUM({payload_sum}) AS total_payload
FROM t1
{join_sql}
GROUP BY
  bucket
"""


def _build_policy(table_count: int) -> DFCPolicy:
    sources = [f"t{idx}" for idx in range(1, table_count + 1)]
    constraint = " AND ".join([f"max(t{idx}.id) >= 1" for idx in range(1, table_count + 1)])
    return DFCPolicy(
        sources=sources,
        constraint=constraint,
        on_fail=Resolution.REMOVE,
        description=f"multi_source_chain_{table_count}",
    )


class MultiSourceStrategy(ExperimentStrategy):
    """Compare DFC vs no-policy overhead on a linear multi-source join chain."""

    def setup(self, context: ExperimentContext) -> None:
        main_conn = context.database_connection
        if main_conn is None:
            raise ValueError("Database connection required in context")

        source_counts = context.strategy_config.get("source_counts", DEFAULT_SOURCE_COUNTS)
        join_counts = context.strategy_config.get("join_counts", DEFAULT_JOIN_COUNTS)
        self.source_counts = [int(count) for count in source_counts]
        self.join_counts = [int(count) for count in join_counts]
        self.num_rows = int(context.strategy_config.get("num_rows", DEFAULT_NUM_ROWS))
        self.warmup_per_setting = int(
            context.strategy_config.get("warmup_per_setting", DEFAULT_WARMUP_PER_SETTING)
        )
        self.runs_per_setting = int(
            context.strategy_config.get("runs_per_setting", DEFAULT_RUNS_PER_SETTING)
        )

        max_join_count = max(self.join_counts)
        max_table_count = max_join_count + 1
        self.local_duckdb = _ensure_smokedduck()
        self.no_policy_conn = self.local_duckdb.connect(":memory:")
        self.dfc_conn = self.local_duckdb.connect(":memory:")

        _setup_chain_tables(self.no_policy_conn, max_table_count, self.num_rows)
        _setup_chain_tables(self.dfc_conn, max_table_count, self.num_rows)

        for conn in [self.no_policy_conn, self.dfc_conn]:
            try:
                conn.execute("COMMIT")
            except Exception:
                try:
                    conn.commit()
                except Exception:
                    with contextlib.suppress(Exception):
                        conn.execute("ROLLBACK")

        self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)

        context.shared_state["source_counts"] = self.source_counts
        context.shared_state["join_counts"] = self.join_counts
        context.shared_state["warmup_per_setting"] = self.warmup_per_setting
        context.shared_state["runs_per_setting"] = self.runs_per_setting
        self.valid_pairs = [
            (join_count, source_count)
            for join_count in self.join_counts
            for source_count in self.source_counts
            if source_count <= join_count
        ]
        context.shared_state["valid_pairs"] = self.valid_pairs

    def _setting_and_run_for_execution(self, execution_number: int) -> tuple[int, int, int]:
        setting_index = (execution_number - 1) // self.runs_per_setting
        run_num = ((execution_number - 1) % self.runs_per_setting) + 1
        join_count, source_count = self.valid_pairs[setting_index]
        return join_count, source_count, run_num

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        join_count, source_count, run_num = self._setting_and_run_for_execution(context.execution_number)
        table_count = join_count + 1

        phase_label = "warmup" if context.is_warmup else f"run {run_num}"
        print(
            f"[Execution {context.execution_number}] "
            f"multi-source sources={source_count} joins={join_count} ({phase_label})"
        )

        query = _build_chain_query(join_count)
        policy = _build_policy(source_count)

        try:
            existing_policies = self.dfc_rewriter.get_dfc_policies()
            for old_policy in existing_policies:
                self.dfc_rewriter.delete_policy(
                    sources=old_policy.sources,
                    constraint=old_policy.constraint,
                    on_fail=old_policy.on_fail,
                )
            self.dfc_rewriter.register_policy(policy)
        except Exception:
            self.dfc_conn = self.local_duckdb.connect(":memory:")
            _setup_chain_tables(self.dfc_conn, max(self.join_counts) + 1, self.num_rows)
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

        custom_metrics = {
            "source_count": source_count,
            "join_count": join_count,
            "table_count": table_count,
            "run_num": run_num or 0,
            "num_rows": self.num_rows,
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
            "source_count",
            "join_count",
            "table_count",
            "run_num",
            "num_rows",
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
        ]

    def get_setting_key(self, context: ExperimentContext) -> tuple[int, int]:
        join_count, source_count, _ = self._setting_and_run_for_execution(context.execution_number)
        return (join_count, source_count)
