"""Microbenchmark strategy for sweeping table width with disjoint query/policy columns."""

import contextlib
import os
from pathlib import Path
import tempfile
import time

from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import DFCPolicy, Resolution, SQLRewriter

from vldb_experiments.baselines.logical_baseline import execute_query_logical_multi
from vldb_experiments.baselines.physical_baseline import execute_query_physical_detailed
from vldb_experiments.correctness import compare_results_exact
from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck

DEFAULT_TABLE_WIDTHS = [32, 64, 128, 256]
DEFAULT_NUM_ROWS = 1_000_000
DEFAULT_WARMUP_PER_WIDTH = 1
DEFAULT_RUNS_PER_WIDTH = 5


class MicrobenchmarkTableWidthStrategy(ExperimentStrategy):
    """Measure No Policy, DFC, Logical, and Physical as table width increases."""

    def setup(self, context: ExperimentContext) -> None:
        main_conn = context.database_connection
        if main_conn is None:
            raise ValueError("Database connection required in context")

        self.table_widths = list(
            context.strategy_config.get("table_widths", DEFAULT_TABLE_WIDTHS)
        )
        self.num_rows = int(context.strategy_config.get("num_rows", DEFAULT_NUM_ROWS))
        self.warmup_per_width = int(
            context.strategy_config.get("warmup_per_width", DEFAULT_WARMUP_PER_WIDTH)
        )
        self.runs_per_width = int(
            context.strategy_config.get("runs_per_width", DEFAULT_RUNS_PER_WIDTH)
        )

        for width in self.table_widths:
            if width < 2 or width % 2 != 0:
                raise ValueError(
                    f"table_width must be even and >= 2, got {width}"
                )

        self.local_duckdb = _ensure_smokedduck()
        fd, db_path = tempfile.mkstemp(prefix="microbenchmark_table_width_", suffix=".duckdb")
        os.close(fd)
        os.unlink(db_path)
        self.shared_db_path = str(Path(db_path))

        self.no_policy_conn = self.local_duckdb.connect(self.shared_db_path)
        self.dfc_conn = self.local_duckdb.connect(self.shared_db_path)
        self.logical_conn = self.local_duckdb.connect(self.shared_db_path)
        self.physical_conn = self.local_duckdb.connect(self.shared_db_path)

        for conn in [
            self.no_policy_conn,
            self.dfc_conn,
            self.logical_conn,
            self.physical_conn,
        ]:
            with contextlib.suppress(Exception):
                conn.execute("SET max_expression_depth TO 20000")
            try:
                conn.execute("COMMIT")
            except Exception:
                with contextlib.suppress(Exception):
                    conn.execute("ROLLBACK")

        self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)
        self.current_width: int | None = None
        self.current_query: str | None = None
        self.current_policy: DFCPolicy | None = None
        self.current_policy_signature: tuple[str, str] | None = None

    def _create_wide_table(self, conn, table_width: int) -> None:
        with contextlib.suppress(Exception):
            conn.execute("DROP TABLE IF EXISTS wide_data")

        col_exprs = [
            f"CAST(((i + {idx}) % 1000) + 1 AS DOUBLE) AS c{idx}"
            for idx in range(1, table_width + 1)
        ]
        conn.execute(
            f"""
            CREATE TABLE wide_data AS
            SELECT
                {", ".join(col_exprs)}
            FROM range({self.num_rows}) t(i)
            """
        )

    def _build_base_query(self, table_width: int) -> str:
        half = table_width // 2
        expr = " + ".join([f"c{idx}" for idx in range(1, half + 1)])
        return f"SELECT SUM({expr}) AS base_sum FROM wide_data"

    def _build_policy(self, table_width: int) -> DFCPolicy:
        half = table_width // 2
        policy_expr = " + ".join([f"wide_data.c{idx}" for idx in range(half + 1, table_width + 1)])
        # Always true for generated positive values, but still forces policy expression evaluation.
        return DFCPolicy(
            sources=["wide_data"],
            constraint=f"sum({policy_expr}) >= 0",
            on_fail=Resolution.REMOVE,
            description=f"wide_table_policy_width_{table_width}",
        )

    def _prepare_width(self, table_width: int) -> None:
        print(f"  Preparing width={table_width}: creating shared wide_data")
        self._create_wide_table(self.no_policy_conn, table_width)
        for conn in [self.no_policy_conn, self.dfc_conn, self.logical_conn, self.physical_conn]:
            with contextlib.suppress(Exception):
                conn.execute("COMMIT")
        print(f"  Prepared width={table_width}: shared table ready")

        self.current_width = table_width
        self.current_query = self._build_base_query(table_width)
        self.current_policy = self._build_policy(table_width)
        self.current_policy_signature = (
            self.current_policy.constraint,
            self.current_policy.on_fail.value,
        )
        self._refresh_policy(self.current_policy)

    def _refresh_policy(self, policy: DFCPolicy) -> None:
        existing_policies = self.dfc_rewriter.get_dfc_policies()
        for old_policy in existing_policies:
            self.dfc_rewriter.delete_policy(
                sources=old_policy.sources,
                constraint=old_policy.constraint,
                on_fail=old_policy.on_fail,
            )
        self.dfc_rewriter.register_policy(policy)

    def _width_and_run_for_execution(self, execution_number: int) -> tuple[int, int]:
        width_index = (execution_number - 1) // self.runs_per_width
        run_num = ((execution_number - 1) % self.runs_per_width) + 1
        return self.table_widths[width_index], run_num

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        table_width, run_num = self._width_and_run_for_execution(context.execution_number)
        phase = "warmup" if context.is_warmup else f"run {run_num}"
        print(
            f"[Execution {context.execution_number}] "
            f"WIDE_AGG width={table_width} rows={self.num_rows} ({phase})"
        )

        if self.current_width != table_width:
            self._prepare_width(table_width)

        query = self.current_query
        policy = self.current_policy
        if query is None or policy is None:
            raise RuntimeError("Width-specific query/policy not initialized")

        no_policy_start = time.perf_counter()
        no_policy_results = self.no_policy_conn.execute(query).fetchall()
        no_policy_exec_time = (time.perf_counter() - no_policy_start) * 1000.0

        dfc_rewrite_start = time.perf_counter()
        dfc_transformed = self.dfc_rewriter.transform_query(query)
        dfc_rewrite_time = (time.perf_counter() - dfc_rewrite_start) * 1000.0
        dfc_exec_start = time.perf_counter()
        dfc_results = self.dfc_conn.execute(dfc_transformed).fetchall()
        dfc_exec_time = (time.perf_counter() - dfc_exec_start) * 1000.0

        logical_results, logical_rewrite_time, logical_exec_time = execute_query_logical_multi(
            self.logical_conn,
            query,
            [policy],
        )

        (
            physical_results,
            physical_timing,
            physical_error,
            _base_query_sql,
            _filter_query_sql,
        ) = execute_query_physical_detailed(
            self.physical_conn,
            query,
            [policy],
        )
        physical_rewrite_time = physical_timing.get("rewrite_time_ms", 0.0)
        physical_base_capture_time = physical_timing.get("base_capture_time_ms", 0.0)
        physical_lineage_query_time = physical_timing.get("lineage_query_time_ms", 0.0)
        physical_runtime = physical_timing.get("runtime_time_ms", 0.0)
        physical_exec_time = physical_runtime

        logical_match, logical_match_error = compare_results_exact(dfc_results, logical_results)
        physical_match, physical_match_error = compare_results_exact(dfc_results, physical_results)
        correctness_match = logical_match and physical_match and not physical_error
        correctness_error = "; ".join(
            err
            for err in [
                f"logical={logical_match_error}" if logical_match_error else "",
                f"physical={physical_match_error}" if physical_match_error else "",
                f"physical_error={physical_error}" if physical_error else "",
            ]
            if err
        )

        total_time = no_policy_exec_time + dfc_exec_time + logical_exec_time + physical_exec_time
        if total_time == 0.0:
            total_time = 0.001

        custom_metrics = {
            "query_type": "WIDE_AGG",
            "table_width": table_width,
            "row_count": self.num_rows,
            "run_num": run_num or 0,
            "policy_count": 1,
            "no_policy_time_ms": no_policy_exec_time,
            "no_policy_exec_time_ms": no_policy_exec_time,
            "dfc_time_ms": dfc_rewrite_time + dfc_exec_time,
            "dfc_rewrite_time_ms": dfc_rewrite_time,
            "dfc_exec_time_ms": dfc_exec_time,
            "logical_time_ms": logical_rewrite_time + logical_exec_time,
            "logical_rewrite_time_ms": logical_rewrite_time,
            "logical_exec_time_ms": logical_exec_time,
            "physical_time_ms": physical_exec_time,
            "physical_runtime_ms": physical_runtime,
            "physical_exec_time_ms": physical_exec_time,
            "physical_rewrite_time_ms": physical_rewrite_time,
            "physical_base_capture_time_ms": physical_base_capture_time,
            "physical_lineage_query_time_ms": physical_lineage_query_time,
            "no_policy_rows": len(no_policy_results),
            "dfc_rows": len(dfc_results),
            "logical_rows": len(logical_results),
            "physical_rows": len(physical_results) if physical_results else 0,
            "correctness_match": correctness_match,
            "correctness_error": correctness_error or "",
            "logical_match": logical_match,
            "logical_match_error": logical_match_error or "",
            "physical_match": physical_match,
            "physical_match_error": physical_match_error or "",
            "physical_error": physical_error or "",
        }

        return ExperimentResult(duration_ms=total_time, custom_metrics=custom_metrics)

    def teardown(self, _context: ExperimentContext) -> None:
        if hasattr(self, "dfc_rewriter"):
            with contextlib.suppress(Exception):
                self.dfc_rewriter.close()
        for conn_name in [
            "no_policy_conn",
            "dfc_conn",
            "logical_conn",
            "physical_conn",
        ]:
            conn = getattr(self, conn_name, None)
            if conn is not None:
                with contextlib.suppress(Exception):
                    conn.close()
        if hasattr(self, "shared_db_path"):
            with contextlib.suppress(Exception):
                Path(self.shared_db_path).unlink()

    def get_metrics(self) -> list[str]:
        return [
            "query_type",
            "table_width",
            "row_count",
            "run_num",
            "policy_count",
            "no_policy_time_ms",
            "no_policy_exec_time_ms",
            "dfc_time_ms",
            "dfc_rewrite_time_ms",
            "dfc_exec_time_ms",
            "logical_time_ms",
            "logical_rewrite_time_ms",
            "logical_exec_time_ms",
            "physical_time_ms",
            "physical_runtime_ms",
            "physical_exec_time_ms",
            "physical_rewrite_time_ms",
            "physical_base_capture_time_ms",
            "physical_lineage_query_time_ms",
            "no_policy_rows",
            "dfc_rows",
            "logical_rows",
            "physical_rows",
            "correctness_match",
            "correctness_error",
            "logical_match",
            "logical_match_error",
            "physical_match",
            "physical_match_error",
            "physical_error",
        ]

    def get_setting_key(self, context: ExperimentContext) -> int:
        table_width, _ = self._width_and_run_for_execution(context.execution_number)
        return table_width
