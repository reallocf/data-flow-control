"""Microbenchmark strategy to compare 1Phase vs 2Phase across rows and policy width."""

from __future__ import annotations

import contextlib
import time

from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import DFCPolicy, Resolution, SQLRewriter

from vldb_experiments.correctness import compare_results_exact
from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck

DEFAULT_ROW_COUNTS = [10000]
DEFAULT_POLICY_COLUMN_COUNTS = [2, 4, 8, 16, 32, 64, 128, 256, 512]
DEFAULT_TOTAL_COLUMNS = 4096  # total columns including pk
DEFAULT_BASE_AGGREGATE_COLUMNS = 128
DEFAULT_JOIN_FANOUTS = [2, 4, 8, 16, 32, 64]
DEFAULT_WARMUP_PER_SETTING = 1
DEFAULT_RUNS_PER_SETTING = 5


class MicrobenchmarkPhaseCompetitionStrategy(ExperimentStrategy):
    """Measure where DFC 1Phase and 2Phase compete (rows vs policy width)."""

    def setup(self, context: ExperimentContext) -> None:
        main_conn = context.database_connection
        if main_conn is None:
            raise ValueError("Database connection required in context")

        self.row_counts = list(context.strategy_config.get("row_counts", DEFAULT_ROW_COUNTS))
        self.policy_column_counts = list(
            context.strategy_config.get("policy_column_counts", DEFAULT_POLICY_COLUMN_COUNTS)
        )
        self.total_columns = int(context.strategy_config.get("total_columns", DEFAULT_TOTAL_COLUMNS))
        self.base_aggregate_columns_default = int(
            context.strategy_config.get("base_aggregate_columns", DEFAULT_BASE_AGGREGATE_COLUMNS)
        )
        self.base_aggregate_columns_list = list(
            context.strategy_config.get(
                "base_aggregate_columns_list", [self.base_aggregate_columns_default]
            )
        )
        self.join_fanouts = list(context.strategy_config.get("join_fanouts", DEFAULT_JOIN_FANOUTS))
        self.warmup_per_setting = int(
            context.strategy_config.get("warmup_per_setting", DEFAULT_WARMUP_PER_SETTING)
        )
        self.runs_per_setting = int(
            context.strategy_config.get("runs_per_setting", DEFAULT_RUNS_PER_SETTING)
        )

        if self.total_columns != 4096:
            raise ValueError(f"total_columns must be 4096 for this experiment, got {self.total_columns}")
        for base_count in self.base_aggregate_columns_list:
            if base_count < 1 or base_count > (self.total_columns - 1):
                raise ValueError(
                    f"base_aggregate_columns must be in [1, {self.total_columns - 1}], got {base_count}"
                )
        for row_count in self.row_counts:
            if row_count < 1:
                raise ValueError(f"row_count must be >= 1, got {row_count}")
        for fanout in self.join_fanouts:
            if fanout < 1:
                raise ValueError(f"join_fanout must be >= 1, got {fanout}")
        for col_count in self.policy_column_counts:
            if col_count < 2 or col_count > (self.total_columns - 1) or col_count % 2 != 0:
                raise ValueError(
                    f"policy_column_count must be even and in [2, {self.total_columns - 1}], got {col_count}"
                )
        for base_count in self.base_aggregate_columns_list:
            for policy_count in self.policy_column_counts:
                if (base_count + policy_count - 1) > (self.total_columns - 1):
                    raise ValueError(
                        "base_aggregate_columns + policy_column_count exceeds available columns: "
                        f"{base_count} + {policy_count} - 1 > {self.total_columns - 1}"
                    )

        self.settings = [
            (row_count, fanout, base_col_count, policy_col_count)
            for row_count in self.row_counts
            for fanout in self.join_fanouts
            for base_col_count in self.base_aggregate_columns_list
            for policy_col_count in self.policy_column_counts
        ]

        self.local_duckdb = _ensure_smokedduck()
        self.conn = self.local_duckdb.connect(":memory:")
        with contextlib.suppress(Exception):
            self.conn.execute("SET max_expression_depth TO 100000")
        self.rewriter = SQLRewriter(conn=self.conn)
        self.current_row_count: int | None = None
        self.current_join_fanout: int | None = None

    def _create_table(self, row_count: int, join_fanout: int) -> None:
        with contextlib.suppress(Exception):
            self.conn.execute("DROP TABLE IF EXISTS wide_data")
        with contextlib.suppress(Exception):
            self.conn.execute("DROP TABLE IF EXISTS join_data")

        value_cols = [
            f"CAST(((i + {idx}) % 1000) + 1 AS DOUBLE) AS c{idx}"  # positive values
            for idx in range(1, self.total_columns)
        ]
        self.conn.execute(
            f"""
            CREATE TABLE wide_data AS
            SELECT
                i AS pk,
                {", ".join(value_cols)}
            FROM range(1, {row_count + 1}) t(i)
            """
        )
        self.conn.execute(
            f"""
            CREATE TABLE join_data AS
            SELECT
                k AS fk,
                r AS replica,
                CAST((((k * 13) + r) % 1000) + 1 AS DOUBLE) AS jv
            FROM range(1, {row_count + 1}) keys(k)
            CROSS JOIN range(1, {join_fanout + 1}) reps(r)
            """
        )

    def _build_query(self, base_aggregate_columns: int) -> str:
        base_expr = " + ".join(
            [f"wide_data.c{i}" for i in range(1, base_aggregate_columns + 1)]
        )
        return " ".join(
            f"""
            SELECT SUM({base_expr}) AS base_sum
            FROM wide_data
            JOIN join_data ON wide_data.pk = join_data.fk
            """.split()
        )

    def _build_policy(self, base_aggregate_columns: int, policy_column_count: int) -> DFCPolicy:
        start_col = base_aggregate_columns
        end_col = start_col + policy_column_count - 1
        sum_expr = " + ".join(
            [f"wide_data.c{i}" for i in range(start_col, end_col + 1)]
        )
        return DFCPolicy(
            sources=["wide_data"],
            constraint=f"sum({sum_expr}) >= 0",
            on_fail=Resolution.REMOVE,
            description=f"phase_competition_policy_cols_{policy_column_count}",
        )

    def _refresh_policy(self, policy: DFCPolicy) -> None:
        existing_policies = self.rewriter.get_dfc_policies()
        for old_policy in existing_policies:
            self.rewriter.delete_policy(
                sources=old_policy.sources,
                constraint=old_policy.constraint,
                on_fail=old_policy.on_fail,
            )
        self.rewriter.register_policy(policy)

    def _setting_and_run_for_execution(self, execution_number: int) -> tuple[int, int, int, int, int]:
        setting_index = (execution_number - 1) // self.runs_per_setting
        run_num = ((execution_number - 1) % self.runs_per_setting) + 1
        row_count, join_fanout, base_aggregate_columns, policy_column_count = self.settings[
            setting_index
        ]
        return row_count, join_fanout, base_aggregate_columns, policy_column_count, run_num

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        row_count, join_fanout, base_aggregate_columns, policy_column_count, run_num = (
            self._setting_and_run_for_execution(context.execution_number)
        )
        phase = "warmup" if context.is_warmup else f"run {run_num}"
        print(
            f"[Execution {context.execution_number}] "
            f"PHASE_COMPETITION rows={row_count} fanout={join_fanout} "
            f"base_cols={base_aggregate_columns} "
            f"policy_cols={policy_column_count} ({phase})"
        )

        if self.current_row_count != row_count or self.current_join_fanout != join_fanout:
            self._create_table(row_count, join_fanout)
            self.current_row_count = row_count
            self.current_join_fanout = join_fanout

        query = self._build_query(base_aggregate_columns)
        policy = self._build_policy(base_aggregate_columns, policy_column_count)
        self._refresh_policy(policy)

        try:
            dfc_1phase_rewrite_start = time.perf_counter()
            dfc_1phase_sql = self.rewriter.transform_query(query)
            dfc_1phase_rewrite_time = (time.perf_counter() - dfc_1phase_rewrite_start) * 1000.0
            dfc_1phase_exec_start = time.perf_counter()
            dfc_1phase_results = self.conn.execute(dfc_1phase_sql).fetchall()
            dfc_1phase_exec_time = (time.perf_counter() - dfc_1phase_exec_start) * 1000.0
            dfc_1phase_error = None
        except Exception as e:
            dfc_1phase_rewrite_time = 0.0
            dfc_1phase_exec_time = 0.0
            dfc_1phase_results = []
            dfc_1phase_error = str(e)

        try:
            dfc_2phase_rewrite_start = time.perf_counter()
            dfc_2phase_sql = self.rewriter.transform_query(query, use_two_phase=True)
            dfc_2phase_rewrite_time = (time.perf_counter() - dfc_2phase_rewrite_start) * 1000.0
            dfc_2phase_exec_start = time.perf_counter()
            dfc_2phase_results = self.conn.execute(dfc_2phase_sql).fetchall()
            dfc_2phase_exec_time = (time.perf_counter() - dfc_2phase_exec_start) * 1000.0
            dfc_2phase_error = None
        except Exception as e:
            dfc_2phase_rewrite_time = 0.0
            dfc_2phase_exec_time = 0.0
            dfc_2phase_results = []
            dfc_2phase_error = str(e)

        if dfc_1phase_error is None and dfc_2phase_error is None:
            match, match_error = compare_results_exact(dfc_1phase_results, dfc_2phase_results)
        else:
            match, match_error = False, f"errors: 1phase={dfc_1phase_error}, 2phase={dfc_2phase_error}"

        total_time = dfc_1phase_exec_time + dfc_2phase_exec_time
        if total_time == 0.0:
            total_time = 0.001

        custom_metrics = {
            "query_type": "PHASE_COMPETITION",
            "row_count": row_count,
            "join_fanout": join_fanout,
            "base_aggregate_columns": base_aggregate_columns,
            "policy_column_count": policy_column_count,
            "variation_row_count": row_count,
            "variation_join_fanout": join_fanout,
            "variation_base_aggregate_columns": base_aggregate_columns,
            "variation_policy_columns": policy_column_count,
            "run_num": run_num if not context.is_warmup else 0,
            "dfc_1phase_time_ms": dfc_1phase_rewrite_time + dfc_1phase_exec_time,
            "dfc_1phase_rewrite_time_ms": dfc_1phase_rewrite_time,
            "dfc_1phase_exec_time_ms": dfc_1phase_exec_time,
            "dfc_2phase_time_ms": dfc_2phase_rewrite_time + dfc_2phase_exec_time,
            "dfc_2phase_rewrite_time_ms": dfc_2phase_rewrite_time,
            "dfc_2phase_exec_time_ms": dfc_2phase_exec_time,
            "dfc_1phase_rows": len(dfc_1phase_results),
            "dfc_2phase_rows": len(dfc_2phase_results),
            "correctness_match": match,
            "correctness_error": match_error or "",
            "dfc_1phase_error": dfc_1phase_error or "",
            "dfc_2phase_error": dfc_2phase_error or "",
        }
        return ExperimentResult(duration_ms=total_time, custom_metrics=custom_metrics)

    def teardown(self, _context: ExperimentContext) -> None:
        if hasattr(self, "conn") and self.conn is not None:
            with contextlib.suppress(Exception):
                self.conn.execute("DROP TABLE IF EXISTS wide_data")
            with contextlib.suppress(Exception):
                self.conn.execute("DROP TABLE IF EXISTS join_data")
        if hasattr(self, "rewriter"):
            with contextlib.suppress(Exception):
                self.rewriter.close()
        if hasattr(self, "conn") and self.conn is not None:
            with contextlib.suppress(Exception):
                self.conn.close()

    def get_setting_key(self, context: ExperimentContext):
        row_count, join_fanout, base_aggregate_columns, policy_column_count, _ = (
            self._setting_and_run_for_execution(context.execution_number)
        )
        return row_count, join_fanout, base_aggregate_columns, policy_column_count

    def get_metrics(self) -> list[str]:
        return [
            "query_type",
            "row_count",
            "join_fanout",
            "base_aggregate_columns",
            "policy_column_count",
            "variation_row_count",
            "variation_join_fanout",
            "variation_base_aggregate_columns",
            "variation_policy_columns",
            "run_num",
            "dfc_1phase_time_ms",
            "dfc_1phase_rewrite_time_ms",
            "dfc_1phase_exec_time_ms",
            "dfc_2phase_time_ms",
            "dfc_2phase_rewrite_time_ms",
            "dfc_2phase_exec_time_ms",
            "dfc_1phase_rows",
            "dfc_2phase_rows",
            "correctness_match",
            "correctness_error",
            "dfc_1phase_error",
            "dfc_2phase_error",
        ]
