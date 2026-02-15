"""Policy-count strategy for JOIN->GROUP_BY microbenchmark query."""

import contextlib
import time

from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import SQLRewriter

from vldb_experiments.baselines.logical_baseline import execute_query_logical_multi
from vldb_experiments.baselines.physical_baseline import execute_query_physical_detailed
from vldb_experiments.correctness import compare_results_exact
from vldb_experiments.data_setup import setup_test_data_with_join_group_by
from vldb_experiments.policy_setup import create_test_policies
from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck

DEFAULT_POLICY_COUNTS = [1, 10, 100, 1000]
DEFAULT_JOIN_COUNTS = [1]
DEFAULT_WARMUP_PER_SETTING = 1
DEFAULT_RUNS_PER_SETTING = 5
DEFAULT_NUM_ROWS = 1_000


class MicrobenchmarkPolicyCountStrategy(ExperimentStrategy):
    """Measure JOIN->GROUP_BY performance as policy count and join count increase."""

    def setup(self, context: ExperimentContext) -> None:
        main_conn = context.database_connection
        if main_conn is None:
            raise ValueError("Database connection required in context")

        self.policy_counts = list(
            context.strategy_config.get("policy_counts", DEFAULT_POLICY_COUNTS)
        )
        self.join_counts = list(
            context.strategy_config.get("join_counts", DEFAULT_JOIN_COUNTS)
        )
        self.warmup_per_setting = int(
            context.strategy_config.get("warmup_per_setting", DEFAULT_WARMUP_PER_SETTING)
        )
        # Backward compatibility with older key names.
        if "warmup_per_setting" not in context.strategy_config:
            self.warmup_per_setting = int(
                context.strategy_config.get(
                    "warmup_per_policy",
                    DEFAULT_WARMUP_PER_SETTING,
                )
            )
        self.runs_per_setting = int(
            context.strategy_config.get("runs_per_setting", DEFAULT_RUNS_PER_SETTING)
        )
        if "runs_per_setting" not in context.strategy_config:
            self.runs_per_setting = int(
                context.strategy_config.get(
                    "runs_per_policy",
                    DEFAULT_RUNS_PER_SETTING,
                )
            )
        self.num_rows = int(context.strategy_config.get("num_rows", DEFAULT_NUM_ROWS))
        self._max_join_count_seen = 0
        self._current_join_count = None
        self._current_query = None
        self.settings = [
            (join_count, policy_count)
            for join_count in self.join_counts
            for policy_count in self.policy_counts
        ]

        self.local_duckdb = _ensure_smokedduck()

        self.no_policy_conn = self.local_duckdb.connect(":memory:")
        self.dfc_conn = self.local_duckdb.connect(":memory:")
        self.logical_conn = self.local_duckdb.connect(":memory:")
        self.physical_conn = self.local_duckdb.connect(":memory:")

        self._refresh_join_data(join_count=self.join_counts[0])

        self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)

    def _build_join_group_by_query(self, join_count: int) -> str:
        if join_count < 1:
            raise ValueError(f"join_count must be >= 1, got {join_count}")

        amount_terms = ["test_data.amount"] + [
            f"j{i}.amount" for i in range(1, join_count + 1)
        ]
        joins = [
            f"JOIN join_data_{i} j{i} ON test_data.id = j{i}.id"
            for i in range(1, join_count + 1)
        ]
        return " ".join(
            f"""
            SELECT test_data.category, COUNT(*), SUM({" + ".join(amount_terms)}) AS total_amount
            FROM test_data
            {" ".join(joins)}
            GROUP BY test_data.category
            """.split()
        )

    def _refresh_join_data(self, join_count: int) -> None:
        drop_join_limit = max(self._max_join_count_seen, join_count)
        for conn in [
            self.no_policy_conn,
            self.dfc_conn,
            self.logical_conn,
            self.physical_conn,
        ]:
            with contextlib.suppress(Exception):
                conn.execute("DROP TABLE IF EXISTS test_data")
            for idx in range(1, drop_join_limit + 1):
                with contextlib.suppress(Exception):
                    conn.execute(f"DROP TABLE IF EXISTS join_data_{idx}")
            setup_test_data_with_join_group_by(
                conn=conn,
                join_count=join_count,
                num_rows=self.num_rows,
            )
            with contextlib.suppress(Exception):
                conn.execute("SET max_expression_depth TO 20000")
            try:
                conn.execute("COMMIT")
            except Exception:
                with contextlib.suppress(Exception):
                    conn.execute("ROLLBACK")
        self._max_join_count_seen = max(self._max_join_count_seen, join_count)
        self._current_join_count = join_count
        self._current_query = self._build_join_group_by_query(join_count)

    def _setting_and_run_for_execution(self, execution_number: int) -> tuple[int, int, int]:
        setting_index = (execution_number - 1) // self.runs_per_setting
        run_num = ((execution_number - 1) % self.runs_per_setting) + 1
        join_count, policy_count = self.settings[setting_index]
        return join_count, policy_count, run_num

    def _refresh_policies(self, policy_count: int) -> list:
        policies = create_test_policies(policy_count=policy_count)
        existing_policies = self.dfc_rewriter.get_dfc_policies()
        for old_policy in existing_policies:
            self.dfc_rewriter.delete_policy(
                sources=old_policy.sources,
                constraint=old_policy.constraint,
                on_fail=old_policy.on_fail,
            )
        for policy in policies:
            self.dfc_rewriter.register_policy(policy)
        return policies

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        join_count, policy_count, run_num = self._setting_and_run_for_execution(context.execution_number)
        phase = "warmup" if context.is_warmup else f"run {run_num}"
        print(
            f"[Execution {context.execution_number}] "
            f"JOIN_GROUP_BY microbenchmark joins={join_count} policies={policy_count} ({phase})"
        )

        if self._current_join_count != join_count:
            self._refresh_join_data(join_count=join_count)
        policies = self._refresh_policies(policy_count)
        query = self._current_query

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
            policies,
        )

        (
            physical_results,
            physical_timing,
            physical_error,
            _physical_base_sql,
            _physical_filter_sql,
        ) = execute_query_physical_detailed(
            self.physical_conn,
            query,
            policies,
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
            "query_type": "JOIN_GROUP_BY",
            "variation_join_count": join_count,
            "join_count": join_count,
            "policy_count": policy_count,
            "run_num": run_num or 0,
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
            "correctness_error": correctness_error,
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

    def get_metrics(self) -> list[str]:
        return [
            "query_type",
            "variation_join_count",
            "join_count",
            "policy_count",
            "run_num",
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

    def get_setting_key(self, context: ExperimentContext) -> tuple[int, int]:
        join_count, policy_count, _ = self._setting_and_run_for_execution(context.execution_number)
        return (join_count, policy_count)
