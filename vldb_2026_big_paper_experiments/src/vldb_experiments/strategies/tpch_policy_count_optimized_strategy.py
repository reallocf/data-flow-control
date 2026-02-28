"""TPC-H Q01 optimized policy count strategy for measuring rewrite overhead."""

import contextlib
import pathlib
import time

from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import DFCPolicy, Resolution

from vldb_experiments.correctness import compare_results_exact
from vldb_experiments.strategies.tpch_policy_count_optimized_queries import (
    get_cached_dfc_1phase_optimized_query,
    get_cached_dfc_1phase_query,
    prime_cached_tpch_q01_optimized_queries,
)
from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck, load_tpch_query

DEFAULT_POLICY_COUNTS = [256, 512, 1024, 2048]
DEFAULT_WARMUP_PER_POLICY = 1
DEFAULT_RUNS_PER_POLICY = 5


def build_tpch_q01_optimized_policies(num_policies: int) -> list[DFCPolicy]:
    """Build optimized Q01 policies using only MAX over lineitem.l_quantity.

    Thresholds monotonically decrease so every policy remains satisfiable on
    TPC-H data while still producing distinct predicates.
    """
    policies = []
    for i in range(num_policies):
        threshold = -i
        policies.append(
            DFCPolicy(
                sources=["lineitem"],
                constraint=f"max(lineitem.l_quantity) > {threshold}",
                on_fail=Resolution.REMOVE,
                description=f"q01_optimized_policy_{i + 1}",
            )
        )
    return policies


def build_tpch_q01_optimized_strongest_policy(num_policies: int) -> DFCPolicy:
    """Build the single strongest policy implied by the optimized policy set."""
    if num_policies < 1:
        msg = "num_policies must be at least 1"
        raise ValueError(msg)
    return DFCPolicy(
        sources=["lineitem"],
        constraint="max(lineitem.l_quantity) > 0",
        on_fail=Resolution.REMOVE,
        description=f"q01_optimized_strongest_policy_{num_policies}",
    )


class TPCHPolicyCountOptimizedStrategy(ExperimentStrategy):
    """Strategy for measuring performance vs optimized policy count on TPC-H Q01."""

    def setup(self, context: ExperimentContext) -> None:
        self.scale_factor = float(context.strategy_config.get("tpch_sf", 1))
        db_path = context.strategy_config.get("tpch_db_path")
        if not db_path:
            db_path = f"./results/tpch_q01_policy_count_optimized_sf{self.scale_factor}.db"
        pathlib.Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.physical_db_path = f"{db_path}_physical"

        policy_counts = context.strategy_config.get("policy_counts", DEFAULT_POLICY_COUNTS)
        warmup_per_policy = int(context.strategy_config.get("warmup_per_policy", DEFAULT_WARMUP_PER_POLICY))
        runs_per_policy = int(context.strategy_config.get("runs_per_policy", DEFAULT_RUNS_PER_POLICY))

        self.local_duckdb = _ensure_smokedduck()
        main_conn = self.local_duckdb.connect(self.db_path)

        # Set up TPC-H data on the main connection
        with contextlib.suppress(Exception):
            main_conn.execute("INSTALL tpch")
        main_conn.execute("LOAD tpch")
        table_exists = main_conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'lineitem'"
        ).fetchone()[0]
        if table_exists == 0:
            main_conn.execute(f"CALL dbgen(sf={self.scale_factor})")

        # Keep both query-rewrite approaches on the same shared connection.
        self.dfc_conn = main_conn
        for conn in [main_conn]:
            try:
                conn.execute("COMMIT")
            except Exception:
                try:
                    conn.commit()
                except Exception:
                    with contextlib.suppress(Exception):
                        conn.execute("ROLLBACK")

        self.policy_counts = list(policy_counts)
        self.warmup_per_policy = warmup_per_policy
        self.runs_per_policy = runs_per_policy
        query_num = int(context.strategy_config.get("tpch_query", 1))
        context.shared_state["tpch_query_num"] = query_num
        context.shared_state["tpch_query"] = load_tpch_query(query_num)
        prime_cached_tpch_q01_optimized_queries(
            self.policy_counts,
            ("dfc_1phase", "dfc_1phase_optimized"),
        )

    def _policy_and_run_for_execution(self, execution_number: int) -> tuple[int, int]:
        policy_index = (execution_number - 1) // self.runs_per_policy
        run_num = ((execution_number - 1) % self.runs_per_policy) + 1
        return self.policy_counts[policy_index], run_num

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        query_num = context.shared_state["tpch_query_num"]
        policy_count, run_num = self._policy_and_run_for_execution(context.execution_number)

        phase_label = "warmup" if context.is_warmup else f"run {run_num}"
        print(
            f"[Execution {context.execution_number}] "
            f"TPC-H Q{query_num:02d} (sf={self.scale_factor}) policies={policy_count} ({phase_label})"
        )
        dfc_1phase_query = get_cached_dfc_1phase_query(policy_count)
        dfc_1phase_optimized_query = get_cached_dfc_1phase_optimized_query(policy_count)

        try:
            dfc_1phase_rewrite_time = 0.0
            dfc_1phase_exec_start = time.perf_counter()
            dfc_1phase_cursor = self.dfc_conn.execute(dfc_1phase_query)
            dfc_1phase_results = dfc_1phase_cursor.fetchall()
            dfc_1phase_exec_time = (time.perf_counter() - dfc_1phase_exec_start) * 1000.0
            dfc_1phase_time = dfc_1phase_rewrite_time + dfc_1phase_exec_time
            dfc_1phase_rows = len(dfc_1phase_results)
            dfc_1phase_error = None
        except Exception as e:
            dfc_1phase_time = 0.0
            dfc_1phase_rewrite_time = 0.0
            dfc_1phase_exec_time = 0.0
            dfc_1phase_results = []
            dfc_1phase_rows = 0
            dfc_1phase_error = str(e)

        try:
            dfc_1phase_optimized_rewrite_time = 0.0
            dfc_1phase_optimized_exec_start = time.perf_counter()
            dfc_1phase_optimized_cursor = self.dfc_conn.execute(dfc_1phase_optimized_query)
            dfc_1phase_optimized_results = dfc_1phase_optimized_cursor.fetchall()
            dfc_1phase_optimized_exec_time = (
                time.perf_counter() - dfc_1phase_optimized_exec_start
            ) * 1000.0
            dfc_1phase_optimized_time = (
                dfc_1phase_optimized_rewrite_time + dfc_1phase_optimized_exec_time
            )
            dfc_1phase_optimized_rows = len(dfc_1phase_optimized_results)
            dfc_1phase_optimized_error = None
        except Exception as e:
            dfc_1phase_optimized_time = 0.0
            dfc_1phase_optimized_rewrite_time = 0.0
            dfc_1phase_optimized_exec_time = 0.0
            dfc_1phase_optimized_results = []
            dfc_1phase_optimized_rows = 0
            dfc_1phase_optimized_error = str(e)

        if dfc_1phase_error is None and dfc_1phase_optimized_error is None:
            (
                dfc_1phase_optimized_match,
                dfc_1phase_optimized_match_error,
            ) = compare_results_exact(dfc_1phase_results, dfc_1phase_optimized_results)
        else:
            dfc_1phase_optimized_match, dfc_1phase_optimized_match_error = None, None

        matches = []
        errors = []
        if dfc_1phase_optimized_match is not None:
            matches.append(dfc_1phase_optimized_match)
            if dfc_1phase_optimized_match_error:
                errors.append(f"dfc_1phase_optimized={dfc_1phase_optimized_match_error}")

        correctness_match = all(matches) if matches else False
        correctness_error = "; ".join(errors) if errors else None
        if dfc_1phase_error or dfc_1phase_optimized_error:
            correctness_error = (
                "Errors: "
                f"dfc_1phase={dfc_1phase_error}, "
                f"dfc_1phase_optimized={dfc_1phase_optimized_error}"
            )

        custom_metrics = {
            "query_num": query_num,
            "query_name": f"q{query_num:02d}",
            "tpch_sf": self.scale_factor,
            "policy_count": policy_count,
            "run_num": run_num or 0,
            "dfc_1phase_time_ms": dfc_1phase_time,
            "dfc_1phase_rewrite_time_ms": dfc_1phase_rewrite_time,
            "dfc_1phase_exec_time_ms": dfc_1phase_exec_time,
            "dfc_1phase_optimized_time_ms": dfc_1phase_optimized_time,
            "dfc_1phase_optimized_rewrite_time_ms": dfc_1phase_optimized_rewrite_time,
            "dfc_1phase_optimized_exec_time_ms": dfc_1phase_optimized_exec_time,
            "dfc_1phase_rows": dfc_1phase_rows,
            "dfc_1phase_optimized_rows": dfc_1phase_optimized_rows,
            "correctness_match": correctness_match,
            "correctness_error": correctness_error or "",
            "dfc_1phase_optimized_match": (
                dfc_1phase_optimized_match if dfc_1phase_optimized_match is not None else ""
            ),
            "dfc_1phase_optimized_match_error": dfc_1phase_optimized_match_error or "",
            "dfc_1phase_error": dfc_1phase_error or "",
            "dfc_1phase_optimized_error": dfc_1phase_optimized_error or "",
        }

        total_time = dfc_1phase_time + dfc_1phase_optimized_time
        return ExperimentResult(duration_ms=total_time, custom_metrics=custom_metrics)

    def teardown(self, _context: ExperimentContext) -> None:
        seen = set()
        for conn_name in ["dfc_conn"]:
            if hasattr(self, conn_name):
                conn = getattr(self, conn_name)
                if id(conn) in seen:
                    continue
                seen.add(id(conn))
                with contextlib.suppress(Exception):
                    conn.close()

    def get_metrics(self) -> list:
        return [
            "query_num",
            "query_name",
            "tpch_sf",
            "policy_count",
            "run_num",
            "dfc_1phase_time_ms",
            "dfc_1phase_rewrite_time_ms",
            "dfc_1phase_exec_time_ms",
            "dfc_1phase_optimized_time_ms",
            "dfc_1phase_optimized_rewrite_time_ms",
            "dfc_1phase_optimized_exec_time_ms",
            "dfc_1phase_rows",
            "dfc_1phase_optimized_rows",
            "correctness_match",
            "correctness_error",
            "dfc_1phase_optimized_match",
            "dfc_1phase_optimized_match_error",
            "dfc_1phase_error",
            "dfc_1phase_optimized_error",
        ]

    def get_setting_key(self, context: ExperimentContext) -> tuple[str, int]:
        policy_count, _ = self._policy_and_run_for_execution(context.execution_number)
        return ("policy_count", policy_count)
