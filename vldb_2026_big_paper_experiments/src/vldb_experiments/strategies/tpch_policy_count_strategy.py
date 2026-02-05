"""TPC-H Q01 policy count strategy for measuring DFC vs Logical overhead."""

import contextlib
import time

import duckdb
from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import DFCPolicy, Resolution, SQLRewriter

from vldb_experiments.baselines.logical_baseline import execute_query_logical_multi
from vldb_experiments.correctness import compare_results_exact
from vldb_experiments.strategies.tpch_strategy import load_tpch_query

DEFAULT_POLICY_COUNTS = [1, 10, 100, 1000]
DEFAULT_WARMUP_PER_POLICY = 1
DEFAULT_RUNS_PER_POLICY = 5


def build_tpch_q01_policies(num_policies: int) -> list[DFCPolicy]:
    """Build a list of distinct policies for TPC-H Q01 on lineitem.

    Policies are constructed to be logically true while using diverse aggregates.
    """
    policies = []
    templates = [
        "max(lineitem.l_quantity + {i}) >= {i_plus}",
        "min(lineitem.l_quantity + {i}) >= {i_plus}",
        "sum(lineitem.l_quantity + {i}) >= {i_plus}",
        "avg(lineitem.l_quantity + {i}) >= {i_plus}",
        "count(lineitem.l_quantity) >= 1 + 0 * {i}",
    ]
    for i in range(num_policies):
        template = templates[i % len(templates)]
        constraint = template.format(i=i, i_plus=i + 1)
        policies.append(
            DFCPolicy(
                source="lineitem",
                constraint=constraint,
                on_fail=Resolution.REMOVE,
                description=f"q01_policy_{i + 1}",
            )
        )
    return policies


class TPCHPolicyCountStrategy(ExperimentStrategy):
    """Strategy for measuring performance vs number of policies on TPC-H Q01."""

    def setup(self, context: ExperimentContext) -> None:
        main_conn = context.database_connection
        if main_conn is None:
            raise ValueError("Database connection required in context")

        self.scale_factor = float(context.strategy_config.get("tpch_sf", 1))
        db_path = context.strategy_config.get("tpch_db_path")

        policy_counts = context.strategy_config.get("policy_counts", DEFAULT_POLICY_COUNTS)
        warmup_per_policy = int(context.strategy_config.get("warmup_per_policy", DEFAULT_WARMUP_PER_POLICY))
        runs_per_policy = int(context.strategy_config.get("runs_per_policy", DEFAULT_RUNS_PER_POLICY))

        # Set up TPC-H data on the main connection
        with contextlib.suppress(Exception):
            main_conn.execute("INSTALL tpch")
        main_conn.execute("LOAD tpch")
        if db_path:
            table_exists = main_conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'lineitem'"
            ).fetchone()[0]
            if table_exists == 0:
                main_conn.execute(f"CALL dbgen(sf={self.scale_factor})")
        else:
            main_conn.execute(f"CALL dbgen(sf={self.scale_factor})")

        target_db = db_path or ":memory:"
        self.dfc_conn = duckdb.connect(target_db)
        self.logical_conn = duckdb.connect(target_db)

        for conn in [self.dfc_conn, self.logical_conn]:
            with contextlib.suppress(Exception):
                conn.execute("INSTALL tpch")
            conn.execute("LOAD tpch")
            if not db_path:
                conn.execute(f"CALL dbgen(sf={self.scale_factor})")

        for conn in [self.dfc_conn, self.logical_conn]:
            try:
                conn.execute("COMMIT")
            except Exception:
                try:
                    conn.commit()
                except Exception:
                    with contextlib.suppress(Exception):
                        conn.execute("ROLLBACK")

        self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)

        context.shared_state["policy_counts"] = list(policy_counts)
        context.shared_state["warmup_per_policy"] = warmup_per_policy
        context.shared_state["runs_per_policy"] = runs_per_policy
        context.shared_state["global_execution_index"] = 0
        query_num = int(context.strategy_config.get("tpch_query", 1))
        context.shared_state["tpch_query_num"] = query_num
        context.shared_state["tpch_query"] = load_tpch_query(query_num)

    def _get_policy_count_for_execution(self, context: ExperimentContext) -> tuple[int, int | None, bool]:
        policy_counts = context.shared_state["policy_counts"]
        warmup_per_policy = context.shared_state["warmup_per_policy"]
        runs_per_policy = context.shared_state["runs_per_policy"]
        warmup_total = len(policy_counts) * warmup_per_policy

        global_index = context.shared_state["global_execution_index"] + 1
        context.shared_state["global_execution_index"] = global_index

        if global_index <= warmup_total:
            policy_index = (global_index - 1) // warmup_per_policy
            return policy_counts[policy_index], None, True

        run_index = global_index - warmup_total - 1
        policy_index = run_index // runs_per_policy
        run_num = (run_index % runs_per_policy) + 1

        return policy_counts[policy_index], run_num, False

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        query = context.shared_state["tpch_query"]
        query_num = context.shared_state["tpch_query_num"]
        policy_count, run_num, is_warmup = self._get_policy_count_for_execution(context)

        phase_label = "warmup" if is_warmup else f"run {run_num}"
        print(
            f"[Execution {context.shared_state['global_execution_index']}] "
            f"TPC-H Q{query_num:02d} (sf={self.scale_factor}) policies={policy_count} ({phase_label})"
        )

        policies = build_tpch_q01_policies(policy_count)

        try:
            existing_policies = self.dfc_rewriter.get_dfc_policies()
            for old_policy in existing_policies:
                self.dfc_rewriter.delete_policy(
                    source=old_policy.source,
                    constraint=old_policy.constraint,
                    on_fail=old_policy.on_fail,
                )
            for policy in policies:
                self.dfc_rewriter.register_policy(policy)
        except Exception:
            self.dfc_conn = duckdb.connect(":memory:")
            with contextlib.suppress(Exception):
                self.dfc_conn.execute("INSTALL tpch")
            self.dfc_conn.execute("LOAD tpch")
            self.dfc_conn.execute(f"CALL dbgen(sf={self.scale_factor})")
            self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)
            for policy in policies:
                self.dfc_rewriter.register_policy(policy)

        try:
            dfc_rewrite_start = time.perf_counter()
            dfc_transformed = self.dfc_rewriter.transform_query(query)
            dfc_rewrite_time = (time.perf_counter() - dfc_rewrite_start) * 1000.0
            dfc_exec_start = time.perf_counter()
            dfc_cursor = self.dfc_conn.execute(dfc_transformed)
            dfc_results = dfc_cursor.fetchall()
            dfc_exec_time = (time.perf_counter() - dfc_exec_start) * 1000.0
            dfc_time = dfc_rewrite_time + dfc_exec_time
            dfc_rows = len(dfc_results)
            dfc_error = None
        except Exception as e:
            dfc_time = 0.0
            dfc_rewrite_time = 0.0
            dfc_exec_time = 0.0
            dfc_results = []
            dfc_rows = 0
            dfc_error = str(e)

        try:
            logical_results, logical_rewrite_time, logical_exec_time = execute_query_logical_multi(
                self.logical_conn, query, policies
            )
            logical_time = logical_rewrite_time + logical_exec_time
            logical_rows = len(logical_results)
            logical_error = None
        except Exception as e:
            logical_time = 0.0
            logical_rewrite_time = 0.0
            logical_exec_time = 0.0
            logical_results = []
            logical_rows = 0
            logical_error = str(e)

        correctness_match = False
        correctness_error = None
        if dfc_error is None and logical_error is None:
            match, error = compare_results_exact(dfc_results, logical_results)
            correctness_match = match
            correctness_error = error
        else:
            correctness_error = f"Errors: dfc={dfc_error}, logical={logical_error}"

        custom_metrics = {
            "query_num": query_num,
            "query_name": f"q{query_num:02d}",
            "tpch_sf": self.scale_factor,
            "policy_count": policy_count,
            "run_num": run_num or 0,
            "dfc_time_ms": dfc_time,
            "dfc_rewrite_time_ms": dfc_rewrite_time,
            "dfc_exec_time_ms": dfc_exec_time,
            "logical_time_ms": logical_time,
            "logical_rewrite_time_ms": logical_rewrite_time,
            "logical_exec_time_ms": logical_exec_time,
            "dfc_rows": dfc_rows,
            "logical_rows": logical_rows,
            "correctness_match": correctness_match,
            "correctness_error": correctness_error or "",
            "dfc_error": dfc_error or "",
            "logical_error": logical_error or "",
        }

        total_time = dfc_time + logical_time
        return ExperimentResult(duration_ms=total_time, custom_metrics=custom_metrics)

    def teardown(self, _context: ExperimentContext) -> None:
        if hasattr(self, "dfc_rewriter"):
            self.dfc_rewriter.close()
        for conn_name in ["dfc_conn", "logical_conn"]:
            if hasattr(self, conn_name):
                with contextlib.suppress(Exception):
                    getattr(self, conn_name).close()

    def get_metrics(self) -> list:
        return [
            "query_num",
            "query_name",
            "tpch_sf",
            "policy_count",
            "run_num",
            "dfc_time_ms",
            "dfc_rewrite_time_ms",
            "dfc_exec_time_ms",
            "logical_time_ms",
            "logical_rewrite_time_ms",
            "logical_exec_time_ms",
            "dfc_rows",
            "logical_rows",
            "correctness_match",
            "correctness_error",
            "dfc_error",
            "logical_error",
        ]
