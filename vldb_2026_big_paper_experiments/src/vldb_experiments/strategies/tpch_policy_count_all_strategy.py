"""TPC-H policy count (fixed) strategy over all supported queries."""

import contextlib
import time

import duckdb
from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import SQLRewriter

from vldb_experiments.baselines.logical_baseline import rewrite_query_logical_multi
from vldb_experiments.correctness import compare_results_exact
from vldb_experiments.strategies.tpch_policy_count_strategy import build_tpch_q01_policies
from vldb_experiments.strategies.tpch_strategy import TPCH_QUERIES, load_tpch_query

DEFAULT_POLICY_COUNT = 1000


class TPCHPolicyCountAllQueriesStrategy(ExperimentStrategy):
    """Run policy-count experiment (fixed count) across all supported TPC-H queries."""

    def setup(self, context: ExperimentContext) -> None:
        main_conn = context.database_connection
        if main_conn is None:
            raise ValueError("Database connection required in context")

        self.scale_factor = float(context.strategy_config.get("tpch_sf", 1))
        db_path = context.strategy_config.get("tpch_db_path")
        self.policy_count = int(context.strategy_config.get("policy_count", DEFAULT_POLICY_COUNT))

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
        self.no_policy_conn = duckdb.connect(target_db)
        self.dfc_conn = duckdb.connect(target_db)
        self.logical_conn = duckdb.connect(target_db)

        for conn in [self.no_policy_conn, self.dfc_conn, self.logical_conn]:
            with contextlib.suppress(Exception):
                conn.execute("INSTALL tpch")
            conn.execute("LOAD tpch")
            if not db_path:
                conn.execute(f"CALL dbgen(sf={self.scale_factor})")

        for conn in [self.no_policy_conn, self.dfc_conn, self.logical_conn]:
            try:
                conn.execute("COMMIT")
            except Exception:
                try:
                    conn.commit()
                except Exception:
                    with contextlib.suppress(Exception):
                        conn.execute("ROLLBACK")

        self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)

        context.shared_state["tpch_queries"] = TPCH_QUERIES

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        tpch_queries = context.shared_state["tpch_queries"]
        query_index = (context.execution_number - 1) % len(tpch_queries)
        query_num = tpch_queries[query_index]
        query = load_tpch_query(query_num)

        print(
            f"[Execution {context.execution_number}] "
            f"TPC-H Q{query_num:02d} (sf={self.scale_factor}) policies={self.policy_count}"
        )

        policies = build_tpch_q01_policies(self.policy_count)
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

        no_policy_start = time.perf_counter()
        try:
            no_policy_cursor = self.no_policy_conn.execute(query)
            no_policy_results = no_policy_cursor.fetchall()
            no_policy_time = (time.perf_counter() - no_policy_start) * 1000.0
            no_policy_rows = len(no_policy_results)
            no_policy_error = None
        except Exception as e:
            no_policy_time = 0.0
            no_policy_results = []
            no_policy_rows = 0
            no_policy_error = str(e)

        dfc_rewrite_start = time.perf_counter()
        try:
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
            dfc_rewrite_time = 0.0
            dfc_exec_time = 0.0
            dfc_time = 0.0
            dfc_results = []
            dfc_rows = 0
            dfc_error = str(e)

        try:
            logical_rewrite_start = time.perf_counter()
            logical_query = rewrite_query_logical_multi(query, policies)
            logical_rewrite_time = (time.perf_counter() - logical_rewrite_start) * 1000.0
            logical_exec_start = time.perf_counter()
            logical_cursor = self.logical_conn.execute(logical_query)
            logical_results = logical_cursor.fetchall()
            logical_exec_time = (time.perf_counter() - logical_exec_start) * 1000.0
            logical_time = logical_rewrite_time + logical_exec_time
            logical_rows = len(logical_results)
            logical_error = None
        except Exception as e:
            logical_rewrite_time = 0.0
            logical_exec_time = 0.0
            logical_time = 0.0
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

        total_time = no_policy_time + dfc_time + logical_time
        custom_metrics = {
            "query_num": query_num,
            "query_name": f"q{query_num:02d}",
            "tpch_sf": self.scale_factor,
            "policy_count": self.policy_count,
            "no_policy_time_ms": no_policy_time,
            "no_policy_exec_time_ms": no_policy_time,
            "dfc_time_ms": dfc_time,
            "dfc_rewrite_time_ms": dfc_rewrite_time,
            "dfc_exec_time_ms": dfc_exec_time,
            "logical_time_ms": logical_time,
            "logical_rewrite_time_ms": logical_rewrite_time,
            "logical_exec_time_ms": logical_exec_time,
            "no_policy_rows": no_policy_rows,
            "dfc_rows": dfc_rows,
            "logical_rows": logical_rows,
            "correctness_match": correctness_match,
            "correctness_error": correctness_error or "",
            "no_policy_error": no_policy_error or "",
            "dfc_error": dfc_error or "",
            "logical_error": logical_error or "",
        }

        return ExperimentResult(duration_ms=total_time, custom_metrics=custom_metrics)

    def teardown(self, _context: ExperimentContext) -> None:
        if hasattr(self, "dfc_rewriter"):
            self.dfc_rewriter.close()
        for conn_name in ["no_policy_conn", "dfc_conn", "logical_conn"]:
            if hasattr(self, conn_name):
                with contextlib.suppress(Exception):
                    getattr(self, conn_name).close()

    def get_metrics(self) -> list:
        return [
            "query_num",
            "query_name",
            "tpch_sf",
            "policy_count",
            "no_policy_time_ms",
            "no_policy_exec_time_ms",
            "dfc_time_ms",
            "dfc_rewrite_time_ms",
            "dfc_exec_time_ms",
            "logical_time_ms",
            "logical_rewrite_time_ms",
            "logical_exec_time_ms",
            "no_policy_rows",
            "dfc_rows",
            "logical_rows",
            "correctness_match",
            "correctness_error",
            "no_policy_error",
            "dfc_error",
            "logical_error",
        ]
