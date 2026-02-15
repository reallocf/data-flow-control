"""TPC-H Q01 policy complexity strategy for DFC vs Logical overhead."""

import contextlib
import time

from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from shared_sql_utils import combine_expressions_balanced
from sql_rewriter import DFCPolicy, Resolution, SQLRewriter
from sqlglot import exp

from vldb_experiments.baselines.logical_baseline import execute_query_logical_multi
from vldb_experiments.baselines.physical_baseline import execute_query_physical_detailed
from vldb_experiments.correctness import compare_results_exact
from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck, load_tpch_query

DEFAULT_COMPLEXITY_TERMS = [1, 10, 100, 1000]
DEFAULT_WARMUP_PER_LEVEL = 1
DEFAULT_RUNS_PER_LEVEL = 5

_NUMERIC_COLUMNS = [
    "lineitem.l_quantity",
    "lineitem.l_extendedprice",
    "lineitem.l_discount",
    "lineitem.l_tax",
    "lineitem.l_linenumber",
    "lineitem.l_orderkey",
    "lineitem.l_partkey",
    "lineitem.l_suppkey",
]


def _build_complex_expression(term_count: int) -> str:
    if term_count <= 0:
        raise ValueError("term_count must be positive")

    terms = [_NUMERIC_COLUMNS[i % len(_NUMERIC_COLUMNS)] for i in range(term_count)]
    return combine_expressions_balanced(terms, exp.Add, dialect="duckdb")


def build_tpch_q01_complexity_policy(term_count: int) -> DFCPolicy:
    """Build a single policy with a complex aggregate predicate."""
    expression = _build_complex_expression(term_count)
    constraint = f"max({expression}) >= 0"
    return DFCPolicy(
        sources=["lineitem"],
        constraint=constraint,
        on_fail=Resolution.REMOVE,
        description=f"q01_complexity_terms_{term_count}",
    )


class TPCHPolicyComplexityStrategy(ExperimentStrategy):
    """Strategy for measuring performance vs predicate complexity on TPC-H Q01."""

    def setup(self, context: ExperimentContext) -> None:
        main_conn = context.database_connection
        if main_conn is None:
            raise ValueError("Database connection required in context")

        self.scale_factor = float(context.strategy_config.get("tpch_sf", 1))
        db_path = context.strategy_config.get("tpch_db_path")

        complexity_terms = context.strategy_config.get("complexity_terms", DEFAULT_COMPLEXITY_TERMS)
        warmup_per_level = int(context.strategy_config.get("warmup_per_level", DEFAULT_WARMUP_PER_LEVEL))
        runs_per_level = int(context.strategy_config.get("runs_per_level", DEFAULT_RUNS_PER_LEVEL))

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
        local_duckdb = _ensure_smokedduck()
        self.local_duckdb = local_duckdb
        self.no_policy_conn = self.local_duckdb.connect(target_db)
        self.dfc_conn = self.local_duckdb.connect(target_db)
        self.logical_conn = self.local_duckdb.connect(target_db)
        physical_db_path = f"{db_path}_physical" if db_path else None
        self.physical_conn = local_duckdb.connect(physical_db_path or ":memory:")

        for conn in [self.no_policy_conn, self.dfc_conn, self.logical_conn, self.physical_conn]:
            with contextlib.suppress(Exception):
                conn.execute("INSTALL tpch")
            conn.execute("LOAD tpch")
            if not db_path:
                conn.execute(f"CALL dbgen(sf={self.scale_factor})")

        if physical_db_path:
            table_exists = self.physical_conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'lineitem'"
            ).fetchone()[0]
            if table_exists == 0:
                self.physical_conn.execute(f"CALL dbgen(sf={self.scale_factor})")

        for conn in [self.no_policy_conn, self.dfc_conn, self.logical_conn, self.physical_conn]:
            try:
                conn.execute("COMMIT")
            except Exception:
                try:
                    conn.commit()
                except Exception:
                    with contextlib.suppress(Exception):
                        conn.execute("ROLLBACK")

        self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)

        self.complexity_terms = list(complexity_terms)
        self.warmup_per_level = warmup_per_level
        self.runs_per_level = runs_per_level
        query_num = int(context.strategy_config.get("tpch_query", 1))
        context.shared_state["tpch_query_num"] = query_num
        context.shared_state["tpch_query"] = load_tpch_query(query_num)

    def _complexity_and_run_for_execution(self, execution_number: int) -> tuple[int, int]:
        level_index = (execution_number - 1) // self.runs_per_level
        run_num = ((execution_number - 1) % self.runs_per_level) + 1
        return self.complexity_terms[level_index], run_num

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        query = context.shared_state["tpch_query"]
        query_num = context.shared_state["tpch_query_num"]
        term_count, run_num = self._complexity_and_run_for_execution(context.execution_number)

        phase_label = "warmup" if context.is_warmup else f"run {run_num}"
        print(
            f"[Execution {context.execution_number}] "
            f"TPC-H Q{query_num:02d} (sf={self.scale_factor}) terms={term_count} ({phase_label})"
        )

        policy = build_tpch_q01_complexity_policy(term_count)
        policies = [policy]

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
            with contextlib.suppress(Exception):
                self.dfc_conn.execute("INSTALL tpch")
            self.dfc_conn.execute("LOAD tpch")
            self.dfc_conn.execute(f"CALL dbgen(sf={self.scale_factor})")
            self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)
            self.dfc_rewriter.register_policy(policy)

        try:
            no_policy_start = time.perf_counter()
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

        try:
            (
                physical_results,
                physical_timing,
                physical_error,
                _physical_base_sql,
                _physical_filter_sql,
            ) = execute_query_physical_detailed(self.physical_conn, query, policies)
            physical_rewrite_time = physical_timing.get("rewrite_time_ms", 0.0)
            physical_base_capture_time = physical_timing.get("base_capture_time_ms", 0.0)
            physical_lineage_query_time = physical_timing.get("lineage_query_time_ms", 0.0)
            physical_runtime = physical_timing.get("runtime_time_ms", 0.0)
            physical_exec_time = physical_runtime
            physical_time = physical_runtime
            physical_rows = len(physical_results) if physical_results else 0
        except Exception as e:
            physical_results = []
            physical_error = str(e)
            physical_rewrite_time = 0.0
            physical_base_capture_time = 0.0
            physical_lineage_query_time = 0.0
            physical_runtime = 0.0
            physical_exec_time = 0.0
            physical_time = 0.0
            physical_rows = 0

        logical_match = None
        logical_match_error = None
        physical_match = None
        physical_match_error = None
        if dfc_error is None and logical_error is None:
            logical_match, logical_match_error = compare_results_exact(dfc_results, logical_results)
        if dfc_error is None and physical_error is None:
            physical_match, physical_match_error = compare_results_exact(dfc_results, physical_results)

        matches = []
        errors = []
        if logical_match is not None:
            matches.append(logical_match)
            if logical_match_error:
                errors.append(f"logical={logical_match_error}")
        if physical_match is not None:
            matches.append(physical_match)
            if physical_match_error:
                errors.append(f"physical={physical_match_error}")

        correctness_match = all(matches) if matches else False
        correctness_error = "; ".join(errors) if errors else None
        if dfc_error or logical_error or physical_error:
            correctness_error = (
                f"Errors: dfc={dfc_error}, logical={logical_error}, physical={physical_error}"
            )

        no_policy_match = False
        no_policy_compare_error = None
        if dfc_error is None and no_policy_error is None:
            match, error = compare_results_exact(dfc_results, no_policy_results)
            no_policy_match = match
            no_policy_compare_error = error
        else:
            no_policy_compare_error = f"Errors: dfc={dfc_error}, no_policy={no_policy_error}"

        custom_metrics = {
            "query_num": query_num,
            "query_name": f"q{query_num:02d}",
            "tpch_sf": self.scale_factor,
            "complexity_terms": term_count,
            "run_num": run_num or 0,
            "no_policy_time_ms": no_policy_time,
            "no_policy_exec_time_ms": no_policy_time,
            "dfc_time_ms": dfc_time,
            "dfc_rewrite_time_ms": dfc_rewrite_time,
            "dfc_exec_time_ms": dfc_exec_time,
            "logical_time_ms": logical_time,
            "logical_rewrite_time_ms": logical_rewrite_time,
            "logical_exec_time_ms": logical_exec_time,
            "physical_time_ms": physical_time,
            "physical_runtime_ms": physical_runtime,
            "physical_exec_time_ms": physical_exec_time,
            "physical_rewrite_time_ms": physical_rewrite_time,
            "physical_base_capture_time_ms": physical_base_capture_time,
            "physical_lineage_query_time_ms": physical_lineage_query_time,
            "no_policy_rows": no_policy_rows,
            "dfc_rows": dfc_rows,
            "logical_rows": logical_rows,
            "physical_rows": physical_rows,
            "correctness_match": correctness_match,
            "correctness_error": correctness_error or "",
            "no_policy_match": no_policy_match,
            "no_policy_error": no_policy_compare_error or "",
            "logical_match": logical_match if logical_match is not None else "",
            "logical_match_error": logical_match_error or "",
            "physical_match": physical_match if physical_match is not None else "",
            "physical_match_error": physical_match_error or "",
            "dfc_error": dfc_error or "",
            "logical_error": logical_error or "",
            "physical_error": physical_error or "",
        }

        total_time = no_policy_time + dfc_time + logical_time + physical_time
        return ExperimentResult(duration_ms=total_time, custom_metrics=custom_metrics)

    def teardown(self, _context: ExperimentContext) -> None:
        if hasattr(self, "dfc_rewriter"):
            self.dfc_rewriter.close()
        for conn_name in ["no_policy_conn", "dfc_conn", "logical_conn", "physical_conn"]:
            if hasattr(self, conn_name):
                with contextlib.suppress(Exception):
                    getattr(self, conn_name).close()

    def get_metrics(self) -> list:
        return [
            "query_num",
            "query_name",
            "tpch_sf",
            "complexity_terms",
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
            "no_policy_match",
            "no_policy_error",
            "logical_match",
            "logical_match_error",
            "physical_match",
            "physical_match_error",
            "dfc_error",
            "logical_error",
            "physical_error",
        ]

    def get_setting_key(self, context: ExperimentContext) -> tuple[str, int]:
        term_count, _ = self._complexity_and_run_for_execution(context.execution_number)
        return ("complexity_terms", term_count)
