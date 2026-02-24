"""TPC-H experiment strategy for measuring SQL rewriting performance on TPC-H benchmark queries."""

import contextlib
import pathlib
import time

from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import DFCPolicy, Resolution, SQLRewriter

from vldb_experiments.baselines.logical_baseline import rewrite_query_logical
from vldb_experiments.baselines.physical_baseline import execute_query_physical_detailed
from vldb_experiments.correctness import compare_results_exact


def load_tpch_query(query_num: int) -> str:
    """Load a TPC-H query from the benchmarks directory.

    Args:
        query_num: Query number (1-22)

    Returns:
        Query SQL string

    Raises:
        FileNotFoundError: If query file doesn't exist
    """
    # Path from vldb_2026_big_paper_experiments/src/vldb_experiments/strategies/
    # to benchmarks/tpch/queries/ at project root
    benchmarks_dir = pathlib.Path(__file__).parent.parent.parent.parent.parent / "benchmarks" / "tpch" / "queries"
    query_file = benchmarks_dir / f"q{query_num:02d}.sql"

    if not query_file.exists():
        raise FileNotFoundError(f"TPC-H query {query_num} not found at {query_file}")

    return query_file.read_text()


# TPC-H queries to test
TPCH_QUERIES = [1, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 18, 19]

# Policies used in test_tpch.py
lineitem_policy = DFCPolicy(
    sources=["lineitem"],
    constraint="max(lineitem.l_quantity) >= 1",
    on_fail=Resolution.REMOVE,
)

_PHYSICAL_SKIP_QUERIES = {4, 18}

_smokedduck_duckdb = None


def _ensure_smokedduck():
    """Ensure SmokedDuck is set up. Called when needed."""
    global _smokedduck_duckdb
    if _smokedduck_duckdb is None:
        from vldb_experiments.use_local_smokedduck import setup_local_smokedduck
        _smokedduck_duckdb = setup_local_smokedduck()
        if _smokedduck_duckdb is None:
            raise ImportError(
                "SmokedDuck is REQUIRED but not available. "
                "Please run ./setup_venv.sh to clone and build SmokedDuck."
            )
    return _smokedduck_duckdb



class TPCHStrategy(ExperimentStrategy):
    """Strategy for measuring SQL rewriting performance on TPC-H benchmark queries.

    This strategy:
    1. Sets up TPC-H data (scale factor 0.1)
    2. Creates DFC rewriter instance (with policy)
    3. For each TPC-H query, runs four versions:
       - no_policy: Original query without any policy
       - dfc: DFC (SQLRewriter) with policy
       - logical: Logical baseline (CTE-based rewriting)
    4. Returns metrics including execution time and correctness
    """

    def setup(self, context: ExperimentContext) -> None:
        """Set up TPC-H data and rewriter instances.

        Args:
            context: Experiment context with database connection
        """
        self.scale_factor = float(context.strategy_config.get("tpch_sf", 0.1))
        db_path = context.strategy_config.get("tpch_db_path")
        if not db_path:
            db_path = f"./results/tpch_sf{self.scale_factor}.db"
        pathlib.Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.physical_db_path = f"{db_path}_physical"

        self.local_duckdb = _ensure_smokedduck()
        main_conn = self.local_duckdb.connect(self.db_path)

        # Set up TPC-H data on the shared disk-backed connection.
        with contextlib.suppress(Exception):
            main_conn.execute("INSTALL tpch")
        main_conn.execute("LOAD tpch")
        table_exists = main_conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'lineitem'"
        ).fetchone()[0]
        if table_exists == 0:
            main_conn.execute(f"CALL dbgen(sf={self.scale_factor})")

        # Keep all non-physical approaches on the exact same connection.
        self.no_policy_conn = main_conn
        self.dfc_conn = main_conn
        self.logical_conn = main_conn
        self.physical_conn = self.local_duckdb.connect(self.physical_db_path)

        # Ensure TPC-H extension is available for physical connection.
        for conn in [self.physical_conn]:
            with contextlib.suppress(Exception):
                conn.execute("INSTALL tpch")
            conn.execute("LOAD tpch")
            table_exists = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'lineitem'"
            ).fetchone()[0]
            if table_exists == 0:
                conn.execute(f"CALL dbgen(sf={self.scale_factor})")

        # Commit any transactions before creating rewriters (needed for SmokedDuck lineage)
        # DuckDB auto-commits, but SmokedDuck lineage may leave transactions open
        for conn in [main_conn, self.physical_conn]:
            try:
                # Try to commit using SQL
                conn.execute("COMMIT")
            except Exception:
                try:
                    # Try Python commit method
                    conn.commit()
                except Exception:
                    # If commit fails, try to rollback and start fresh
                    with contextlib.suppress(Exception):
                        conn.execute("ROLLBACK")

        # Create DFC rewriter (policy will be registered per execution)
        self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)

        # Store query list in shared state
        context.shared_state["tpch_queries"] = TPCH_QUERIES

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        """Execute TPC-H query with all four approaches.

        Args:
            context: Experiment context with current execution number

        Returns:
            ExperimentResult with timing and performance metrics for all approaches
        """
        tpch_queries = context.shared_state["tpch_queries"]

        # Determine which query to run based on execution number
        # Each execution tests one query
        query_index = (context.execution_number - 1) % len(tpch_queries)
        query_num = tpch_queries[query_index]
        query = load_tpch_query(query_num)

        # Determine which policy to use
        policy = lineitem_policy

        print(f"[Execution {context.execution_number}] TPC-H Q{query_num:02d} (sf={self.scale_factor})")

        # Delete old policies and register new one
        try:
            existing_policies = self.dfc_rewriter.get_dfc_policies()
            for old_policy in existing_policies:
                self.dfc_rewriter.delete_policy(
                    sources=old_policy.sources,
                    constraint=old_policy.constraint,
                    on_fail=old_policy.on_fail
                )
            self.dfc_rewriter.register_policy(policy)
        except Exception:
            # If rewriter/connection is broken, recreate it on the shared disk DB.
            self.dfc_conn = self.local_duckdb.connect(self.db_path)
            self.no_policy_conn = self.dfc_conn
            self.logical_conn = self.dfc_conn
            with contextlib.suppress(Exception):
                self.dfc_conn.execute("INSTALL tpch")
            self.dfc_conn.execute("LOAD tpch")
            table_exists = self.dfc_conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'lineitem'"
            ).fetchone()[0]
            if table_exists == 0:
                self.dfc_conn.execute(f"CALL dbgen(sf={self.scale_factor})")
            self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)
            self.dfc_rewriter.register_policy(policy)

        # Get connections for each approach
        no_policy_conn = self.no_policy_conn
        logical_conn = self.logical_conn
        physical_conn = self.physical_conn

        # 0. Run no_policy baseline (original query without any policy)
        no_policy_start = time.perf_counter()
        try:
            no_policy_cursor = no_policy_conn.execute(query)
            no_policy_results = no_policy_cursor.fetchall()
            no_policy_time = (time.perf_counter() - no_policy_start) * 1000.0
            no_policy_rows = len(no_policy_results)
            no_policy_error = None
        except Exception as e:
            no_policy_time = 0.0
            no_policy_results = []
            no_policy_rows = 0
            no_policy_error = str(e)

        # 1. Run DFC one-phase approach (SQLRewriter with policy)
        dfc_1phase_rewrite_start = time.perf_counter()
        try:
            dfc_1phase_transformed = self.dfc_rewriter.transform_query(query)
            dfc_1phase_rewrite_time = (time.perf_counter() - dfc_1phase_rewrite_start) * 1000.0
            dfc_1phase_exec_start = time.perf_counter()
            dfc_1phase_cursor = self.dfc_conn.execute(dfc_1phase_transformed)
            dfc_1phase_results = dfc_1phase_cursor.fetchall()
            dfc_1phase_exec_time = (time.perf_counter() - dfc_1phase_exec_start) * 1000.0
            dfc_1phase_time = dfc_1phase_rewrite_time + dfc_1phase_exec_time
            dfc_1phase_rows = len(dfc_1phase_results)
            dfc_1phase_error = None
        except Exception as e:
            dfc_1phase_rewrite_time = 0.0
            dfc_1phase_exec_time = 0.0
            dfc_1phase_time = 0.0
            dfc_1phase_results = []
            dfc_1phase_rows = 0
            dfc_1phase_error = str(e)

        # 2. Run DFC two-phase approach
        dfc_2phase_rewrite_start = time.perf_counter()
        try:
            dfc_2phase_transformed = self.dfc_rewriter.transform_query(query, use_two_phase=True)
            dfc_2phase_rewrite_time = (time.perf_counter() - dfc_2phase_rewrite_start) * 1000.0
            dfc_2phase_exec_start = time.perf_counter()
            dfc_2phase_cursor = self.dfc_conn.execute(dfc_2phase_transformed)
            dfc_2phase_results = dfc_2phase_cursor.fetchall()
            dfc_2phase_exec_time = (time.perf_counter() - dfc_2phase_exec_start) * 1000.0
            dfc_2phase_time = dfc_2phase_rewrite_time + dfc_2phase_exec_time
            dfc_2phase_rows = len(dfc_2phase_results)
            dfc_2phase_error = None
        except Exception as e:
            dfc_2phase_rewrite_time = 0.0
            dfc_2phase_exec_time = 0.0
            dfc_2phase_time = 0.0
            dfc_2phase_results = []
            dfc_2phase_rows = 0
            dfc_2phase_error = str(e)

        # 3. Run Logical baseline
        try:
            logical_rewrite_start = time.perf_counter()
            logical_query = rewrite_query_logical(query, policy)
            logical_rewrite_time = (time.perf_counter() - logical_rewrite_start) * 1000.0
            logical_exec_start = time.perf_counter()
            logical_cursor = logical_conn.execute(logical_query)
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

        # 4. Run Physical baseline (SmokedDuck REQUIRED)
        if query_num in _PHYSICAL_SKIP_QUERIES:
            physical_results = []
            physical_error = "skipped_for_physical"
            physical_rewrite_time = 0.0
            physical_base_capture_time = 0.0
            physical_lineage_query_time = 0.0
            physical_runtime = 0.0
            physical_time = 0.0
            physical_rows = 0
        else:
            try:
                (
                    physical_results,
                    physical_timing,
                    physical_error,
                    _base_query_sql,
                    _filter_query_sql,
                ) = execute_query_physical_detailed(
                    physical_conn,
                    query,
                    policy,
                )
                physical_rewrite_time = physical_timing.get("rewrite_time_ms", 0.0)
                physical_base_capture_time = physical_timing.get("base_capture_time_ms", 0.0)
                physical_lineage_query_time = physical_timing.get("lineage_query_time_ms", 0.0)
                physical_runtime = physical_timing.get("runtime_time_ms", 0.0)
                physical_time = physical_runtime
                physical_rows = len(physical_results) if physical_results else 0
            except Exception as e:
                physical_results = []
                physical_error = str(e)
                physical_rewrite_time = 0.0
                physical_base_capture_time = 0.0
                physical_lineage_query_time = 0.0
                physical_runtime = 0.0
                physical_time = 0.0
                physical_rows = 0

        # Verify correctness (compare DFC with logical and physical when available)
        correctness_match = False
        correctness_error = None
        logical_match = None
        logical_match_error = None
        physical_match = None
        physical_match_error = None

        if dfc_1phase_error is None and logical_error is None:
            logical_match, logical_match_error = compare_results_exact(dfc_1phase_results, logical_results)
        if dfc_1phase_error is None and physical_error is None:
            physical_match, physical_match_error = compare_results_exact(dfc_1phase_results, physical_results)
        if dfc_1phase_error is None and dfc_2phase_error is None:
            dfc_2phase_match, dfc_2phase_match_error = compare_results_exact(
                dfc_1phase_results, dfc_2phase_results
            )
        else:
            dfc_2phase_match, dfc_2phase_match_error = None, None

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
        if matches:
            correctness_match = all(matches)
        if errors:
            correctness_error = "; ".join(errors)
        if dfc_1phase_error is not None or dfc_2phase_error is not None:
            correctness_error = (
                "Errors: "
                f"dfc_1phase={dfc_1phase_error}, "
                f"dfc_2phase={dfc_2phase_error}, "
                f"logical={logical_error}, physical={physical_error}"
            )

        # Total execution time
        total_time = no_policy_time + dfc_1phase_time + dfc_2phase_time + logical_time + physical_time

        # Build custom metrics
        custom_metrics = {
            "query_num": query_num,
            "query_name": f"q{query_num:02d}",
            "tpch_sf": self.scale_factor,
            "no_policy_time_ms": no_policy_time,
            "no_policy_exec_time_ms": no_policy_time,
            "dfc_1phase_time_ms": dfc_1phase_time,
            "dfc_1phase_rewrite_time_ms": dfc_1phase_rewrite_time,
            "dfc_1phase_exec_time_ms": dfc_1phase_exec_time,
            "dfc_2phase_time_ms": dfc_2phase_time,
            "dfc_2phase_rewrite_time_ms": dfc_2phase_rewrite_time,
            "dfc_2phase_exec_time_ms": dfc_2phase_exec_time,
            "logical_time_ms": logical_time,
            "logical_rewrite_time_ms": logical_rewrite_time,
            "logical_exec_time_ms": logical_exec_time,
            "physical_time_ms": physical_time,
            "physical_runtime_ms": physical_runtime,
            "physical_rewrite_time_ms": physical_rewrite_time,
            "physical_base_capture_time_ms": physical_base_capture_time,
            "physical_lineage_query_time_ms": physical_lineage_query_time,
            "no_policy_rows": no_policy_rows,
            "dfc_1phase_rows": dfc_1phase_rows,
            "dfc_2phase_rows": dfc_2phase_rows,
            "logical_rows": logical_rows,
            "physical_rows": physical_rows,
            "correctness_match": correctness_match,
            "correctness_error": correctness_error or "",
            "no_policy_error": no_policy_error or "",
            "dfc_1phase_error": dfc_1phase_error or "",
            "dfc_2phase_error": dfc_2phase_error or "",
            "dfc_2phase_match": dfc_2phase_match if dfc_2phase_match is not None else "",
            "dfc_2phase_match_error": dfc_2phase_match_error or "",
            "logical_error": logical_error or "",
            "physical_error": physical_error or "",
        }

        return ExperimentResult(
            duration_ms=total_time,
            custom_metrics=custom_metrics
        )

    def teardown(self, _context: ExperimentContext) -> None:
        """Clean up resources.

        Args:
            context: Experiment context
        """
        # Close DFC rewriter and its connection
        if hasattr(self, "dfc_rewriter"):
            self.dfc_rewriter.close()
        # Close all connections
        seen = set()
        for conn_name in ["no_policy_conn", "dfc_conn", "logical_conn", "physical_conn"]:
            if hasattr(self, conn_name):
                conn = getattr(self, conn_name)
                if id(conn) in seen:
                    continue
                seen.add(id(conn))
                with contextlib.suppress(Exception):
                    conn.close()

    def get_metrics(self) -> list:
        """Return list of custom metric names.

        Returns:
            List of metric name strings
        """
        return [
            "query_num",
            "query_name",
            "tpch_sf",
            "no_policy_time_ms",
            "no_policy_exec_time_ms",
            "dfc_1phase_time_ms",
            "dfc_1phase_rewrite_time_ms",
            "dfc_1phase_exec_time_ms",
            "dfc_2phase_time_ms",
            "dfc_2phase_rewrite_time_ms",
            "dfc_2phase_exec_time_ms",
            "logical_time_ms",
            "logical_rewrite_time_ms",
            "logical_exec_time_ms",
            "physical_time_ms",
            "physical_runtime_ms",
            "physical_rewrite_time_ms",
            "physical_base_capture_time_ms",
            "physical_lineage_query_time_ms",
            "no_policy_rows",
            "dfc_1phase_rows",
            "dfc_2phase_rows",
            "logical_rows",
            "physical_rows",
            "correctness_match",
            "correctness_error",
            "no_policy_error",
            "dfc_1phase_error",
            "dfc_2phase_error",
            "dfc_2phase_match",
            "dfc_2phase_match_error",
            "logical_error",
            "physical_error",
        ]

    def get_setting_key(self, context: ExperimentContext) -> tuple[str, int]:
        tpch_queries = context.shared_state.get("tpch_queries", TPCH_QUERIES)
        query_index = (context.execution_number - 1) % len(tpch_queries)
        query_num = tpch_queries[query_index]
        return ("tpch_query", query_num)
