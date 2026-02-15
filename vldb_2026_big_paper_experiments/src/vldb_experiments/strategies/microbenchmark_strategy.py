"""Microbenchmark experiment strategy for measuring SQL rewriting performance."""

import contextlib
import time

from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import SQLRewriter

from vldb_experiments.baselines.logical_baseline import (
    rewrite_query_logical,
    rewrite_query_logical_multi,
)
from vldb_experiments.baselines.physical_baseline import execute_query_physical_detailed
from vldb_experiments.correctness import compare_results_exact
from vldb_experiments.data_setup import (
    setup_test_data,
    setup_test_data_with_groups,
    setup_test_data_with_join_group_by,
    setup_test_data_with_join_matches,
)
from vldb_experiments.policy_setup import create_test_policies
from vldb_experiments.query_definitions import get_query_definitions, get_query_order
from vldb_experiments.variations import generate_variation_parameters

# SmokedDuck is REQUIRED for physical baseline
# Only set up when actually needed (lazy import to allow testing without SmokedDuck)
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


class MicrobenchmarkStrategy(ExperimentStrategy):
    """Strategy for measuring SQL rewriting performance impact.

    This strategy:
    1. Sets up fixed test data
    2. Creates DFC rewriter instance (with policy)
    3. For each query type, runs four versions:
       - no_policy: Original query without any policy
       - dfc: DFC (SQLRewriter) with policy
       - logical: Logical baseline (CTE-based rewriting)
       - physical: Physical baseline (SmokedDuck lineage)
    4. Returns metrics including execution time and correctness
    """

    def __init__(
        self,
        policy_count: int = 1,
        num_variations: int = 4,
        num_runs_per_variation: int = 5,
        enable_physical: bool | None = None,
        query_types: list[str] | None = None,
    ) -> None:
        """Initialize the microbenchmark strategy.

        Args:
            policy_count: Number of policies to register per run (default: 1)
            num_variations: Number of variation values per query type (default: 4)
            num_runs_per_variation: Number of runs per variation (default: 5)
        """
        self.policy_count = policy_count
        self.num_variations = num_variations
        self.num_runs_per_variation = num_runs_per_variation
        self.enable_physical_override = enable_physical
        self.query_types = query_types
        self._policy_cache: dict[tuple[str, int], list] = {}
        self._active_policy_signature: tuple[str, int] | None = None

    def _get_policies(self, signature: str, threshold: int | None = None) -> list:
        cache_key = (signature, self.policy_count)
        if cache_key in self._policy_cache:
            return self._policy_cache[cache_key]

        if signature == "threshold":
            if threshold is None:
                raise ValueError("threshold is required for threshold policy signature")
            policies = create_test_policies(
                threshold=threshold,
                policy_count=self.policy_count,
            )
        else:
            policies = create_test_policies(policy_count=self.policy_count)

        self._policy_cache[cache_key] = policies
        return policies

    def _ensure_policies(self, signature: tuple[str, int], policies: list) -> None:
        if self._active_policy_signature == signature:
            return

        # Clear any active transaction before mutating policies.
        with contextlib.suppress(Exception):
            self.dfc_conn.execute("ROLLBACK")

        existing_policies = self.dfc_rewriter.get_dfc_policies()
        for old_policy in existing_policies:
            self.dfc_rewriter.delete_policy(
                sources=old_policy.sources,
                constraint=old_policy.constraint,
                on_fail=old_policy.on_fail,
            )
        for policy in policies:
            self.dfc_rewriter.register_policy(policy)
        self._active_policy_signature = signature

    def _physical_supported_for_query(self, query_type: str) -> bool:
        """Return whether the physical baseline is supported for this query type."""
        return query_type in {"GROUP_BY", "JOIN_GROUP_BY"}

    def _build_join_group_by_query(self, join_count: int) -> str:
        """Build a JOIN->GROUP_BY query with the requested number of joins."""
        if join_count < 1:
            raise ValueError(f"join_count must be >= 1, got {join_count}")

        amount_terms = ["test_data.amount"] + [f"j{i}.amount" for i in range(1, join_count + 1)]
        joins = [
            f"JOIN join_data_{i} j{i} ON test_data.id = j{i}.id"
            for i in range(1, join_count + 1)
        ]

        query = f"""
            SELECT test_data.category, COUNT(*), SUM({" + ".join(amount_terms)}) AS total_amount
            FROM test_data
            {" ".join(joins)}
            GROUP BY test_data.category
        """
        return " ".join(query.split())

    def setup(self, context: ExperimentContext) -> None:
        """Set up test data and rewriter instances.

        Args:
            context: Experiment context with database connection
        """
        self.enable_physical = (
            self.enable_physical_override
            if self.enable_physical_override is not None
            else self.policy_count <= 1
        )
        query_order = self.query_types or get_query_order()
        self._physical_enabled_for_run = (
            self.enable_physical
            and any(self._physical_supported_for_query(query) for query in query_order)
        )
        # Use locally built SmokedDuck DuckDB for all benchmark connections.
        self.local_duckdb = _ensure_smokedduck()
        local_duckdb = self.local_duckdb if self._physical_enabled_for_run else None

        # Use the connection from context to set up data
        # But create separate connections for each rewriter to avoid UDF conflicts
        main_conn = context.database_connection
        if main_conn is None:
            raise ValueError("Database connection required in context")

        # Set up fixed test data on the main connection
        setup_test_data(main_conn, num_rows=1_000_000)

        # Create separate connections for each approach to avoid conflicts
        # Use local DuckDB for physical connection (SmokedDuck build)
        self.no_policy_conn = self.local_duckdb.connect(":memory:")
        self.dfc_conn = self.local_duckdb.connect(":memory:")
        self.logical_conn = self.local_duckdb.connect(":memory:")
        self.physical_conn = local_duckdb.connect(":memory:") if local_duckdb else None

        # Set up data in each connection
        setup_test_data(self.no_policy_conn, num_rows=1_000_000)
        setup_test_data(self.dfc_conn, num_rows=1_000_000)
        setup_test_data(self.logical_conn, num_rows=1_000_000)
        if self.physical_conn is not None:
            setup_test_data(self.physical_conn, num_rows=1_000_000)

        # Commit any transactions before creating rewriters (needed for SmokedDuck lineage)
        # DuckDB auto-commits, but SmokedDuck lineage may leave transactions open
        for conn in [self.no_policy_conn, self.dfc_conn, self.logical_conn, self.physical_conn]:
            if conn is None:
                continue
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

        # Create DFC rewriter (policies will be registered per execution with variations)
        self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)
        self._active_policy_signature = None
        self._max_join_group_by_count_seen = 0

        # Store queries in shared state
        context.shared_state["queries"] = get_query_definitions()
        context.shared_state["query_order"] = query_order
        context.shared_state["current_query_index"] = 0

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        """Execute microbenchmark for current query with all four approaches.

        Args:
            context: Experiment context with current execution number

        Returns:
            ExperimentResult with timing and performance metrics for all approaches
        """
        queries = context.shared_state["queries"]
        query_order = context.shared_state["query_order"]

        # Determine which query to run based on execution number
        # Each execution tests one query type
        query_index = (context.execution_number - 1) % len(query_order)
        query_type = query_order[query_index]
        query = queries[query_type]

        # Generate variation parameters for this execution
        # Structure: 4 variations x 5 runs = 20 executions per query type
        variation_params = generate_variation_parameters(
            query_type=query_type,
            execution_number=context.execution_number,
            num_variations=self.num_variations,
            num_runs_per_variation=self.num_runs_per_variation,
            num_query_types=len(query_order)
        )

        # Print execution details for logging
        variation_info = ""
        if query_type in ["SELECT", "WHERE", "ORDER_BY"]:
            variation_info = f"rows_to_remove={variation_params.get('rows_to_remove', 'N/A')}"
        elif query_type == "JOIN":
            variation_info = f"join_matches={variation_params.get('join_matches', 'N/A')}"
        elif query_type == "GROUP_BY":
            variation_info = f"num_groups={variation_params.get('num_groups', 'N/A')}"
        elif query_type == "JOIN_GROUP_BY":
            variation_info = f"join_count={variation_params.get('join_count', 'N/A')}"
        print(f"[Execution {context.execution_number}] {query_type} - Variation {variation_params.get('variation_num', 'N/A')}, Run {variation_params.get('run_num', 'N/A')} ({variation_info})")

        # Get connections for each approach
        no_policy_conn = self.no_policy_conn
        logical_conn = self.logical_conn
        physical_conn = self.physical_conn

        # Setup data and policies based on variation type
        if query_type in ["SELECT", "WHERE", "ORDER_BY"]:
            # Vary policy threshold - no need to regenerate data
            policy_threshold = variation_params["policy_threshold"]
            policies = self._get_policies("threshold", threshold=policy_threshold)
            self._ensure_policies(("threshold", policy_threshold), policies)

        elif query_type == "JOIN":
            # Vary join matches - regenerate data
            join_matches = variation_params["join_matches"]
            # Drop and recreate tables with new data
            for conn in [no_policy_conn, self.dfc_conn, logical_conn, physical_conn]:
                if conn is None:
                    continue
                with contextlib.suppress(Exception):
                    conn.execute("DROP TABLE IF EXISTS test_data")
                setup_test_data_with_join_matches(conn, num_rows=1_000_000, join_matches=join_matches)
            # Use default policies for JOIN
            policies = self._get_policies("default")
            # Delete old policies
            try:
                self._ensure_policies(("default", 0), policies)
            except Exception:
                # If rewriter/connection is broken, recreate it
                self.dfc_conn = self.local_duckdb.connect(":memory:")
                setup_test_data_with_join_matches(self.dfc_conn, num_rows=1_000_000, join_matches=join_matches)
                self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)
                self._active_policy_signature = None
                self._ensure_policies(("default", 0), policies)

        elif query_type == "GROUP_BY":
            # Vary number of groups - regenerate data
            num_groups = variation_params["num_groups"]
            # Drop and recreate tables with new data
            for conn in [no_policy_conn, self.dfc_conn, logical_conn, physical_conn]:
                if conn is None:
                    continue
                with contextlib.suppress(Exception):
                    conn.execute("DROP TABLE IF EXISTS test_data")
                setup_test_data_with_groups(conn, num_rows=1_000_000, num_groups=num_groups)
            # Use default policies for GROUP_BY
            policies = self._get_policies("default")
            # Delete old policies
            try:
                self._ensure_policies(("default", 0), policies)
            except Exception:
                # If rewriter/connection is broken, recreate it
                self.dfc_conn = self.local_duckdb.connect(":memory:")
                setup_test_data_with_groups(self.dfc_conn, num_rows=1_000_000, num_groups=num_groups)
                self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)
                self._active_policy_signature = None
                self._ensure_policies(("default", 0), policies)

        elif query_type == "JOIN_GROUP_BY":
            join_count = variation_params["join_count"]
            query = self._build_join_group_by_query(join_count)

            drop_join_limit = max(self._max_join_group_by_count_seen, join_count)
            for conn in [no_policy_conn, self.dfc_conn, logical_conn, physical_conn]:
                if conn is None:
                    continue
                with contextlib.suppress(Exception):
                    conn.execute("DROP TABLE IF EXISTS test_data")
                for idx in range(1, drop_join_limit + 1):
                    with contextlib.suppress(Exception):
                        conn.execute(f"DROP TABLE IF EXISTS join_data_{idx}")
                setup_test_data_with_join_group_by(
                    conn=conn,
                    join_count=join_count,
                    num_rows=1_000,
                )
            self._max_join_group_by_count_seen = max(
                self._max_join_group_by_count_seen,
                join_count,
            )

            policies = self._get_policies("default")
            try:
                self._ensure_policies(("default", 0), policies)
            except Exception:
                self.dfc_conn = self.local_duckdb.connect(":memory:")
                setup_test_data_with_join_group_by(
                    conn=self.dfc_conn,
                    join_count=join_count,
                    num_rows=1_000,
                )
                self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)
                self._active_policy_signature = None
                self._ensure_policies(("default", 0), policies)

        else:
            # Default policy
            policies = self._get_policies("default")
            # Delete old policies
            try:
                self._ensure_policies(("default", 0), policies)
            except Exception:
                # If rewriter/connection is broken, recreate it
                self.dfc_conn = self.local_duckdb.connect(":memory:")
                setup_test_data(self.dfc_conn, num_rows=1_000_000)
                self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)
                self._active_policy_signature = None
                self._ensure_policies(("default", 0), policies)

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

        # 1. Run DFC approach (SQLRewriter with policy)
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

        # 2. Run Logical baseline
        try:
            logical_rewrite_start = time.perf_counter()
            if len(policies) == 1:
                logical_query = rewrite_query_logical(query, policies[0])
            else:
                logical_query = rewrite_query_logical_multi(query, policies)
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

        # 3. Run Physical baseline (SmokedDuck REQUIRED)
        try:
            if query_type == "SELECT":
                physical_results = []
                physical_time = 0.0
                physical_runtime = 0.0
                physical_rewrite_time = 0.0
                physical_base_capture_time = 0.0
                physical_lineage_query_time = 0.0
                physical_error = "skipped_for_select"
                physical_rows = 0
            elif not self._physical_supported_for_query(query_type):
                physical_results = []
                physical_time = 0.0
                physical_runtime = 0.0
                physical_rewrite_time = 0.0
                physical_base_capture_time = 0.0
                physical_lineage_query_time = 0.0
                physical_error = "skipped_not_supported_for_query"
                physical_rows = 0
            elif self._physical_enabled_for_run and len(policies) == 1:
                # SmokedDuck is REQUIRED - execute_query_physical_simple will raise if not available
                (
                    physical_results,
                    physical_timing,
                    physical_error,
                    _base_query_sql,
                    _filter_query_sql,
                ) = execute_query_physical_detailed(
                    physical_conn,
                    query,
                    policies[0],
                )
                physical_rewrite_time = physical_timing.get("rewrite_time_ms", 0.0)
                physical_base_capture_time = physical_timing.get("base_capture_time_ms", 0.0)
                physical_lineage_query_time = physical_timing.get("lineage_query_time_ms", 0.0)
                physical_runtime = physical_timing.get("runtime_time_ms", 0.0)
                physical_rows = len(physical_results) if physical_results else 0
                physical_time = physical_runtime
            else:
                physical_results = []
                physical_time = 0.0
                physical_runtime = 0.0
                physical_rewrite_time = 0.0
                physical_base_capture_time = 0.0
                physical_lineage_query_time = 0.0
                physical_error = (
                    "skipped_for_multi_policy"
                    if len(policies) != 1
                    else "skipped_physical_disabled"
                )
                physical_rows = 0
        except ImportError:
            # Re-raise ImportError as-is (SmokedDuck is required)
            raise
        except Exception as e:
            physical_time = 0.0
            physical_runtime = 0.0
            physical_rewrite_time = 0.0
            physical_base_capture_time = 0.0
            physical_lineage_query_time = 0.0
            physical_results = []
            physical_rows = 0
            physical_error = str(e)

        # Verify correctness (always compare DFC vs Physical when available)
        correctness_match = False
        correctness_error = None
        physical_match = None
        physical_match_error = None
        logical_match = None
        logical_match_error = None

        if dfc_error is None and physical_error is None:
            physical_match, physical_match_error = compare_results_exact(
                dfc_results,
                physical_results,
            )

        if dfc_error is None and logical_error is None:
            logical_match, logical_match_error = compare_results_exact(
                dfc_results,
                logical_results,
            )

        if dfc_error is None:
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
        else:
            correctness_error = f"Errors: dfc={dfc_error}, logical={logical_error}, physical={physical_error}"

        # Total execution time (all four approaches)
        total_time = no_policy_time + dfc_time + logical_time + physical_runtime

        # Build custom metrics with variation parameters
        custom_metrics = {
            "query_type": query_type,
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
            "physical_rewrite_time_ms": physical_rewrite_time,
            "physical_base_capture_time_ms": physical_base_capture_time,
            "physical_lineage_query_time_ms": physical_lineage_query_time,
            "no_policy_rows": no_policy_rows,
            "dfc_rows": dfc_rows,
            "logical_rows": logical_rows,
            "physical_rows": physical_rows,
            "correctness_match": correctness_match,
            "correctness_error": correctness_error or "",
            "no_policy_error": no_policy_error or "",
            "dfc_error": dfc_error or "",
            "logical_error": logical_error or "",
            "physical_error": physical_error or "",
            "policy_count": len(policies),
            # Variation metrics - always include all, set to None if not applicable
            "variation_type": variation_params.get("variation_type", ""),
            "variation_index": variation_params.get("variation_index", 0),
            "variation_num": variation_params.get("variation_num", 0),
            "run_index": variation_params.get("run_index", 0),
            "run_num": variation_params.get("run_num", 0),
            "variation_rows_to_remove": variation_params.get("rows_to_remove"),
            "variation_policy_threshold": variation_params.get("policy_threshold"),
            "variation_join_matches": variation_params.get("join_matches"),
            "variation_num_groups": variation_params.get("num_groups"),
            "variation_join_count": variation_params.get("join_count"),
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
        for conn_name in ["no_policy_conn", "dfc_conn", "logical_conn", "physical_conn"]:
            if hasattr(self, conn_name):
                with contextlib.suppress(Exception):
                    getattr(self, conn_name).close()

    def get_metrics(self) -> list:
        """Return list of custom metric names.

        Returns:
            List of metric name strings
        """
        return [
            "query_type",
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
            "physical_rewrite_time_ms",
            "physical_base_capture_time_ms",
            "physical_lineage_query_time_ms",
            "no_policy_rows",
            "dfc_rows",
            "logical_rows",
            "physical_rows",
            "correctness_match",
            "correctness_error",
            "no_policy_error",
            "dfc_error",
            "logical_error",
            "physical_error",
            "policy_count",
            "variation_type",
            "variation_index",
            "variation_num",
            "variation_rows_to_remove",
            "variation_policy_threshold",
            "variation_join_matches",
            "variation_num_groups",
            "variation_join_count",
        ]
