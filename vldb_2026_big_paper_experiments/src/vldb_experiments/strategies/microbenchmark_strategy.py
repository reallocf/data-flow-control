"""Microbenchmark experiment strategy for measuring SQL rewriting performance."""

import contextlib
import time

import duckdb
from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import SQLRewriter

from vldb_experiments.baselines.logical_baseline import execute_query_logical
from vldb_experiments.baselines.physical_baseline import execute_query_physical_simple
from vldb_experiments.correctness import compare_results
from vldb_experiments.data_setup import (
    setup_test_data,
    setup_test_data_with_groups,
    setup_test_data_with_join_matches,
)
from vldb_experiments.policy_setup import create_test_policy
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

    def setup(self, context: ExperimentContext) -> None:
        """Set up test data and rewriter instances.

        Args:
            context: Experiment context with database connection
        """
        # Use locally built SmokedDuck DuckDB for physical baseline
        # SmokedDuck is REQUIRED - ensure it's set up
        local_duckdb = _ensure_smokedduck()

        # Use the connection from context to set up data
        # But create separate connections for each rewriter to avoid UDF conflicts
        main_conn = context.database_connection
        if main_conn is None:
            raise ValueError("Database connection required in context")

        # Set up fixed test data on the main connection
        setup_test_data(main_conn, num_rows=1_000_000)

        # Create separate connections for each approach to avoid conflicts
        # Use local DuckDB for physical connection (SmokedDuck build)
        self.no_policy_conn = duckdb.connect(":memory:")
        self.dfc_conn = duckdb.connect(":memory:")
        self.logical_conn = duckdb.connect(":memory:")
        self.physical_conn = local_duckdb.connect(":memory:")

        # Set up data in each connection
        setup_test_data(self.no_policy_conn, num_rows=1_000_000)
        setup_test_data(self.dfc_conn, num_rows=1_000_000)
        setup_test_data(self.logical_conn, num_rows=1_000_000)
        setup_test_data(self.physical_conn, num_rows=1_000_000)

        # Commit any transactions before creating rewriters (needed for SmokedDuck lineage)
        # DuckDB auto-commits, but SmokedDuck lineage may leave transactions open
        for conn in [self.no_policy_conn, self.dfc_conn, self.logical_conn, self.physical_conn]:
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

        # Create DFC rewriter (policy will be registered per execution with variations)
        self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)

        # Store queries in shared state
        context.shared_state["queries"] = get_query_definitions()
        context.shared_state["query_order"] = get_query_order()
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
            num_variations=4,
            num_runs_per_variation=5,
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
        print(f"[Execution {context.execution_number}] {query_type} - Variation {variation_params.get('variation_num', 'N/A')}, Run {variation_params.get('run_num', 'N/A')} ({variation_info})")

        # Get connections for each approach
        no_policy_conn = self.no_policy_conn
        logical_conn = self.logical_conn
        physical_conn = self.physical_conn

        # Setup data and policy based on variation type
        if query_type in ["SELECT", "WHERE", "ORDER_BY"]:
            # Vary policy threshold - no need to regenerate data
            policy_threshold = variation_params["policy_threshold"]
            policy = create_test_policy(threshold=policy_threshold)
            # Delete old policy and register new one
            # Get existing policies to delete them
            existing_policies = self.dfc_rewriter.get_dfc_policies()
            for old_policy in existing_policies:
                self.dfc_rewriter.delete_policy(
                    source=old_policy.source,
                    constraint=old_policy.constraint,
                    on_fail=old_policy.on_fail
                )
            self.dfc_rewriter.register_policy(policy)

        elif query_type == "JOIN":
            # Vary join matches - regenerate data
            join_matches = variation_params["join_matches"]
            # Drop and recreate tables with new data
            for conn in [no_policy_conn, self.dfc_conn, logical_conn, physical_conn]:
                with contextlib.suppress(Exception):
                    conn.execute("DROP TABLE IF EXISTS test_data")
                setup_test_data_with_join_matches(conn, num_rows=1_000_000, join_matches=join_matches)
            # Use default policy for JOIN
            policy = create_test_policy()
            # Delete old policies
            try:
                existing_policies = self.dfc_rewriter.get_dfc_policies()
                for old_policy in existing_policies:
                    self.dfc_rewriter.delete_policy(
                        source=old_policy.source,
                        constraint=old_policy.constraint,
                        on_fail=old_policy.on_fail
                    )
                self.dfc_rewriter.register_policy(policy)
            except Exception:
                # If rewriter/connection is broken, recreate it
                self.dfc_conn = duckdb.connect(":memory:")
                setup_test_data_with_join_matches(self.dfc_conn, num_rows=1_000_000, join_matches=join_matches)
                self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)
                self.dfc_rewriter.register_policy(policy)

        elif query_type == "GROUP_BY":
            # Vary number of groups - regenerate data
            num_groups = variation_params["num_groups"]
            # Drop and recreate tables with new data
            for conn in [no_policy_conn, self.dfc_conn, logical_conn, physical_conn]:
                with contextlib.suppress(Exception):
                    conn.execute("DROP TABLE IF EXISTS test_data")
                setup_test_data_with_groups(conn, num_rows=1_000_000, num_groups=num_groups)
            # Use default policy for GROUP_BY
            policy = create_test_policy()
            # Delete old policies
            try:
                existing_policies = self.dfc_rewriter.get_dfc_policies()
                for old_policy in existing_policies:
                    self.dfc_rewriter.delete_policy(
                        source=old_policy.source,
                        constraint=old_policy.constraint,
                        on_fail=old_policy.on_fail
                    )
                self.dfc_rewriter.register_policy(policy)
            except Exception:
                # If rewriter/connection is broken, recreate it
                self.dfc_conn = duckdb.connect(":memory:")
                setup_test_data_with_groups(self.dfc_conn, num_rows=1_000_000, num_groups=num_groups)
                self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)
                self.dfc_rewriter.register_policy(policy)

        else:
            # Default policy
            policy = create_test_policy()
            # Delete old policies
            try:
                existing_policies = self.dfc_rewriter.get_dfc_policies()
                for old_policy in existing_policies:
                    self.dfc_rewriter.delete_policy(
                        source=old_policy.source,
                        constraint=old_policy.constraint,
                        on_fail=old_policy.on_fail
                    )
                self.dfc_rewriter.register_policy(policy)
            except Exception:
                # If rewriter/connection is broken, recreate it
                self.dfc_conn = duckdb.connect(":memory:")
                setup_test_data(self.dfc_conn, num_rows=1_000_000)
                self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)
                self.dfc_rewriter.register_policy(policy)

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
        dfc_start = time.perf_counter()
        try:
            dfc_cursor = self.dfc_rewriter.execute(query)
            dfc_results = dfc_cursor.fetchall()
            dfc_time = (time.perf_counter() - dfc_start) * 1000.0
            dfc_rows = len(dfc_results)
            dfc_error = None
        except Exception as e:
            dfc_time = 0.0
            dfc_results = []
            dfc_rows = 0
            dfc_error = str(e)

        # 2. Run Logical baseline
        try:
            logical_results, logical_time = execute_query_logical(logical_conn, query, policy)
            logical_rows = len(logical_results)
            logical_error = None
        except Exception as e:
            logical_time = 0.0
            logical_results = []
            logical_rows = 0
            logical_error = str(e)

        # 3. Run Physical baseline (SmokedDuck REQUIRED)
        try:
            # SmokedDuck is REQUIRED - execute_query_physical_simple will raise if not available
            physical_results, physical_time, physical_error = execute_query_physical_simple(physical_conn, query, policy)
            physical_rows = len(physical_results) if physical_results else 0
        except ImportError:
            # Re-raise ImportError as-is (SmokedDuck is required)
            raise
        except Exception as e:
            physical_time = 0.0
            physical_results = []
            physical_rows = 0
            physical_error = str(e)

        # Verify correctness (compare DFC, logical, and physical - they should all match)
        correctness_match = False
        correctness_error = None
        if dfc_error is None and logical_error is None and physical_error is None:
            match, error = compare_results(dfc_results, logical_results, physical_results)
            correctness_match = match
            correctness_error = error
        else:
            correctness_error = f"Errors: dfc={dfc_error}, logical={logical_error}, physical={physical_error}"

        # Total execution time (all four approaches)
        total_time = no_policy_time + dfc_time + logical_time + physical_time

        # Build custom metrics with variation parameters
        custom_metrics = {
            "query_type": query_type,
            "no_policy_time_ms": no_policy_time,
            "dfc_time_ms": dfc_time,
            "logical_time_ms": logical_time,
            "physical_time_ms": physical_time,
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
            "dfc_time_ms",
            "logical_time_ms",
            "physical_time_ms",
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
            "variation_type",
            "variation_index",
            "variation_num",
            "variation_rows_to_remove",
            "variation_policy_threshold",
            "variation_join_matches",
            "variation_num_groups",
        ]
