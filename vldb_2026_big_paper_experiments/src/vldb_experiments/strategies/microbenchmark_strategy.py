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
    setup_join_data_only,
    setup_test_data,
    setup_test_data_with_join_group_by,
)
from vldb_experiments.policy_setup import create_test_policies
from vldb_experiments.query_definitions import get_query_definitions, get_query_order

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
        self.base_num_rows = 10_000_000
        self._policy_cache: dict[tuple[str, int], list] = {}
        self._active_policy_signature: tuple[str, int] | None = None
        self.execution_plan: list[dict] = []
        self._join_match_values = [100, 1000, 10000, 100000]
        self._simple_agg_num_rows_values = [1_000, 10_000, 100_000, 1_000_000]
        self._group_by_num_groups_values = [10, 100, 1000, 10000]

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
        return query_type in {"JOIN", "SIMPLE_AGG", "GROUP_BY", "JOIN_GROUP_BY"}

    def _configure_connection(self, conn) -> None:
        """Apply per-connection settings required by large generated queries."""
        with contextlib.suppress(Exception):
            conn.execute("SET max_expression_depth TO 20000")

    def _benchmark_connections(self) -> list:
        """Return unique active benchmark connections."""
        connections = []
        seen = set()
        for conn in [
            getattr(self, "no_policy_conn", None),
            getattr(self, "dfc_conn", None),
            getattr(self, "logical_conn", None),
            getattr(self, "physical_conn", None),
        ]:
            if conn is None:
                continue
            conn_id = id(conn)
            if conn_id in seen:
                continue
            seen.add(conn_id)
            connections.append(conn)
        return connections

    def _variation_parameters_for(
        self,
        query_type: str,
        variation_index: int,
        run_index: int,
    ) -> dict:
        """Generate variation parameters for a specific setting and run."""
        variation_num = variation_index + 1
        run_num = run_index + 1

        if query_type in ["SELECT", "WHERE", "ORDER_BY"]:
            rows_to_remove_values = [0, 1_000_000, 2_000_000, 4_000_000, 8_000_000]
            rows_to_remove = rows_to_remove_values[variation_index]
            policy_threshold = rows_to_remove if rows_to_remove > 0 else 0
            return {
                "variation_type": "policy_threshold",
                "rows_to_remove": rows_to_remove,
                "policy_threshold": policy_threshold,
                "variation_index": variation_index,
                "variation_num": variation_num,
                "run_index": run_index,
                "run_num": run_num,
            }

        if query_type == "JOIN":
            return {
                "variation_type": "join_matches",
                "join_matches": self._join_match_values[variation_index],
                "variation_index": variation_index,
                "variation_num": variation_num,
                "run_index": run_index,
                "run_num": run_num,
            }

        if query_type == "GROUP_BY":
            return {
                "variation_type": "num_groups",
                "num_groups": self._group_by_num_groups_values[variation_index],
                "variation_index": variation_index,
                "variation_num": variation_num,
                "run_index": run_index,
                "run_num": run_num,
            }

        if query_type == "SIMPLE_AGG":
            return {
                "variation_type": "num_rows",
                "num_rows": self._simple_agg_num_rows_values[variation_index],
                "variation_index": variation_index,
                "variation_num": variation_num,
                "run_index": run_index,
                "run_num": run_num,
            }

        if query_type == "JOIN_GROUP_BY":
            join_count_values = [16, 32, 64, 128]
            return {
                "variation_type": "join_count",
                "join_count": join_count_values[variation_index],
                "variation_index": variation_index,
                "variation_num": variation_num,
                "run_index": run_index,
                "run_num": run_num,
            }

        return {
            "variation_type": "none",
            "variation_index": variation_index,
            "variation_num": variation_num,
            "run_index": run_index,
            "run_num": run_num,
        }

    def _build_execution_plan(self, query_order: list[str]) -> list[dict]:
        """Build an execution plan grouped by setting, then run number."""
        plan = []
        for query_type in query_order:
            for variation_index in range(self.num_variations):
                for run_index in range(self.num_runs_per_variation):
                    plan.append(
                        {
                            "query_type": query_type,
                            "variation_params": self._variation_parameters_for(
                                query_type,
                                variation_index,
                                run_index,
                            ),
                        }
                    )
        return plan

    def _plan_entry(self, context: ExperimentContext) -> dict:
        """Return the execution-plan entry for the current execution number."""
        plan_index = context.execution_number - 1
        if plan_index < 0 or plan_index >= len(self.execution_plan):
            raise IndexError(
                f"Execution number {context.execution_number} is out of bounds for "
                f"execution plan of size {len(self.execution_plan)}"
            )
        return self.execution_plan[plan_index]

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

    def _build_join_query(self, join_matches: int) -> str:
        """Build a JOIN query against a prebuilt join-data table."""
        return (
            "SELECT test_data.id, other.value "
            "FROM test_data "
            f"JOIN join_data_{join_matches} other ON test_data.id = other.id"
        )

    def _build_simple_agg_query(self, num_rows: int) -> str:
        """Build a SIMPLE_AGG query over a prefix of the base table."""
        return f"SELECT SUM(amount) FROM test_data WHERE id <= {num_rows}"

    def _build_group_by_query(self, num_groups: int) -> str:
        """Build a GROUP_BY query with synthetic grouping over the base table."""
        return (
            "SELECT "
            f"CAST(((id - 1) % {num_groups}) AS VARCHAR) AS category, "
            "COUNT(*), "
            "SUM(amount) "
            "FROM test_data "
            f"WHERE id <= {min(self.base_num_rows, 1_000_000)} "
            f"GROUP BY CAST(((id - 1) % {num_groups}) AS VARCHAR)"
        )

    def setup(self, context: ExperimentContext) -> None:
        """Set up test data and rewriter instances.

        Args:
            context: Experiment context with database connection
        """
        self.enable_physical = (
            self.enable_physical_override
            if self.enable_physical_override is not None
            else True
        )
        query_order = self.query_types or get_query_order()
        self._physical_enabled_for_run = (
            self.enable_physical
            and any(self._physical_supported_for_query(query) for query in query_order)
        )
        # Use locally built SmokedDuck DuckDB for all benchmark connections.
        self.local_duckdb = _ensure_smokedduck()
        # Use a single shared SmokedDuck connection for all approaches so large
        # benchmark tables are populated once instead of once per approach.
        self.shared_conn = self.local_duckdb.connect(":memory:")
        self.no_policy_conn = self.shared_conn
        self.dfc_conn = self.shared_conn
        self.logical_conn = self.shared_conn
        self.physical_conn = self.shared_conn if self._physical_enabled_for_run else None

        setup_test_data(self.shared_conn, num_rows=self.base_num_rows)
        for join_matches in self._join_match_values:
            setup_join_data_only(
                self.shared_conn,
                join_matches=join_matches,
                table_name=f"join_data_{join_matches}",
            )
        self._configure_connection(self.shared_conn)

        # Commit any transactions before creating rewriters (needed for SmokedDuck lineage)
        # DuckDB auto-commits, but SmokedDuck lineage may leave transactions open
        for conn in self._benchmark_connections():
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
        self.execution_plan = self._build_execution_plan(query_order)

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        """Execute microbenchmark for current query with all four approaches.

        Args:
            context: Experiment context with current execution number

        Returns:
            ExperimentResult with timing and performance metrics for all approaches
        """
        queries = context.shared_state["queries"]
        plan_entry = self._plan_entry(context)
        query_type = plan_entry["query_type"]
        query = queries[query_type]
        variation_params = plan_entry["variation_params"]

        # Print execution details for logging
        variation_info = ""
        if query_type in ["SELECT", "WHERE", "ORDER_BY"]:
            variation_info = f"rows_to_remove={variation_params.get('rows_to_remove', 'N/A')}"
        elif query_type == "SIMPLE_AGG":
            variation_info = f"num_rows={variation_params.get('num_rows', 'N/A')}"
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

        elif query_type == "SIMPLE_AGG":
            # Vary input rows for simple aggregation benchmark.
            num_rows = variation_params["num_rows"]
            query = self._build_simple_agg_query(num_rows)
            # Keep default policy fixed; with these row counts it should pass.
            policies = self._get_policies("default")
            self._ensure_policies(("default", 0), policies)

        elif query_type == "JOIN":
            # Vary join matches - regenerate data
            join_matches = variation_params["join_matches"]
            query = self._build_join_query(join_matches)
            # Use default policies for JOIN
            policies = self._get_policies("default")
            self._ensure_policies(("default", 0), policies)

        elif query_type == "GROUP_BY":
            # Vary number of groups - regenerate data
            num_groups = variation_params["num_groups"]
            query = self._build_group_by_query(num_groups)
            # Use default policies for GROUP_BY
            policies = self._get_policies("default")
            self._ensure_policies(("default", 0), policies)

        elif query_type == "JOIN_GROUP_BY":
            join_count = variation_params["join_count"]
            query = self._build_join_group_by_query(join_count)
            if context.is_warmup:
                drop_join_limit = max(self._max_join_group_by_count_seen, join_count)
                with contextlib.suppress(Exception):
                    self.dfc_conn.execute("DROP TABLE IF EXISTS test_data")
                for idx in range(1, drop_join_limit + 1):
                    with contextlib.suppress(Exception):
                        self.dfc_conn.execute(f"DROP TABLE IF EXISTS join_data_{idx}")
                self._reset_shared_connection(
                    setup_test_data_with_join_group_by,
                    join_count=join_count,
                    num_rows=1_000,
                )
                self._max_join_group_by_count_seen = max(
                    self._max_join_group_by_count_seen,
                    join_count,
                )

            policies = self._get_policies("default")
            self._ensure_policies(("default", 0), policies)

        else:
            # Default policy
            policies = self._get_policies("default")
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

        # 1. Run DFC one-phase approach
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
            elif self._physical_enabled_for_run:
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
                    policies,
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
                physical_error = "skipped_physical_disabled"
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

        if dfc_1phase_error is None and physical_error is None:
            physical_match, physical_match_error = compare_results_exact(
                dfc_1phase_results,
                physical_results,
            )

        if dfc_1phase_error is None and logical_error is None:
            logical_match, logical_match_error = compare_results_exact(
                dfc_1phase_results,
                logical_results,
            )
        if dfc_1phase_error is None and dfc_2phase_error is None:
            dfc_2phase_match, dfc_2phase_match_error = compare_results_exact(
                dfc_1phase_results,
                dfc_2phase_results,
            )
        else:
            dfc_2phase_match, dfc_2phase_match_error = None, None

        if dfc_1phase_error is None and dfc_2phase_error is None:
            matches = []
            errors = []
            if dfc_2phase_match is not None:
                matches.append(dfc_2phase_match)
                if dfc_2phase_match_error:
                    errors.append(f"dfc_2phase={dfc_2phase_match_error}")
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
            correctness_error = (
                "Errors: "
                f"dfc_1phase={dfc_1phase_error}, "
                f"dfc_2phase={dfc_2phase_error}, "
                f"logical={logical_error}, physical={physical_error}"
            )

        # Total execution time (all four approaches)
        total_time = no_policy_time + dfc_1phase_time + dfc_2phase_time + logical_time + physical_runtime
        if total_time == 0.0:
            # Keep non-zero to avoid runner edge case that reads timing before context exit.
            total_time = 0.001

        # Build custom metrics with variation parameters
        custom_metrics = {
            "query_type": query_type,
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
            "variation_num_rows": variation_params.get("num_rows"),
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
            with contextlib.suppress(Exception):
                self.dfc_rewriter.close()

    def get_metrics(self) -> list:
        """Return list of custom metric names.

        Returns:
            List of metric name strings
        """
        return [
            "query_type",
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

    def get_setting_key(self, context: ExperimentContext) -> tuple:
        """Group warmups/runs by query variation setting."""
        plan_entry = self._plan_entry(context)
        query_type = plan_entry["query_type"]
        variation_params = plan_entry["variation_params"]
        return (
            query_type,
            variation_params.get("variation_type"),
            variation_params.get("variation_index"),
            variation_params.get("rows_to_remove"),
            variation_params.get("policy_threshold"),
            variation_params.get("join_matches"),
            variation_params.get("num_groups"),
            variation_params.get("join_count"),
        )
