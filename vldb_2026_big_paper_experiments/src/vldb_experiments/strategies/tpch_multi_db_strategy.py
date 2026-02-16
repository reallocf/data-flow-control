"""TPC-H multi-database strategy (DuckDB + external engines like Umbra)."""

from __future__ import annotations

import contextlib
import pathlib
import time

from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import SQLRewriter

from vldb_experiments.baselines.logical_baseline import rewrite_query_logical
from vldb_experiments.correctness import compare_results_approx
from vldb_experiments.multi_db import (
    DataFusionClient,
    PostgresClient,
    SQLServerClient,
    UmbraClient,
)
from vldb_experiments.strategies.tpch_strategy import (
    TPCH_QUERIES,
    _ensure_smokedduck,
    lineitem_policy,
    load_tpch_query,
)

MULTI_DB_DATA_DIR = pathlib.Path("results") / "multi_db"


def _schema_for_scale(scale_factor: float) -> str:
    formatted = f"{scale_factor}".rstrip("0").rstrip(".")
    return f"tpch_sf{formatted}".replace(".", "_")


class TPCHMultiDBStrategy(ExperimentStrategy):
    """Compare DuckDB (No Policy/DFC/Logical) against external engines (No Policy)."""

    def setup(self, context: ExperimentContext) -> None:
        self.scale_factor = float(context.strategy_config.get("tpch_sf", 1))
        db_path = context.strategy_config.get("tpch_db_path")
        if not db_path:
            db_path = f"./results/tpch_multi_db_sf{self.scale_factor}.db"
        pathlib.Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path

        self.local_duckdb = _ensure_smokedduck()
        main_conn = self.local_duckdb.connect(self.db_path)

        with contextlib.suppress(Exception):
            main_conn.execute("INSTALL tpch")
        main_conn.execute("LOAD tpch")
        table_exists = main_conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'lineitem'"
        ).fetchone()[0]
        if table_exists == 0:
            main_conn.execute(f"CALL dbgen(sf={self.scale_factor})")

        self.no_policy_conn = main_conn
        self.dfc_conn = main_conn
        self.logical_conn = main_conn

        for conn in [main_conn]:
            try:
                conn.execute("COMMIT")
            except Exception:
                try:
                    conn.commit()
                except Exception:
                    with contextlib.suppress(Exception):
                        conn.execute("ROLLBACK")

        self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)

        configured_engines = context.strategy_config.get("external_engines")
        if configured_engines is None:
            enabled_engines = {"umbra", "postgres", "datafusion", "sqlserver"}
        else:
            enabled_engines = {str(engine).lower() for engine in configured_engines}
        self.enabled_engines = sorted(enabled_engines)

        sf_dir = MULTI_DB_DATA_DIR / f"sf{self.scale_factor}"
        sqlserver_schema = _schema_for_scale(self.scale_factor)
        available_clients = {
            "umbra": UmbraClient(sf_dir / "umbra"),
            "postgres": PostgresClient(sf_dir / "postgres"),
            "datafusion": DataFusionClient(sf_dir / "datafusion"),
            "sqlserver": SQLServerClient(sf_dir / "sqlserver", schema=sqlserver_schema),
        }
        self.external_clients = {
            name: client for name, client in available_clients.items() if name in enabled_engines
        }
        self.external_client_errors: dict[str, str] = {}
        for name, client in self.external_clients.items():
            try:
                client.start()
                client.wait_ready()
                client.connect()
                client.ensure_tpch_data(self.no_policy_conn)
            except Exception as exc:
                self.external_client_errors[name] = str(exc)

        context.shared_state["external_engines"] = list(self.external_clients.keys())
        context.shared_state["tpch_queries"] = TPCH_QUERIES

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        tpch_queries = context.shared_state["tpch_queries"]
        query_index = (context.execution_number - 1) % len(tpch_queries)
        query_num = tpch_queries[query_index]
        query = load_tpch_query(query_num)

        print(f"[Execution {context.execution_number}] TPC-H Q{query_num:02d} (sf={self.scale_factor})")

        policy = lineitem_policy
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

        try:
            logical_rewrite_start = time.perf_counter()
            logical_query = rewrite_query_logical(query, policy)
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

        external_results: dict[str, list[tuple]] = {}
        external_times: dict[str, float] = {}
        external_rows: dict[str, int] = {}
        external_errors: dict[str, str | None] = {}
        external_dfc_1phase_results: dict[str, list[tuple]] = {}
        external_dfc_1phase_times: dict[str, float] = {}
        external_dfc_1phase_rows: dict[str, int] = {}
        external_dfc_1phase_errors: dict[str, str | None] = {}
        external_dfc_2phase_results: dict[str, list[tuple]] = {}
        external_dfc_2phase_times: dict[str, float] = {}
        external_dfc_2phase_rows: dict[str, int] = {}
        external_dfc_2phase_errors: dict[str, str | None] = {}
        external_logical_results: dict[str, list[tuple]] = {}
        external_logical_times: dict[str, float] = {}
        external_logical_rows: dict[str, int] = {}
        external_logical_errors: dict[str, str | None] = {}

        for name, client in self.external_clients.items():
            if name in self.external_client_errors:
                external_results[name] = []
                external_times[name] = 0.0
                external_rows[name] = 0
                external_errors[name] = self.external_client_errors[name]
                external_dfc_1phase_results[name] = []
                external_dfc_1phase_times[name] = 0.0
                external_dfc_1phase_rows[name] = 0
                external_dfc_1phase_errors[name] = self.external_client_errors[name]
                external_dfc_2phase_results[name] = []
                external_dfc_2phase_times[name] = 0.0
                external_dfc_2phase_rows[name] = 0
                external_dfc_2phase_errors[name] = self.external_client_errors[name]
                external_logical_results[name] = []
                external_logical_times[name] = 0.0
                external_logical_rows[name] = 0
                external_logical_errors[name] = self.external_client_errors[name]
                continue

            start = time.perf_counter()
            try:
                results = client.fetchall(query)
                external_times[name] = (time.perf_counter() - start) * 1000.0
                external_results[name] = results
                external_rows[name] = len(results)
                external_errors[name] = None
            except Exception as e:
                external_times[name] = 0.0
                external_results[name] = []
                external_rows[name] = 0
                external_errors[name] = str(e)

            if dfc_1phase_error is None:
                try:
                    dfc_start = time.perf_counter()
                    dfc_results_external = client.fetchall(dfc_1phase_transformed)
                    external_dfc_1phase_times[name] = (time.perf_counter() - dfc_start) * 1000.0
                    external_dfc_1phase_results[name] = dfc_results_external
                    external_dfc_1phase_rows[name] = len(dfc_results_external)
                    external_dfc_1phase_errors[name] = None
                except Exception as e:
                    external_dfc_1phase_times[name] = 0.0
                    external_dfc_1phase_results[name] = []
                    external_dfc_1phase_rows[name] = 0
                    external_dfc_1phase_errors[name] = str(e)
            else:
                external_dfc_1phase_errors[name] = f"duckdb dfc_1phase error: {dfc_1phase_error}"

            if dfc_2phase_error is None:
                try:
                    dfc_start = time.perf_counter()
                    dfc_results_external = client.fetchall(dfc_2phase_transformed)
                    external_dfc_2phase_times[name] = (time.perf_counter() - dfc_start) * 1000.0
                    external_dfc_2phase_results[name] = dfc_results_external
                    external_dfc_2phase_rows[name] = len(dfc_results_external)
                    external_dfc_2phase_errors[name] = None
                except Exception as e:
                    external_dfc_2phase_times[name] = 0.0
                    external_dfc_2phase_results[name] = []
                    external_dfc_2phase_rows[name] = 0
                    external_dfc_2phase_errors[name] = str(e)
            else:
                external_dfc_2phase_errors[name] = f"duckdb dfc_2phase error: {dfc_2phase_error}"

            if logical_error is None:
                try:
                    logical_start = time.perf_counter()
                    logical_results_external = client.fetchall(logical_query)
                    external_logical_times[name] = (
                        time.perf_counter() - logical_start
                    ) * 1000.0
                    external_logical_results[name] = logical_results_external
                    external_logical_rows[name] = len(logical_results_external)
                    external_logical_errors[name] = None
                except Exception as e:
                    external_logical_times[name] = 0.0
                    external_logical_results[name] = []
                    external_logical_rows[name] = 0
                    external_logical_errors[name] = str(e)
            else:
                external_logical_errors[name] = f"duckdb logical error: {logical_error}"

        correctness_match = False
        correctness_error = None
        if dfc_1phase_error is None and logical_error is None:
            match, error = compare_results_approx(dfc_1phase_results, logical_results)
            correctness_match = match
            correctness_error = error
        else:
            correctness_error = f"Errors: dfc_1phase={dfc_1phase_error}, logical={logical_error}"

        external_correctness_match: dict[str, bool] = {}
        external_correctness_error: dict[str, str] = {}
        for name in context.shared_state.get("external_engines", []):
            error = external_errors[name]
            if no_policy_error is None and error is None:
                match, compare_error = compare_results_approx(
                    no_policy_results,
                    external_results[name],
                )
                external_correctness_match[name] = match
                external_correctness_error[name] = compare_error or ""
            else:
                external_correctness_match[name] = False
                external_correctness_error[name] = (
                    f"Errors: duckdb={no_policy_error}, {name}={error}"
                )

        external_dfc_1phase_correctness_match: dict[str, bool] = {}
        external_dfc_1phase_correctness_error: dict[str, str] = {}
        external_dfc_2phase_correctness_match: dict[str, bool] = {}
        external_dfc_2phase_correctness_error: dict[str, str] = {}
        external_logical_correctness_match: dict[str, bool] = {}
        external_logical_correctness_error: dict[str, str] = {}
        for name in context.shared_state.get("external_engines", []):
            dfc_err = external_dfc_1phase_errors.get(name)
            if dfc_1phase_error is None and dfc_err is None:
                match, compare_error = compare_results_approx(
                    dfc_1phase_results, external_dfc_1phase_results.get(name, [])
                )
                external_dfc_1phase_correctness_match[name] = match
                external_dfc_1phase_correctness_error[name] = compare_error or ""
            else:
                external_dfc_1phase_correctness_match[name] = False
                external_dfc_1phase_correctness_error[name] = (
                    f"Errors: duckdb={dfc_1phase_error}, {name}={dfc_err}"
                )

            dfc2_err = external_dfc_2phase_errors.get(name)
            if dfc_2phase_error is None and dfc2_err is None:
                match, compare_error = compare_results_approx(
                    dfc_2phase_results, external_dfc_2phase_results.get(name, [])
                )
                external_dfc_2phase_correctness_match[name] = match
                external_dfc_2phase_correctness_error[name] = compare_error or ""
            else:
                external_dfc_2phase_correctness_match[name] = False
                external_dfc_2phase_correctness_error[name] = (
                    f"Errors: duckdb={dfc_2phase_error}, {name}={dfc2_err}"
                )

            logical_err = external_logical_errors.get(name)
            if dfc_1phase_error is None and logical_err is None:
                match, compare_error = compare_results_approx(
                    dfc_1phase_results, external_logical_results.get(name, [])
                )
                external_logical_correctness_match[name] = match
                external_logical_correctness_error[name] = compare_error or ""
            else:
                external_logical_correctness_match[name] = False
                external_logical_correctness_error[name] = (
                    f"Errors: duckdb={dfc_1phase_error}, {name}={logical_err}"
                )

        total_time = no_policy_time + dfc_1phase_time + dfc_2phase_time + logical_time + sum(external_times.values())
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
            "no_policy_rows": no_policy_rows,
            "dfc_1phase_rows": dfc_1phase_rows,
            "dfc_2phase_rows": dfc_2phase_rows,
            "logical_rows": logical_rows,
            "correctness_match": correctness_match,
            "correctness_error": correctness_error or "",
            "no_policy_error": no_policy_error or "",
            "dfc_1phase_error": dfc_1phase_error or "",
            "dfc_2phase_error": dfc_2phase_error or "",
            "logical_error": logical_error or "",
        }
        for name in context.shared_state.get("external_engines", []):
            custom_metrics[f"{name}_dfc_1phase_time_ms"] = external_dfc_1phase_times.get(name, 0.0)
            custom_metrics[f"{name}_dfc_1phase_rows"] = external_dfc_1phase_rows.get(name, 0)
            custom_metrics[f"{name}_dfc_1phase_error"] = external_dfc_1phase_errors.get(name) or ""
            custom_metrics[f"{name}_dfc_1phase_correctness_match"] = external_dfc_1phase_correctness_match.get(
                name, False
            )
            custom_metrics[f"{name}_dfc_1phase_correctness_error"] = external_dfc_1phase_correctness_error.get(
                name, ""
            )
            custom_metrics[f"{name}_dfc_2phase_time_ms"] = external_dfc_2phase_times.get(name, 0.0)
            custom_metrics[f"{name}_dfc_2phase_rows"] = external_dfc_2phase_rows.get(name, 0)
            custom_metrics[f"{name}_dfc_2phase_error"] = external_dfc_2phase_errors.get(name) or ""
            custom_metrics[f"{name}_dfc_2phase_correctness_match"] = external_dfc_2phase_correctness_match.get(
                name, False
            )
            custom_metrics[f"{name}_dfc_2phase_correctness_error"] = external_dfc_2phase_correctness_error.get(
                name, ""
            )
            custom_metrics[f"{name}_logical_time_ms"] = external_logical_times.get(name, 0.0)
            custom_metrics[f"{name}_logical_rows"] = external_logical_rows.get(name, 0)
            custom_metrics[f"{name}_logical_error"] = (
                external_logical_errors.get(name) or ""
            )
            custom_metrics[
                f"{name}_logical_correctness_match"
            ] = external_logical_correctness_match.get(name, False)
            custom_metrics[f"{name}_logical_correctness_error"] = (
                external_logical_correctness_error.get(name, "")
            )
        for name in context.shared_state.get("external_engines", []):
            custom_metrics[f"{name}_time_ms"] = external_times.get(name, 0.0)
            custom_metrics[f"{name}_rows"] = external_rows.get(name, 0)
            custom_metrics[f"{name}_error"] = external_errors.get(name) or ""
            custom_metrics[f"{name}_correctness_match"] = external_correctness_match.get(
                name, False
            )
            custom_metrics[f"{name}_correctness_error"] = external_correctness_error.get(
                name, ""
            )

        return ExperimentResult(duration_ms=total_time, custom_metrics=custom_metrics)

    def teardown(self, _context: ExperimentContext) -> None:
        if hasattr(self, "dfc_rewriter"):
            self.dfc_rewriter.close()
        seen = set()
        for conn_name in ["no_policy_conn", "dfc_conn", "logical_conn"]:
            if hasattr(self, conn_name):
                conn = getattr(self, conn_name)
                if id(conn) in seen:
                    continue
                seen.add(id(conn))
                with contextlib.suppress(Exception):
                    conn.close()
        if hasattr(self, "external_clients"):
            for client in self.external_clients.values():
                with contextlib.suppress(Exception):
                    client.close()

    def get_metrics(self) -> list:
        metrics = [
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
            "no_policy_rows",
            "dfc_1phase_rows",
            "dfc_2phase_rows",
            "logical_rows",
            "correctness_match",
            "correctness_error",
            "no_policy_error",
            "dfc_1phase_error",
            "dfc_2phase_error",
            "logical_error",
        ]
        enabled = getattr(self, "enabled_engines", [])
        for engine in enabled:
            metrics.extend(
                [
                    f"{engine}_time_ms",
                    f"{engine}_rows",
                    f"{engine}_error",
                    f"{engine}_correctness_match",
                    f"{engine}_correctness_error",
                    f"{engine}_dfc_1phase_time_ms",
                    f"{engine}_dfc_1phase_rows",
                    f"{engine}_dfc_1phase_error",
                    f"{engine}_dfc_1phase_correctness_match",
                    f"{engine}_dfc_1phase_correctness_error",
                    f"{engine}_dfc_2phase_time_ms",
                    f"{engine}_dfc_2phase_rows",
                    f"{engine}_dfc_2phase_error",
                    f"{engine}_dfc_2phase_correctness_match",
                    f"{engine}_dfc_2phase_correctness_error",
                    f"{engine}_logical_time_ms",
                    f"{engine}_logical_rows",
                    f"{engine}_logical_error",
                    f"{engine}_logical_correctness_match",
                    f"{engine}_logical_correctness_error",
                ]
            )
        return metrics

    def get_setting_key(self, context: ExperimentContext) -> tuple[str, int]:
        tpch_queries = context.shared_state.get("tpch_queries", TPCH_QUERIES)
        query_index = (context.execution_number - 1) % len(tpch_queries)
        query_num = tpch_queries[query_index]
        return ("tpch_query", query_num)
