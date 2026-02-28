"""TPC-H Q01 self-join alias-policy experiment strategy."""

from __future__ import annotations

import contextlib
import pathlib
import time

from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy

from vldb_experiments.correctness import compare_results_exact
from vldb_experiments.strategies.tpch_self_join_policy_queries import (
    DEFAULT_SELF_JOIN_COUNTS,
    get_cached_tpch_q01_self_join_1phase_optimized_query,
    get_cached_tpch_q01_self_join_1phase_query,
    get_cached_tpch_q01_self_join_no_policy_query,
    prime_cached_tpch_q01_self_join_queries,
)
from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck

DEFAULT_WARMUP_PER_SETTING = 1
DEFAULT_RUNS_PER_SETTING = 5


class TPCHSelfJoinPolicyStrategy(ExperimentStrategy):
    """Measure self-join alias-policy behavior on TPC-H Q01."""

    def setup(self, context: ExperimentContext) -> None:
        self.scale_factor = float(context.strategy_config.get("tpch_sf", 1))
        db_path = context.strategy_config.get("tpch_db_path")
        if not db_path:
            db_path = f"./results/tpch_q01_self_join_policy_sf{self.scale_factor}.db"
        pathlib.Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path

        self_join_counts = context.strategy_config.get(
            "self_join_counts",
            DEFAULT_SELF_JOIN_COUNTS,
        )
        warmup_per_setting = int(
            context.strategy_config.get("warmup_per_setting", DEFAULT_WARMUP_PER_SETTING)
        )
        runs_per_setting = int(
            context.strategy_config.get("runs_per_setting", DEFAULT_RUNS_PER_SETTING)
        )

        self.local_duckdb = _ensure_smokedduck()
        self.conn = self.local_duckdb.connect(self.db_path)
        with contextlib.suppress(Exception):
            self.conn.execute("INSTALL tpch")
        self.conn.execute("LOAD tpch")
        table_exists = self.conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'lineitem'"
        ).fetchone()[0]
        if table_exists == 0:
            self.conn.execute(f"CALL dbgen(sf={self.scale_factor})")

        try:
            self.conn.execute("COMMIT")
        except Exception:
            with contextlib.suppress(Exception):
                self.conn.commit()

        self.self_join_counts = list(self_join_counts)
        self.warmup_per_setting = warmup_per_setting
        self.runs_per_setting = runs_per_setting
        prime_cached_tpch_q01_self_join_queries(self.self_join_counts)

    def _setting_and_run(self, execution_number: int) -> tuple[int, int]:
        setting_index = (execution_number - 1) // self.runs_per_setting
        run_num = ((execution_number - 1) % self.runs_per_setting) + 1
        return self.self_join_counts[setting_index], run_num

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        self_join_count, run_num = self._setting_and_run(context.execution_number)
        phase_label = "warmup" if context.is_warmup else f"run {run_num}"
        print(
            f"[Execution {context.execution_number}] "
            f"TPC-H Q01 self-joins={self_join_count} ({phase_label})"
        )

        no_policy_query = get_cached_tpch_q01_self_join_no_policy_query(self_join_count)
        dfc_1phase_query = get_cached_tpch_q01_self_join_1phase_query(self_join_count)
        dfc_1phase_optimized_query = get_cached_tpch_q01_self_join_1phase_optimized_query(
            self_join_count
        )

        try:
            no_policy_start = time.perf_counter()
            no_policy_results = self.conn.execute(no_policy_query).fetchall()
            no_policy_time = (time.perf_counter() - no_policy_start) * 1000.0
            no_policy_error = None
        except Exception as exc:
            no_policy_results = []
            no_policy_time = 0.0
            no_policy_error = str(exc)

        try:
            dfc_1phase_start = time.perf_counter()
            dfc_1phase_results = self.conn.execute(dfc_1phase_query).fetchall()
            dfc_1phase_time = (time.perf_counter() - dfc_1phase_start) * 1000.0
            dfc_1phase_error = None
        except Exception as exc:
            dfc_1phase_results = []
            dfc_1phase_time = 0.0
            dfc_1phase_error = str(exc)

        try:
            dfc_1phase_optimized_start = time.perf_counter()
            dfc_1phase_optimized_results = self.conn.execute(
                dfc_1phase_optimized_query
            ).fetchall()
            dfc_1phase_optimized_time = (
                time.perf_counter() - dfc_1phase_optimized_start
            ) * 1000.0
            dfc_1phase_optimized_error = None
        except Exception as exc:
            dfc_1phase_optimized_results = []
            dfc_1phase_optimized_time = 0.0
            dfc_1phase_optimized_error = str(exc)

        no_policy_match = None
        no_policy_match_error = None
        dfc_1phase_optimized_match = None
        dfc_1phase_optimized_match_error = None
        if no_policy_error is None and dfc_1phase_error is None:
            no_policy_match, no_policy_match_error = compare_results_exact(
                no_policy_results,
                dfc_1phase_results,
            )
        if dfc_1phase_error is None and dfc_1phase_optimized_error is None:
            dfc_1phase_optimized_match, dfc_1phase_optimized_match_error = compare_results_exact(
                dfc_1phase_results,
                dfc_1phase_optimized_results,
            )

        matches = [
            m for m in [no_policy_match, dfc_1phase_optimized_match] if m is not None
        ]
        correctness_match = all(matches) if matches else False
        errors = []
        if no_policy_match_error:
            errors.append(f"no_policy={no_policy_match_error}")
        if dfc_1phase_optimized_match_error:
            errors.append(f"dfc_1phase_optimized={dfc_1phase_optimized_match_error}")
        correctness_error = "; ".join(errors) if errors else None
        if no_policy_error or dfc_1phase_error or dfc_1phase_optimized_error:
            correctness_error = (
                "Errors: "
                f"no_policy={no_policy_error}, "
                f"dfc_1phase={dfc_1phase_error}, "
                f"dfc_1phase_optimized={dfc_1phase_optimized_error}"
            )

        custom_metrics = {
            "self_join_count": self_join_count,
            "run_num": run_num,
            "tpch_sf": self.scale_factor,
            "no_policy_time_ms": no_policy_time,
            "dfc_1phase_time_ms": dfc_1phase_time,
            "dfc_1phase_optimized_time_ms": dfc_1phase_optimized_time,
            "no_policy_rows": len(no_policy_results),
            "dfc_1phase_rows": len(dfc_1phase_results),
            "dfc_1phase_optimized_rows": len(dfc_1phase_optimized_results),
            "correctness_match": correctness_match,
            "correctness_error": correctness_error or "",
            "no_policy_match": no_policy_match if no_policy_match is not None else "",
            "no_policy_match_error": no_policy_match_error or "",
            "dfc_1phase_optimized_match": (
                dfc_1phase_optimized_match
                if dfc_1phase_optimized_match is not None
                else ""
            ),
            "dfc_1phase_optimized_match_error": dfc_1phase_optimized_match_error or "",
            "no_policy_error": no_policy_error or "",
            "dfc_1phase_error": dfc_1phase_error or "",
            "dfc_1phase_optimized_error": dfc_1phase_optimized_error or "",
        }
        return ExperimentResult(
            duration_ms=no_policy_time + dfc_1phase_time + dfc_1phase_optimized_time,
            custom_metrics=custom_metrics,
        )

    def teardown(self, _context: ExperimentContext) -> None:
        if hasattr(self, "conn"):
            with contextlib.suppress(Exception):
                self.conn.close()

    def get_metrics(self) -> list[str]:
        return [
            "self_join_count",
            "run_num",
            "tpch_sf",
            "no_policy_time_ms",
            "dfc_1phase_time_ms",
            "dfc_1phase_optimized_time_ms",
            "no_policy_rows",
            "dfc_1phase_rows",
            "dfc_1phase_optimized_rows",
            "correctness_match",
            "correctness_error",
            "no_policy_match",
            "no_policy_match_error",
            "dfc_1phase_optimized_match",
            "dfc_1phase_optimized_match_error",
            "no_policy_error",
            "dfc_1phase_error",
            "dfc_1phase_optimized_error",
        ]

    def get_setting_key(self, context: ExperimentContext) -> tuple[str, int]:
        self_join_count, _ = self._setting_and_run(context.execution_number)
        return ("self_join_count", self_join_count)
