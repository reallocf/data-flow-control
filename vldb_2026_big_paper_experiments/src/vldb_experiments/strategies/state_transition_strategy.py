"""State-transition UPDATE experiment strategy."""

from __future__ import annotations

import contextlib
import pathlib
import time
from typing import Any

from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import DFCPolicy, Resolution, SQLRewriter

from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck

DEFAULT_NUM_ROWS = 1000
DEFAULT_NUM_UPDATES = 1000
DEFAULT_WARMUP_RUNS = 1
DEFAULT_MEASURED_RUNS = 5
DEFAULT_VALID_RATIO = 0.7


def build_state_transition_policy() -> DFCPolicy:
    """Build the state-transition policy used by the experiment."""
    return DFCPolicy(
        sources=["t"],
        sink="t",
        sink_alias="t2",
        constraint=(
            "count(distinct t.id) = 1 AND "
            "max(t.id) = t2.id AND "
            "case "
            "when max(t.state) = 'A' then t2.state = 'B' "
            "when max(t.state) = 'B' then t2.state in ('A', 'C') "
            "when max(t.state) = 'C' then false "
            "end"
        ),
        on_fail=Resolution.REMOVE,
        description="state_transition_policy",
    )


def _valid_next_state(current_state: str, row_id: int) -> str:
    if current_state == "A":
        return "B"
    if current_state == "B":
        return "C" if row_id % 2 == 1 else "A"
    raise ValueError("valid transitions cannot originate from state C")


def _invalid_next_state(current_state: str, invalid_count_for_row: int) -> str:
    if current_state == "A":
        return "C"
    if current_state == "C":
        return "A" if invalid_count_for_row % 2 == 0 else "B"
    raise ValueError("invalid transitions must originate from state A or C")


def _simulate_state_transition_counts(
    workload: list[dict[str, Any]],
    num_rows: int,
    enforce_policy: bool,
) -> dict[str, int]:
    state_model = dict.fromkeys(range(1, num_rows + 1), "A")
    for step in workload:
        row_id = step["row_id"]
        if enforce_policy and not step["is_valid"]:
            continue
        state_model[row_id] = step["next_state"]

    counts = {"A": 0, "B": 0, "C": 0}
    for state in state_model.values():
        counts[state] += 1
    return counts


def _pick_next_row_with_state(
    row_ids: list[int],
    state_model: dict[int, str],
    target_state: str,
    cursor: int,
) -> tuple[int | None, int]:
    if not row_ids:
        return None, cursor

    num_rows = len(row_ids)
    for offset in range(num_rows):
        idx = (cursor + offset) % num_rows
        row_id = row_ids[idx]
        if state_model[row_id] == target_state:
            return row_id, idx + 1
    return None, cursor


def build_state_transition_update(row_id: int, next_state: str) -> str:
    """Build a single UPDATE statement for the workload."""
    return (
        f"UPDATE t AS t2 SET state = '{next_state}' "
        f"FROM t WHERE t.id = t2.id AND t.id = {row_id}"
    )


def generate_state_transition_workload(
    num_rows: int = DEFAULT_NUM_ROWS,
    num_updates: int = DEFAULT_NUM_UPDATES,
    valid_ratio: float = DEFAULT_VALID_RATIO,
) -> list[dict[str, Any]]:
    """Generate a deterministic workload with 70% valid and 30% invalid updates."""
    if num_rows < 1:
        raise ValueError("num_rows must be >= 1")
    if num_updates < 1:
        raise ValueError("num_updates must be >= 1")
    if not 0 < valid_ratio < 1:
        raise ValueError("valid_ratio must be between 0 and 1")

    valid_updates = int(num_updates * valid_ratio)
    invalid_updates = num_updates - valid_updates

    state_model = dict.fromkeys(range(1, num_rows + 1), "A")
    invalid_counts_by_row = dict.fromkeys(range(1, num_rows + 1), 0)
    workload: list[dict[str, Any]] = []

    valid_cursor = 0
    a_cursor = 0
    b_cursor = 0
    invalid_a_cursor = 0
    invalid_c_cursor = 0
    invalid_cursor = 0
    row_ids = list(range(1, num_rows + 1))

    for update_index in range(num_updates):
        block_position = update_index % 10
        is_valid = (update_index % 10) < 7 and valid_cursor < valid_updates
        if invalid_cursor >= invalid_updates:
            is_valid = True
        if valid_cursor >= valid_updates:
            is_valid = False

        if is_valid:
            prefer_a_state = block_position < 4
            preferred_state = "A" if prefer_a_state else "B"
            fallback_state = "B" if prefer_a_state else "A"
            preferred_cursor = a_cursor if preferred_state == "A" else b_cursor
            row_id, next_cursor = _pick_next_row_with_state(
                row_ids,
                state_model,
                preferred_state,
                preferred_cursor,
            )
            if row_id is not None:
                if preferred_state == "A":
                    a_cursor = next_cursor
                else:
                    b_cursor = next_cursor
            else:
                fallback_cursor = a_cursor if fallback_state == "A" else b_cursor
                row_id, next_cursor = _pick_next_row_with_state(
                    row_ids,
                    state_model,
                    fallback_state,
                    fallback_cursor,
                )
                if row_id is None:
                    raise ValueError("workload generation requires at least one A- or B-state row")
                if fallback_state == "A":
                    a_cursor = next_cursor
                else:
                    b_cursor = next_cursor
            if row_id is None:
                raise ValueError("workload generation requires at least one A- or B-state row")
            current_state = state_model[row_id]
            next_state = _valid_next_state(current_state, row_id)
            state_model[row_id] = next_state
            valid_cursor += 1
        else:
            prefer_a_state = block_position == 8
            preferred_state = "A" if prefer_a_state else "C"
            fallback_state = "C" if prefer_a_state else "A"
            preferred_cursor = invalid_a_cursor if preferred_state == "A" else invalid_c_cursor
            row_id, next_cursor = _pick_next_row_with_state(
                row_ids,
                state_model,
                preferred_state,
                preferred_cursor,
            )
            if row_id is not None:
                if preferred_state == "A":
                    invalid_a_cursor = next_cursor
                else:
                    invalid_c_cursor = next_cursor
            else:
                fallback_cursor = invalid_a_cursor if fallback_state == "A" else invalid_c_cursor
                row_id, next_cursor = _pick_next_row_with_state(
                    row_ids,
                    state_model,
                    fallback_state,
                    fallback_cursor,
                )
                if row_id is None:
                    raise ValueError(
                        "workload generation requires at least one A- or C-state row before invalid transitions"
                    )
                if fallback_state == "A":
                    invalid_a_cursor = next_cursor
                else:
                    invalid_c_cursor = next_cursor
            current_state = state_model[row_id]
            next_state = _invalid_next_state(current_state, invalid_counts_by_row[row_id])
            invalid_counts_by_row[row_id] += 1
            invalid_cursor += 1

        workload.append(
            {
                "update_num": update_index + 1,
                "row_id": row_id,
                "current_state": current_state,
                "next_state": next_state,
                "is_valid": is_valid,
                "sql": build_state_transition_update(row_id, next_state),
            }
        )

    return workload


class StateTransitionStrategy(ExperimentStrategy):
    """Run the state-transition workload with and without DFC policies."""

    def setup(self, context: ExperimentContext) -> None:
        self.num_rows = int(context.strategy_config.get("num_rows", DEFAULT_NUM_ROWS))
        self.num_updates = int(context.strategy_config.get("num_updates", DEFAULT_NUM_UPDATES))
        self.valid_ratio = float(context.strategy_config.get("valid_ratio", DEFAULT_VALID_RATIO))
        db_path = context.strategy_config.get("db_path", "./results/state_transition.duckdb")
        pathlib.Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self.local_duckdb = _ensure_smokedduck()
        self.no_policy_conn = self.local_duckdb.connect(":memory:")
        self.dfc_conn = self.local_duckdb.connect(":memory:")

        self._reset_table(self.no_policy_conn)
        self._reset_table(self.dfc_conn)

        self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)
        self.dfc_rewriter.register_policy(build_state_transition_policy())

        self.workload = generate_state_transition_workload(
            num_rows=self.num_rows,
            num_updates=self.num_updates,
            valid_ratio=self.valid_ratio,
        )
        self.expected_valid_updates = sum(1 for step in self.workload if step["is_valid"])
        self.expected_invalid_updates = len(self.workload) - self.expected_valid_updates
        self.expected_no_policy = _simulate_state_transition_counts(
            self.workload,
            self.num_rows,
            enforce_policy=False,
        )
        self.expected_dfc = _simulate_state_transition_counts(
            self.workload,
            self.num_rows,
            enforce_policy=True,
        )

    def _reset_table(self, conn) -> None:
        conn.execute("DROP TABLE IF EXISTS t")
        conn.execute(
            f"CREATE TABLE t AS SELECT i AS id, 'A' AS state FROM range(1, {self.num_rows + 1}) t(i)"
        )

    def _state_counts(self, conn) -> dict[str, int]:
        rows = conn.execute(
            "SELECT state, COUNT(*) FROM t GROUP BY state ORDER BY state"
        ).fetchall()
        counts = {"A": 0, "B": 0, "C": 0}
        for state, count in rows:
            counts[state] = count
        return counts

    def _run_no_policy(self) -> tuple[float, dict[str, int]]:
        self._reset_table(self.no_policy_conn)
        start = time.perf_counter()
        for step in self.workload:
            self.no_policy_conn.execute(step["sql"])
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return elapsed_ms, self._state_counts(self.no_policy_conn)

    def _run_dfc_1phase(self) -> tuple[float, float, float, dict[str, int]]:
        self._reset_table(self.dfc_conn)
        rewrite_ms = 0.0
        exec_ms = 0.0

        for step in self.workload:
            rewrite_start = time.perf_counter()
            transformed = self.dfc_rewriter.transform_query(step["sql"])
            rewrite_ms += (time.perf_counter() - rewrite_start) * 1000.0

            exec_start = time.perf_counter()
            self.dfc_conn.execute(transformed)
            exec_ms += (time.perf_counter() - exec_start) * 1000.0

        total_ms = rewrite_ms + exec_ms
        return total_ms, rewrite_ms, exec_ms, self._state_counts(self.dfc_conn)

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        phase_label = "warmup" if context.is_warmup else f"run {context.execution_number}"
        print(
            f"[Execution {context.execution_number}] "
            f"State transition workload rows={self.num_rows} updates={self.num_updates} ({phase_label})"
        )

        no_policy_time_ms, no_policy_counts = self._run_no_policy()
        (
            dfc_1phase_time_ms,
            dfc_1phase_rewrite_time_ms,
            dfc_1phase_exec_time_ms,
            dfc_1phase_counts,
        ) = self._run_dfc_1phase()

        custom_metrics = {
            "num_rows": self.num_rows,
            "num_updates": self.num_updates,
            "valid_ratio": self.valid_ratio,
            "expected_valid_updates": self.expected_valid_updates,
            "expected_invalid_updates": self.expected_invalid_updates,
            "no_policy_time_ms": no_policy_time_ms,
            "no_policy_a_count": no_policy_counts["A"],
            "no_policy_b_count": no_policy_counts["B"],
            "no_policy_c_count": no_policy_counts["C"],
            "no_policy_matches_expected": no_policy_counts == self.expected_no_policy,
            "dfc_1phase_time_ms": dfc_1phase_time_ms,
            "dfc_1phase_rewrite_time_ms": dfc_1phase_rewrite_time_ms,
            "dfc_1phase_exec_time_ms": dfc_1phase_exec_time_ms,
            "dfc_1phase_a_count": dfc_1phase_counts["A"],
            "dfc_1phase_b_count": dfc_1phase_counts["B"],
            "dfc_1phase_c_count": dfc_1phase_counts["C"],
            "dfc_1phase_matches_expected": dfc_1phase_counts == self.expected_dfc,
        }
        return ExperimentResult(
            duration_ms=no_policy_time_ms + dfc_1phase_time_ms,
            custom_metrics=custom_metrics,
        )

    def teardown(self, _context: ExperimentContext) -> None:
        for conn in [self.no_policy_conn, self.dfc_conn]:
            with contextlib.suppress(Exception):
                conn.close()
