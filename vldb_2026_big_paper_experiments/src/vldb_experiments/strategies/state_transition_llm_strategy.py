"""State-transition experiment variant that asks an LLM to gate each UPDATE."""

from __future__ import annotations

import contextlib
from dataclasses import replace
import json
import re
import time

from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import SQLRewriter

from agent_harness.config import HarnessConfig
from agent_harness.llm_factory import create_chat_model
from vldb_experiments.strategies.state_transition_strategy import (
    DEFAULT_NUM_ROWS,
    DEFAULT_NUM_UPDATES,
    DEFAULT_VALID_RATIO,
    _simulate_state_transition_counts,
    build_state_transition_policy,
    generate_state_transition_workload,
)
from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck

DEFAULT_GPT_MODEL = "gpt-5.2"
APPROACH_NO_POLICY = "no_policy"
APPROACH_GPT = "gpt_5_2"


def _build_state_transition_llm_prompt(sql: str, current_state: str, row_id: int) -> str:
    return (
        "You are validating whether a single SQL UPDATE should be allowed.\n"
        "The UPDATE always targets exactly one row.\n"
        "Allowed state transitions for that row are:\n"
        "- A -> B is allowed\n"
        "- B -> A is allowed\n"
        "- B -> C is allowed\n"
        "- A -> C is NOT allowed\n"
        "- C -> A is NOT allowed\n"
        "- C -> B is NOT allowed\n"
        "- Any transition not listed as allowed should be rejected\n\n"
        f"Current row id: {row_id}\n"
        f"Current row state before the UPDATE: {current_state}\n"
        "SQL UPDATE:\n"
        f"{sql}\n\n"
        'Return JSON only: {"allow_update": true|false}\n'
    )


def _parse_allow_update(raw_text: str) -> bool | None:
    text = raw_text.strip()
    if not text:
        return None
    try:
        decoded = json.loads(text)
        if isinstance(decoded, dict) and isinstance(decoded.get("allow_update"), bool):
            return bool(decoded["allow_update"])
    except json.JSONDecodeError:
        pass

    lower = text.lower()
    match = re.search(r"\ballow_update\b[^a-zA-Z]*(true|false)", lower)
    if match:
        return match.group(1) == "true"
    if "true" in lower and "false" not in lower:
        return True
    if "false" in lower and "true" not in lower:
        return False
    return None


class StateTransitionLLMStrategy(ExperimentStrategy):
    """Run the state-transition workload with an LLM gating every UPDATE."""

    def setup(self, context: ExperimentContext) -> None:
        self.num_rows = int(context.strategy_config.get("num_rows", DEFAULT_NUM_ROWS))
        self.num_updates = int(context.strategy_config.get("num_updates", DEFAULT_NUM_UPDATES))
        self.valid_ratio = float(context.strategy_config.get("valid_ratio", DEFAULT_VALID_RATIO))
        self.gpt_model = str(context.strategy_config.get("gpt_model", DEFAULT_GPT_MODEL))

        self.local_duckdb = _ensure_smokedduck()
        self.no_policy_conn = self.local_duckdb.connect(":memory:")
        self.dfc_conn = self.local_duckdb.connect(":memory:")
        self.gpt_conn = self.local_duckdb.connect(":memory:")

        self._reset_table(self.no_policy_conn)
        self._reset_table(self.dfc_conn)
        self._reset_table(self.gpt_conn)

        self.workload = generate_state_transition_workload(
            num_rows=self.num_rows,
            num_updates=self.num_updates,
            valid_ratio=self.valid_ratio,
        )
        self.expected_no_policy = _simulate_state_transition_counts(
            self.workload,
            self.num_rows,
            enforce_policy=False,
        )
        self.expected_gpt = _simulate_state_transition_counts(
            self.workload,
            self.num_rows,
            enforce_policy=True,
        )

        self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)
        self.dfc_rewriter.register_policy(build_state_transition_policy())

        base_cfg = HarnessConfig.from_env()
        openai_cfg = replace(base_cfg, provider="openai", openai_model=self.gpt_model)
        self.model = create_chat_model(openai_cfg)

    def _reset_table(self, conn) -> None:
        conn.execute("DROP TABLE IF EXISTS t")
        conn.execute(
            f"CREATE TABLE t AS SELECT i AS id, 'A' AS state FROM range(1, {self.num_rows + 1}) t(i)"
        )

    def _state_counts(self, conn) -> dict[str, int]:
        rows = conn.execute("SELECT state, COUNT(*) FROM t GROUP BY state ORDER BY state").fetchall()
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

    def _run_gpt(self) -> tuple[float, float, float, int, int, int, dict[str, int], int]:
        self._reset_table(self.gpt_conn)
        llm_ms = 0.0
        sql_exec_ms = 0.0
        chars_sent = 0
        correct_decisions = 0
        allowed_updates = 0
        blocked_updates = 0
        parse_failures = 0

        for step in self.workload:
            prompt = _build_state_transition_llm_prompt(
                sql=step["sql"],
                current_state=str(step["current_state"]),
                row_id=int(step["row_id"]),
            )
            chars_sent += len(prompt)
            llm_start = time.perf_counter()
            response = self.model.invoke(prompt)
            llm_ms += (time.perf_counter() - llm_start) * 1000.0
            raw = getattr(response, "content", response)
            raw_text = raw if isinstance(raw, str) else str(raw)
            allow_update = _parse_allow_update(raw_text)
            if allow_update is None:
                parse_failures += 1
                allow_update = False

            if allow_update == bool(step["is_valid"]):
                correct_decisions += 1

            if allow_update:
                exec_start = time.perf_counter()
                self.gpt_conn.execute(step["sql"])
                sql_exec_ms += (time.perf_counter() - exec_start) * 1000.0
                allowed_updates += 1
            else:
                blocked_updates += 1

        total_ms = llm_ms + sql_exec_ms
        return (
            total_ms,
            llm_ms,
            sql_exec_ms,
            chars_sent,
            correct_decisions,
            allowed_updates,
            self._state_counts(self.gpt_conn),
            parse_failures,
        )

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        phase_label = "warmup" if context.is_warmup else f"run {context.execution_number}"
        print(
            f"[Execution {context.execution_number}] "
            f"State transition LLM workload rows={self.num_rows} updates={self.num_updates} ({phase_label})"
        )

        no_policy_time_ms, no_policy_counts = self._run_no_policy()
        (
            dfc_1phase_time_ms,
            dfc_1phase_rewrite_time_ms,
            dfc_1phase_exec_time_ms,
            dfc_1phase_counts,
        ) = self._run_dfc_1phase()
        (
            gpt_time_ms,
            gpt_llm_time_ms,
            gpt_sql_exec_time_ms,
            gpt_chars_sent,
            gpt_correct_decisions,
            gpt_allowed_updates,
            gpt_counts,
            gpt_parse_failures,
        ) = self._run_gpt()

        custom_metrics = {
            "num_rows": self.num_rows,
            "num_updates": self.num_updates,
            "valid_ratio": self.valid_ratio,
            "approach_gpt_model": self.gpt_model,
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
            "dfc_1phase_matches_expected": dfc_1phase_counts == self.expected_gpt,
            "gpt_5_2_time_ms": gpt_time_ms,
            "gpt_5_2_llm_time_ms": gpt_llm_time_ms,
            "gpt_5_2_sql_exec_time_ms": gpt_sql_exec_time_ms,
            "gpt_5_2_chars_sent": gpt_chars_sent,
            "gpt_5_2_correct_decisions": gpt_correct_decisions,
            "gpt_5_2_decision_accuracy": gpt_correct_decisions / len(self.workload),
            "gpt_5_2_allowed_updates": gpt_allowed_updates,
            "gpt_5_2_blocked_updates": len(self.workload) - gpt_allowed_updates,
            "gpt_5_2_parse_failures": gpt_parse_failures,
            "gpt_5_2_a_count": gpt_counts["A"],
            "gpt_5_2_b_count": gpt_counts["B"],
            "gpt_5_2_c_count": gpt_counts["C"],
            "gpt_5_2_matches_expected": gpt_counts == self.expected_gpt,
        }
        return ExperimentResult(
            duration_ms=no_policy_time_ms + dfc_1phase_time_ms + gpt_time_ms,
            custom_metrics=custom_metrics,
        )

    def teardown(self, _context: ExperimentContext) -> None:
        for conn in [self.no_policy_conn, self.dfc_conn, self.gpt_conn]:
            with contextlib.suppress(Exception):
                conn.close()
