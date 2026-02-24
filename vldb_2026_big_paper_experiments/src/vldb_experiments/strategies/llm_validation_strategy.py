"""LLM policy-violation validation strategy using TPC-H queries."""

from __future__ import annotations

import contextlib
from dataclasses import replace
import json
import pathlib
import re
import time
from typing import Any

from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import DFCPolicy, Resolution, SQLRewriter

from agent_harness.config import HarnessConfig
from agent_harness.llm_factory import create_chat_model
from vldb_experiments.strategies.tpch_strategy import (
    TPCH_QUERIES,
    _ensure_smokedduck,
    load_tpch_query,
)

DEFAULT_POLICY_COUNTS = [1, 2, 4, 8, 16, 32]
DEFAULT_RUNS_PER_SETTING = 5
DEFAULT_TPCH_SF = 1.0
DEFAULT_CLAUDE_MODEL = "claude-4.6-opus"
DEFAULT_GPT_MODEL = "gpt-5.2"
TPCH_QUERIES_ALL = list(TPCH_QUERIES)

APPROACH_DFC_1PHASE = "dfc_1phase"
APPROACH_OPUS_QUERY_ONLY = "opus_query_only"
APPROACH_GPT_QUERY_ONLY = "gpt_query_only"
APPROACH_OPUS_QUERY_RESULTS = "opus_query_results"
APPROACH_GPT_QUERY_RESULTS = "gpt_query_results"


def _policy_catalog() -> list[tuple[str, str]]:
    return [
        ("avg(lineitem.l_quantity) <= 30", "Average quantity should remain at or below 30."),
        ("avg(lineitem.l_quantity) >= 20", "Average quantity should be at least 20."),
        ("max(lineitem.l_quantity) <= 50", "No line item quantity should exceed 50 units."),
        ("min(lineitem.l_quantity) >= 1", "All line item quantities must be at least 1."),
        ("avg(lineitem.l_discount) <= 0.06", "Average discount should not exceed 6%."),
        ("max(lineitem.l_discount) <= 0.07", "No single discount should exceed 7%."),
        ("min(lineitem.l_discount) >= 0.00", "Discount should be non-negative."),
        ("avg(lineitem.l_tax) <= 0.06", "Average tax rate should stay under 6%."),
        ("max(lineitem.l_tax) <= 0.06", "No single tax rate should exceed 6%."),
        ("min(lineitem.l_tax) >= 0.00", "Tax should be non-negative."),
        ("min(lineitem.l_extendedprice) >= 0", "Extended prices should be non-negative."),
        ("avg(lineitem.l_extendedprice) <= 60000", "Average extended price should not exceed 60,000."),
        ("max(lineitem.l_extendedprice) <= 90000", "Single extended price should not exceed 90,000."),
        ("count(lineitem.l_orderkey) >= 1", "There should be at least one line item."),
        ("count(distinct lineitem.l_shipmode) <= 5", "Distinct ship modes should not exceed 5."),
        ("count(distinct lineitem.l_returnflag) <= 2", "Distinct return flags should not exceed 2."),
        ("count(distinct lineitem.l_linestatus) <= 2", "Distinct line statuses should not exceed 2."),
        ("max(lineitem.l_linenumber) <= 5", "Line numbers should stay within 1..5."),
        ("min(lineitem.l_linenumber) >= 1", "Line numbers should be at least 1."),
        ("min(lineitem.l_shipdate) >= DATE '1994-01-01'", "Ship dates should be on/after 1994-01-01."),
        ("max(lineitem.l_shipdate) <= DATE '1997-12-31'", "Ship dates should be on/before 1997-12-31."),
        ("min(lineitem.l_receiptdate) >= DATE '1994-01-01'", "Receipt dates should be on/after 1994-01-01."),
        ("max(lineitem.l_receiptdate) <= DATE '1997-12-31'", "Receipt dates should be on/before 1997-12-31."),
        ("min(lineitem.l_commitdate) >= DATE '1994-01-01'", "Commit dates should be on/after 1994-01-01."),
        ("max(lineitem.l_commitdate) <= DATE '1997-12-31'", "Commit dates should be on/before 1997-12-31."),
        ("sum(lineitem.l_discount) <= 180000", "Total discounts should stay below 180,000."),
        ("sum(lineitem.l_quantity) <= 80000000", "Total quantity should stay below 80M."),
        ("min(lineitem.l_orderkey) >= 1", "Order keys should be positive."),
        ("max(lineitem.l_orderkey) <= 3000000", "Order keys should remain within expected TPCH range."),
        ("min(lineitem.l_partkey) >= 1", "Part keys should be positive."),
        ("max(lineitem.l_partkey) <= 100000", "Part keys should remain within expected TPCH range."),
        ("max(lineitem.l_suppkey) <= 5000", "Supplier keys should remain within expected TPCH range."),
    ]


def _build_policies(policy_count: int) -> list[DFCPolicy]:
    selected = _policy_catalog()[:policy_count]
    return [
        DFCPolicy(
            sources=["lineitem"],
            constraint=constraint,
            on_fail=Resolution.INVALIDATE,
            description=description,
        )
        for constraint, description in selected
    ]


def _message_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and item.get("text"):
                parts.append(str(item["text"]))
        return "\n".join(parts).strip()
    return str(content)


def _parse_violation_prediction(raw_text: str) -> bool | None:
    text = raw_text.strip()
    if not text:
        return None
    try:
        decoded = json.loads(text)
        if isinstance(decoded, dict) and isinstance(decoded.get("violates_policy"), bool):
            return bool(decoded["violates_policy"])
    except json.JSONDecodeError:
        pass

    lower = text.lower()
    if re.search(r"\bviolates_policy\b[^a-zA-Z]*(true|false)", lower):
        match = re.search(r"\bviolates_policy\b[^a-zA-Z]*(true|false)", lower)
        if match:
            return match.group(1) == "true"
    if "true" in lower and "false" not in lower:
        return True
    if "false" in lower and "true" not in lower:
        return False
    return None


def _format_result_rows(columns: list[str], rows: list[tuple[Any, ...]]) -> str:
    records = [{col: row[i] for i, col in enumerate(columns)} for row in rows]
    return json.dumps(records, default=str)


def _build_query_only_prompt(query: str, policy_descriptions: list[str]) -> str:
    policies_block = "\n".join([f"{idx + 1}. {desc}" for idx, desc in enumerate(policy_descriptions)])
    return (
        "You are validating whether query output would violate active data policies.\n"
        "Decide if ANY policy is violated.\n"
        'Return JSON only: {"violates_policy": true|false}\n\n'
        "SQL Query:\n"
        f"{query}\n\n"
        "Policies:\n"
        f"{policies_block}\n"
    )


def _build_query_results_prompt(query: str, policy_descriptions: list[str], sample_rows_json: str) -> str:
    policies_block = "\n".join([f"{idx + 1}. {desc}" for idx, desc in enumerate(policy_descriptions)])
    return (
        "You are validating whether query output violates active data policies.\n"
        "Decide if ANY policy is violated based on the query and provided sample rows.\n"
        'Return JSON only: {"violates_policy": true|false}\n\n'
        "SQL Query:\n"
        f"{query}\n\n"
        "Policies:\n"
        f"{policies_block}\n\n"
        "First 100 result rows:\n"
        f"{sample_rows_json}\n"
    )


class LLMValidationStrategy(ExperimentStrategy):
    """Evaluate policy-violation identification with 1Phase and LLM baselines."""

    def setup(self, context: ExperimentContext) -> None:
        self.scale_factor = float(context.strategy_config.get("tpch_sf", DEFAULT_TPCH_SF))
        self.policy_counts = [int(v) for v in context.strategy_config.get("policy_counts", DEFAULT_POLICY_COUNTS)]
        self.runs_per_setting = int(context.strategy_config.get("runs_per_setting", DEFAULT_RUNS_PER_SETTING))
        self.include_bedrock = bool(context.strategy_config.get("include_bedrock", True))
        self.include_openai = bool(context.strategy_config.get("include_openai", True))
        self.claude_model = str(context.strategy_config.get("claude_model", DEFAULT_CLAUDE_MODEL))
        self.gpt_model = str(context.strategy_config.get("gpt_model", DEFAULT_GPT_MODEL))

        db_path = context.strategy_config.get("tpch_db_path")
        if not db_path:
            db_path = f"./results/llm_validation_sf{self.scale_factor}.db"
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
        self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)

        configured_queries = context.strategy_config.get("queries", TPCH_QUERIES_ALL)
        self.query_nums = [int(q) for q in configured_queries]
        if not self.query_nums:
            raise ValueError("queries must contain at least one TPC-H query number")
        self.query_result_cache: dict[int, tuple[list[str], list[tuple[Any, ...]], float]] = {}
        self.truth_cache: dict[tuple[int, int], tuple[bool, float, float, int]] = {}

        base_cfg = HarnessConfig.from_env()
        self.llm_clients: dict[str, Any] = {}
        if self.include_openai:
            openai_cfg = replace(base_cfg, provider="openai", openai_model=self.gpt_model)
            self.llm_clients["gpt"] = create_chat_model(openai_cfg)
        if self.include_bedrock:
            bedrock_cfg = replace(base_cfg, provider="bedrock", bedrock_model_id=self.claude_model)
            self.llm_clients["opus"] = create_chat_model(bedrock_cfg)

        self.settings: list[tuple[int, int, str]] = []
        for query_num in self.query_nums:
            for policy_count in self.policy_counts:
                self.settings.append((query_num, policy_count, APPROACH_DFC_1PHASE))
                if self.include_bedrock:
                    self.settings.append((query_num, policy_count, APPROACH_OPUS_QUERY_ONLY))
                if self.include_openai:
                    self.settings.append((query_num, policy_count, APPROACH_GPT_QUERY_ONLY))
                if self.include_bedrock:
                    self.settings.append((query_num, policy_count, APPROACH_OPUS_QUERY_RESULTS))
                if self.include_openai:
                    self.settings.append((query_num, policy_count, APPROACH_GPT_QUERY_RESULTS))

    def _setting_and_run(self, execution_number: int) -> tuple[int, int, str, int]:
        setting_index = (execution_number - 1) // self.runs_per_setting
        run_num = ((execution_number - 1) % self.runs_per_setting) + 1
        query_num, policy_count, approach = self.settings[setting_index]
        return query_num, policy_count, approach, run_num

    def _clear_and_register_policies(self, policy_count: int) -> list[DFCPolicy]:
        existing = self.dfc_rewriter.get_dfc_policies()
        for old_policy in existing:
            self.dfc_rewriter.delete_policy(
                sources=old_policy.sources,
                constraint=old_policy.constraint,
                on_fail=old_policy.on_fail,
            )
        policies = _build_policies(policy_count)
        for policy in policies:
            self.dfc_rewriter.register_policy(policy)
        return policies

    def _query_result_sample(self, query_num: int, query: str) -> tuple[list[str], list[tuple[Any, ...]], bool]:
        if query_num in self.query_result_cache:
            columns, rows, _ = self.query_result_cache[query_num]
            return columns, rows, True
        start = time.perf_counter()
        cursor = self.no_policy_conn.execute(query)
        all_rows = cursor.fetchall()
        query_time_ms = (time.perf_counter() - start) * 1000.0
        columns = [d[0] for d in (cursor.description or [])]
        rows = all_rows[:100]
        self.query_result_cache[query_num] = (columns, rows, query_time_ms)
        return columns, rows, False

    def _dfc_truth(self, query_num: int, policy_count: int, query: str) -> tuple[bool, float, float, int]:
        key = (query_num, policy_count)
        if key in self.truth_cache:
            return self.truth_cache[key]

        policies = self._clear_and_register_policies(policy_count)
        rewrite_start = time.perf_counter()
        transformed = self.dfc_rewriter.transform_query(query, use_two_phase=False)
        rewrite_ms = (time.perf_counter() - rewrite_start) * 1000.0

        exec_start = time.perf_counter()
        cursor = self.dfc_conn.execute(transformed)
        rows = cursor.fetchall()
        exec_ms = (time.perf_counter() - exec_start) * 1000.0
        columns = [d[0] for d in (cursor.description or [])]
        lower_cols = [c.lower() for c in columns]
        violation = False
        if "valid" in lower_cols:
            valid_idx = lower_cols.index("valid")
            violation = any(not bool(row[valid_idx]) for row in rows)
        result = (violation, rewrite_ms, exec_ms, len(policies))
        self.truth_cache[key] = result
        return result

    def _run_llm(self, approach: str, query: str, policy_descriptions: list[str], sample_json: str | None) -> tuple[bool | None, float, int, str]:
        is_query_results = approach in {APPROACH_OPUS_QUERY_RESULTS, APPROACH_GPT_QUERY_RESULTS}
        if is_query_results:
            prompt = _build_query_results_prompt(query, policy_descriptions, sample_json or "[]")
        else:
            prompt = _build_query_only_prompt(query, policy_descriptions)
        prompt_chars = len(prompt)

        model = self.llm_clients["opus"] if approach.startswith("opus") else self.llm_clients["gpt"]

        start = time.perf_counter()
        response = model.invoke(prompt)
        runtime_ms = (time.perf_counter() - start) * 1000.0
        raw = _message_text(getattr(response, "content", response))
        predicted = _parse_violation_prediction(raw)
        return predicted, runtime_ms, prompt_chars, raw

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        query_num, policy_count, approach, run_num = self._setting_and_run(context.execution_number)
        phase = "warmup" if context.is_warmup else f"run {run_num}"
        print(
            f"[Execution {context.execution_number}] llm_validation q{query_num:02d} "
            f"policies={policy_count} approach={approach} ({phase})"
        )

        query = load_tpch_query(query_num)
        truth_violation, rewrite_ms, dfc_exec_ms, effective_policy_count = self._dfc_truth(
            query_num=query_num,
            policy_count=policy_count,
            query=query,
        )
        policy_descriptions = [
            p.description or p.constraint for p in _build_policies(policy_count)
        ]

        try:
            if approach == APPROACH_DFC_1PHASE:
                custom = {
                    "query_num": query_num,
                    "tpch_sf": self.scale_factor,
                    "policy_count": policy_count,
                    "effective_policy_count": effective_policy_count,
                    "approach": approach,
                    "provider": "none",
                    "model_name": "none",
                    "runtime_ms": dfc_exec_ms,
                    "dfc_1phase_rewrite_time_ms": rewrite_ms,
                    "dfc_1phase_exec_time_ms": dfc_exec_ms,
                    "ground_truth_violation": truth_violation,
                    "predicted_violation": truth_violation,
                    "correct_identification": True,
                    "llm_chars_sent": 0,
                    "query_results_cache_hit": True,
                    "query_results_rows": 0,
                    "raw_response": "",
                }
                return ExperimentResult(duration_ms=dfc_exec_ms, custom_metrics=custom)

            sample_json = None
            cache_hit = True
            sample_rows_count = 0
            if approach in {APPROACH_OPUS_QUERY_RESULTS, APPROACH_GPT_QUERY_RESULTS}:
                cols, rows, cache_hit = self._query_result_sample(query_num, query)
                sample_rows_count = len(rows)
                sample_json = _format_result_rows(cols, rows)

            predicted, runtime_ms, chars_sent, raw_response = self._run_llm(
                approach=approach,
                query=query,
                policy_descriptions=policy_descriptions,
                sample_json=sample_json,
            )
            custom = {
                "query_num": query_num,
                "tpch_sf": self.scale_factor,
                "policy_count": policy_count,
                "effective_policy_count": effective_policy_count,
                "approach": approach,
                "provider": "bedrock" if approach.startswith("opus") else "openai",
                "model_name": self.claude_model if approach.startswith("opus") else self.gpt_model,
                "runtime_ms": runtime_ms,
                "dfc_1phase_rewrite_time_ms": 0.0,
                "dfc_1phase_exec_time_ms": 0.0,
                "ground_truth_violation": truth_violation,
                "predicted_violation": predicted,
                "correct_identification": (predicted == truth_violation),
                "llm_chars_sent": chars_sent,
                "query_results_cache_hit": cache_hit,
                "query_results_rows": sample_rows_count,
                "raw_response": raw_response[:2000],
            }
            return ExperimentResult(duration_ms=runtime_ms, custom_metrics=custom)
        except Exception as exc:
            return ExperimentResult(
                duration_ms=0.0,
                error=str(exc),
                custom_metrics={
                    "query_num": query_num,
                    "tpch_sf": self.scale_factor,
                    "policy_count": policy_count,
                    "effective_policy_count": effective_policy_count,
                    "approach": approach,
                    "provider": "bedrock" if approach.startswith("opus") else ("openai" if approach.startswith("gpt") else "none"),
                    "model_name": self.claude_model if approach.startswith("opus") else (self.gpt_model if approach.startswith("gpt") else "none"),
                    "ground_truth_violation": truth_violation,
                    "predicted_violation": "",
                    "correct_identification": "",
                    "llm_chars_sent": 0 if approach == APPROACH_DFC_1PHASE else -1,
                    "query_results_cache_hit": "",
                    "query_results_rows": 0,
                    "raw_response": "",
                },
            )

    def teardown(self, _context: ExperimentContext) -> None:
        return None

    def get_metrics(self) -> list[str]:
        return [
            "query_num",
            "tpch_sf",
            "policy_count",
            "effective_policy_count",
            "approach",
            "provider",
            "model_name",
            "runtime_ms",
            "dfc_1phase_rewrite_time_ms",
            "dfc_1phase_exec_time_ms",
            "ground_truth_violation",
            "predicted_violation",
            "correct_identification",
            "llm_chars_sent",
            "query_results_cache_hit",
            "query_results_rows",
            "raw_response",
        ]

    def get_setting_key(self, context: ExperimentContext) -> Any | None:
        query_num, policy_count, approach, _ = self._setting_and_run(context.execution_number)
        return (query_num, policy_count, approach)
