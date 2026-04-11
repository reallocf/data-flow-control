"""LLM validation strategy that varies both query and database."""

from __future__ import annotations

import contextlib
from dataclasses import replace
import pathlib
import time
from typing import Any
import uuid

from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import SQLRewriter

from agent_harness.config import HarnessConfig
from agent_harness.llm_factory import create_chat_model
from vldb_experiments.strategies.llm_validation_common import (
    APPROACH_DFC_1PHASE,
    APPROACH_GPT_QUERY_ONLY,
    APPROACH_GPT_QUERY_RESULTS,
    APPROACH_OPUS_QUERY_ONLY,
    APPROACH_OPUS_QUERY_RESULTS,
    DEFAULT_CLAUDE_MODEL,
    DEFAULT_GPT_MODEL,
    DEFAULT_POLICY_COUNTS,
    DEFAULT_RUNS_PER_SETTING,
    build_policies,
    build_query_only_prompt,
    build_query_results_prompt,
    default_database_specs,
    format_result_rows,
    message_text,
    normalize_database_specs,
    parse_violation_prediction,
)
from vldb_experiments.strategies.llm_validation_strategy import TPCH_QUERIES_ALL
from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck, load_tpch_query

DEFAULT_DATABASE_SFS = [0.1, 0.5, 1.0, 5.0, 10.0]


class LLMValidationGridStrategy(ExperimentStrategy):
    """Evaluate policy-violation identification across queries and databases."""

    def setup(self, context: ExperimentContext) -> None:
        self.policy_counts = [int(v) for v in context.strategy_config.get("policy_counts", DEFAULT_POLICY_COUNTS)]
        self.runs_per_setting = int(context.strategy_config.get("runs_per_setting", DEFAULT_RUNS_PER_SETTING))
        self.include_bedrock = bool(context.strategy_config.get("include_bedrock", True))
        self.include_openai = bool(context.strategy_config.get("include_openai", True))
        self.include_gpt_query_only = bool(context.strategy_config.get("include_gpt_query_only", self.include_openai))
        self.include_gpt_query_results = bool(
            context.strategy_config.get("include_gpt_query_results", self.include_openai)
        )
        self.include_opus_query_only = bool(context.strategy_config.get("include_opus_query_only", self.include_bedrock))
        self.include_opus_query_results = bool(
            context.strategy_config.get("include_opus_query_results", self.include_bedrock)
        )
        self.claude_model = str(context.strategy_config.get("claude_model", DEFAULT_CLAUDE_MODEL))
        self.gpt_model = str(context.strategy_config.get("gpt_model", DEFAULT_GPT_MODEL))
        configured_queries = context.strategy_config.get("queries", TPCH_QUERIES_ALL)
        self.query_nums = [int(q) for q in configured_queries]
        if not self.query_nums:
            raise ValueError("queries must contain at least one TPC-H query number")

        raw_database_specs = context.strategy_config.get("database_specs")
        raw_database_sfs = context.strategy_config.get("database_sfs", DEFAULT_DATABASE_SFS)
        self.database_specs = normalize_database_specs(
            database_specs=raw_database_specs,
            database_sfs=[float(v) for v in raw_database_sfs] if raw_database_sfs is not None else None,
            base_filename_prefix="llm_validation_grid",
        )
        if not self.database_specs:
            self.database_specs = default_database_specs(DEFAULT_DATABASE_SFS, "llm_validation_grid")

        self.local_duckdb = _ensure_smokedduck()
        self.no_policy_conns: dict[str, Any] = {}
        self.dfc_conns: dict[str, Any] = {}
        self.dfc_rewriters: dict[str, SQLRewriter] = {}
        self.database_sf_by_label: dict[str, float] = {}
        self.database_path_by_label: dict[str, str] = {}

        for spec in self.database_specs:
            label = str(spec["label"])
            scale_factor = float(spec["tpch_sf"])
            db_path = str(spec["db_path"])
            pathlib.Path(db_path).parent.mkdir(parents=True, exist_ok=True)

            conn = self.local_duckdb.connect(db_path)
            with contextlib.suppress(Exception):
                conn.execute("INSTALL tpch")
            conn.execute("LOAD tpch")
            table_exists = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'lineitem'"
            ).fetchone()[0]
            if table_exists == 0:
                conn.execute(f"CALL dbgen(sf={scale_factor})")

            self.no_policy_conns[label] = conn
            self.dfc_conns[label] = conn
            self.dfc_rewriters[label] = SQLRewriter(conn=conn)
            self.database_sf_by_label[label] = scale_factor
            self.database_path_by_label[label] = db_path

        base_cfg = HarnessConfig.from_env()
        self.llm_clients: dict[str, Any] = {}
        if self.include_openai and (self.include_gpt_query_only or self.include_gpt_query_results):
            openai_cfg = replace(base_cfg, provider="openai", openai_model=self.gpt_model)
            self.llm_clients["gpt"] = create_chat_model(openai_cfg)
        if self.include_bedrock and (self.include_opus_query_only or self.include_opus_query_results):
            bedrock_cfg = replace(base_cfg, provider="bedrock", bedrock_model_id=self.claude_model)
            self.llm_clients["opus"] = create_chat_model(bedrock_cfg)

        self.query_result_cache: dict[tuple[str, int], tuple[list[str], list[tuple[Any, ...]], float]] = {}
        self.truth_cache: dict[tuple[str, int, int], tuple[bool, float, float, int]] = {}
        self.settings: list[tuple[str, int, int, str]] = []
        for db_spec in self.database_specs:
            db_label = str(db_spec["label"])
            for query_num in self.query_nums:
                for policy_count in self.policy_counts:
                    self.settings.append((db_label, query_num, policy_count, APPROACH_DFC_1PHASE))
                    if self.include_opus_query_only:
                        self.settings.append((db_label, query_num, policy_count, APPROACH_OPUS_QUERY_ONLY))
                    if self.include_gpt_query_only:
                        self.settings.append((db_label, query_num, policy_count, APPROACH_GPT_QUERY_ONLY))
                    if self.include_opus_query_results:
                        self.settings.append((db_label, query_num, policy_count, APPROACH_OPUS_QUERY_RESULTS))
                    if self.include_gpt_query_results:
                        self.settings.append((db_label, query_num, policy_count, APPROACH_GPT_QUERY_RESULTS))

    def _setting_and_run(self, execution_number: int) -> tuple[str, int, int, str, int]:
        setting_index = (execution_number - 1) // self.runs_per_setting
        run_num = ((execution_number - 1) % self.runs_per_setting) + 1
        db_label, query_num, policy_count, approach = self.settings[setting_index]
        return db_label, query_num, policy_count, approach, run_num

    def _clear_and_register_policies(self, db_label: str, policy_count: int) -> list[Any]:
        rewriter = self.dfc_rewriters[db_label]
        existing = rewriter.get_dfc_policies()
        for old_policy in existing:
            rewriter.delete_policy(
                sources=old_policy.sources,
                constraint=old_policy.constraint,
                on_fail=old_policy.on_fail,
            )
        policies = build_policies(policy_count)
        for policy in policies:
            rewriter.register_policy(policy)
        return policies

    def _query_result_sample(self, db_label: str, query_num: int, query: str) -> tuple[list[str], list[tuple[Any, ...]], bool]:
        key = (db_label, query_num)
        if key in self.query_result_cache:
            columns, rows, _ = self.query_result_cache[key]
            return columns, rows, True
        start = time.perf_counter()
        cursor = self.no_policy_conns[db_label].execute(query)
        all_rows = cursor.fetchall()
        query_time_ms = (time.perf_counter() - start) * 1000.0
        columns = [d[0] for d in (cursor.description or [])]
        rows = all_rows[:100]
        self.query_result_cache[key] = (columns, rows, query_time_ms)
        return columns, rows, False

    def _dfc_truth(self, db_label: str, query_num: int, policy_count: int, query: str) -> tuple[bool, float, float, int]:
        key = (db_label, query_num, policy_count)
        if key in self.truth_cache:
            return self.truth_cache[key]

        policies = self._clear_and_register_policies(db_label, policy_count)
        rewriter = self.dfc_rewriters[db_label]
        conn = self.dfc_conns[db_label]

        rewrite_start = time.perf_counter()
        transformed = rewriter.transform_query(query, use_two_phase=False)
        rewrite_ms = (time.perf_counter() - rewrite_start) * 1000.0

        exec_start = time.perf_counter()
        cursor = conn.execute(transformed)
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

    def _run_llm(
        self,
        approach: str,
        query: str,
        policy_descriptions: list[str],
        sample_json: str | None,
        run_nonce: str,
    ) -> tuple[bool | None, float, int, str]:
        is_query_results = approach in {APPROACH_OPUS_QUERY_RESULTS, APPROACH_GPT_QUERY_RESULTS}
        if is_query_results:
            prompt = build_query_results_prompt(query, policy_descriptions, sample_json or "[]", run_nonce=run_nonce)
        else:
            prompt = build_query_only_prompt(query, policy_descriptions, run_nonce=run_nonce)
        prompt_chars = len(prompt)

        model = self.llm_clients["opus"] if approach.startswith("opus") else self.llm_clients["gpt"]

        start = time.perf_counter()
        response = model.invoke(prompt)
        runtime_ms = (time.perf_counter() - start) * 1000.0
        raw = message_text(getattr(response, "content", response))
        predicted = parse_violation_prediction(raw)
        return predicted, runtime_ms, prompt_chars, raw

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        db_label, query_num, policy_count, approach, run_num = self._setting_and_run(context.execution_number)
        phase = "warmup" if context.is_warmup else f"run {run_num}"
        print(
            f"[Execution {context.execution_number}] llm_validation_grid db={db_label} "
            f"q{query_num:02d} policies={policy_count} approach={approach} ({phase})"
        )

        query = load_tpch_query(query_num)
        truth_violation, rewrite_ms, dfc_exec_ms, effective_policy_count = self._dfc_truth(
            db_label=db_label,
            query_num=query_num,
            policy_count=policy_count,
            query=query,
        )
        policy_descriptions = [p.description or p.constraint for p in build_policies(policy_count)]

        try:
            if approach == APPROACH_DFC_1PHASE:
                custom = {
                    "database_label": db_label,
                    "database_tpch_sf": self.database_sf_by_label[db_label],
                    "database_path": self.database_path_by_label[db_label],
                    "query_num": query_num,
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
                cols, rows, cache_hit = self._query_result_sample(db_label, query_num, query)
                sample_rows_count = len(rows)
                sample_json = format_result_rows(cols, rows)

            run_nonce = str(uuid.uuid4())
            predicted, runtime_ms, chars_sent, raw_response = self._run_llm(
                approach=approach,
                query=query,
                policy_descriptions=policy_descriptions,
                sample_json=sample_json,
                run_nonce=run_nonce,
            )
            custom = {
                "database_label": db_label,
                "database_tpch_sf": self.database_sf_by_label[db_label],
                "database_path": self.database_path_by_label[db_label],
                "query_num": query_num,
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
                "correct_identification": predicted == truth_violation,
                "llm_chars_sent": chars_sent,
                "query_results_cache_hit": cache_hit,
                "query_results_rows": sample_rows_count,
                "prompt_nonce": run_nonce,
                "raw_response": raw_response[:2000],
            }
            return ExperimentResult(duration_ms=runtime_ms, custom_metrics=custom)
        except Exception as exc:
            return ExperimentResult(
                duration_ms=0.0,
                error=str(exc),
                custom_metrics={
                    "database_label": db_label,
                    "database_tpch_sf": self.database_sf_by_label[db_label],
                    "database_path": self.database_path_by_label[db_label],
                    "query_num": query_num,
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
                    "prompt_nonce": "",
                    "raw_response": "",
                },
            )

    def teardown(self, _context: ExperimentContext) -> None:
        return None

    def get_metrics(self) -> list[str]:
        return [
            "database_label",
            "database_tpch_sf",
            "database_path",
            "query_num",
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
            "prompt_nonce",
            "raw_response",
        ]

    def get_setting_key(self, context: ExperimentContext) -> Any | None:
        db_label, query_num, policy_count, approach, _ = self._setting_and_run(context.execution_number)
        return (db_label, query_num, policy_count, approach)
