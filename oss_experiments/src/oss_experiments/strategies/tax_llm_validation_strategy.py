"""Tax-domain LLM validation experiment strategy."""

from __future__ import annotations

import pathlib
from typing import Any
import uuid

import duckdb
from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy

from oss_experiments.data_setup import (
    DEFAULT_BATCH_ROW_COUNT,
    DEFAULT_INPUT_ROW_COUNT,
    EXPERIMENT_MODE_AGG,
    QUERY_SETS,
    QueryRecord,
    build_query_catalog,
    default_dataset_label,
    evaluate_dfc_truth,
    execute_raw_candidate_rows,
    fetch_all_input_relations,
    normalize_experiment_mode,
    reset_dataset,
)
from oss_experiments.llm_clients import (
    APPROACH_DFC,
    APPROACH_LLM,
    DEFAULT_BEDROCK_MAX_TOKENS,
    DEFAULT_CLAUDE_MODEL,
    DEFAULT_GPT_MODEL,
    SUPPORTED_REASONING_EFFORTS,
    BedrockLLMClient,
    OpenAILLMClient,
)
from oss_experiments.policies import (
    DEFAULT_POLICY_SET_ID,
    policy_descriptions,
)
from oss_experiments.prompts import (
    INCLUDED_CONTEXTS,
    build_llm_prompt,
)

DEFAULT_RUNS_PER_SETTING = 5
DEFAULT_LLM_MAX_ATTEMPTS = 3


class TaxLLMValidationStrategy(ExperimentStrategy):
    def setup(self, context: ExperimentContext) -> None:
        cfg = context.strategy_config or {}
        self.runs_per_setting = int(cfg.get("runs_per_setting", DEFAULT_RUNS_PER_SETTING))
        self.input_row_count = int(cfg.get("input_row_count", DEFAULT_INPUT_ROW_COUNT))
        self.batch_row_count = int(cfg.get("batch_row_count", DEFAULT_BATCH_ROW_COUNT))
        self.mode = normalize_experiment_mode(str(cfg.get("mode", EXPERIMENT_MODE_AGG)))
        self.dataset_label = str(
            cfg.get(
                "dataset_label",
                default_dataset_label(self.mode, self.input_row_count),
            )
        )
        self.dfc_only = bool(cfg.get("dfc_only", False))
        self.include_openai = bool(cfg.get("include_openai", True)) and not self.dfc_only
        self.include_bedrock = bool(cfg.get("include_bedrock", True)) and not self.dfc_only
        self.gpt_model = str(cfg.get("gpt_model", DEFAULT_GPT_MODEL))
        self.claude_model = str(cfg.get("claude_model", DEFAULT_CLAUDE_MODEL))
        self.bedrock_max_tokens = int(cfg.get("bedrock_max_tokens", DEFAULT_BEDROCK_MAX_TOKENS))
        self.llm_max_attempts = max(1, int(cfg.get("llm_max_attempts", DEFAULT_LLM_MAX_ATTEMPTS)))
        self.fail_on_cache_hit = bool(cfg.get("fail_on_cache_hit", False))

        query_sets = cfg.get("query_sets", list(QUERY_SETS))
        query_ids = cfg.get("query_ids")
        self.included_contexts = list(cfg.get("included_contexts", list(INCLUDED_CONTEXTS)))
        self.reasoning_efforts = list(
            cfg.get("reasoning_efforts", list(SUPPORTED_REASONING_EFFORTS))
        )

        all_queries = build_query_catalog(
            mode=self.mode,
            batch_row_count=self.batch_row_count,
        )
        self.queries: dict[tuple[str, str], QueryRecord] = {
            query.key: query for query in all_queries
        }
        selected_queries: list[QueryRecord] = []
        for query_set in query_sets:
            for query in all_queries:
                if query.query_set != query_set:
                    continue
                if query_ids and query.query_id not in query_ids:
                    continue
                selected_queries.append(query)

        db_path = str(cfg.get("db_path", f"./results/{self.dataset_label}.db"))
        pathlib.Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.raw_conn = duckdb.connect(db_path)
        reset_dataset(
            self.raw_conn,
            mode=self.mode,
            input_row_count=self.input_row_count,
            batch_row_count=self.batch_row_count,
        )

        self.openai_client: OpenAILLMClient | None = None
        self.bedrock_client: BedrockLLMClient | None = None
        if self.include_openai:
            openai_module = cfg.get("openai_client")
            if openai_module is None and not self.dfc_only:
                from openai import OpenAI

                openai_module = OpenAI()
            if openai_module is not None:
                self.openai_client = OpenAILLMClient(openai_module, self.gpt_model)
        if self.include_bedrock:
            bedrock_module = cfg.get("bedrock_client")
            if bedrock_module is None and not self.dfc_only:
                import boto3

                bedrock_module = boto3.client("bedrock-runtime")
            if bedrock_module is not None:
                self.bedrock_client = BedrockLLMClient(
                    bedrock_module,
                    self.claude_model,
                    max_tokens=self.bedrock_max_tokens,
                )

        self.candidate_cache: dict[
            tuple[str, str], tuple[list[str], list[tuple[Any, ...]], int]
        ] = {}
        self.all_input_cache: list[tuple[str, list[str], list[tuple[Any, ...]]]] | None = None
        self.truth_cache: dict[tuple[str, str], dict[str, Any]] = {}

        self.settings: list[tuple[str, str, str, str, str, str, str]] = []
        for query in selected_queries:
            self.settings.append(
                (
                    query.query_set,
                    query.query_id,
                    DEFAULT_POLICY_SET_ID,
                    APPROACH_DFC,
                    "none",
                    "none",
                    "none",
                )
            )
            if not self.dfc_only:
                if self.include_openai:
                    for included_context in self.included_contexts:
                        for reasoning_effort in self.reasoning_efforts:
                            self.settings.append(
                                (
                                    query.query_set,
                                    query.query_id,
                                    DEFAULT_POLICY_SET_ID,
                                    APPROACH_LLM,
                                    self.gpt_model,
                                    included_context,
                                    reasoning_effort,
                                )
                            )
                if self.include_bedrock:
                    for included_context in self.included_contexts:
                        for reasoning_effort in self.reasoning_efforts:
                            self.settings.append(
                                (
                                    query.query_set,
                                    query.query_id,
                                    DEFAULT_POLICY_SET_ID,
                                    APPROACH_LLM,
                                    self.claude_model,
                                    included_context,
                                    reasoning_effort,
                                )
                            )

    def _setting_and_run(
        self, execution_number: int
    ) -> tuple[str, str, str, str, str, str, str, int]:
        setting_index = (execution_number - 1) // self.runs_per_setting
        run_num = ((execution_number - 1) % self.runs_per_setting) + 1
        setting = self.settings[setting_index]
        return (*setting, run_num)

    def _get_query(self, query_set: str, query_id: str) -> QueryRecord:
        return self.queries[(query_set, query_id)]

    def _candidate_rows(self, query: QueryRecord) -> tuple[list[str], list[tuple[Any, ...]], int]:
        if query.key in self.candidate_cache:
            cols, rows, count = self.candidate_cache[query.key]
            return cols, rows, count
        cols, rows = execute_raw_candidate_rows(
            self.raw_conn,
            query,
            mode=self.mode,
            input_row_count=self.input_row_count,
            batch_row_count=self.batch_row_count,
        )
        self.candidate_cache[query.key] = (cols, rows, len(rows))
        return cols, rows, len(rows)

    def _all_input_relations(self) -> list[tuple[str, list[str], list[tuple[Any, ...]]]]:
        if self.all_input_cache is not None:
            return self.all_input_cache
        self.all_input_cache = fetch_all_input_relations(self.raw_conn, mode=self.mode)
        return self.all_input_cache

    def _dfc_truth(self, query: QueryRecord) -> dict[str, Any]:
        if query.key in self.truth_cache:
            return self.truth_cache[query.key]
        result = evaluate_dfc_truth(
            self.raw_conn,
            query,
            mode=self.mode,
            input_row_count=self.input_row_count,
            batch_row_count=self.batch_row_count,
        )
        self.truth_cache[query.key] = result
        return result

    def _provider_for_model(self, model_name: str) -> str:
        if model_name == "none":
            return "none"
        if model_name == self.gpt_model:
            return "openai"
        return "bedrock"

    def _base_metrics(self, query: QueryRecord) -> dict[str, Any]:
        return {
            "dataset_label": self.dataset_label,
            "experiment_mode": self.mode,
            "query_set": query.query_set,
            "query_id": query.query_id,
            "batch_id": query.batch_id,
            "input_receipt_id_start": query.input_receipt_id_start,
            "input_receipt_id_end": query.input_receipt_id_end,
            "rows_selected": query.rows_selected,
            "query_kind": query.query_kind,
            "policy_set_id": query.policy_set_id,
            "policy_ids": ",".join(query.policy_ids),
            "input_row_count": self.input_row_count,
        }

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        (
            query_set,
            query_id,
            _policy_set_id,
            approach,
            model_name,
            included_context,
            reasoning_effort,
            run_num,
        ) = self._setting_and_run(context.execution_number)
        query = self._get_query(query_set, query_id)
        phase = "warmup" if context.is_warmup else f"run {run_num}"
        print(
            f"[Execution {context.execution_number}] tax_llm_validation "
            f"query_set={query_set} query_id={query_id} approach={approach} "
            f"model={model_name} context={included_context} effort={reasoning_effort} ({phase})"
        )

        truth = self._dfc_truth(query)
        ground_truth_violation = bool(truth["ground_truth_violation"])
        base = self._base_metrics(query)
        provider = self._provider_for_model(model_name)

        try:
            if approach == APPROACH_DFC:
                custom = {
                    **base,
                    "approach": APPROACH_DFC,
                    "provider": "none",
                    "model_name": "none",
                    "included_context": "none",
                    "requested_reasoning_effort": "none",
                    "effective_reasoning_effort": "none",
                    "runtime_ms": truth["dfc_exec_time_ms"],
                    "ground_truth_violation": ground_truth_violation,
                    "predicted_violation": ground_truth_violation,
                    "correct_identification": True,
                    "parse_ok": True,
                    "base_inserted_row_count": query.rows_selected,
                    "result_context_row_count": 0,
                    "input_context_row_count": 0,
                    "dfc_killed": truth["dfc_killed"],
                    "expected_dfc_kill": truth["expected_dfc_kill"],
                    "expected_meal_limit_kill": truth["expected_meal_limit_kill"],
                    "dfc_truth_matches_expected": truth["dfc_truth_matches_expected"],
                    "dfc_rewrite_time_ms": truth["dfc_rewrite_time_ms"],
                    "dfc_exec_time_ms": truth["dfc_exec_time_ms"],
                    "rewritten_sql": truth["rewritten_sql"],
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cached_input_tokens": 0,
                    "cache_write_input_tokens": 0,
                    "cache_read_input_tokens": 0,
                    "cache_contaminated": False,
                    "prompt_chars": 0,
                    "prompt_nonce": "",
                    "request_id": "",
                    "raw_response": "",
                    "temperature_effective": "n/a",
                    "llm_attempts": 0,
                    "llm_max_attempts": self.llm_max_attempts,
                    "llm_retry_errors": "",
                }
                return ExperimentResult(
                    duration_ms=truth["dfc_exec_time_ms"], custom_metrics=custom
                )

            expense_cols, expense_rows, row_count = self._candidate_rows(query)
            input_relations: list[tuple[str, list[str], list[tuple[Any, ...]]]] = []
            input_context_count = 0
            if included_context == "query_and_results_and_input":
                input_relations = self._all_input_relations()
                input_context_count = sum(len(rows) for _, _, rows in input_relations)

            llm_result = None
            accepted_nonce = ""
            last_response_nonce = ""
            retry_errors: list[str] = []
            attempts_used = 0
            for attempt in range(1, self.llm_max_attempts + 1):
                attempts_used = attempt
                nonce = str(uuid.uuid4())
                prompt = build_llm_prompt(
                    nonce=nonce,
                    query_set=query.query_set,
                    query_id=query.query_id,
                    batch_id=query.batch_id,
                    rows_selected=query.rows_selected,
                    sql=query.sql,
                    policy_descriptions=policy_descriptions(mode=self.mode),
                    included_context=included_context,
                    expense_columns=expense_cols,
                    expense_rows=expense_rows,
                    input_relations=input_relations,
                )
                try:
                    if provider == "openai":
                        if self.openai_client is None:
                            raise RuntimeError("OpenAI client not configured")
                        current_result = self.openai_client.invoke(
                            prompt,
                            nonce=nonce,
                            requested_reasoning_effort=reasoning_effort,
                        )
                    else:
                        if self.bedrock_client is None:
                            raise RuntimeError("Bedrock client not configured")
                        current_result = self.bedrock_client.invoke(
                            prompt,
                            requested_reasoning_effort=reasoning_effort,
                        )

                    if self.fail_on_cache_hit and current_result.cache_contaminated:
                        retry_errors.append(
                            f"attempt {attempt}: cache contamination detected: cached_input_tokens="
                            f"{current_result.cached_input_tokens}"
                        )
                        continue
                    llm_result = current_result
                    last_response_nonce = nonce
                    if current_result.parse_ok:
                        accepted_nonce = nonce
                        break
                    retry_errors.append(f"attempt {attempt}: parse_failed")
                except Exception as exc:
                    retry_errors.append(f"attempt {attempt}: {type(exc).__name__}: {exc}")
                if attempt == self.llm_max_attempts:
                    break

            if llm_result is None:
                raise RuntimeError(
                    "LLM call failed without a response after "
                    f"{self.llm_max_attempts} attempts: {' | '.join(retry_errors)}"
                )
            if not accepted_nonce:
                accepted_nonce = last_response_nonce

            predicted = llm_result.predicted_violation
            correct = predicted == ground_truth_violation if llm_result.parse_ok else False
            custom = {
                **base,
                "approach": APPROACH_LLM,
                "provider": provider,
                "model_name": model_name,
                "included_context": included_context,
                "requested_reasoning_effort": llm_result.requested_reasoning_effort,
                "effective_reasoning_effort": llm_result.effective_reasoning_effort,
                "runtime_ms": llm_result.runtime_ms,
                "ground_truth_violation": ground_truth_violation,
                "predicted_violation": predicted if predicted is not None else "",
                "correct_identification": correct,
                "parse_ok": llm_result.parse_ok,
                "base_inserted_row_count": row_count,
                "result_context_row_count": len(expense_rows),
                "input_context_row_count": input_context_count,
                "dfc_killed": truth["dfc_killed"],
                "expected_dfc_kill": truth["expected_dfc_kill"],
                "expected_meal_limit_kill": truth["expected_meal_limit_kill"],
                "dfc_truth_matches_expected": truth["dfc_truth_matches_expected"],
                "dfc_rewrite_time_ms": truth["dfc_rewrite_time_ms"],
                "dfc_exec_time_ms": truth["dfc_exec_time_ms"],
                "rewritten_sql": truth["rewritten_sql"],
                "input_tokens": llm_result.input_tokens,
                "output_tokens": llm_result.output_tokens,
                "cached_input_tokens": llm_result.cached_input_tokens,
                "cache_write_input_tokens": llm_result.cache_write_input_tokens,
                "cache_read_input_tokens": llm_result.cache_read_input_tokens,
                "cache_contaminated": llm_result.cache_contaminated,
                "prompt_chars": llm_result.prompt_chars,
                "prompt_nonce": accepted_nonce,
                "request_id": llm_result.request_id,
                "raw_response": llm_result.raw_response,
                "temperature_effective": llm_result.temperature_effective,
                "llm_attempts": attempts_used,
                "llm_max_attempts": self.llm_max_attempts,
                "llm_retry_errors": " | ".join(retry_errors),
            }
            return ExperimentResult(duration_ms=llm_result.runtime_ms, custom_metrics=custom)
        except Exception as exc:
            return ExperimentResult(
                duration_ms=0.0,
                error=str(exc),
                custom_metrics={
                    **base,
                    "approach": approach,
                    "provider": provider,
                    "model_name": model_name,
                    "included_context": included_context,
                    "requested_reasoning_effort": reasoning_effort,
                    "effective_reasoning_effort": "",
                    "ground_truth_violation": ground_truth_violation,
                    "predicted_violation": "",
                    "correct_identification": False,
                    "parse_ok": False,
                    "dfc_killed": truth.get("dfc_killed", ""),
                    "expected_dfc_kill": truth.get("expected_dfc_kill", ""),
                    "cache_contaminated": "",
                    "prompt_nonce": "",
                    "raw_response": "",
                    "llm_attempts": "",
                    "llm_max_attempts": getattr(self, "llm_max_attempts", ""),
                    "llm_retry_errors": "",
                },
            )

    def teardown(self, _context: ExperimentContext) -> None:
        if hasattr(self, "raw_conn"):
            self.raw_conn.close()

    def get_setting_key(
        self, context: ExperimentContext
    ) -> tuple[str, str, str, str, str, str, str]:
        (
            query_set,
            query_id,
            policy_set_id,
            approach,
            model_name,
            included_context,
            reasoning_effort,
            _,
        ) = self._setting_and_run(context.execution_number)
        return (
            query_set,
            query_id,
            policy_set_id,
            approach,
            model_name,
            included_context,
            reasoning_effort,
        )

    def get_metrics(self) -> list[str]:
        return [
            "dataset_label",
            "experiment_mode",
            "query_set",
            "query_id",
            "batch_id",
            "input_receipt_id_start",
            "input_receipt_id_end",
            "rows_selected",
            "query_kind",
            "policy_set_id",
            "policy_ids",
            "approach",
            "provider",
            "model_name",
            "included_context",
            "requested_reasoning_effort",
            "effective_reasoning_effort",
            "runtime_ms",
            "ground_truth_violation",
            "predicted_violation",
            "correct_identification",
            "parse_ok",
            "input_row_count",
            "base_inserted_row_count",
            "result_context_row_count",
            "input_context_row_count",
            "dfc_killed",
            "expected_dfc_kill",
            "expected_meal_limit_kill",
            "dfc_truth_matches_expected",
            "dfc_rewrite_time_ms",
            "dfc_exec_time_ms",
            "rewritten_sql",
            "input_tokens",
            "output_tokens",
            "cached_input_tokens",
            "cache_write_input_tokens",
            "cache_read_input_tokens",
            "cache_contaminated",
            "prompt_chars",
            "prompt_nonce",
            "request_id",
            "raw_response",
            "temperature_effective",
            "llm_attempts",
            "llm_max_attempts",
            "llm_retry_errors",
        ]
