"""LLM policy-violation validation strategy using TPC-H queries."""

from __future__ import annotations

import contextlib
from dataclasses import replace
import pathlib
import re
import time
from typing import Any

from data_flow_control import Policy, Resolution
from data_flow_control.adapters.duckdb import DuckDBAdapter
from data_flow_control.connection import Connection
import duckdb
from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy

from agent_harness.config import HarnessConfig
from agent_harness.llm_factory import create_chat_model
from vldb_experiments.strategies.llm_validation_common import (
    APPROACH_DFC_1PHASE,
    DEFAULT_CLAUDE_MODEL,
    DEFAULT_GPT_MODEL,
    DEFAULT_POLICY_COUNTS,
    DEFAULT_RUNS_PER_SETTING,
    DEFAULT_TPCH_SF,
    POLICY_VARIANT_GROWING,
    SUPPORTED_POLICY_VARIANTS,
    build_query_only_prompt,
    build_query_results_prompt,
    format_result_rows,
    message_text,
    parse_violation_prediction,
    policy_catalog,
    policy_specs_for_variant,
)
from vldb_experiments.strategies.regulation_z_benchmark import BedrockPolicyGuard
from vldb_experiments.strategies.tpch_strategy import (
    TPCH_QUERIES,
    load_tpch_query,
)

TPCH_QUERIES_ALL = list(TPCH_QUERIES)
PROMPT_MODE_QUERY_ONLY = "query_only"
PROMPT_MODE_QUERY_RESULTS = "query_results"
SUPPORTED_PROMPT_MODES = [PROMPT_MODE_QUERY_ONLY, PROMPT_MODE_QUERY_RESULTS]
DEFAULT_PROMPT_MODES = SUPPORTED_PROMPT_MODES.copy()


def build_validation_settings(
    query_nums: list[int],
    policy_counts: list[int],
    model_keys: list[str],
    prompt_modes: list[str] | None = None,
) -> list[tuple[int, int, str]]:
    """Build the deterministic execution matrix for the validation benchmark."""
    selected_modes = DEFAULT_PROMPT_MODES if prompt_modes is None else prompt_modes
    invalid_modes = sorted(set(selected_modes) - set(SUPPORTED_PROMPT_MODES))
    if invalid_modes:
        raise ValueError(f"Unsupported prompt modes: {invalid_modes}")
    if not selected_modes:
        raise ValueError("prompt_modes must contain at least one mode")

    settings: list[tuple[int, int, str]] = []
    for query_num in query_nums:
        for policy_count in policy_counts:
            settings.append((query_num, policy_count, APPROACH_DFC_1PHASE))
            for model_key in model_keys:
                for prompt_mode in selected_modes:
                    settings.append((query_num, policy_count, f"{model_key}_{prompt_mode}"))
    return settings


def _policy_catalog(
    policy_variant: str = POLICY_VARIANT_GROWING,
) -> list[tuple[str, str]]:
    return policy_catalog(policy_variant)


def _build_policies(
    policy_count: int,
    policy_variant: str = POLICY_VARIANT_GROWING,
) -> list[Policy]:
    return [
        Policy(
            sources=list(spec.sources),
            constraint=spec.constraint,
            on_fail=Resolution.REMOVE,
            description=spec.description,
        )
        for spec in policy_specs_for_variant(policy_variant)[:policy_count]
    ]


def _message_text(content: Any) -> str:
    return message_text(content)


def _parse_violation_prediction(raw_text: str) -> bool | None:
    return parse_violation_prediction(raw_text)


def _response_usage(response: Any) -> tuple[int, int, int, int]:
    """Read normalized LangChain usage, with Bedrock metadata as a fallback."""
    usage = getattr(response, "usage_metadata", None) or {}
    response_metadata = getattr(response, "response_metadata", None) or {}
    if not usage:
        usage = response_metadata.get("usage", {})
    input_details = usage.get("input_token_details", {}) or {}
    return (
        int(usage.get("input_tokens") or usage.get("inputTokens") or 0),
        int(usage.get("output_tokens") or usage.get("outputTokens") or 0),
        int(
            input_details.get("cache_read")
            or usage.get("cache_read_input_tokens")
            or usage.get("cacheReadInputTokens")
            or 0
        ),
        int(
            input_details.get("cache_creation")
            or usage.get("cache_write_input_tokens")
            or usage.get("cacheWriteInputTokens")
            or 0
        ),
    )


def _format_result_rows(columns: list[str], rows: list[tuple[Any, ...]]) -> str:
    return format_result_rows(columns, rows)


def _build_query_only_prompt(query: str, policy_descriptions: list[str]) -> str:
    return build_query_only_prompt(query, policy_descriptions)


def _build_query_results_prompt(
    query: str,
    policy_descriptions: list[str],
    sample_rows_json: str,
) -> str:
    return build_query_results_prompt(query, policy_descriptions, sample_rows_json)


class LLMValidationStrategy(ExperimentStrategy):
    """Evaluate policy-violation identification with 1Phase and LLM baselines."""

    def setup(self, context: ExperimentContext) -> None:
        self.scale_factor = float(context.strategy_config.get("tpch_sf", DEFAULT_TPCH_SF))
        self.policy_counts = [
            int(v) for v in context.strategy_config.get("policy_counts", DEFAULT_POLICY_COUNTS)
        ]
        self.runs_per_setting = int(
            context.strategy_config.get("runs_per_setting", DEFAULT_RUNS_PER_SETTING)
        )
        self.policy_variant = str(
            context.strategy_config.get("policy_variant", POLICY_VARIANT_GROWING)
        )
        if self.policy_variant not in SUPPORTED_POLICY_VARIANTS:
            raise ValueError(f"Unsupported policy variant: {self.policy_variant!r}")
        self.include_bedrock = bool(context.strategy_config.get("include_bedrock", True))
        self.include_openai = bool(context.strategy_config.get("include_openai", True))
        self.claude_model = str(context.strategy_config.get("claude_model", DEFAULT_CLAUDE_MODEL))
        self.gpt_model = str(context.strategy_config.get("gpt_model", DEFAULT_GPT_MODEL))

        db_path = context.strategy_config.get("tpch_db_path")
        if not db_path:
            db_path = f"./results/llm_validation_sf{self.scale_factor}.db"
        pathlib.Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path

        main_conn = duckdb.connect(self.db_path)
        main_conn.execute("SET threads = 1")
        main_conn.execute("SET memory_limit = '512MB'")
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
        self.dfc_rewriter = Connection(DuckDBAdapter(self.dfc_conn))

        configured_queries = context.strategy_config.get("queries", TPCH_QUERIES_ALL)
        self.query_nums = [int(q) for q in configured_queries]
        if not self.query_nums:
            raise ValueError("queries must contain at least one TPC-H query number")
        self.prompt_modes = [
            str(mode) for mode in context.strategy_config.get("prompt_modes", DEFAULT_PROMPT_MODES)
        ]
        self.query_result_cache: dict[int, tuple[list[str], list[tuple[Any, ...]], float]] = {}
        self.query_result_counts: dict[int, int] = {}
        self.truth_cache: dict[tuple[int, int], tuple[bool, float, float, int]] = {}

        raw_model_specs = context.strategy_config.get("models")
        if raw_model_specs is None:
            raw_model_specs = []
            if self.include_openai:
                raw_model_specs.append(
                    {
                        "key": "gpt",
                        "display_name": self.gpt_model,
                        "provider": "openai",
                        "model_id": self.gpt_model,
                    }
                )
            if self.include_bedrock:
                raw_model_specs.append(
                    {
                        "key": "opus",
                        "display_name": self.claude_model,
                        "provider": "bedrock",
                        "model_id": self.claude_model,
                    }
                )

        self.model_specs: dict[str, dict[str, str]] = {}
        for raw_spec in raw_model_specs:
            spec = {str(key): str(value) for key, value in raw_spec.items()}
            key = spec.get("key", "")
            provider = spec.get("provider", "")
            model_id = spec.get("model_id", "")
            if not re.fullmatch(r"[a-z0-9_]+", key):
                raise ValueError(f"Invalid model key: {key!r}")
            if provider not in {"openai", "bedrock"}:
                raise ValueError(f"Unsupported provider for {key}: {provider!r}")
            if not model_id:
                raise ValueError(f"model_id is required for {key}")
            if key in self.model_specs:
                raise ValueError(f"Duplicate model key: {key}")
            spec.setdefault("display_name", key)
            self.model_specs[key] = spec
        if not self.model_specs:
            raise ValueError("At least one model must be configured")

        base_cfg = HarnessConfig.from_env()
        self.llm_clients: dict[str, Any] = {}
        self.openai_bedrock_guards: dict[str, BedrockPolicyGuard] = {}
        openai_bedrock_specs = [
            spec
            for spec in self.model_specs.values()
            if spec["provider"] == "bedrock" and spec.get("bedrock_provider") == "openai"
        ]
        if openai_bedrock_specs:
            supplied_client = context.strategy_config.get("bedrock_client")
            if supplied_client is None:
                import boto3

                region = str(
                    context.strategy_config.get("aws_region") or base_cfg.aws_region or "us-east-1"
                )
                profile = context.strategy_config.get("aws_profile") or base_cfg.aws_profile
                session = (
                    boto3.Session(profile_name=profile, region_name=region)
                    if profile
                    else boto3.Session(region_name=region)
                )
                supplied_client = session.client("bedrock-runtime")
            self.openai_bedrock_guards = {
                spec["key"]: BedrockPolicyGuard(
                    supplied_client,
                    spec["model_id"],
                    provider="openai",
                )
                for spec in openai_bedrock_specs
            }
        for key, spec in self.model_specs.items():
            if key in self.openai_bedrock_guards:
                continue
            provider = spec["provider"]
            model_id = spec["model_id"]
            model_cfg = replace(
                base_cfg,
                provider=provider,
                openai_model=model_id if provider == "openai" else base_cfg.openai_model,
                bedrock_model_id=model_id if provider == "bedrock" else base_cfg.bedrock_model_id,
                bedrock_provider=spec.get("bedrock_provider", base_cfg.bedrock_provider),
            )
            self.llm_clients[key] = create_chat_model(model_cfg)

        self.settings = build_validation_settings(
            self.query_nums,
            self.policy_counts,
            list(self.model_specs),
            self.prompt_modes,
        )

    def _setting_and_run(self, execution_number: int) -> tuple[int, int, str, int]:
        setting_index = (execution_number - 1) // self.runs_per_setting
        run_num = ((execution_number - 1) % self.runs_per_setting) + 1
        query_num, policy_count, approach = self.settings[setting_index]
        return query_num, policy_count, approach, run_num

    def _clear_and_register_policies(self, policy_count: int) -> list[Policy]:
        existing = self.dfc_rewriter.policies()
        for old_policy in existing:
            self.dfc_rewriter.delete_policy(
                constraint=old_policy.constraint,
                on_fail=old_policy.on_fail,
            )
        policy_variant = getattr(self, "policy_variant", POLICY_VARIANT_GROWING)
        policies = _build_policies(policy_count, policy_variant)
        for policy in policies:
            self.dfc_rewriter.register_policy(policy)
        return policies

    def _query_result_sample(
        self, query_num: int, query: str
    ) -> tuple[list[str], list[tuple[Any, ...]], bool]:
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
        self.query_result_counts[query_num] = len(all_rows)
        return columns, rows, False

    def _dfc_truth(
        self, query_num: int, policy_count: int, query: str
    ) -> tuple[bool, float, float, int]:
        key = (query_num, policy_count)
        if key in self.truth_cache:
            return self.truth_cache[key]

        result = self._measure_dfc(query_num, policy_count, query)
        self.truth_cache[key] = result
        return result

    def _measure_dfc(
        self, query_num: int, policy_count: int, query: str
    ) -> tuple[bool, float, float, int]:
        """Run DFC once; unlike ground-truth lookup, this is never cached."""
        if query_num not in self.query_result_counts:
            self._query_result_sample(query_num, query)
        policies = self._clear_and_register_policies(policy_count)
        rewrite_start = time.perf_counter()
        transformed = self.dfc_rewriter.transform_query(query)
        rewrite_ms = (time.perf_counter() - rewrite_start) * 1000.0

        exec_start = time.perf_counter()
        cursor = self.dfc_conn.execute(transformed)
        rows = cursor.fetchall()
        exec_ms = (time.perf_counter() - exec_start) * 1000.0
        violation = len(rows) < self.query_result_counts[query_num]
        return violation, rewrite_ms, exec_ms, len(policies)

    def prepare_inputs(self) -> None:
        """Populate query-result samples before inference workers begin."""
        include_query_results = PROMPT_MODE_QUERY_RESULTS in self.prompt_modes
        for query_num in self.query_nums:
            query = load_tpch_query(query_num)
            if include_query_results:
                self._query_result_sample(query_num, query)

    def prepare_ground_truth(self) -> None:
        """Fill any truth entries not produced by measured DFC executions."""
        for query_num in self.query_nums:
            query = load_tpch_query(query_num)
            for policy_count in self.policy_counts:
                self._dfc_truth(query_num, policy_count, query)

    def _run_llm(
        self,
        approach: str,
        query: str,
        policy_descriptions: list[str],
        sample_json: str | None,
    ) -> tuple[bool | None, float, int, str, int, int, int, int]:
        is_query_results = approach.endswith("_query_results")
        if is_query_results:
            prompt = _build_query_results_prompt(
                query,
                policy_descriptions,
                sample_json or "[]",
            )
        else:
            prompt = _build_query_only_prompt(query, policy_descriptions)
        prompt_chars = len(prompt)

        model_key = approach.removesuffix("_query_results").removesuffix("_query_only")
        if model_key in self.openai_bedrock_guards:
            guard_response = self.openai_bedrock_guards[model_key].invoke(prompt)
            runtime_ms = guard_response.runtime_ms
            raw = guard_response.raw_response
            predicted = _parse_violation_prediction(raw)
            input_tokens = guard_response.input_tokens
            output_tokens = guard_response.output_tokens
            cache_read_tokens = guard_response.cache_read_input_tokens
            cache_write_tokens = guard_response.cache_write_input_tokens
        else:
            model = self.llm_clients[model_key]
            start = time.perf_counter()
            response = model.invoke(prompt)
            runtime_ms = (time.perf_counter() - start) * 1000.0
            raw = _message_text(getattr(response, "content", response))
            predicted = _parse_violation_prediction(raw)
            input_tokens, output_tokens, cache_read_tokens, cache_write_tokens = _response_usage(
                response
            )
        return (
            predicted,
            runtime_ms,
            prompt_chars,
            raw,
            input_tokens,
            output_tokens,
            cache_read_tokens,
            cache_write_tokens,
        )

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        query_num, policy_count, approach, run_num = self._setting_and_run(context.execution_number)
        phase = "warmup" if context.is_warmup else f"run {run_num}"
        print(
            f"[Execution {context.execution_number}] llm_validation q{query_num:02d} "
            f"policies={policy_count} approach={approach} ({phase})"
        )

        query = load_tpch_query(query_num)
        policy_descriptions = [
            p.description or p.constraint
            for p in _build_policies(policy_count, self.policy_variant)
        ]

        try:
            if approach == APPROACH_DFC_1PHASE:
                (
                    measured_violation,
                    rewrite_ms,
                    dfc_exec_ms,
                    effective_policy_count,
                ) = self._measure_dfc(query_num, policy_count, query)
                truth_key = (query_num, policy_count)
                self.truth_cache.setdefault(
                    truth_key,
                    (measured_violation, rewrite_ms, dfc_exec_ms, effective_policy_count),
                )
                truth_violation = self.truth_cache[truth_key][0]
                custom = {
                    "run_num": run_num,
                    "query_num": query_num,
                    "tpch_sf": self.scale_factor,
                    "policy_count": policy_count,
                    "policy_variant": self.policy_variant,
                    "effective_policy_count": effective_policy_count,
                    "approach": approach,
                    "provider": "none",
                    "model_name": "none",
                    "model_display_name": "none",
                    "runtime_ms": dfc_exec_ms,
                    "dfc_1phase_rewrite_time_ms": rewrite_ms,
                    "dfc_1phase_exec_time_ms": dfc_exec_ms,
                    "ground_truth_violation": truth_violation,
                    "predicted_violation": measured_violation,
                    "correct_identification": measured_violation == truth_violation,
                    "llm_chars_sent": 0,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cache_read_input_tokens": 0,
                    "cache_write_input_tokens": 0,
                    "estimated_cost_usd": 0.0,
                    "query_results_cache_hit": True,
                    "query_results_rows": 0,
                    "raw_response": "",
                }
                return ExperimentResult(duration_ms=dfc_exec_ms, custom_metrics=custom)

            truth_violation, _, _, effective_policy_count = self._dfc_truth(
                query_num=query_num,
                policy_count=policy_count,
                query=query,
            )
            sample_json = None
            cache_hit = True
            sample_rows_count = 0
            if approach.endswith("_query_results"):
                cols, rows, cache_hit = self._query_result_sample(query_num, query)
                sample_rows_count = len(rows)
                sample_json = _format_result_rows(cols, rows)

            (
                predicted,
                runtime_ms,
                chars_sent,
                raw_response,
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_write_tokens,
            ) = self._run_llm(
                approach=approach,
                query=query,
                policy_descriptions=policy_descriptions,
                sample_json=sample_json,
            )
            model_key = approach.removesuffix("_query_results").removesuffix("_query_only")
            model_spec = self.model_specs[model_key]
            estimated_cost_usd = (
                input_tokens * float(model_spec.get("input_usd_per_million", 0.0))
                + output_tokens * float(model_spec.get("output_usd_per_million", 0.0))
                + cache_read_tokens * float(model_spec.get("cache_read_usd_per_million", 0.0))
                + cache_write_tokens * float(model_spec.get("cache_write_usd_per_million", 0.0))
            ) / 1_000_000.0
            custom = {
                "run_num": run_num,
                "query_num": query_num,
                "tpch_sf": self.scale_factor,
                "policy_count": policy_count,
                "policy_variant": self.policy_variant,
                "effective_policy_count": effective_policy_count,
                "approach": approach,
                "provider": model_spec["provider"],
                "model_name": model_spec["model_id"],
                "model_display_name": model_spec["display_name"],
                "runtime_ms": runtime_ms,
                "dfc_1phase_rewrite_time_ms": 0.0,
                "dfc_1phase_exec_time_ms": 0.0,
                "ground_truth_violation": truth_violation,
                "predicted_violation": predicted,
                "correct_identification": (predicted == truth_violation),
                "llm_chars_sent": chars_sent,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cache_read_input_tokens": cache_read_tokens,
                "cache_write_input_tokens": cache_write_tokens,
                "estimated_cost_usd": estimated_cost_usd,
                "query_results_cache_hit": cache_hit,
                "query_results_rows": sample_rows_count,
                "raw_response": raw_response[:2000],
            }
            return ExperimentResult(duration_ms=runtime_ms, custom_metrics=custom)
        except Exception as exc:
            model_key = approach.removesuffix("_query_results").removesuffix("_query_only")
            model_spec = self.model_specs.get(model_key, {})
            cached_truth = self.truth_cache.get((query_num, policy_count))
            return ExperimentResult(
                duration_ms=0.0,
                error=str(exc),
                custom_metrics={
                    "run_num": run_num,
                    "query_num": query_num,
                    "tpch_sf": self.scale_factor,
                    "policy_count": policy_count,
                    "policy_variant": self.policy_variant,
                    "effective_policy_count": (
                        cached_truth[3] if cached_truth is not None else policy_count
                    ),
                    "approach": approach,
                    "provider": model_spec.get("provider", "none"),
                    "model_name": model_spec.get("model_id", "none"),
                    "model_display_name": model_spec.get("display_name", "none"),
                    "ground_truth_violation": (cached_truth[0] if cached_truth is not None else ""),
                    "predicted_violation": "",
                    "correct_identification": "",
                    "llm_chars_sent": 0 if approach == APPROACH_DFC_1PHASE else -1,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cache_read_input_tokens": 0,
                    "cache_write_input_tokens": 0,
                    "estimated_cost_usd": 0.0,
                    "query_results_cache_hit": "",
                    "query_results_rows": 0,
                    "raw_response": "",
                },
            )

    def teardown(self, _context: ExperimentContext) -> None:
        self.no_policy_conn.close()

    def get_metrics(self) -> list[str]:
        return [
            "run_num",
            "query_num",
            "tpch_sf",
            "policy_count",
            "policy_variant",
            "effective_policy_count",
            "approach",
            "provider",
            "model_name",
            "model_display_name",
            "runtime_ms",
            "dfc_1phase_rewrite_time_ms",
            "dfc_1phase_exec_time_ms",
            "ground_truth_violation",
            "predicted_violation",
            "correct_identification",
            "llm_chars_sent",
            "input_tokens",
            "output_tokens",
            "cache_read_input_tokens",
            "cache_write_input_tokens",
            "estimated_cost_usd",
            "query_results_cache_hit",
            "query_results_rows",
            "raw_response",
        ]

    def get_setting_key(self, context: ExperimentContext) -> Any | None:
        query_num, policy_count, approach, _ = self._setting_and_run(context.execution_number)
        return (query_num, policy_count, approach)
