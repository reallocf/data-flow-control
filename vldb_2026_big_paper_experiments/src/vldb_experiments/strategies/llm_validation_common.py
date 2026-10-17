"""Shared helpers for LLM validation experiments."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any

from sql_rewriter import DFCPolicy, Resolution

DEFAULT_POLICY_COUNTS = [1, 2, 4, 8, 16, 32]
DEFAULT_RUNS_PER_SETTING = 5
DEFAULT_TPCH_SF = 1.0
DEFAULT_CLAUDE_MODEL = "claude-4.6-opus"
DEFAULT_GPT_MODEL = "gpt-5.2"
POLICY_VARIANT_GROWING = "growing_violations"
POLICY_VARIANT_FIXED_4 = "fixed_4_violations"
SUPPORTED_POLICY_VARIANTS = [POLICY_VARIANT_GROWING, POLICY_VARIANT_FIXED_4]

DEFAULT_BEDROCK_MODEL_SPECS = [
    {
        "key": "astra",
        "display_name": "Astra 6",
        "provider": "bedrock",
        "bedrock_provider": "openai",
        "input_usd_per_million": 11.0,
        "output_usd_per_million": 55.0,
        "cache_read_usd_per_million": 1.1,
        "cache_write_usd_per_million": 13.75,
        "model_id": (
            "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/ybl9l7bkcovm"
        ),
    },
    {
        "key": "sol_6_1",
        "display_name": "Sol 6.1",
        "provider": "bedrock",
        "bedrock_provider": "openai",
        "input_usd_per_million": 2.2,
        "output_usd_per_million": 11.0,
        "cache_read_usd_per_million": 0.11,
        "cache_write_usd_per_million": 2.75,
        "model_id": (
            "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/793tfgrfes18"
        ),
    },
    {
        "key": "luna",
        "display_name": "Luna 6",
        "provider": "bedrock",
        "bedrock_provider": "openai",
        "input_usd_per_million": 0.11,
        "output_usd_per_million": 0.55,
        "cache_read_usd_per_million": 0.011,
        "cache_write_usd_per_million": 0.1375,
        "model_id": (
            "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/cn56cvd6ntch"
        ),
    },
    {
        "key": "fable_5_1",
        "display_name": "Fable 5.1",
        "provider": "bedrock",
        "bedrock_provider": "anthropic",
        "input_usd_per_million": 11.0,
        "output_usd_per_million": 55.0,
        "cache_read_usd_per_million": 0.275,
        "cache_write_usd_per_million": 13.75,
        "model_id": (
            "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/7mwcoskr36hk"
        ),
    },
    {
        "key": "opus_5_5",
        "display_name": "Opus 5.5",
        "provider": "bedrock",
        "bedrock_provider": "anthropic",
        "input_usd_per_million": 4.4,
        "output_usd_per_million": 22.0,
        "cache_read_usd_per_million": 0.22,
        "cache_write_usd_per_million": 5.5,
        "model_id": (
            "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/d0lzabehot7u"
        ),
    },
]

APPROACH_DFC_1PHASE = "dfc_1phase"
APPROACH_OPUS_QUERY_ONLY = "opus_query_only"
APPROACH_GPT_QUERY_ONLY = "gpt_query_only"
APPROACH_OPUS_QUERY_RESULTS = "opus_query_results"
APPROACH_GPT_QUERY_RESULTS = "gpt_query_results"


@dataclass(frozen=True)
class PolicySpec:
    sources: tuple[str, ...]
    constraint: str
    description: str


def policy_specs() -> list[PolicySpec]:
    return [
        PolicySpec(
            ("lineitem",),
            "sum(lineitem.l_quantity - 30) <= 0",
            "Average quantity should remain at or below 30.",
        ),
        PolicySpec(
            ("lineitem",),
            "sum(lineitem.l_quantity - 14) >= 0",
            "Average quantity should be at least 14.",
        ),
        PolicySpec(
            ("lineitem", "part"),
            "sum(lineitem.l_quantity - 20) >= 0",
            "For flows using lineitem and part, average quantity should be at least 20.",
        ),
        PolicySpec(
            ("lineitem",),
            "sum(lineitem.l_quantity - 10) >= 0",
            "Average quantity should be at least 10.",
        ),
        PolicySpec(
            ("lineitem", "partsupp"),
            "max(lineitem.l_discount) <= 0.07",
            "For flows using lineitem and partsupp, no discount should exceed 7%.",
        ),
        PolicySpec(
            ("lineitem",),
            "sum(lineitem.l_discount - 0.01) >= 0",
            "Average discount should be at least 1%.",
        ),
        PolicySpec(
            ("lineitem",),
            "sum(lineitem.l_tax - 0.01) >= 0",
            "Average tax rate should be at least 1%.",
        ),
        PolicySpec(
            ("lineitem",),
            "sum(lineitem.l_extendedprice - 10000) >= 0",
            "Average extended price should be at least 10,000.",
        ),
        PolicySpec(
            ("lineitem", "part", "region"),
            "max(lineitem.l_discount) <= 0.07",
            "For flows using lineitem, part, and region, no discount should exceed 7%.",
        ),
        PolicySpec(
            ("lineitem",),
            "sum(lineitem.l_extendedprice - 1000 * lineitem.l_quantity) >= 0",
            "Average unit list price, weighted by quantity, should be at least 1,000.",
        ),
        PolicySpec(
            ("lineitem", "part"),
            "sum(lineitem.l_quantity - 30) <= 0",
            "For flows using lineitem and part, average quantity should not exceed 30.",
        ),
        PolicySpec(
            ("lineitem", "part"),
            "sum(lineitem.l_discount - 0.06) <= 0",
            "For flows using lineitem and part, average discount should not exceed 6%.",
        ),
        PolicySpec(
            ("lineitem", "part"),
            "sum(lineitem.l_tax - 0.05) <= 0",
            "For flows using lineitem and part, average tax rate should not exceed 5%.",
        ),
        PolicySpec(
            ("lineitem", "part"),
            "sum(lineitem.l_extendedprice - 60000) <= 0",
            "For flows using lineitem and part, average extended price should not exceed 60,000.",
        ),
        PolicySpec(
            ("lineitem", "region"),
            "sum(lineitem.l_quantity - 30) <= 0",
            "For flows using lineitem and region, average quantity should not exceed 30.",
        ),
        PolicySpec(
            ("lineitem", "region"),
            "sum(lineitem.l_discount - 0.06) <= 0",
            "For flows using lineitem and region, average discount should not exceed 6%.",
        ),
        PolicySpec(
            ("lineitem", "region"),
            "max(lineitem.l_discount) <= 0.07",
            "For flows using lineitem and region, no discount should exceed 7%.",
        ),
        PolicySpec(
            ("lineitem", "region"),
            "sum(lineitem.l_tax - 0.05) <= 0",
            "For flows using lineitem and region, average tax rate should not exceed 5%.",
        ),
        PolicySpec(
            ("lineitem", "region"),
            "sum(lineitem.l_extendedprice - 60000) <= 0",
            "For flows using lineitem and region, average extended price should not exceed 60,000.",
        ),
        PolicySpec(
            ("lineitem", "partsupp"),
            "sum(lineitem.l_quantity - 30) <= 0",
            "For flows using lineitem and partsupp, average quantity should not exceed 30.",
        ),
        PolicySpec(
            ("lineitem", "partsupp"),
            "sum(lineitem.l_discount - 0.06) <= 0",
            "For flows using lineitem and partsupp, average discount should not exceed 6%.",
        ),
        PolicySpec(
            ("lineitem", "partsupp"),
            "sum(lineitem.l_tax - 0.05) <= 0",
            "For flows using lineitem and partsupp, average tax rate should not exceed 5%.",
        ),
        PolicySpec(
            ("lineitem", "partsupp"),
            "sum(lineitem.l_extendedprice - 60000) <= 0",
            "For flows using lineitem and partsupp, average extended price should not exceed 60,000.",
        ),
        PolicySpec(
            ("lineitem", "customer"),
            "sum(lineitem.l_quantity - 20) >= 0",
            "For flows using lineitem and customer, average quantity should be at least 20.",
        ),
        PolicySpec(
            ("lineitem", "customer"),
            "sum(lineitem.l_discount - 0.09) <= 0",
            "For flows using lineitem and customer, average discount should not exceed 9%.",
        ),
        PolicySpec(
            ("lineitem", "customer"),
            "sum(lineitem.l_tax - 0.07) <= 0",
            "For flows using lineitem and customer, average tax rate should not exceed 7%.",
        ),
        PolicySpec(
            ("lineitem", "customer"),
            "sum(lineitem.l_extendedprice - 80000) <= 0",
            "For flows using lineitem and customer, average extended price should not exceed 80,000.",
        ),
        PolicySpec(
            ("lineitem", "supplier"),
            "sum(lineitem.l_quantity - 30) <= 0",
            "For flows using lineitem and supplier, average quantity should not exceed 30.",
        ),
        PolicySpec(
            ("lineitem", "supplier"),
            "sum(lineitem.l_discount - 0.06) <= 0",
            "For flows using lineitem and supplier, average discount should not exceed 6%.",
        ),
        PolicySpec(
            ("lineitem", "supplier"),
            "sum(lineitem.l_tax - 0.05) <= 0",
            "For flows using lineitem and supplier, average tax rate should not exceed 5%.",
        ),
        PolicySpec(
            ("lineitem", "supplier"),
            "sum(lineitem.l_extendedprice - 60000) <= 0",
            "For flows using lineitem and supplier, average extended price should not exceed 60,000.",
        ),
        PolicySpec(
            ("lineitem",),
            "sum(lineitem.l_discount - 0.06) <= 0",
            "Average discount should not exceed 6%.",
        ),
    ]


def fixed_4_violation_policy_specs() -> list[PolicySpec]:
    """Keep policy 1's four-query violation set unchanged as policies accumulate."""
    specs = policy_specs()
    replacements = {
        2: PolicySpec(
            ("lineitem",),
            "sum(lineitem.l_quantity - 10.5) >= 0",
            "Average quantity should be at least 10.5.",
        ),
        3: PolicySpec(
            ("lineitem", "part"),
            "sum(lineitem.l_quantity - 10) >= 0",
            "For flows using lineitem and part, average quantity should be at least 10.",
        ),
        5: PolicySpec(
            ("lineitem", "partsupp"),
            "sum(lineitem.l_discount - 0.055) <= 0",
            "For flows using lineitem and partsupp, average discount should not exceed 5.5%.",
        ),
        9: PolicySpec(
            ("lineitem", "part", "region"),
            "sum(lineitem.l_discount - 0.055) <= 0",
            (
                "For flows using lineitem, part, and region, average discount should not "
                "exceed 5.5%."
            ),
        ),
        17: PolicySpec(
            ("lineitem", "region"),
            "sum(lineitem.l_discount - 0.055) <= 0",
            "For flows using lineitem and region, average discount should not exceed 5.5%.",
        ),
    }
    for one_based_index, replacement in replacements.items():
        specs[one_based_index - 1] = replacement
    return specs


def policy_specs_for_variant(policy_variant: str) -> list[PolicySpec]:
    if policy_variant == POLICY_VARIANT_GROWING:
        return policy_specs()
    if policy_variant == POLICY_VARIANT_FIXED_4:
        return fixed_4_violation_policy_specs()
    raise ValueError(f"Unsupported policy variant: {policy_variant!r}")


def policy_catalog(
    policy_variant: str = POLICY_VARIANT_GROWING,
) -> list[tuple[str, str]]:
    """Return the constraint/description view retained for prompt callers."""
    return [
        (spec.constraint, spec.description) for spec in policy_specs_for_variant(policy_variant)
    ]


def build_policies(
    policy_count: int,
    policy_variant: str = POLICY_VARIANT_GROWING,
) -> list[DFCPolicy]:
    selected = policy_specs_for_variant(policy_variant)[:policy_count]
    return [
        DFCPolicy(
            sources=list(spec.sources),
            constraint=spec.constraint,
            on_fail=Resolution.INVALIDATE,
            description=spec.description,
        )
        for spec in selected
    ]


def message_text(content: Any) -> str:
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


def parse_violation_prediction(raw_text: str) -> bool | None:
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
    match = re.search(r"\bviolates_policy\b[^a-zA-Z]*(true|false)", lower)
    if match:
        return match.group(1) == "true"
    if "true" in lower and "false" not in lower:
        return True
    if "false" in lower and "true" not in lower:
        return False
    return None


def format_result_rows(columns: list[str], rows: list[tuple[Any, ...]]) -> str:
    records = [{col: row[i] for i, col in enumerate(columns)} for row in rows]
    return json.dumps(records, default=str)


def build_run_nonce_preamble(run_nonce: str) -> str:
    return (
        "Run metadata:\n"
        f"- nonce: {run_nonce}\n"
        "- treat this evaluation as independent from any prior request\n\n"
    )


def build_query_only_prompt(
    query: str,
    policy_descriptions: list[str],
    run_nonce: str | None = None,
) -> str:
    policies_block = "\n".join(
        [f"{idx + 1}. {desc}" for idx, desc in enumerate(policy_descriptions)]
    )
    preamble = build_run_nonce_preamble(run_nonce) if run_nonce else ""
    return (
        f"{preamble}"
        "You are validating whether query output would violate active data policies.\n"
        "Decide if ANY policy is violated.\n"
        'Return JSON only: {"violates_policy": true|false}\n\n'
        "SQL Query:\n"
        f"{query}\n\n"
        "Policies:\n"
        f"{policies_block}\n"
    )


def build_query_results_prompt(
    query: str,
    policy_descriptions: list[str],
    sample_rows_json: str,
    run_nonce: str | None = None,
    results_label: str = "First 100 result rows:",
) -> str:
    policies_block = "\n".join(
        [f"{idx + 1}. {desc}" for idx, desc in enumerate(policy_descriptions)]
    )
    preamble = build_run_nonce_preamble(run_nonce) if run_nonce else ""
    return (
        f"{preamble}"
        "You are validating whether query output violates active data policies.\n"
        "Decide if ANY policy is violated based on the query and provided sample rows.\n"
        'Return JSON only: {"violates_policy": true|false}\n\n'
        "SQL Query:\n"
        f"{query}\n\n"
        "Policies:\n"
        f"{policies_block}\n\n"
        f"{results_label}\n"
        f"{sample_rows_json}\n"
    )


def default_database_specs(
    database_sfs: list[float] | None,
    base_filename_prefix: str,
) -> list[dict[str, Any]]:
    selected_sfs = database_sfs or [0.1, 1.0, 10.0]
    specs: list[dict[str, Any]] = []
    for sf in selected_sfs:
        label = f"sf{sf:g}"
        specs.append(
            {
                "label": label,
                "tpch_sf": float(sf),
                "db_path": f"./results/{base_filename_prefix}_{label}.db",
            }
        )
    return specs


def normalize_database_specs(
    database_specs: list[dict[str, Any]] | None,
    database_sfs: list[float] | None,
    base_filename_prefix: str,
) -> list[dict[str, Any]]:
    if not database_specs:
        return default_database_specs(database_sfs, base_filename_prefix)

    normalized: list[dict[str, Any]] = []
    for spec in database_specs:
        tpch_sf = float(spec["tpch_sf"])
        label = str(spec.get("label", f"sf{tpch_sf:g}"))
        db_path = str(spec.get("db_path", f"./results/{base_filename_prefix}_{label}.db"))
        normalized.append(
            {
                "label": label,
                "tpch_sf": tpch_sf,
                "db_path": db_path,
            }
        )
    return normalized
