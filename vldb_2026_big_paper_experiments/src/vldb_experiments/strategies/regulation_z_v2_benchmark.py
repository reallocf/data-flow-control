"""Regulation Z v2 credit-card repricing benchmark."""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
import csv
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import time
from typing import Any, TextIO
import uuid

from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import DFCPolicy, Resolution, SQLRewriter

from vldb_experiments.strategies.llm_validation_common import DEFAULT_BEDROCK_MODEL_SPECS
from vldb_experiments.strategies.regulation_z_benchmark import (
    BedrockGuardResponse,
    BedrockPolicyGuard,
)
from vldb_experiments.strategies.regulation_z_v2_data import ProductTerm, load_product_terms
from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck

DEFAULT_NUM_ACCOUNTS = 1000
DEFAULT_NUM_UPDATES = 1000
DEFAULT_MEASURED_RUNS = 1
DEFAULT_WARMUP_RUNS = 0
DEFAULT_AWS_REGION = "us-east-1"
DEFAULT_MAX_CONCURRENCY = 50
V2_MODEL_KEYS = (
    "astra",
    "sol_6_1",
    "luna",
    "opus_5_5",
    "fable_5_1",
    "haiku_5_5",
)
V2_OPEN_WEIGHT_MODEL_KEYS = (
    "minimax_m2_5",
    "gpt_oss_120b",
    "kimi_k3",
    "glm_5_3",
    "deepseek_v3_2",
    "qwen3_coder_480b",
)
_MODEL_SPECS_BY_KEY = {str(spec["key"]): spec for spec in DEFAULT_BEDROCK_MODEL_SPECS}
_MODEL_SPECS_BY_KEY["luna"] = {
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
}
_MODEL_SPECS_BY_KEY["haiku_5_5"] = {
    "key": "haiku_5_5",
    "display_name": "Haiku 5.5",
    "provider": "bedrock",
    "bedrock_provider": "anthropic",
    "input_usd_per_million": 0.11,
    "output_usd_per_million": 0.55,
    "cache_read_usd_per_million": 0.011,
    "cache_write_usd_per_million": 0.1375,
    "model_id": (
        "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/arsxf1bg0gok"
    ),
}
_MODEL_SPECS_BY_KEY.update(
    {
        "grok_4_7": {
            "key": "grok_4_7",
            "display_name": "Grok 4.7",
            "provider": "bedrock",
            "bedrock_provider": "xai",
            "aws_region": "us-east-1",
            "input_usd_per_million": 2.20,
            "output_usd_per_million": 6.60,
            "cache_read_usd_per_million": 0.55,
            "prompt_nonce": True,
            "model_id": (
                "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/7nsy832zbewd"
            ),
        },
        "minimax_m2_5": {
            "key": "minimax_m2_5",
            "display_name": "MiniMax M2.5",
            "provider": "bedrock",
            "bedrock_provider": "minimax",
            "aws_region": "us-east-1",
            "input_usd_per_million": 0.30,
            "output_usd_per_million": 1.20,
            "model_id": (
                "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/6ruvu29x7n10"
            ),
        },
        "gpt_oss_120b": {
            "key": "gpt_oss_120b",
            "display_name": "GPT-OSS 120B",
            "provider": "bedrock",
            "input_usd_per_million": 0.15,
            "output_usd_per_million": 0.60,
            "model_id": (
                "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/pkufk9ti7io0"
            ),
        },
        "deepseek_v3_2": {
            "key": "deepseek_v3_2",
            "display_name": "DeepSeek V3.2",
            "provider": "bedrock",
            "input_usd_per_million": 0.62,
            "output_usd_per_million": 1.85,
            "model_id": (
                "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/t50ikzu69kj6"
            ),
        },
        "kimi_k3": {
            "key": "kimi_k3",
            "display_name": "Kimi K3",
            "provider": "bedrock",
            "input_usd_per_million": 3.30,
            "output_usd_per_million": 16.50,
            "cache_read_usd_per_million": 0.33,
            "cache_write_usd_per_million": 4.125,
            "prompt_nonce": True,
            "model_id": (
                "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/oqbsnrt7wyjd"
            ),
        },
        "glm_5_3": {
            "key": "glm_5_3",
            "display_name": "GLM 5.3",
            "provider": "bedrock",
            "input_usd_per_million": 1.848,
            "output_usd_per_million": 5.808,
            "cache_read_usd_per_million": 0.3432,
            "cache_write_usd_per_million": 2.31,
            "prompt_nonce": True,
            "model_id": (
                "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/eeh6z2sh6ogq"
            ),
        },
        "kimi_k2_5": {
            "key": "kimi_k2_5",
            "display_name": "Kimi K2.5",
            "provider": "bedrock",
            "input_usd_per_million": 0.60,
            "output_usd_per_million": 3.00,
            "model_id": (
                "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/nq59u2vnkrsk"
            ),
        },
        "glm_5": {
            "key": "glm_5",
            "display_name": "GLM 5",
            "provider": "bedrock",
            "input_usd_per_million": 1.00,
            "output_usd_per_million": 3.20,
            "model_id": (
                "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/mowkrzgnjr3c"
            ),
        },
        "qwen3_coder_next": {
            "key": "qwen3_coder_next",
            "display_name": "Qwen3 Coder Next",
            "provider": "bedrock",
            "input_usd_per_million": 0.50,
            "output_usd_per_million": 1.20,
            "model_id": (
                "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/4xjpnmmeyy8o"
            ),
        },
        "qwen3_coder_480b": {
            "key": "qwen3_coder_480b",
            "display_name": "Qwen3 Coder 480B-A35B",
            "provider": "bedrock",
            "aws_region": "us-east-2",
            "input_usd_per_million": 0.45,
            "output_usd_per_million": 1.80,
            "model_id": (
                "arn:aws:bedrock:us-east-2:920736616554:application-inference-profile/8c9en39x62hq"
            ),
        },
        "mistral_large_3": {
            "key": "mistral_large_3",
            "display_name": "Mistral Large 3",
            "provider": "bedrock",
            "input_usd_per_million": 0.50,
            "output_usd_per_million": 1.50,
            "model_id": (
                "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/e362nxh6mw5z"
            ),
        },
    }
)
DEFAULT_V2_BEDROCK_MODEL_SPECS = [_MODEL_SPECS_BY_KEY[key] for key in V2_MODEL_KEYS]
OPEN_WEIGHT_V2_BEDROCK_MODEL_SPECS = [_MODEL_SPECS_BY_KEY[key] for key in V2_OPEN_WEIGHT_MODEL_KEYS]
AVAILABLE_V2_BEDROCK_MODEL_SPECS = [
    *DEFAULT_V2_BEDROCK_MODEL_SPECS,
    *OPEN_WEIGHT_V2_BEDROCK_MODEL_SPECS,
    _MODEL_SPECS_BY_KEY["grok_4_7"],
]
DEFAULT_CATALOG_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "regulation_z_v2"
    / "tccp_2025_h2_product_terms.csv"
)

PROGRESS_FIELDNAMES = [
    "timestamp",
    "execution_number",
    "is_warmup",
    "approach",
    "model_id",
    "update_number",
    "num_updates",
    "rate_history_id",
    "publication_id",
    "statement_id",
    "account_id",
    "case_type",
    "source_record_id",
    "expected_allow",
    "actual_allow",
    "correct",
    "elapsed_ms",
    "rewrite_ms",
    "llm_ms",
    "sql_execution_ms",
    "input_tokens",
    "output_tokens",
    "cache_read_input_tokens",
    "cache_write_input_tokens",
    "parse_failure",
    "request_id",
    "error",
]

POLICY_TEXT = """A product-wide purchase APR repricing update may be applied only when all of these
are true:
1. The account is active, the product is active, and the account, product, and approved APR
   configuration all identify the same product.
2. The update is a purchase APR product repricing. Its new rate and effective date must match the
   approved APR configuration. Treat rates as matching if they are within 0.000001.
3. The notice belongs to the same account, describes a purchase APR change, and its old rate, new
   rate, and effective date match the update.
4. The approved APR configuration, notice, system change ticket, and update must carry the same
   change ticket. The APR configuration must have an approver and must be approved no later than
   the date the notice was sent.
5. Exactly one delivery record must identify this notice and account. It must say delivered, its
   sent date must match the notice sent date, and that date must be at least 45 days before the
   effective date.
6. The system change ticket must have compliance signoff no later than the notice sent date. Its
   implementation date must match the effective date and implementation must be verified.
Reject the update if any requirement is not met. The public product term is provenance for the
test data; it is not evidence that a particular consumer received notice."""

INVALID_CASE_TYPES = (
    "account_product_mismatch",
    "config_product_mismatch",
    "candidate_rate_mismatch",
    "notice_account_mismatch",
    "notice_old_rate_mismatch",
    "notice_new_rate_mismatch",
    "notice_effective_date_mismatch",
    "insufficient_advance_notice",
    "delivery_sent_date_mismatch",
    "delivery_notice_mismatch",
    "delivery_account_mismatch",
    "failed_delivery",
    "missing_delivery_record",
    "ticket_mismatch",
    "missing_compliance_signoff",
    "late_compliance_signoff",
    "implementation_date_mismatch",
    "implementation_unverified",
    "config_not_approved",
    "inactive_account",
    "wrong_apr_type",
    "wrong_reason_code",
)


@dataclass(frozen=True)
class RateChangeCase:
    """One proposed account-level rate-history update and its relational evidence."""

    case_number: int
    rate_history_id: str
    account_id: str
    account_product_code: str
    account_status: str
    product_row_code: str
    product_name: str
    product_status: str
    source_record_id: str
    source_row: int
    source_report_date: date
    source_purchase_apr: float
    source_rate_variable: bool
    source_index_name: str | None
    source_url: str
    config_id: str
    config_product_code: str
    config_rate: float
    config_effective_date: date
    config_approved_by: str | None
    config_approval_date: date
    config_ticket_id: str
    existing_old_rate: float
    candidate_apr_type: str
    candidate_old_rate: float
    candidate_new_rate: float
    candidate_effective_date: date
    candidate_reason_code: str
    candidate_change_event_id: str
    candidate_ticket_id: str
    notice_account_id: str
    notice_changed_term: str
    notice_old_rate: float
    notice_new_rate: float
    notice_sent_date: date
    notice_effective_date: date
    notice_ticket_id: str
    delivery_present: bool
    delivery_document_ref_id: str
    delivery_account_id: str
    delivery_sent_date: date
    delivery_status: str
    ticket_row_id: str
    compliance_signoff: bool
    compliance_signoff_date: date | None
    implementation_date: date
    implementation_verified: bool
    case_type: str
    is_valid: bool
    sql: str


def build_regulation_z_v2_policy() -> DFCPolicy:
    """Build the benchmark's narrow product-repricing policy."""
    return DFCPolicy(
        sources=[
            "accounts",
            "products",
            "apr_config",
            "change_notices",
            "delivery_logs",
            "change_management",
        ],
        sink="rate_history",
        sink_alias="rh",
        constraint=(
            "max(accounts.account_id) = rh.account_id AND "
            "max(accounts.status) = 'active' AND "
            "max(products.status) = 'active' AND "
            "max(accounts.product_code) = max(products.product_code) AND "
            "max(products.product_code) = max(apr_config.product_code) AND "
            "rh.apr_type = 'purchase' AND "
            "rh.reason_code = 'product_reprice' AND "
            "max(apr_config.apr_type) = rh.apr_type AND "
            "abs(rh.new_rate - max(apr_config.rate_value)) <= 0.000001 AND "
            "max(apr_config.effective_date) = rh.effective_date AND "
            "max(apr_config.approved_by) IS NOT NULL AND "
            "max(apr_config.approval_date) <= max(change_notices.notice_sent_date) AND "
            "max(change_notices.account_id) = rh.account_id AND "
            "max(change_notices.changed_term) = 'purchase_apr' AND "
            "abs(rh.old_rate - max(change_notices.old_value)) <= 0.000001 AND "
            "abs(rh.new_rate - max(change_notices.new_value)) <= 0.000001 AND "
            "max(change_notices.effective_date) = rh.effective_date AND "
            "max(change_notices.change_event_id) = rh.change_event_id AND "
            "max(apr_config.change_ticket_id) = rh.change_ticket_id AND "
            "max(change_notices.change_ticket_id) = rh.change_ticket_id AND "
            "max(change_management.change_ticket_id) = rh.change_ticket_id AND "
            "count(delivery_logs.delivery_id) = 1 AND "
            "max(delivery_logs.document_type) = 'notice' AND "
            "max(delivery_logs.document_ref_id) = max(change_notices.change_event_id) AND "
            "max(delivery_logs.account_id) = rh.account_id AND "
            "max(delivery_logs.delivery_status) = 'delivered' AND "
            "max(delivery_logs.sent_date) = max(change_notices.notice_sent_date) AND "
            "max(delivery_logs.sent_date) <= rh.effective_date - INTERVAL 45 DAY AND "
            "max(change_management.compliance_signoff) AND "
            "max(change_management.compliance_signoff_date) <= "
            "max(change_notices.notice_sent_date) AND "
            "max(change_management.implementation_date) = rh.effective_date AND "
            "max(change_management.implementation_verified)"
        ),
        on_fail=Resolution.REMOVE,
        description="Account-level purchase APR product-repricing control",
    )


def _build_update_sql(case: RateChangeCase) -> str:
    delivery_id = f"DLV{case.case_number:07d}"
    return (
        "UPDATE rate_history AS rh "
        f"SET apr_type = '{case.candidate_apr_type}', "
        f"old_rate = {case.candidate_old_rate:.6f}, "
        f"new_rate = {case.candidate_new_rate:.6f}, "
        f"effective_date = DATE '{case.candidate_effective_date.isoformat()}', "
        f"reason_code = '{case.candidate_reason_code}', "
        f"change_event_id = '{case.candidate_change_event_id}', "
        f"change_ticket_id = '{case.candidate_ticket_id}' "
        "FROM accounts "
        f"JOIN products ON products.product_code = '{case.product_row_code}' "
        f"JOIN apr_config ON apr_config.config_id = '{case.config_id}' "
        "JOIN change_notices ON "
        f"change_notices.change_event_id = '{case.candidate_change_event_id}' "
        f"LEFT JOIN delivery_logs ON delivery_logs.delivery_id = '{delivery_id}' "
        "JOIN change_management ON "
        f"change_management.change_ticket_id = '{case.ticket_row_id}' "
        f"WHERE rh.rate_history_id = '{case.rate_history_id}' "
        f"AND accounts.account_id = '{case.account_id}'"
    )


def _base_case(case_number: int, term: ProductTerm) -> RateChangeCase:
    account_id = f"A{case_number:07d}"
    product_code = f"P{case_number:07d}"
    effective_date = date(2027, 3, 1) + timedelta(days=case_number * 2)
    notice_sent_date = effective_date - timedelta(days=50)
    new_rate = term.purchase_apr
    old_rate = round(max(0.5, new_rate - 2.0), 3)
    ticket_id = f"CHG{case_number:07d}"
    event_id = f"EVT{case_number:07d}"
    case = RateChangeCase(
        case_number=case_number,
        rate_history_id=f"RH{case_number:08d}",
        account_id=account_id,
        account_product_code=product_code,
        account_status="active",
        product_row_code=product_code,
        product_name=term.product_name,
        product_status="active",
        source_record_id=term.source_record_id,
        source_row=term.source_row,
        source_report_date=term.report_date,
        source_purchase_apr=term.purchase_apr,
        source_rate_variable=term.rate_variable,
        source_index_name=term.index_name,
        source_url=term.source_url,
        config_id=f"CFG{case_number:07d}",
        config_product_code=product_code,
        config_rate=new_rate,
        config_effective_date=effective_date,
        config_approved_by="pricing-approver",
        config_approval_date=notice_sent_date - timedelta(days=10),
        config_ticket_id=ticket_id,
        existing_old_rate=old_rate,
        candidate_apr_type="purchase",
        candidate_old_rate=old_rate,
        candidate_new_rate=new_rate,
        candidate_effective_date=effective_date,
        candidate_reason_code="product_reprice",
        candidate_change_event_id=event_id,
        candidate_ticket_id=ticket_id,
        notice_account_id=account_id,
        notice_changed_term="purchase_apr",
        notice_old_rate=old_rate,
        notice_new_rate=new_rate,
        notice_sent_date=notice_sent_date,
        notice_effective_date=effective_date,
        notice_ticket_id=ticket_id,
        delivery_present=True,
        delivery_document_ref_id=event_id,
        delivery_account_id=account_id,
        delivery_sent_date=notice_sent_date,
        delivery_status="delivered",
        ticket_row_id=ticket_id,
        compliance_signoff=True,
        compliance_signoff_date=notice_sent_date - timedelta(days=12),
        implementation_date=effective_date,
        implementation_verified=True,
        case_type="valid_product_reprice",
        is_valid=True,
        sql="",
    )
    return replace(case, sql=_build_update_sql(case))


def _make_invalid(case: RateChangeCase, case_type: str) -> RateChangeCase:
    alternate_product = f"X{case.case_number:07d}"
    alternate_account = f"Z{case.case_number:07d}"
    alternate_ticket = f"ALT{case.case_number:07d}"
    alternate_event = f"OTH{case.case_number:07d}"
    changes: dict[str, Any] = {"case_type": case_type, "is_valid": False}
    if case_type == "account_product_mismatch":
        changes["account_product_code"] = alternate_product
    elif case_type == "config_product_mismatch":
        changes["config_product_code"] = alternate_product
    elif case_type == "candidate_rate_mismatch":
        changes["candidate_new_rate"] = case.candidate_new_rate + 1.25
    elif case_type == "notice_account_mismatch":
        changes["notice_account_id"] = alternate_account
    elif case_type == "notice_old_rate_mismatch":
        changes["notice_old_rate"] = case.notice_old_rate + 1.0
    elif case_type == "notice_new_rate_mismatch":
        changes["notice_new_rate"] = case.notice_new_rate + 1.0
    elif case_type == "notice_effective_date_mismatch":
        changes["notice_effective_date"] = case.notice_effective_date + timedelta(days=1)
    elif case_type == "insufficient_advance_notice":
        late_date = case.candidate_effective_date - timedelta(days=20)
        changes["notice_sent_date"] = late_date
        changes["delivery_sent_date"] = late_date
    elif case_type == "delivery_sent_date_mismatch":
        changes["delivery_sent_date"] = case.delivery_sent_date + timedelta(days=1)
    elif case_type == "delivery_notice_mismatch":
        changes["delivery_document_ref_id"] = alternate_event
    elif case_type == "delivery_account_mismatch":
        changes["delivery_account_id"] = alternate_account
    elif case_type == "failed_delivery":
        changes["delivery_status"] = "bounced"
    elif case_type == "missing_delivery_record":
        changes["delivery_present"] = False
    elif case_type == "ticket_mismatch":
        changes["notice_ticket_id"] = alternate_ticket
    elif case_type == "missing_compliance_signoff":
        changes["compliance_signoff"] = False
        changes["compliance_signoff_date"] = None
    elif case_type == "late_compliance_signoff":
        changes["compliance_signoff_date"] = case.notice_sent_date + timedelta(days=1)
    elif case_type == "implementation_date_mismatch":
        changes["implementation_date"] = case.implementation_date + timedelta(days=1)
    elif case_type == "implementation_unverified":
        changes["implementation_verified"] = False
    elif case_type == "config_not_approved":
        changes["config_approved_by"] = None
    elif case_type == "inactive_account":
        changes["account_status"] = "closed"
    elif case_type == "wrong_apr_type":
        changes["candidate_apr_type"] = "cash_advance"
    elif case_type == "wrong_reason_code":
        changes["candidate_reason_code"] = "index_change"
    else:
        raise ValueError(f"Unknown invalid case type: {case_type}")
    invalid = replace(case, **changes)
    return replace(invalid, sql=_build_update_sql(invalid))


def generate_rate_change_workload(
    product_terms: list[ProductTerm],
    *,
    num_accounts: int = DEFAULT_NUM_ACCOUNTS,
    num_updates: int = DEFAULT_NUM_UPDATES,
) -> list[RateChangeCase]:
    """Generate a deterministic, balanced collection of valid and invalid repricings."""
    if num_updates < 1:
        raise ValueError("num_updates must be at least 1")
    if num_accounts < num_updates:
        raise ValueError("num_accounts must be at least num_updates")
    if not product_terms:
        raise ValueError("product_terms must not be empty")

    case_types = tuple(
        case_type
        for invalid_case_type in INVALID_CASE_TYPES
        for case_type in ("valid_product_reprice", invalid_case_type)
    )
    cases: list[RateChangeCase] = []
    for index in range(num_updates):
        case_number = index + 1
        base = _base_case(case_number, product_terms[index % len(product_terms)])
        case_type = case_types[index % len(case_types)]
        case = base if case_type == "valid_product_reprice" else _make_invalid(base, case_type)
        cases.append(case)
    return cases


def build_policy_prompt(case: RateChangeCase, nonce: str) -> str:
    """Build a nonce-first prompt containing the exact evidence selected by the update."""
    delivery_row = None
    if case.delivery_present:
        delivery_row = {
            "delivery_id": f"DLV{case.case_number:07d}",
            "document_type": "notice",
            "document_ref_id": case.delivery_document_ref_id,
            "account_id": case.delivery_account_id,
            "sent_date": case.delivery_sent_date.isoformat(),
            "delivery_status": case.delivery_status,
        }
    context = {
        "existing_rate_history_row": {
            "rate_history_id": case.rate_history_id,
            "account_id": case.account_id,
            "apr_type": "purchase",
            "old_rate": case.existing_old_rate,
        },
        "account_row_used_by_query": {
            "account_id": case.account_id,
            "product_code": case.account_product_code,
            "status": case.account_status,
        },
        "product_row_used_by_query": {
            "product_code": case.product_row_code,
            "product_name": case.product_name,
            "status": case.product_status,
        },
        "approved_apr_config_row_used_by_query": {
            "config_id": case.config_id,
            "product_code": case.config_product_code,
            "apr_type": "purchase",
            "rate_value": case.config_rate,
            "effective_date": case.config_effective_date.isoformat(),
            "approved_by": case.config_approved_by,
            "approval_date": case.config_approval_date.isoformat(),
            "change_ticket_id": case.config_ticket_id,
        },
        "change_notice_row_used_by_query": {
            "change_event_id": case.candidate_change_event_id,
            "account_id": case.notice_account_id,
            "changed_term": case.notice_changed_term,
            "old_value": case.notice_old_rate,
            "new_value": case.notice_new_rate,
            "notice_sent_date": case.notice_sent_date.isoformat(),
            "effective_date": case.notice_effective_date.isoformat(),
            "change_ticket_id": case.notice_ticket_id,
        },
        "delivery_row_used_by_query": delivery_row,
        "change_ticket_row_used_by_query": {
            "change_ticket_id": case.ticket_row_id,
            "compliance_signoff": case.compliance_signoff,
            "compliance_signoff_date": (
                case.compliance_signoff_date.isoformat() if case.compliance_signoff_date else None
            ),
            "implementation_date": case.implementation_date.isoformat(),
            "implementation_verified": case.implementation_verified,
        },
        "public_product_provenance": {
            "source": "CFPB Terms of Credit Card Plans survey",
            "source_record_id": case.source_record_id,
            "report_date": case.source_report_date.isoformat(),
            "reported_purchase_apr": case.source_purchase_apr,
            "rate_variable": case.source_rate_variable,
            "index_name": case.source_index_name,
        },
    }
    return (
        f"Run nonce: {nonce}\n"
        "Policy:\n"
        f"{POLICY_TEXT}\n\n"
        "Candidate SQL update:\n"
        f"{case.sql}\n\n"
        "Relevant database rows:\n"
        f"{json.dumps(context, sort_keys=True)}\n\n"
        "Should the candidate update be allowed? "
        'Return JSON only: {"allow_update": true|false}\n'
    )


def reset_rate_change_database(
    conn: Any,
    workload: list[RateChangeCase],
) -> None:
    """Create and populate the public staging and operational CCS-shaped tables."""
    for table in [
        "rate_history",
        "change_management",
        "delivery_logs",
        "change_notices",
        "apr_config",
        "accounts",
        "product_sources",
        "products",
        "public_product_terms",
    ]:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.execute(
        "CREATE TABLE public_product_terms (source_record_id VARCHAR, source_row INTEGER, "
        "report_date DATE, purchase_apr DOUBLE, rate_variable BOOLEAN, index_name VARCHAR, "
        "source_url VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE products (product_code VARCHAR, product_name VARCHAR, status VARCHAR, "
        "effective_date DATE)"
    )
    conn.execute("CREATE TABLE product_sources (product_code VARCHAR, source_record_id VARCHAR)")
    conn.execute("CREATE TABLE accounts (account_id VARCHAR, product_code VARCHAR, status VARCHAR)")
    conn.execute(
        "CREATE TABLE apr_config (config_id VARCHAR, product_code VARCHAR, apr_type VARCHAR, "
        "rate_value DOUBLE, rate_variable BOOLEAN, index_name VARCHAR, effective_date DATE, "
        "approved_by VARCHAR, approval_date DATE, change_ticket_id VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE change_notices (change_event_id VARCHAR, account_id VARCHAR, "
        "changed_term VARCHAR, old_value DOUBLE, new_value DOUBLE, notice_sent_date DATE, "
        "effective_date DATE, change_ticket_id VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE delivery_logs (delivery_id VARCHAR, document_type VARCHAR, "
        "document_ref_id VARCHAR, account_id VARCHAR, sent_date DATE, delivery_status VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE change_management (change_ticket_id VARCHAR, compliance_signoff BOOLEAN, "
        "compliance_signoff_date DATE, implementation_date DATE, "
        "implementation_verified BOOLEAN)"
    )
    conn.execute(
        "CREATE TABLE rate_history (rate_history_id VARCHAR, account_id VARCHAR, "
        "apr_type VARCHAR, old_rate DOUBLE, new_rate DOUBLE, effective_date DATE, "
        "reason_code VARCHAR, change_event_id VARCHAR, change_ticket_id VARCHAR)"
    )

    terms_by_id = {case.source_record_id: case for case in workload}
    conn.executemany(
        "INSERT INTO public_product_terms VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (
                case.source_record_id,
                case.source_row,
                case.source_report_date,
                case.source_purchase_apr,
                case.source_rate_variable,
                case.source_index_name,
                case.source_url,
            )
            for case in terms_by_id.values()
        ],
    )
    product_rows: dict[str, tuple[Any, ...]] = {}
    source_rows: dict[str, tuple[str, str]] = {}
    for case in workload:
        product_rows[case.product_row_code] = (
            case.product_row_code,
            case.product_name,
            case.product_status,
            case.source_report_date,
        )
        source_rows[case.product_row_code] = (
            case.product_row_code,
            case.source_record_id,
        )
        if case.account_product_code != case.product_row_code:
            product_rows[case.account_product_code] = (
                case.account_product_code,
                f"Alternate {case.product_name}",
                "active",
                case.source_report_date,
            )
        if case.config_product_code not in product_rows:
            product_rows[case.config_product_code] = (
                case.config_product_code,
                f"Alternate {case.product_name}",
                "active",
                case.source_report_date,
            )
    conn.executemany("INSERT INTO products VALUES (?, ?, ?, ?)", list(product_rows.values()))
    conn.executemany(
        "INSERT INTO product_sources VALUES (?, ?)",
        list(source_rows.values()),
    )
    conn.executemany(
        "INSERT INTO accounts VALUES (?, ?, ?)",
        [(case.account_id, case.account_product_code, case.account_status) for case in workload],
    )
    conn.executemany(
        "INSERT INTO apr_config VALUES (?, ?, 'purchase', ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                case.config_id,
                case.config_product_code,
                case.config_rate,
                case.source_rate_variable,
                case.source_index_name,
                case.config_effective_date,
                case.config_approved_by,
                case.config_approval_date,
                case.config_ticket_id,
            )
            for case in workload
        ],
    )
    conn.executemany(
        "INSERT INTO change_notices VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                case.candidate_change_event_id,
                case.notice_account_id,
                case.notice_changed_term,
                case.notice_old_rate,
                case.notice_new_rate,
                case.notice_sent_date,
                case.notice_effective_date,
                case.notice_ticket_id,
            )
            for case in workload
        ],
    )
    conn.executemany(
        "INSERT INTO delivery_logs VALUES (?, 'notice', ?, ?, ?, ?)",
        [
            (
                f"DLV{case.case_number:07d}",
                case.delivery_document_ref_id,
                case.delivery_account_id,
                case.delivery_sent_date,
                case.delivery_status,
            )
            for case in workload
            if case.delivery_present
        ],
    )
    conn.executemany(
        "INSERT INTO change_management VALUES (?, ?, ?, ?, ?)",
        [
            (
                case.ticket_row_id,
                case.compliance_signoff,
                case.compliance_signoff_date,
                case.implementation_date,
                case.implementation_verified,
            )
            for case in workload
        ],
    )
    conn.executemany(
        "INSERT INTO rate_history VALUES (?, ?, 'purchase', ?, ?, ?, "
        "'manual_override', NULL, NULL)",
        [
            (
                case.rate_history_id,
                case.account_id,
                case.existing_old_rate,
                case.existing_old_rate,
                case.candidate_effective_date,
            )
            for case in workload
        ],
    )


class RegulationZV2BenchmarkStrategy(ExperimentStrategy):
    """Compare DFC, four Bedrock guards, and no policy on APR repricing updates."""

    def setup(self, context: ExperimentContext) -> None:
        self.num_accounts = int(context.strategy_config.get("num_accounts", DEFAULT_NUM_ACCOUNTS))
        self.num_updates = int(context.strategy_config.get("num_updates", DEFAULT_NUM_UPDATES))
        self.max_concurrency = int(
            context.strategy_config.get("max_concurrency", DEFAULT_MAX_CONCURRENCY)
        )
        if not 1 <= self.max_concurrency <= DEFAULT_MAX_CONCURRENCY:
            raise ValueError(f"max_concurrency must be between 1 and {DEFAULT_MAX_CONCURRENCY}")
        catalog_path = Path(str(context.strategy_config.get("catalog_path", DEFAULT_CATALOG_PATH)))
        self.product_terms = load_product_terms(catalog_path)
        self.workload = generate_rate_change_workload(
            self.product_terms,
            num_accounts=self.num_accounts,
            num_updates=self.num_updates,
        )
        raw_specs = context.strategy_config.get("models", DEFAULT_V2_BEDROCK_MODEL_SPECS)
        self.model_specs = [dict(spec) for spec in raw_specs]

        self.local_duckdb = _ensure_smokedduck()
        self.no_policy_conn = self.local_duckdb.connect(":memory:")
        self.dfc_conn = self.local_duckdb.connect(":memory:")
        self.model_connections = {
            str(spec["key"]): self.local_duckdb.connect(":memory:") for spec in self.model_specs
        }
        for conn in [self.no_policy_conn, self.dfc_conn, *self.model_connections.values()]:
            reset_rate_change_database(conn, self.workload)

        self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)
        self.dfc_rewriter.register_policy(build_regulation_z_v2_policy())

        supplied_client = context.strategy_config.get("bedrock_client")
        if supplied_client is None:
            import boto3

            region = str(context.strategy_config.get("aws_region", DEFAULT_AWS_REGION))
            profile = context.strategy_config.get("aws_profile")
            session = (
                boto3.Session(profile_name=profile, region_name=region)
                if profile
                else boto3.Session(region_name=region)
            )
            supplied_client = session.client("bedrock-runtime")
        self.guards = {
            str(spec["key"]): BedrockPolicyGuard(supplied_client, str(spec["model_id"]))
            for spec in self.model_specs
        }

        self.progress_file: TextIO | None = None
        self.progress_writer: csv.DictWriter | None = None
        progress_path_value = context.strategy_config.get("progress_path")
        if progress_path_value:
            progress_path = Path(str(progress_path_value))
            progress_path.parent.mkdir(parents=True, exist_ok=True)
            write_header = not progress_path.exists() or progress_path.stat().st_size == 0
            self.progress_file = progress_path.open("a", encoding="utf-8", newline="", buffering=1)
            self.progress_writer = csv.DictWriter(
                self.progress_file,
                fieldnames=PROGRESS_FIELDNAMES,
                extrasaction="ignore",
            )
            if write_header:
                self.progress_writer.writeheader()
                self.progress_file.flush()

    def _write_progress(self, context: ExperimentContext, **values: Any) -> None:
        if self.progress_writer is None or self.progress_file is None:
            return
        row = dict.fromkeys(PROGRESS_FIELDNAMES, "")
        row.update(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "execution_number": context.execution_number,
                "is_warmup": context.is_warmup,
                "num_updates": self.num_updates,
            }
        )
        row.update(values)
        self.progress_writer.writerow(row)
        self.progress_file.flush()

    def _reset_database(self, conn: Any) -> None:
        reset_rate_change_database(conn, self.workload)

    def _build_prompt(self, case: RateChangeCase, nonce: str) -> str:
        prompt = build_policy_prompt(case, nonce)
        return prompt if nonce else prompt.partition("\n")[2]

    @staticmethod
    def _update_applied(conn: Any, case: RateChangeCase) -> bool:
        reason_code = conn.execute(
            "SELECT reason_code FROM rate_history WHERE rate_history_id = ?",
            [case.rate_history_id],
        ).fetchone()[0]
        return reason_code != "manual_override"

    def _progress_identity(self, case: RateChangeCase) -> dict[str, Any]:
        return {
            "rate_history_id": case.rate_history_id,
            "account_id": case.account_id,
            "case_type": case.case_type,
            "source_record_id": case.source_record_id,
            "expected_allow": case.is_valid,
        }

    def _run_no_policy(self, context: ExperimentContext) -> tuple[float, int]:
        self._reset_database(self.no_policy_conn)
        correct = 0
        elapsed_ms = 0.0
        for update_number, case in enumerate(self.workload, start=1):
            start = time.perf_counter()
            try:
                self.no_policy_conn.execute(case.sql)
            except Exception as error:
                case_ms = (time.perf_counter() - start) * 1000.0
                self._write_progress(
                    context,
                    approach="no_policy",
                    update_number=update_number,
                    elapsed_ms=case_ms,
                    sql_execution_ms=case_ms,
                    error=str(error),
                    **self._progress_identity(case),
                )
                raise
            case_ms = (time.perf_counter() - start) * 1000.0
            elapsed_ms += case_ms
            actual_allow = self._update_applied(self.no_policy_conn, case)
            is_correct = actual_allow == case.is_valid
            correct += int(is_correct)
            self._write_progress(
                context,
                approach="no_policy",
                update_number=update_number,
                actual_allow=actual_allow,
                correct=is_correct,
                elapsed_ms=case_ms,
                sql_execution_ms=case_ms,
                **self._progress_identity(case),
            )
        return elapsed_ms, correct

    def _run_dfc(self, context: ExperimentContext) -> tuple[float, float, float, int]:
        self._reset_database(self.dfc_conn)
        rewrite_ms = 0.0
        execution_ms = 0.0
        correct = 0
        for update_number, case in enumerate(self.workload, start=1):
            rewrite_start = time.perf_counter()
            try:
                transformed = self.dfc_rewriter.transform_query(case.sql)
            except Exception as error:
                case_rewrite_ms = (time.perf_counter() - rewrite_start) * 1000.0
                self._write_progress(
                    context,
                    approach="dfc",
                    update_number=update_number,
                    elapsed_ms=case_rewrite_ms,
                    rewrite_ms=case_rewrite_ms,
                    error=str(error),
                    **self._progress_identity(case),
                )
                raise
            case_rewrite_ms = (time.perf_counter() - rewrite_start) * 1000.0
            rewrite_ms += case_rewrite_ms
            execution_start = time.perf_counter()
            try:
                self.dfc_conn.execute(transformed)
            except Exception as error:
                case_execution_ms = (time.perf_counter() - execution_start) * 1000.0
                self._write_progress(
                    context,
                    approach="dfc",
                    update_number=update_number,
                    elapsed_ms=case_rewrite_ms + case_execution_ms,
                    rewrite_ms=case_rewrite_ms,
                    sql_execution_ms=case_execution_ms,
                    error=str(error),
                    **self._progress_identity(case),
                )
                raise
            case_execution_ms = (time.perf_counter() - execution_start) * 1000.0
            execution_ms += case_execution_ms
            actual_allow = self._update_applied(self.dfc_conn, case)
            is_correct = actual_allow == case.is_valid
            correct += int(is_correct)
            self._write_progress(
                context,
                approach="dfc",
                update_number=update_number,
                actual_allow=actual_allow,
                correct=is_correct,
                elapsed_ms=case_rewrite_ms + case_execution_ms,
                rewrite_ms=case_rewrite_ms,
                sql_execution_ms=case_execution_ms,
                **self._progress_identity(case),
            )
        return rewrite_ms + execution_ms, rewrite_ms, execution_ms, correct

    def _run_model(
        self,
        context: ExperimentContext,
        model_key: str,
        model_id: str,
        prompt_nonce: bool,
    ) -> dict[str, int | float]:
        conn = self.model_connections[model_key]
        self._reset_database(conn)
        guard = self.guards[model_key]
        total_ms = 0.0
        sql_execution_ms = 0.0
        correct = 0
        false_positives = 0
        false_negatives = 0
        parse_failures = 0
        input_tokens = 0
        output_tokens = 0
        cache_read_tokens = 0
        cache_write_tokens = 0
        workload_iterator = iter(enumerate(self.workload, start=1))
        max_workers = min(self.max_concurrency, len(self.workload))
        wall_start = time.perf_counter()
        pending: dict[Future[BedrockGuardResponse], tuple[int, RateChangeCase]] = {}

        def submit_next(executor: ThreadPoolExecutor) -> bool:
            try:
                update_number, case = next(workload_iterator)
            except StopIteration:
                return False
            nonce = str(uuid.uuid4()) if prompt_nonce else ""
            prompt = self._build_prompt(case, nonce)
            pending[executor.submit(guard.invoke, prompt)] = (update_number, case)
            return True

        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=f"regulation-z-v2-{model_key}",
        ) as executor:
            for _ in range(max_workers):
                submit_next(executor)
            while pending:
                completed, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in completed:
                    update_number, case = pending.pop(future)
                    try:
                        response = future.result()
                    except Exception as error:
                        self._write_progress(
                            context,
                            approach=model_key,
                            model_id=model_id,
                            update_number=update_number,
                            error=str(error),
                            **self._progress_identity(case),
                        )
                        for pending_future in pending:
                            pending_future.cancel()
                        raise
                    total_ms += response.runtime_ms
                    input_tokens += response.input_tokens
                    output_tokens += response.output_tokens
                    cache_read_tokens += response.cache_read_input_tokens
                    cache_write_tokens += response.cache_write_input_tokens
                    allow_update = response.allow_update
                    parse_failure = allow_update is None
                    if allow_update is None:
                        parse_failures += 1
                        allow_update = False

                    case_sql_execution_ms = 0.0
                    if allow_update:
                        execution_start = time.perf_counter()
                        try:
                            conn.execute(case.sql)
                        except Exception as error:
                            case_sql_execution_ms = (time.perf_counter() - execution_start) * 1000.0
                            self._write_progress(
                                context,
                                approach=model_key,
                                model_id=model_id,
                                update_number=update_number,
                                actual_allow=allow_update,
                                elapsed_ms=response.runtime_ms + case_sql_execution_ms,
                                llm_ms=response.runtime_ms,
                                sql_execution_ms=case_sql_execution_ms,
                                input_tokens=response.input_tokens,
                                output_tokens=response.output_tokens,
                                cache_read_input_tokens=response.cache_read_input_tokens,
                                cache_write_input_tokens=response.cache_write_input_tokens,
                                parse_failure=parse_failure,
                                request_id=response.request_id,
                                error=str(error),
                                **self._progress_identity(case),
                            )
                            for pending_future in pending:
                                pending_future.cancel()
                            raise
                        case_sql_execution_ms = (time.perf_counter() - execution_start) * 1000.0
                        sql_execution_ms += case_sql_execution_ms
                    is_correct = allow_update == case.is_valid
                    correct += int(is_correct)
                    false_positives += int(not allow_update and case.is_valid)
                    false_negatives += int(allow_update and not case.is_valid)
                    self._write_progress(
                        context,
                        approach=model_key,
                        model_id=model_id,
                        update_number=update_number,
                        actual_allow=allow_update,
                        correct=is_correct,
                        elapsed_ms=response.runtime_ms + case_sql_execution_ms,
                        llm_ms=response.runtime_ms,
                        sql_execution_ms=case_sql_execution_ms,
                        input_tokens=response.input_tokens,
                        output_tokens=response.output_tokens,
                        cache_read_input_tokens=response.cache_read_input_tokens,
                        cache_write_input_tokens=response.cache_write_input_tokens,
                        parse_failure=parse_failure,
                        request_id=response.request_id,
                        **self._progress_identity(case),
                    )
                    submit_next(executor)

        return {
            "time_ms": total_ms + sql_execution_ms,
            "llm_time_ms": total_ms,
            "wall_time_ms": (time.perf_counter() - wall_start) * 1000.0,
            "sql_execution_ms": sql_execution_ms,
            "correct": correct,
            "false_positives": false_positives,
            "false_negatives": false_negatives,
            "parse_failures": parse_failures,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cache_read_input_tokens": cache_read_tokens,
            "cache_write_input_tokens": cache_write_tokens,
        }

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        no_policy_ms, no_policy_correct = self._run_no_policy(context)
        dfc_ms, dfc_rewrite_ms, dfc_execution_ms, dfc_correct = self._run_dfc(context)
        custom_metrics: dict[str, Any] = {
            "num_accounts": self.num_accounts,
            "num_updates": self.num_updates,
            "num_public_product_terms": len(self.product_terms),
            "valid_updates": sum(case.is_valid for case in self.workload),
            "invalid_updates": sum(not case.is_valid for case in self.workload),
            "no_policy_time_ms": no_policy_ms,
            "no_policy_correct": no_policy_correct,
            "no_policy_accuracy": no_policy_correct / self.num_updates,
            "dfc_time_ms": dfc_ms,
            "dfc_rewrite_time_ms": dfc_rewrite_ms,
            "dfc_execution_time_ms": dfc_execution_ms,
            "dfc_correct": dfc_correct,
            "dfc_accuracy": dfc_correct / self.num_updates,
        }
        duration_ms = no_policy_ms + dfc_ms
        for spec in self.model_specs:
            key = str(spec["key"])
            metrics = self._run_model(
                context,
                key,
                str(spec["model_id"]),
                bool(spec.get("prompt_nonce", False)),
            )
            duration_ms += float(metrics["wall_time_ms"])
            custom_metrics[f"{key}_display_name"] = spec["display_name"]
            custom_metrics[f"{key}_model_id"] = spec["model_id"]
            for metric_name, value in metrics.items():
                custom_metrics[f"{key}_{metric_name}"] = value
            custom_metrics[f"{key}_accuracy"] = int(metrics["correct"]) / self.num_updates
            custom_metrics[f"{key}_cache_contaminated"] = bool(metrics["cache_read_input_tokens"])
        return ExperimentResult(duration_ms=duration_ms, custom_metrics=custom_metrics)

    def teardown(self, _context: ExperimentContext) -> None:
        for conn in [self.no_policy_conn, self.dfc_conn, *self.model_connections.values()]:
            conn.close()
        if self.progress_file is not None:
            self.progress_file.close()
