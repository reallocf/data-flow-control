"""Regulation Z ability-to-repay application benchmark."""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
import time
from typing import Any, TextIO
import uuid

from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import DFCPolicy, Resolution, SQLRewriter

from vldb_experiments.strategies.llm_validation_common import DEFAULT_BEDROCK_MODEL_SPECS
from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck

DEFAULT_NUM_APPLICATIONS = 1000
DEFAULT_NUM_UPDATES = 1000
DEFAULT_MEASURED_RUNS = 1
DEFAULT_WARMUP_RUNS = 0
DEFAULT_AWS_REGION = "us-east-1"
DEFAULT_MAX_CONCURRENCY = 50

PROGRESS_FIELDNAMES = [
    "timestamp",
    "execution_number",
    "is_warmup",
    "approach",
    "model_id",
    "update_number",
    "num_updates",
    "application_id",
    "case_type",
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

POLICY_TEXT = """An APPROVED mortgage decision is allowed only when all of these are true:
1. The application, verified income, verified obligations, loan terms, and underwriting
   standard all belong to the same application and product.
2. income_used is positive and no greater than verified_monthly_income. Applicant-claimed
   income is not verified income.
3. The verified income and verified obligations rows must be third party verified.
4. obligations_used exactly equals existing monthly debt plus the proposed mortgage payment,
   mortgage-related obligations, and any simultaneous-loan payment.
5. dti equals obligations_used divided by income_used. Treat the dti as matching if it's
   within 0.000001.
6. dti is no greater than the product's max_dti.
A DECLINED decision is allowed. Reject an update if any approval requirement is not met."""


@dataclass(frozen=True)
class UnderwritingCase:
    """One application and its candidate decision update."""

    application_id: int
    borrower_id: int
    product_id: int
    verified_monthly_income: int
    claimed_monthly_income: int
    existing_monthly_debt: int
    omittable_monthly_debt: int
    proposed_payment: int
    mortgage_related_payment: int
    simultaneous_loan_payment: int
    max_dti: float
    candidate_outcome: str
    candidate_income_used: int
    candidate_obligations_used: int
    candidate_dti: float
    application_source_ids: tuple[int, ...]
    income_application_id: int
    obligations_application_id: int
    loan_terms_application_id: int
    standard_product_id: int
    income_third_party_verified: bool
    obligations_third_party_verified: bool
    income_evidence_present: bool
    case_type: str
    is_valid: bool
    sql: str


@dataclass(frozen=True)
class BedrockGuardResponse:
    """Parsed Bedrock policy-guard response and request metadata."""

    allow_update: bool | None
    raw_response: str
    runtime_ms: float
    input_tokens: int
    output_tokens: int
    cache_read_input_tokens: int
    cache_write_input_tokens: int
    request_id: str


def _parse_allow_update(raw_text: str) -> bool | None:
    # Reasoning may discuss the opposite decision before emitting the final JSON.
    text = re.sub(
        r"<(reasoning|think)>.*?(?:</\1>|$)", "", raw_text, flags=re.DOTALL | re.IGNORECASE
    ).strip()
    if not text:
        return None
    try:
        decoded = json.loads(text)
        if isinstance(decoded, dict) and isinstance(decoded.get("allow_update"), bool):
            return bool(decoded["allow_update"])
    except json.JSONDecodeError:
        pass

    match = re.search(r"\ballow_update\b[^a-zA-Z]*(true|false)", text.lower())
    if match:
        return match.group(1) == "true"
    return None


def _bedrock_response_text(response: dict[str, Any]) -> str:
    content = response.get("output", {}).get("message", {}).get("content", [])
    return "\n".join(
        str(block["text"]) for block in content if isinstance(block, dict) and "text" in block
    )


def _openai_invoke_response_text(response: dict[str, Any]) -> str:
    choices = response.get("choices", [])
    if choices:
        content = choices[0].get("message", {}).get("content", "")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            return "\n".join(
                str(part["text"]) for part in content if isinstance(part, dict) and "text" in part
            )
    output = response.get("output", [])
    return "\n".join(
        str(part["text"])
        for item in output
        if isinstance(item, dict)
        for part in item.get("content", [])
        if isinstance(part, dict) and "text" in part
    )


def build_bedrock_request(model_id: str, prompt: str) -> dict[str, Any]:
    """Build a Converse request using provider-default sampling and no cache checkpoints."""
    return {
        "modelId": model_id,
        "messages": [{"role": "user", "content": [{"text": prompt}]}],
    }


def build_openai_invoke_request(model_id: str, prompt: str) -> dict[str, Any]:
    """Build an OpenAI-native InvokeModel request with prompt caching disabled."""
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Evaluate this independent benchmark case.\n",
                        "prompt_cache_breakpoint": {"mode": "explicit"},
                    },
                    {"type": "text", "text": prompt},
                ],
            }
        ],
        "prompt_cache_options": {"mode": "explicit"},
    }
    return {
        "modelId": model_id,
        "body": json.dumps(body),
        "accept": "application/json",
        "contentType": "application/json",
    }


class BedrockPolicyGuard:
    """Small Bedrock client for policy decisions."""

    def __init__(self, client: Any, model_id: str, provider: str | None = None) -> None:
        self.client = client
        self.model_id = model_id
        self.provider = provider

    def invoke(self, prompt: str) -> BedrockGuardResponse:
        if self.provider in {"openai", "minimax", "xai"}:
            return self._invoke_native(prompt)
        request = build_bedrock_request(self.model_id, prompt)
        start = time.perf_counter()
        response = self.client.converse(**request)
        runtime_ms = (time.perf_counter() - start) * 1000.0
        raw_response = _bedrock_response_text(response)
        usage = response.get("usage", {})
        return BedrockGuardResponse(
            allow_update=_parse_allow_update(raw_response),
            raw_response=raw_response,
            runtime_ms=runtime_ms,
            input_tokens=int(usage.get("inputTokens") or 0),
            output_tokens=int(usage.get("outputTokens") or 0),
            cache_read_input_tokens=int(usage.get("cacheReadInputTokens") or 0),
            cache_write_input_tokens=int(usage.get("cacheWriteInputTokens") or 0),
            request_id=str(response.get("ResponseMetadata", {}).get("RequestId", "")),
        )

    def _invoke_native(self, prompt: str) -> BedrockGuardResponse:
        if self.provider in {"minimax", "xai"}:
            request = {
                "modelId": self.model_id,
                "body": json.dumps(
                    {
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": 8192,
                    }
                ),
                "accept": "application/json",
                "contentType": "application/json",
            }
        else:
            request = build_openai_invoke_request(self.model_id, prompt)
        start = time.perf_counter()
        response = self.client.invoke_model(**request)
        runtime_ms = (time.perf_counter() - start) * 1000.0
        response_body = response["body"].read()
        if isinstance(response_body, bytes):
            response_body = response_body.decode("utf-8")
        decoded = json.loads(response_body)
        usage = decoded.get("usage", {})
        input_details = usage.get("input_tokens_details", {})
        prompt_details = usage.get("prompt_tokens_details", {})
        total_input_tokens = int(usage.get("input_tokens") or usage.get("prompt_tokens") or 0)
        cache_read_input_tokens = int(
            input_details.get("cached_tokens") or prompt_details.get("cached_tokens") or 0
        )
        cache_write_input_tokens = int(
            input_details.get("cache_write_tokens") or prompt_details.get("cache_write_tokens") or 0
        )
        raw_response = _openai_invoke_response_text(decoded)
        return BedrockGuardResponse(
            allow_update=_parse_allow_update(raw_response),
            raw_response=raw_response,
            runtime_ms=runtime_ms,
            input_tokens=max(
                0,
                total_input_tokens - cache_read_input_tokens - cache_write_input_tokens,
            ),
            output_tokens=int(usage.get("output_tokens") or usage.get("completion_tokens") or 0),
            cache_read_input_tokens=cache_read_input_tokens,
            cache_write_input_tokens=cache_write_input_tokens,
            request_id=str(response.get("ResponseMetadata", {}).get("RequestId", "")),
        )


def build_regulation_z_policy() -> DFCPolicy:
    """Build the benchmark's simplified ability-to-repay policy."""
    return DFCPolicy(
        sources=[
            "applications",
            "verified_income",
            "verified_obligations",
            "loan_terms",
            "underwriting_standards",
        ],
        sink="underwriting_decisions",
        sink_alias="d",
        constraint=(
            "d.outcome <> 'APPROVED' OR ("
            "max(applications.application_id) = d.application_id AND "
            "max(verified_income.application_id) = d.application_id AND "
            "max(verified_obligations.application_id) = d.application_id AND "
            "max(loan_terms.application_id) = d.application_id AND "
            "max(applications.product_id) = max(underwriting_standards.product_id) AND "
            "max(verified_income.third_party_verified) AND "
            "max(verified_obligations.third_party_verified) AND "
            "d.income_used > 0 AND "
            "d.income_used <= max(verified_income.verified_monthly_income) AND "
            "d.obligations_used = max(verified_obligations.existing_monthly_debt) + "
            "max(loan_terms.proposed_payment) + "
            "max(loan_terms.mortgage_related_payment) + "
            "max(loan_terms.simultaneous_loan_payment) AND "
            "abs(d.dti - d.obligations_used / d.income_used) <= 0.000001 AND "
            "d.dti <= max(underwriting_standards.max_dti))"
        ),
        on_fail=Resolution.REMOVE,
        description="Regulation Z ability-to-repay benchmark policy",
    )


def _build_update_sql(
    *,
    application_id: int,
    application_source_ids: tuple[int, ...],
    income_application_id: int,
    obligations_application_id: int,
    loan_terms_application_id: int,
    standard_product_id: int,
    income_join: str,
    outcome: str,
    income_used: int,
    obligations_used: int,
    dti: float,
) -> str:
    application_ids = ", ".join(str(value) for value in application_source_ids)
    return (
        "UPDATE underwriting_decisions AS d "
        f"SET outcome = '{outcome}', income_used = {income_used}, "
        f"obligations_used = {obligations_used}, dti = {dti:.8f} "
        f"FROM applications {income_join} verified_income ON "
        f"verified_income.application_id = {income_application_id} "
        "JOIN verified_obligations ON "
        f"verified_obligations.application_id = {obligations_application_id} "
        "JOIN loan_terms ON "
        f"loan_terms.application_id = {loan_terms_application_id} "
        "JOIN underwriting_standards ON "
        f"underwriting_standards.product_id = {standard_product_id} "
        f"WHERE d.application_id = {application_id} "
        f"AND applications.application_id IN ({application_ids})"
    )


def generate_underwriting_workload(
    num_applications: int = DEFAULT_NUM_APPLICATIONS,
    num_updates: int = DEFAULT_NUM_UPDATES,
) -> list[UnderwritingCase]:
    """Generate a deterministic, balanced workload of valid and invalid decisions."""
    if num_applications < num_updates:
        raise ValueError("num_applications must be at least num_updates")
    if num_updates < 1:
        raise ValueError("num_updates must be at least 1")

    product_thresholds = {1: 0.36, 2: 0.43, 3: 0.50}
    invalid_case_types = (
        "unverified_income",
        "omitted_obligation",
        "incorrect_payment",
        "cross_application",
        "threshold_violation",
        "unverified_income_record",
        "unverified_obligations_record",
        "product_mismatch",
        "income_application_mismatch",
        "obligations_application_mismatch",
        "loan_terms_application_mismatch",
        "nonpositive_income",
        "incorrect_dti",
        "missing_income_record",
        "application_mismatch",
    )
    case_types = tuple(
        case_type
        for index, invalid_case_type in enumerate(invalid_case_types)
        for case_type in (
            "valid_approval" if index % 2 == 0 else "valid_decline",
            invalid_case_type,
        )
    )
    cases: list[UnderwritingCase] = []

    for index in range(num_updates):
        application_id = index + 1
        borrower_id = 100_000 + application_id
        product_id = 1 + index % len(product_thresholds)
        max_dti = product_thresholds[product_id]
        existing_debt = 350 + (application_id * 83) % 1450
        omittable_debt = 75 + (application_id * 29) % min(existing_debt, 350)
        proposed_payment = 900 + (application_id * 97) % 1500
        mortgage_payment = 175 + (application_id * 31) % 325
        simultaneous_payment = 250 if application_id % 7 == 0 else 0
        total_obligations = (
            existing_debt + proposed_payment + mortgage_payment + simultaneous_payment
        )
        case_type = case_types[index % len(case_types)]
        baseline_should_pass = case_type not in {"valid_decline", "threshold_violation"}
        target_ratio = max_dti - 0.05 if baseline_should_pass else max_dti + 0.08
        verified_income = max(2500, math.ceil(total_obligations / target_ratio))
        claimed_income = math.ceil(verified_income * 1.35)
        outcome = "DECLINED" if case_type == "valid_decline" else "APPROVED"
        income_used = verified_income
        obligations_used = total_obligations
        application_source_ids = (application_id,)
        income_application_id = application_id
        obligations_application_id = application_id
        loan_terms_application_id = application_id
        standard_product_id = product_id
        standard_max_dti = max_dti
        income_third_party_verified = True
        obligations_third_party_verified = True
        income_evidence_present = True
        income_join = "JOIN"
        is_valid = case_type in {"valid_approval", "valid_decline"}
        alternate_application_id = -application_id

        if case_type == "unverified_income":
            income_used = claimed_income
        elif case_type == "omitted_obligation":
            obligations_used -= omittable_debt
        elif case_type == "incorrect_payment":
            obligations_used -= max(1, proposed_payment // 4)
        elif case_type == "cross_application":
            income_application_id = alternate_application_id
            obligations_application_id = alternate_application_id
            loan_terms_application_id = alternate_application_id
        elif case_type == "unverified_income_record":
            income_third_party_verified = False
        elif case_type == "unverified_obligations_record":
            obligations_third_party_verified = False
        elif case_type == "product_mismatch":
            standard_product_id = 99
            standard_max_dti = 0.55
        elif case_type == "income_application_mismatch":
            income_application_id = alternate_application_id
        elif case_type == "obligations_application_mismatch":
            obligations_application_id = alternate_application_id
        elif case_type == "loan_terms_application_mismatch":
            loan_terms_application_id = alternate_application_id
        elif case_type == "nonpositive_income":
            income_used = 0
        elif case_type == "missing_income_record":
            income_application_id = alternate_application_id
            income_evidence_present = False
            income_join = "LEFT JOIN"
        elif case_type == "application_mismatch":
            application_source_ids = (alternate_application_id,)

        candidate_dti = obligations_used / income_used if income_used else 0.0
        if case_type == "incorrect_dti":
            candidate_dti += 0.02
        sql = _build_update_sql(
            application_id=application_id,
            application_source_ids=application_source_ids,
            income_application_id=income_application_id,
            obligations_application_id=obligations_application_id,
            loan_terms_application_id=loan_terms_application_id,
            standard_product_id=standard_product_id,
            income_join=income_join,
            outcome=outcome,
            income_used=income_used,
            obligations_used=obligations_used,
            dti=candidate_dti,
        )
        cases.append(
            UnderwritingCase(
                application_id=application_id,
                borrower_id=borrower_id,
                product_id=product_id,
                verified_monthly_income=verified_income,
                claimed_monthly_income=claimed_income,
                existing_monthly_debt=existing_debt,
                omittable_monthly_debt=omittable_debt,
                proposed_payment=proposed_payment,
                mortgage_related_payment=mortgage_payment,
                simultaneous_loan_payment=simultaneous_payment,
                max_dti=standard_max_dti,
                candidate_outcome=outcome,
                candidate_income_used=income_used,
                candidate_obligations_used=obligations_used,
                candidate_dti=candidate_dti,
                application_source_ids=application_source_ids,
                income_application_id=income_application_id,
                obligations_application_id=obligations_application_id,
                loan_terms_application_id=loan_terms_application_id,
                standard_product_id=standard_product_id,
                income_third_party_verified=income_third_party_verified,
                obligations_third_party_verified=obligations_third_party_verified,
                income_evidence_present=income_evidence_present,
                case_type=case_type,
                is_valid=is_valid,
                sql=sql,
            )
        )
    return cases


def build_policy_prompt(case: UnderwritingCase, nonce: str) -> str:
    """Build a nonce-first, self-contained policy-validation prompt."""
    income_row = None
    if case.income_evidence_present:
        income_row = {
            "application_id": case.income_application_id,
            "verified_monthly_income": case.verified_monthly_income,
            "third_party_verified": case.income_third_party_verified,
        }
    context = {
        "target_decision": {
            "application_id": case.application_id,
        },
        "application_rows_used_by_query": [
            {
                "application_id": application_source_id,
                "borrower_id": case.borrower_id,
                "product_id": case.product_id,
            }
            for application_source_id in case.application_source_ids
        ],
        "verified_income_row_used_by_query": income_row,
        "applicant_claim": {
            "application_id": case.application_id,
            "claimed_monthly_income": case.claimed_monthly_income,
        },
        "verified_obligations_row_used_by_query": {
            "application_id": case.obligations_application_id,
            "existing_monthly_debt": case.existing_monthly_debt,
            "third_party_verified": case.obligations_third_party_verified,
        },
        "loan_terms_row_used_by_query": {
            "application_id": case.loan_terms_application_id,
            "proposed_payment": case.proposed_payment,
            "mortgage_related_payment": case.mortgage_related_payment,
            "simultaneous_loan_payment": case.simultaneous_loan_payment,
        },
        "underwriting_standard": {
            "product_id": case.standard_product_id,
            "max_dti": case.max_dti,
        },
        "target_application_reference": {
            "application_id": case.application_id,
            "borrower_id": case.borrower_id,
            "product_id": case.product_id,
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


def reset_underwriting_database(conn: Any, workload: list[UnderwritingCase]) -> None:
    """Create and populate the relational lending database for one approach."""
    for table in [
        "underwriting_decisions",
        "underwriting_standards",
        "loan_terms",
        "verified_obligations",
        "verified_income",
        "applications",
    ]:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.execute(
        "CREATE TABLE applications (application_id INTEGER, borrower_id INTEGER, product_id INTEGER)"
    )
    conn.execute(
        "CREATE TABLE verified_income (application_id INTEGER, "
        "verified_monthly_income INTEGER, third_party_verified BOOLEAN)"
    )
    conn.execute(
        "CREATE TABLE verified_obligations (application_id INTEGER, "
        "existing_monthly_debt INTEGER, third_party_verified BOOLEAN)"
    )
    conn.execute(
        "CREATE TABLE loan_terms (application_id INTEGER, proposed_payment INTEGER, "
        "mortgage_related_payment INTEGER, simultaneous_loan_payment INTEGER)"
    )
    conn.execute("CREATE TABLE underwriting_standards (product_id INTEGER, max_dti DOUBLE)")
    conn.execute(
        "CREATE TABLE underwriting_decisions (application_id INTEGER, outcome VARCHAR, "
        "income_used INTEGER, obligations_used INTEGER, dti DOUBLE)"
    )
    conn.executemany(
        "INSERT INTO underwriting_standards VALUES (?, ?)",
        [(1, 0.36), (2, 0.43), (3, 0.50), (99, 0.55)],
    )
    application_rows = [(c.application_id, c.borrower_id, c.product_id) for c in workload]
    application_rows.extend(
        (source_id, c.borrower_id, c.product_id)
        for c in workload
        for source_id in c.application_source_ids
        if source_id != c.application_id
    )
    conn.executemany(
        "INSERT INTO applications VALUES (?, ?, ?)",
        application_rows,
    )
    income_rows = [
        (
            c.application_id,
            c.verified_monthly_income,
            c.income_third_party_verified,
        )
        for c in workload
    ]
    income_rows.extend(
        (
            c.income_application_id,
            c.verified_monthly_income,
            c.income_third_party_verified,
        )
        for c in workload
        if c.income_application_id != c.application_id and c.income_evidence_present
    )
    conn.executemany(
        "INSERT INTO verified_income VALUES (?, ?, ?)",
        income_rows,
    )
    obligation_rows = [
        (
            c.application_id,
            c.existing_monthly_debt,
            c.obligations_third_party_verified,
        )
        for c in workload
    ]
    obligation_rows.extend(
        (
            c.obligations_application_id,
            c.existing_monthly_debt,
            c.obligations_third_party_verified,
        )
        for c in workload
        if c.obligations_application_id != c.application_id
    )
    conn.executemany(
        "INSERT INTO verified_obligations VALUES (?, ?, ?)",
        obligation_rows,
    )
    loan_term_rows = [
        (
            c.application_id,
            c.proposed_payment,
            c.mortgage_related_payment,
            c.simultaneous_loan_payment,
        )
        for c in workload
    ]
    loan_term_rows.extend(
        (
            c.loan_terms_application_id,
            c.proposed_payment,
            c.mortgage_related_payment,
            c.simultaneous_loan_payment,
        )
        for c in workload
        if c.loan_terms_application_id != c.application_id
    )
    conn.executemany(
        "INSERT INTO loan_terms VALUES (?, ?, ?, ?)",
        loan_term_rows,
    )
    conn.executemany(
        "INSERT INTO underwriting_decisions VALUES (?, 'PENDING', NULL, NULL, NULL)",
        [(c.application_id,) for c in workload],
    )


class RegulationZBenchmarkStrategy(ExperimentStrategy):
    """Compare DFC, four Bedrock guards, and no policy on underwriting updates."""

    def setup(self, context: ExperimentContext) -> None:
        self.num_applications = int(
            context.strategy_config.get("num_applications", DEFAULT_NUM_APPLICATIONS)
        )
        self.num_updates = int(context.strategy_config.get("num_updates", DEFAULT_NUM_UPDATES))
        self.max_concurrency = int(
            context.strategy_config.get("max_concurrency", DEFAULT_MAX_CONCURRENCY)
        )
        if not 1 <= self.max_concurrency <= DEFAULT_MAX_CONCURRENCY:
            raise ValueError(f"max_concurrency must be between 1 and {DEFAULT_MAX_CONCURRENCY}")
        self.workload = generate_underwriting_workload(
            num_applications=self.num_applications,
            num_updates=self.num_updates,
        )
        raw_specs = context.strategy_config.get("models", DEFAULT_BEDROCK_MODEL_SPECS)
        self.model_specs = [dict(spec) for spec in raw_specs]

        self.local_duckdb = _ensure_smokedduck()
        self.no_policy_conn = self.local_duckdb.connect(":memory:")
        self.dfc_conn = self.local_duckdb.connect(":memory:")
        self.model_connections = {
            str(spec["key"]): self.local_duckdb.connect(":memory:") for spec in self.model_specs
        }
        for conn in [self.no_policy_conn, self.dfc_conn, *self.model_connections.values()]:
            reset_underwriting_database(conn, self.workload)

        self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)
        self.dfc_rewriter.register_policy(build_regulation_z_policy())

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

    @staticmethod
    def _update_applied(conn: Any, case: UnderwritingCase) -> bool:
        outcome = conn.execute(
            "SELECT outcome FROM underwriting_decisions WHERE application_id = ?",
            [case.application_id],
        ).fetchone()[0]
        return outcome != "PENDING"

    def _run_no_policy(self, context: ExperimentContext) -> tuple[float, int]:
        reset_underwriting_database(self.no_policy_conn, self.workload)
        correct = 0
        elapsed_ms = 0.0
        for update_number, case in enumerate(self.workload, start=1):
            start = time.perf_counter()
            try:
                self.no_policy_conn.execute(case.sql)
            except Exception as error:
                case_ms = (time.perf_counter() - start) * 1000.0
                elapsed_ms += case_ms
                self._write_progress(
                    context,
                    approach="no_policy",
                    update_number=update_number,
                    application_id=case.application_id,
                    case_type=case.case_type,
                    expected_allow=case.is_valid,
                    elapsed_ms=case_ms,
                    sql_execution_ms=case_ms,
                    error=str(error),
                )
                raise
            case_ms = (time.perf_counter() - start) * 1000.0
            elapsed_ms += case_ms
            actual_allow = self._update_applied(self.no_policy_conn, case)
            is_correct = actual_allow == case.is_valid
            if is_correct:
                correct += 1
            self._write_progress(
                context,
                approach="no_policy",
                update_number=update_number,
                application_id=case.application_id,
                case_type=case.case_type,
                expected_allow=case.is_valid,
                actual_allow=actual_allow,
                correct=is_correct,
                elapsed_ms=case_ms,
                sql_execution_ms=case_ms,
            )
        return elapsed_ms, correct

    def _run_dfc(self, context: ExperimentContext) -> tuple[float, float, float, int]:
        reset_underwriting_database(self.dfc_conn, self.workload)
        rewrite_ms = 0.0
        execution_ms = 0.0
        correct = 0
        for update_number, case in enumerate(self.workload, start=1):
            rewrite_start = time.perf_counter()
            try:
                transformed = self.dfc_rewriter.transform_query(case.sql)
            except Exception as error:
                case_rewrite_ms = (time.perf_counter() - rewrite_start) * 1000.0
                rewrite_ms += case_rewrite_ms
                self._write_progress(
                    context,
                    approach="dfc",
                    update_number=update_number,
                    application_id=case.application_id,
                    case_type=case.case_type,
                    expected_allow=case.is_valid,
                    elapsed_ms=case_rewrite_ms,
                    rewrite_ms=case_rewrite_ms,
                    error=str(error),
                )
                raise
            case_rewrite_ms = (time.perf_counter() - rewrite_start) * 1000.0
            rewrite_ms += case_rewrite_ms
            execution_start = time.perf_counter()
            try:
                self.dfc_conn.execute(transformed)
            except Exception as error:
                case_execution_ms = (time.perf_counter() - execution_start) * 1000.0
                execution_ms += case_execution_ms
                self._write_progress(
                    context,
                    approach="dfc",
                    update_number=update_number,
                    application_id=case.application_id,
                    case_type=case.case_type,
                    expected_allow=case.is_valid,
                    elapsed_ms=case_rewrite_ms + case_execution_ms,
                    rewrite_ms=case_rewrite_ms,
                    sql_execution_ms=case_execution_ms,
                    error=str(error),
                )
                raise
            case_execution_ms = (time.perf_counter() - execution_start) * 1000.0
            execution_ms += case_execution_ms
            actual_allow = self._update_applied(self.dfc_conn, case)
            is_correct = actual_allow == case.is_valid
            if is_correct:
                correct += 1
            self._write_progress(
                context,
                approach="dfc",
                update_number=update_number,
                application_id=case.application_id,
                case_type=case.case_type,
                expected_allow=case.is_valid,
                actual_allow=actual_allow,
                correct=is_correct,
                elapsed_ms=case_rewrite_ms + case_execution_ms,
                rewrite_ms=case_rewrite_ms,
                sql_execution_ms=case_execution_ms,
            )
        return rewrite_ms + execution_ms, rewrite_ms, execution_ms, correct

    def _run_model(
        self,
        context: ExperimentContext,
        model_key: str,
        model_id: str,
    ) -> dict[str, int | float]:
        conn = self.model_connections[model_key]
        reset_underwriting_database(conn, self.workload)
        guard = self.guards[model_key]
        total_ms = 0.0
        sql_execution_ms = 0.0
        correct = 0
        false_allows = 0
        false_blocks = 0
        parse_failures = 0
        input_tokens = 0
        output_tokens = 0
        cache_read_tokens = 0
        cache_write_tokens = 0

        workload_iterator = iter(enumerate(self.workload, start=1))
        max_workers = min(self.max_concurrency, len(self.workload))
        wall_start = time.perf_counter()
        pending: dict[Future[BedrockGuardResponse], tuple[int, UnderwritingCase]] = {}

        def submit_next(executor: ThreadPoolExecutor) -> bool:
            try:
                update_number, case = next(workload_iterator)
            except StopIteration:
                return False
            nonce = str(uuid.uuid4())
            prompt = build_policy_prompt(case, nonce)
            pending[executor.submit(guard.invoke, prompt)] = (update_number, case)
            return True

        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=f"regulation-z-{model_key}",
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
                            application_id=case.application_id,
                            case_type=case.case_type,
                            expected_allow=case.is_valid,
                            error=str(error),
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
                            sql_execution_ms += case_sql_execution_ms
                            self._write_progress(
                                context,
                                approach=model_key,
                                model_id=model_id,
                                update_number=update_number,
                                application_id=case.application_id,
                                case_type=case.case_type,
                                expected_allow=case.is_valid,
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
                            )
                            for pending_future in pending:
                                pending_future.cancel()
                            raise
                        case_sql_execution_ms = (time.perf_counter() - execution_start) * 1000.0
                        sql_execution_ms += case_sql_execution_ms
                    is_correct = allow_update == case.is_valid
                    if is_correct:
                        correct += 1
                    elif allow_update:
                        false_allows += 1
                    else:
                        false_blocks += 1
                    self._write_progress(
                        context,
                        approach=model_key,
                        model_id=model_id,
                        update_number=update_number,
                        application_id=case.application_id,
                        case_type=case.case_type,
                        expected_allow=case.is_valid,
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
                    )
                    submit_next(executor)

        wall_time_ms = (time.perf_counter() - wall_start) * 1000.0

        return {
            "time_ms": total_ms + sql_execution_ms,
            "llm_time_ms": total_ms,
            "wall_time_ms": wall_time_ms,
            "sql_execution_ms": sql_execution_ms,
            "correct": correct,
            "false_allows": false_allows,
            "false_blocks": false_blocks,
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
            "num_applications": self.num_applications,
            "num_updates": self.num_updates,
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
            metrics = self._run_model(context, key, str(spec["model_id"]))
            duration_ms += float(metrics["wall_time_ms"])
            custom_metrics[f"{key}_display_name"] = spec["display_name"]
            custom_metrics[f"{key}_model_id"] = spec["model_id"]
            for metric_name, value in metrics.items():
                custom_metrics[f"{key}_{metric_name}"] = value
            custom_metrics[f"{key}_accuracy"] = int(metrics["correct"]) / self.num_updates
            custom_metrics[f"{key}_cache_contaminated"] = bool(
                metrics["cache_read_input_tokens"] or metrics["cache_write_input_tokens"]
            )
        return ExperimentResult(duration_ms=duration_ms, custom_metrics=custom_metrics)

    def teardown(self, _context: ExperimentContext) -> None:
        for conn in [self.no_policy_conn, self.dfc_conn, *self.model_connections.values()]:
            conn.close()
        if self.progress_file is not None:
            self.progress_file.close()
