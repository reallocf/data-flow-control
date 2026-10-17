from __future__ import annotations

import csv
from io import BytesIO
import json
from typing import TYPE_CHECKING

import duckdb
from experiment_harness import ExperimentContext
import pytest
from sql_rewriter import SQLRewriter

from scripts.run_regulation_z_v2_rust_dfc import _rust_constraint

if TYPE_CHECKING:
    from pathlib import Path

from vldb_experiments.strategies.regulation_z_statement_benchmark import (
    DEFAULT_CATALOG_PATH,
    DEFAULT_V2_BEDROCK_MODEL_SPECS,
    EVIDENCE_CONVENTIONS,
    FULL_DATABASE_EVIDENCE_CONVENTIONS,
    INVALID_CASE_TYPES,
    POLICY_TEXT,
    STATEMENT_POLICY_SPECS,
    RegulationZV2BenchmarkStrategy,
    build_full_database_statement_policy_prompt,
    build_regulation_z_statement_policies,
    build_statement_policy_prompt,
    generate_statement_workload,
    reset_statement_database,
)
from vldb_experiments.strategies.regulation_z_v2_benchmark import (
    OPEN_WEIGHT_V2_BEDROCK_MODEL_SPECS,
)
from vldb_experiments.strategies.regulation_z_v2_data import (
    ProductTerm,
    load_product_terms,
    write_product_terms,
)


def test_packaged_catalog_is_public_pseudonymized_data() -> None:
    terms = load_product_terms(DEFAULT_CATALOG_PATH)

    assert len(terms) == 24
    assert all(term.source_record_id.startswith("tccp-") for term in terms)
    assert all(term.product_name.startswith("Public Card Product ") for term in terms)
    assert all(term.source_url.startswith("https://files.consumerfinance.gov/") for term in terms)
    assert min(term.purchase_apr for term in terms) == 3.99
    assert max(term.purchase_apr for term in terms) == 35.99


def test_default_models_include_project_specific_bedrock_profiles() -> None:
    specs = {spec["key"]: spec for spec in DEFAULT_V2_BEDROCK_MODEL_SPECS}

    assert list(specs) == [
        "astra",
        "sol_6_1",
        "luna",
        "opus_5_5",
        "fable_5_1",
        "haiku_5_5",
    ]
    assert specs["luna"] == {
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
    assert specs["haiku_5_5"] == {
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


def test_open_weight_models_only_nonce_implicit_cache_models() -> None:
    specs = {spec["key"]: spec for spec in OPEN_WEIGHT_V2_BEDROCK_MODEL_SPECS}

    assert list(specs) == [
        "minimax_m2_5",
        "gpt_oss_120b",
        "kimi_k3",
        "glm_5_3",
        "deepseek_v3_2",
        "qwen3_coder_480b",
    ]
    assert {key for key, spec in specs.items() if spec.get("prompt_nonce", False)} == {
        "kimi_k3",
        "glm_5_3",
    }


def test_normalized_catalog_round_trip(tmp_path: Path) -> None:
    original = load_product_terms(DEFAULT_CATALOG_PATH)[:2]
    output = tmp_path / "catalog.csv"

    write_product_terms(output, original)

    assert load_product_terms(output) == original


def test_catalog_rejects_fractional_apr_scale(tmp_path: Path) -> None:
    output = tmp_path / "bad.csv"
    with output.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=[
                "source_record_id",
                "source_row",
                "product_code",
                "product_name",
                "purchase_apr",
                "rate_variable",
                "index_name",
                "report_date",
                "source_url",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "source_record_id": "tccp-test",
                "source_row": 1,
                "product_code": "RZP001",
                "product_name": "Public Card Product 001",
                "purchase_apr": 0.0,
                "rate_variable": "false",
                "index_name": "",
                "report_date": "2025-12-31",
                "source_url": "https://example.com/source.xlsx",
            }
        )

    with pytest.raises(
        ValueError,
        match=r"^purchase_apr must be expressed in percentage points: 0\.0$",
    ):
        load_product_terms(output)


def test_workload_is_balanced_and_covers_each_failure_mode() -> None:
    terms = load_product_terms(DEFAULT_CATALOG_PATH)
    size = len(INVALID_CASE_TYPES) * 2

    cases = generate_statement_workload(terms, num_accounts=size, num_updates=size)

    assert sum(case.is_valid for case in cases) == size // 2
    assert sum(not case.is_valid for case in cases) == size // 2
    assert {case.case_type for case in cases} == {
        "valid_statement",
        *INVALID_CASE_TYPES,
    }


def test_workload_supports_one_input_without_losing_public_provenance() -> None:
    term = load_product_terms(DEFAULT_CATALOG_PATH)[0]

    cases = generate_statement_workload([term], num_accounts=1, num_updates=1)

    assert len(cases) == 1
    assert cases[0].is_valid is True
    assert cases[0].source_record_id == term.source_record_id
    assert cases[0].purchase_apr == term.purchase_apr


def test_no_policy_publishes_every_candidate_statement() -> None:
    terms = load_product_terms(DEFAULT_CATALOG_PATH)
    cases = generate_statement_workload(terms, num_accounts=50, num_updates=50)
    conn = duckdb.connect(":memory:")
    reset_statement_database(conn, cases)

    for case in cases:
        conn.execute(case.sql)

    statuses = conn.execute(
        "SELECT status FROM statement_publications ORDER BY publication_id"
    ).fetchall()
    assert [row[0] for row in statuses] == ["published"] * len(cases)
    conn.close()


def test_policies_are_small_independently_named_controls() -> None:
    policies = build_regulation_z_statement_policies()

    assert [policy.description for policy in policies] == [
        description for description, _, _ in STATEMENT_POLICY_SPECS
    ]
    assert [policy.constraint for policy in policies] == [
        constraint for _, _, constraint in STATEMENT_POLICY_SPECS
    ]
    assert all(statement in POLICY_TEXT for _, statement, _ in STATEMENT_POLICY_SPECS)
    assert all(policy.sources == ["statement_reconciliation"] for policy in policies)
    assert all(policy.sink == "statement_publications" for policy in policies)
    assert all(policy.sink_alias == "pub" for policy in policies)
    assert all(policy.on_fail.value == "REMOVE" for policy in policies)


def test_rust_policy_removes_legacy_source_aggregation() -> None:
    policy = build_regulation_z_statement_policies()[0]

    assert _rust_constraint(policy.constraint) == (
        "statement_reconciliation.statement_id = pub.statement_id AND "
        "statement_reconciliation.account_id = pub.account_id AND "
        "pub.status = 'published'"
    )


def test_dfc_policy_matches_oracle_for_every_failure_mode() -> None:
    terms = load_product_terms(DEFAULT_CATALOG_PATH)
    size = len(INVALID_CASE_TYPES) * 2
    cases = generate_statement_workload(terms, num_accounts=size, num_updates=size)
    conn = duckdb.connect(":memory:")
    reset_statement_database(conn, cases)
    rewriter = SQLRewriter(conn=conn)
    for policy in build_regulation_z_statement_policies():
        rewriter.register_policy(policy)

    actual = []
    for case in cases:
        conn.execute(rewriter.transform_query(case.sql))
        status = conn.execute(
            "SELECT status FROM statement_publications WHERE publication_id = ?",
            [case.publication_id],
        ).fetchone()[0]
        actual.append(status == "published")

    assert actual == [case.is_valid for case in cases]
    conn.close()


def test_prompt_is_complete_and_explicit_about_reconciliation_conventions() -> None:
    term = load_product_terms(DEFAULT_CATALOG_PATH)[0]
    case = generate_statement_workload([term], num_accounts=1, num_updates=1)[0]

    prompt = build_statement_policy_prompt(case)

    assert prompt.startswith("Policy:\n")
    assert "nonce" not in prompt.lower()
    assert POLICY_TEXT in prompt
    assert EVIDENCE_CONVENTIONS in prompt
    assert case.sql in prompt
    assert '"public_product_provenance"' not in prompt
    context_text = prompt.split("Relevant database rows:\n", maxsplit=1)[1].split(
        "\n\nDaily purchase balances", maxsplit=1
    )[0]
    context = json.loads(context_text)
    assert context["statement_publication"] == {
        "account_id": case.account_id,
        "publication_id": case.publication_id,
        "published_at": None,
        "statement_id": case.statement_id,
        "status": "draft",
    }
    assert len(context["transactions"]) == 6
    assert all(row["account_id"] == case.account_id for row in context["transactions"])
    assert all(row["statement_id"] == case.statement_id for row in context["transactions"])
    daily_table = prompt.split("balance_category=purchase:\n", maxsplit=1)[1].split(
        "\n\nShould", maxsplit=1
    )[0]
    daily_lines = daily_table.splitlines()
    assert daily_lines[0] == ("balance_date\tdaily_balance\tapr_applied\tdaily_periodic_rate")
    assert len(daily_lines[1:]) == 30
    assert all(len(line.split("\t")) == 4 for line in daily_lines[1:])
    assert f"account_id={case.account_id}" in prompt
    assert f"statement_id={case.statement_id}" in prompt
    assert "negative of the sum" in POLICY_TEXT
    assert "daily_periodic_rate / 100" in POLICY_TEXT
    assert "all and only" in EVIDENCE_CONVENTIONS
    assert "effective for this product throughout" in EVIDENCE_CONVENTIONS


def test_full_database_prompt_contains_all_rows_and_shuffles_each_table() -> None:
    terms = load_product_terms(DEFAULT_CATALOG_PATH)
    database_workload = generate_statement_workload(
        terms,
        num_accounts=100,
        num_updates=100,
    )
    case = database_workload[0]

    prompt = build_full_database_statement_policy_prompt(
        case,
        database_workload,
        shuffle_seed=42,
    )
    repeated_prompt = build_full_database_statement_policy_prompt(
        case,
        database_workload,
        shuffle_seed=42,
    )
    other_seed_prompt = build_full_database_statement_policy_prompt(
        case,
        database_workload,
        shuffle_seed=43,
    )

    assert prompt == repeated_prompt
    assert prompt != other_seed_prompt
    assert FULL_DATABASE_EVIDENCE_CONVENTIONS in prompt
    assert prompt.count("\nTABLE ") == 8
    assert prompt.count("\nA0000001\tSTM00000001\t") == 30
    assert prompt.count("\tA0000001\tSTM00000001\t") == 6
    assert all(database_case.publication_id in prompt for database_case in database_workload)
    daily_section = prompt.split("TABLE daily_balances\n", maxsplit=1)[1].split(
        "\n\nTABLE statement_publications", maxsplit=1
    )[0]
    relevant_positions = [
        index
        for index, line in enumerate(daily_section.splitlines()[1:])
        if line.startswith(f"{case.account_id}\t{case.statement_id}\t")
    ]
    assert len(relevant_positions) == 30
    assert relevant_positions != list(
        range(relevant_positions[0], relevant_positions[0] + len(relevant_positions))
    )


def test_strategy_only_prepends_nonce_when_requested() -> None:
    case = generate_statement_workload(
        load_product_terms(DEFAULT_CATALOG_PATH),
        num_accounts=1,
        num_updates=1,
    )[0]
    strategy = RegulationZV2BenchmarkStrategy()
    strategy.prompt_scope = "relevant_rows"

    assert strategy._build_prompt(case, "nonce-123").startswith("Run nonce: nonce-123\nPolicy:\n")
    assert strategy._build_prompt(case, "").startswith("Policy:\n")


def test_strategy_full_database_mode_materializes_all_accounts() -> None:
    class AlwaysBlockClient:
        def invoke_model(self, **request):
            body = json.loads(request["body"])
            prompt = body["messages"][0]["content"][1]["text"]
            assert "Complete input database snapshot" in prompt
            assert "PUB00000004" in prompt
            return {
                "body": BytesIO(
                    json.dumps(
                        {
                            "choices": [{"message": {"content": '{"allow_update": false}'}}],
                            "usage": {"input_tokens": 10, "output_tokens": 5},
                        }
                    ).encode("utf-8")
                ),
                "ResponseMetadata": {"RequestId": "fake-request"},
            }

    context = ExperimentContext(
        execution_number=1,
        strategy_config={
            "num_accounts": 4,
            "num_updates": 1,
            "catalog_path": str(DEFAULT_CATALOG_PATH),
            "bedrock_client": AlwaysBlockClient(),
            "prompt_scope": "full_database",
            "models": [
                {
                    "key": "astra",
                    "display_name": "Astra 6",
                    "model_id": "fake-profile",
                    "bedrock_provider": "openai",
                }
            ],
        },
    )
    strategy = RegulationZV2BenchmarkStrategy()
    strategy.setup(context)
    try:
        result = strategy.execute(context)
    finally:
        strategy.teardown(context)

    assert len(strategy.database_workload) == 4
    assert len(strategy.workload) == 1
    assert result.custom_metrics["num_accounts"] == 4
    assert result.custom_metrics["num_updates"] == 1


def test_product_term_validation_accepts_percentage_points(tmp_path: Path) -> None:
    term = ProductTerm(
        source_record_id="tccp-test",
        source_row=12,
        product_code="RZP001",
        product_name="Public Card Product 001",
        purchase_apr=19.99,
        rate_variable=True,
        index_name="Prime",
        report_date=load_product_terms(DEFAULT_CATALOG_PATH)[0].report_date,
        source_url="https://example.com/source.xlsx",
    )
    output = tmp_path / "catalog.csv"

    write_product_terms(output, [term])

    assert load_product_terms(output) == [term]


def test_strategy_executes_all_paths_with_a_fake_bedrock_client(tmp_path: Path) -> None:
    class AlwaysBlockClient:
        def invoke_model(self, **request):
            body = json.loads(request["body"])
            content = body["messages"][0]["content"]
            assert content[0] == {
                "type": "text",
                "text": "Evaluate this independent benchmark case.\n",
                "prompt_cache_breakpoint": {"mode": "explicit"},
            }
            prompt = content[1]["text"]
            assert prompt.startswith("Policy:\n")
            assert body["prompt_cache_options"] == {"mode": "explicit"}
            return {
                "body": BytesIO(
                    json.dumps(
                        {
                            "choices": [{"message": {"content": '{"allow_update": false}'}}],
                            "usage": {
                                "input_tokens": 10,
                                "output_tokens": 5,
                                "input_tokens_details": {
                                    "cached_tokens": 0,
                                    "cache_write_tokens": 0,
                                },
                            },
                        }
                    ).encode("utf-8")
                ),
                "ResponseMetadata": {"RequestId": "fake-request"},
            }

    progress_path = tmp_path / "progress.csv"
    context = ExperimentContext(
        execution_number=1,
        strategy_config={
            "num_accounts": 4,
            "num_updates": 4,
            "catalog_path": str(DEFAULT_CATALOG_PATH),
            "max_concurrency": 4,
            "progress_path": str(progress_path),
            "bedrock_client": AlwaysBlockClient(),
            "models": [
                {
                    "key": "astra",
                    "display_name": "Astra 6",
                    "model_id": "fake-profile",
                    "bedrock_provider": "openai",
                }
            ],
        },
    )
    strategy = RegulationZV2BenchmarkStrategy()
    strategy.setup(context)
    try:
        result = strategy.execute(context)
    finally:
        strategy.teardown(context)

    assert result.custom_metrics["dfc_correct"] == 4
    assert result.custom_metrics["no_policy_correct"] == 2
    assert result.custom_metrics["astra_correct"] == 2
    assert result.custom_metrics["astra_false_positives"] == 2
    assert result.custom_metrics["astra_false_negatives"] == 0
    with progress_path.open(encoding="utf-8", newline="") as progress_file:
        rows = list(csv.DictReader(progress_file))
    assert len(rows) == 12
    assert {row["approach"] for row in rows} == {"no_policy", "dfc", "astra"}
