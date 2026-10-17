from __future__ import annotations

from collections import Counter
import csv
from io import BytesIO
import json
import threading
from typing import TYPE_CHECKING

from data_flow_control import Resolution
from data_flow_control.adapters.duckdb import DuckDBAdapter
from data_flow_control.connection import Connection
import duckdb
from experiment_harness import ExperimentContext
import pytest

from vldb_experiments.strategies.regulation_z_53_full_policy_catalog import (
    FULL_REGULATION_Z_53_POLICY_SPECS,
)
from vldb_experiments.strategies.regulation_z_v3_evidence import EVIDENCE_TABLES

if TYPE_CHECKING:
    from pathlib import Path

from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
    DEFAULT_V3_BEDROCK_MODEL_SPECS,
    MATERIALIZED_VIEW_NAMES,
    POLICY_TEXT,
    RegulationZV3BenchmarkStrategy,
    build_payment_allocation_prompt,
    build_regulation_z_53_policies,
    generate_payment_allocation_workload,
    reset_payment_allocation_database,
    selected_balance_transaction,
)


def test_default_models_exclude_unavailable_haiku_profile() -> None:
    assert [spec["key"] for spec in DEFAULT_V3_BEDROCK_MODEL_SPECS] == [
        "astra",
        "sol_6_1",
        "luna",
        "opus_5_5",
        "fable_5_1",
    ]


def test_shared_pool_interleaves_models_and_caps_inflight_requests(tmp_path: Path) -> None:
    from vldb_experiments.strategies.regulation_z_benchmark import BedrockGuardResponse

    barrier = threading.Barrier(2)
    lock = threading.Lock()
    active = set()
    batches = []

    class Guard:
        def __init__(self, key):
            self.key = key

        def invoke(self, _prompt):
            with lock:
                active.add(self.key)
            barrier.wait(timeout=10)
            with lock:
                batches.append(set(active))
            barrier.wait(timeout=10)
            with lock:
                active.remove(self.key)
            barrier.wait(timeout=10)
            return BedrockGuardResponse(
                allow_update=False,
                raw_response='{"allow_update": false, "reason": "test"}',
                runtime_ms=1,
                input_tokens=1,
                output_tokens=1,
                cache_read_input_tokens=0,
                cache_write_input_tokens=0,
                request_id="test",
            )

    context = ExperimentContext(
        execution_number=1,
        strategy_config={
            "num_accounts": 4,
            "num_updates": 4,
            "max_concurrency": 2,
            "bedrock_client": object(),
            "progress_path": str(tmp_path / "parallel.csv"),
            "models": [{"key": k, "model_id": k, "display_name": k} for k in ("a", "b")],
        },
    )
    strategy = RegulationZV3BenchmarkStrategy()
    strategy.setup(context)
    strategy.guards = {k: Guard(k) for k in ("a", "b")}
    try:
        result, _ = strategy._run_models_parallel(context)
    finally:
        strategy.teardown(context)
    assert batches == [{"a", "b"}] * 8
    assert {k: v["input_tokens"] for k, v in result.items()} == {"a": 4, "b": 4}
    with (tmp_path / "parallel.csv").open() as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 8
    assert all(row["raw_response"] == '{"allow_update": false, "reason": "test"}' for row in rows)


def test_workload_is_balanced_in_every_complete_pair() -> None:
    workload = generate_payment_allocation_workload(
        num_accounts=10,
        num_updates=10,
        workload_seed=17,
    )

    assert sum(case.is_valid for case in workload) == 5
    assert sum(not case.is_valid for case in workload) == 5
    for pair_start in range(0, len(workload), 2):
        assert {case.is_valid for case in workload[pair_start : pair_start + 2]} == {
            False,
            True,
        }


@pytest.mark.parametrize("seed", [17, 23, 20261012])
def test_workload_balances_selected_identity_and_validity(seed) -> None:
    cases = generate_payment_allocation_workload(
        num_accounts=100,
        num_updates=100,
        workload_seed=seed,
    )
    assert Counter((c.expected_selected_transaction_id[-1], c.is_valid) for c in cases) == {
        ("A", True): 25,
        ("A", False): 25,
        ("B", True): 25,
        ("B", False): 25,
    }
    for start in range(0, 100, 4):
        assert {
            (c.expected_selected_transaction_id[-1], c.is_valid) for c in cases[start : start + 4]
        } == {
            ("A", True),
            ("A", False),
            ("B", True),
            ("B", False),
        }
    assert cases[:10] == generate_payment_allocation_workload(
        num_accounts=100,
        num_updates=10,
        workload_seed=seed,
    )


@pytest.mark.parametrize("decision", [False, None])
def test_parallel_failures_do_not_discard_other_results_and_skips_are_honored(
    tmp_path: Path,
    decision: bool | None,
) -> None:
    from vldb_experiments.strategies.regulation_z_benchmark import BedrockGuardResponse

    class Guard:
        def __init__(self, fail):
            self.fail = fail

        def invoke(self, _prompt):
            if self.fail:
                raise TimeoutError("test timeout")
            return BedrockGuardResponse(decision, "test response", 1, 1, 1, 0, 0, "ok")

    path = tmp_path / "failure.csv"
    context = ExperimentContext(
        execution_number=1,
        strategy_config={
            "num_accounts": 2,
            "num_updates": 2,
            "max_concurrency": 2,
            "bedrock_client": object(),
            "progress_path": str(path),
            "models": [{"key": k, "model_id": k, "display_name": k} for k in ("bad", "good")],
            "skip_model_updates": {"good": [1]},
        },
    )
    strategy = RegulationZV3BenchmarkStrategy()
    strategy.setup(context)
    strategy.guards = {"bad": Guard(True), "good": Guard(False)}
    try:
        result, _ = strategy._run_models_parallel(context)
    finally:
        strategy.teardown(context)
    assert result["bad"]["api_errors"] == 2
    assert result["good"]["completed_requests"] == 1
    assert result["good"]["parse_failures"] == int(decision is None)
    if decision is None:
        assert result["good"]["correct"] == 0
        assert result["good"]["false_positives"] == 0
        assert result["good"]["false_negatives"] == 0
    with path.open() as handle:
        rows = list(csv.DictReader(handle))
    assert sorted((r["approach"], r["update_number"], r["error"]) for r in rows) == [
        ("bad", "1", "test timeout"),
        ("bad", "2", "test timeout"),
        ("good", "2", ""),
    ]


def test_workload_satisfies_unenforced_allocation_invariants_by_construction() -> None:
    workload = generate_payment_allocation_workload(
        num_accounts=10,
        num_updates=10,
        workload_seed=17,
    )

    for case in workload:
        excess = case.total_payment - case.minimum_payment_portion
        selected = next(
            transaction
            for transaction in case.transactions
            if transaction.transaction_id == case.expected_selected_transaction_id
        )
        assert excess == 100.0
        assert 0.0 <= excess <= selected.amount


def test_database_physical_order_matches_hidden_oracle() -> None:
    workload = generate_payment_allocation_workload(
        num_accounts=8,
        num_updates=8,
        workload_seed=23,
    )
    conn = duckdb.connect(":memory:")
    try:
        reset_payment_allocation_database(conn, workload)

        assert [selected_balance_transaction(conn, case) for case in workload] == [
            case.expected_selected_transaction_id for case in workload
        ]
        assert [
            selected_balance_transaction(conn, case) == case.undisputed_transaction_id
            for case in workload
        ] == [case.is_valid for case in workload]
    finally:
        conn.close()


def test_database_separates_base_relations_from_materialized_views() -> None:
    workload = generate_payment_allocation_workload(
        num_accounts=2,
        num_updates=2,
        workload_seed=23,
    )
    conn = duckdb.connect(":memory:")
    try:
        reset_payment_allocation_database(conn, workload)

        relation_names = {
            table_name
            for (table_name,) in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' ORDER BY table_name"
            ).fetchall()
        }
        assert relation_names == set(EVIDENCE_TABLES) | {
            "allocation_snapshot_context",
            "balance_components",
            "daily_balances",
            "payment_allocations",
            "statements",
            "transactions",
        }
        assert {
            "allocation_snapshot_context",
            "balance_components",
            "regz_context",
        } == MATERIALIZED_VIEW_NAMES
        assert relation_names - MATERIALIZED_VIEW_NAMES == (
            set(EVIDENCE_TABLES) - {"regz_context"}
        ) | {
            "daily_balances",
            "payment_allocations",
            "statements",
            "transactions",
        }
        assert conn.execute(
            "SELECT account_id, statement_id, snapshot_date, "
            "highest_effective_apr "
            "FROM allocation_snapshot_context ORDER BY account_id"
        ).fetchall() == [
            (case.account_id, case.statement_id, case.cycle_end, 25.0) for case in workload
        ]
    finally:
        conn.close()


def test_prompt_hides_insertion_order_without_warning_and_shows_identical_result() -> None:
    cases = generate_payment_allocation_workload(
        num_accounts=2,
        num_updates=2,
        workload_seed=5,
    )

    prompts = [build_payment_allocation_prompt(case) for case in cases]

    for prompt in prompts:
        assert prompt.rsplit("\n\n", 1)[-1] == (
            "Should this completed payment allocation be retained? "
            'Return JSON only: {"allow_update": true|false}\n'
        )

    assert all(prompt.startswith(POLICY_TEXT) for prompt in prompts)
    assert all("expected_selected_transaction_id" not in prompt for prompt in prompts)
    assert all("physical" not in prompt.lower() for prompt in prompts)
    assert all("sorted for presentation" not in prompt.lower() for prompt in prompts)
    assert all("dfc" not in prompt.lower() for prompt in prompts)
    assert all("equivalent policy" not in prompt.lower() for prompt in prompts)
    assert all('"billing_disputes"' not in prompt for prompt in prompts)
    assert all('"is_disputed": true' in prompt for prompt in prompts)
    assert all('"is_disputed": false' in prompt for prompt in prompts)
    assert all('"protected_balance_amounts"' not in prompt for prompt in prompts)
    assert all('"promo_config"' not in prompt for prompt in prompts)
    assert all('"deferred_interest_programs"' not in prompt for prompt in prompts)
    assert all('"deferred_interest_windows"' not in prompt for prompt in prompts)
    assert all('"payment_allocation_requests"' not in prompt for prompt in prompts)
    assert all('"accounts"' not in prompt for prompt in prompts)
    assert all('"days_after_cycle_end"' not in prompt for prompt in prompts)
    assert all('"snapshot_day_offset"' not in prompt for prompt in prompts)
    assert all(
        "Retain the completed payment allocation if and only if every applicable requirement"
        in prompt
        for prompt in prompts
    )
    assert all('"cash_advance_allocation": 100.0' in prompt for prompt in prompts)
    for prompt in prompts:
        evidence_text = prompt.split("Database evidence:\n", 1)[1].split(
            "\n\nShould this completed", 1
        )[0]
        transactions = json.loads(evidence_text)["transactions"]
        assert [row["transaction_id"] for row in transactions] == sorted(
            row["transaction_id"] for row in transactions
        )


def test_full_policy_set_binds_all_catalog_predicates() -> None:
    policies = build_regulation_z_53_policies()
    specs = [spec for spec in FULL_REGULATION_Z_53_POLICY_SPECS if spec.constraint]
    assert len(policies) == 31
    assert [policy.description for policy in policies] == [
        f"{spec.proposition_id}: {spec.name}" for spec in specs
    ]
    assert [policy.constraint for policy in policies] == [spec.constraint for spec in specs]
    assert all(policy.sink == "payment_allocations" for policy in policies)
    assert all(policy.sink_alias == "allocation" for policy in policies)
    assert all(policy.on_fail == Resolution.REMOVE for policy in policies)
    assert all(policy.sources == ["regz_context"] for policy in policies)


def test_rust_dfc_blocks_only_disputed_top_one_contributors() -> None:
    workload = generate_payment_allocation_workload(
        num_accounts=10,
        num_updates=10,
        workload_seed=29,
    )
    conn = duckdb.connect(":memory:")
    try:
        reset_payment_allocation_database(conn, workload)
        rewriter = Connection(DuckDBAdapter(conn))
        for policy in build_regulation_z_53_policies():
            rewriter.register_policy(policy)

        for case in workload:
            conn.execute(rewriter.transform_query(case.sql))

        actual = {
            allocation_id: float(amount) > 0.0
            for allocation_id, amount in conn.execute(
                "SELECT allocation_id, cash_advance_allocation "
                "FROM payment_allocations ORDER BY allocation_id"
            ).fetchall()
        }
        assert actual == {case.allocation_id: case.is_valid for case in workload}
    finally:
        conn.close()


@pytest.mark.parametrize("no_ties", [False, True])
def test_only_billing_dispute_policy_blocks_current_workload(no_ties: bool) -> None:
    workload = generate_payment_allocation_workload(
        no_ties=no_ties,
        num_accounts=10,
        num_updates=10,
        workload_seed=29,
    )
    expected_blocked = {case.allocation_id for case in workload if not case.is_valid}

    for policy in build_regulation_z_53_policies():
        conn = duckdb.connect(":memory:")
        try:
            reset_payment_allocation_database(conn, workload)
            rewriter = Connection(DuckDBAdapter(conn))
            rewriter.register_policy(policy)
            for case in workload:
                conn.execute(rewriter.transform_query(case.sql))
            blocked = {
                allocation_id
                for allocation_id, amount in conn.execute(
                    "SELECT allocation_id, cash_advance_allocation FROM payment_allocations"
                ).fetchall()
                if float(amount) == 0.0
            }
            assert blocked == (expected_blocked if policy.description.startswith("P17:") else set())
        finally:
            conn.close()


def test_no_policy_produces_same_visible_allocation_for_every_case() -> None:
    workload = generate_payment_allocation_workload(
        num_accounts=6,
        num_updates=6,
        workload_seed=31,
    )
    conn = duckdb.connect(":memory:")
    try:
        reset_payment_allocation_database(conn, workload)
        for case in workload:
            conn.execute(case.sql)

        assert conn.execute(
            "SELECT cash_advance_allocation FROM payment_allocations ORDER BY allocation_id"
        ).fetchall() == [(100.0,)] * len(workload)
    finally:
        conn.close()


def test_strategy_executes_all_paths_with_fake_bedrock(tmp_path: Path) -> None:
    class AlwaysBlockClient:
        def invoke_model(self, **request):
            body = json.loads(request["body"])
            prompt = body["messages"][0]["content"][1]["text"]
            assert prompt.startswith("Regulatory requirements for this benchmark:\n")
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
            "workload_seed": 37,
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
    strategy = RegulationZV3BenchmarkStrategy()
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
    assert all(row["expected_selected_transaction_id"] for row in rows)


def test_dfc_only_never_calls_models_or_baseline(tmp_path: Path) -> None:
    context = ExperimentContext(
        execution_number=1,
        strategy_config={
            "num_accounts": 4,
            "num_updates": 4,
            "dfc_only": True,
            "no_ties": True,
            "progress_path": str(tmp_path / "dfc.csv"),
        },
    )
    strategy = RegulationZV3BenchmarkStrategy()
    strategy.setup(context)
    try:
        assert strategy.model_specs == []

        def forbidden(*_args):
            raise AssertionError("DFC-only must not run the baseline")

        strategy._run_no_policy = forbidden
        result = strategy.execute(context)
        assert result.custom_metrics["no_ties"] is True
        assert result.custom_metrics["policy_count"] == 31
        assert result.custom_metrics["dfc_correct"] == 4
        assert "no_policy_correct" not in result.custom_metrics
    finally:
        strategy.teardown(context)
    with (tmp_path / "dfc.csv").open() as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 4
    assert {row["approach"] for row in rows} == {"dfc"}


def test_contexts_cover_both_candidates_and_rewrite_preserves_selection() -> None:
    cases = generate_payment_allocation_workload(num_accounts=2, num_updates=2)
    with duckdb.connect(":memory:") as conn:
        reset_payment_allocation_database(conn, cases)
        for case in cases:
            contexts = conn.execute(
                "SELECT c.batch_id, c.protected_reduction, c.active_programs, c.window_programs "
                "FROM regz_context c JOIN regz_proposals p USING(batch_id) WHERE p.payment_id = ? ORDER BY c.batch_id",
                [case.payment_transaction_id],
            ).fetchall()
            assert contexts == sorted(
                [
                    (case.disputed_transaction_id, 10000, 1, 0),
                    (case.undisputed_transaction_id, 0, 1, 0),
                ]
            )
        rewriter = Connection(DuckDBAdapter(conn))
        for policy in build_regulation_z_53_policies():
            rewriter.register_policy(policy)
        # Change actual execution order while retaining all precomputed contexts
        # and oracle labels. Decisions must follow selected source rows, not labels.
        rows = conn.execute(
            "SELECT * FROM transactions ORDER BY account_id, is_disputed DESC"
        ).fetchall()
        conn.execute("DELETE FROM transactions")
        conn.executemany("INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        for case in cases:
            assert selected_balance_transaction(conn, case) == case.disputed_transaction_id
            conn.execute(rewriter.transform_query(case.sql))
        assert conn.execute(
            "SELECT cash_advance_allocation FROM payment_allocations ORDER BY allocation_id"
        ).fetchall() == [(0,), (0,)]


def test_no_ties_has_unique_rates_in_data_prompt_and_both_insertion_orders() -> None:
    cases = generate_payment_allocation_workload(num_accounts=4, num_updates=4, no_ties=True)
    assert sum(case.is_valid for case in cases) == 2
    with duckdb.connect(":memory:") as conn:
        reset_payment_allocation_database(conn, cases)
        rewriter = Connection(DuckDBAdapter(conn))
        for policy in build_regulation_z_53_policies():
            rewriter.register_policy(policy)
        for reverse in (False, True):
            direction = "DESC" if reverse else "ASC"
            rows = conn.execute(
                f"SELECT * FROM transactions ORDER BY transaction_id {direction}"
            ).fetchall()
            conn.execute("DELETE FROM transactions")
            conn.executemany("INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
            conn.execute("UPDATE payment_allocations SET cash_advance_allocation = 0")
            for case in cases:
                assert (
                    selected_balance_transaction(conn, case)
                    == case.expected_selected_transaction_id
                )
                conn.execute(rewriter.transform_query(case.sql))
                assert RegulationZV3BenchmarkStrategy._update_applied(conn, case) == case.is_valid
        for case in cases:
            prompt = build_payment_allocation_prompt(case)
            evidence = json.loads(
                prompt.split("Database evidence:\n")[1].split("\n\nShould this completed")[0]
            )
            components = evidence["balance_components"]
            assert sorted(row["effective_apr"] for row in components) == [24.0, 25.0]
            selected = max(components, key=lambda row: row["effective_apr"])["component_id"]
            assert selected == case.expected_selected_transaction_id
            rates = {
                row["balance_category"]: row["apr_applied"] for row in evidence["daily_balances"]
            }
            assert rates == {"cash_advance": 25.0, "cash_advance_lower": 24.0}
            actual = conn.execute(
                "SELECT component_id, effective_apr FROM balance_components WHERE account_id = ? ORDER BY component_id",
                [case.account_id],
            ).fetchall()
            assert actual == [(row["component_id"], row["effective_apr"]) for row in components]


def test_ties_remain_default() -> None:
    default = generate_payment_allocation_workload(num_accounts=2, num_updates=2)
    assert default == generate_payment_allocation_workload(
        num_accounts=2, num_updates=2, no_ties=False
    )
    assert all(
        {row.apr_applied for row in case.transactions if row.transaction_type == "cash_advance"}
        == {25.0}
        for case in default
    )
