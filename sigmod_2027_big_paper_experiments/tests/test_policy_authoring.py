"""Offline harness tests: no model requests and no consumption of Codex allowance."""

from argparse import Namespace
import json
import os

from data_flow_control import Policy
import pytest

from policy_authoring.harness import (
    IDS,
    ROOT,
    build_prompts,
    container_command,
    grade,
    normalize_rows,
    parse_events,
    prepare_oracle,
    query_only,
    validate_submission,
    write_json,
)


def reference_submission(approach):
    policies = []
    for policy_id in IDS:
        pgn = (ROOT.parent / "results/regulation_z_dfc" / f"{policy_id}.pgn").read_text()
        predicate = Policy.from_pgn(pgn).constraint
        if approach == "dfc":
            code = pgn
        elif approach == "gprom":
            code = f"SELECT batch_id, CASE WHEN {predicate} THEN 1 ELSE 0 END AS passed FROM regz_context"
        else:
            code = f"SELECT batch_id, ({predicate}) IS TRUE AS passed FROM regz_context"
        policies.append({"id": policy_id, "code": code})
    return {"policies": policies}


@pytest.fixture(scope="module")
def oracle(tmp_path_factory):
    folder = tmp_path_factory.mktemp("oracle") / "snapshot"
    prepare_oracle(folder)
    return folder


def test_prompt_assembly_is_exact_and_does_not_embed_reference():
    build_prompts()
    for approach in ("dfc", "gprom", "provsql"):
        pieces = [
            "task.md",
            "specification.md",
            "evidence.md",
            "clarifications.md",
            f"{approach}.md",
        ]
        expected = (
            "\n\n".join((ROOT / "prompts" / part).read_text().strip() for part in pieces) + "\n"
        )
        assert (ROOT / "prompts" / f"exact-{approach}.txt").read_text() == expected
        assert expected.count("regz_context.protected_reduction <=") == 0


@pytest.mark.parametrize("approach", ["dfc", "gprom"])
def test_reference_roundtrip(oracle, tmp_path, approach):
    submission, output = tmp_path / "submission.json", tmp_path / "grade.json"
    write_json(submission, reference_submission(approach))
    grade(Namespace(approach=approach, submission=submission, oracle=oracle, output=output))
    report = json.loads(output.read_text())
    assert report["errors"] == []
    assert report["disagreements"] == []
    assert report["overall_disagreements"] == []
    assert report["reference_agreement"] is True
    assert report["oracle_status"] == "provisional_reference_not_legal_ground_truth"


def test_postgres_reference_roundtrip(oracle, tmp_path):
    if os.environ.get("POLICY_AUTHORING_DOCKER") != "1":
        pytest.skip("Set POLICY_AUTHORING_DOCKER=1 for an isolated PostgreSQL container")
    submission, output = tmp_path / "submission.json", tmp_path / "grade.json"
    write_json(submission, reference_submission("provsql"))
    grade(
        Namespace(
            approach="provsql",
            submission=submission,
            oracle=oracle,
            output=output,
            postgres_image="postgres:16",
        )
    )
    report = json.loads(output.read_text())
    assert report["errors"] == []
    assert report["disagreements"] == []
    assert report["reference_agreement"] is True


def test_wrong_policy_keeps_evidence_for_adjudication(oracle, tmp_path):
    value = reference_submission("gprom")
    next(p for p in value["policies"] if p["id"] == "P17")["code"] = (
        "SELECT batch_id, 1 AS passed FROM regz_context"
    )
    submission, output = tmp_path / "submission.json", tmp_path / "grade.json"
    write_json(submission, value)
    grade(Namespace(approach="gprom", submission=submission, oracle=oracle, output=output))
    report = json.loads(output.read_text())
    assert report["reference_agreement"] is False
    assert {row["policy_id"] for row in report["disagreements"]} == {"P17"}
    assert {row["classification"] for row in report["disagreements"]} == {"unresolved"}
    assert all(row["context"]["batch_id"] == row["case_id"] for row in report["disagreements"])
    assert bool(report["overall_disagreements"]) is True


@pytest.mark.parametrize(
    "sql",
    ["DROP TABLE regz_context", "SELECT 1; SELECT 2", "SELECT * INTO leaked FROM regz_context"],
)
def test_reject_nonquery_submission(sql):
    with pytest.raises(ValueError):
        query_only(sql, "postgres")


def test_reject_filtered_or_duplicate_results():
    with pytest.raises(ValueError):
        normalize_rows([("a", True)], ["a", "b"])
    with pytest.raises(ValueError):
        normalize_rows([("a", True), ("a", False)], ["a"])
    with pytest.raises(ValueError):
        normalize_rows([("a", None)], ["a"])


def test_strict_submission_contract():
    value = reference_submission("dfc")
    assert len(validate_submission(value)) == 31
    value["policies"].pop()
    with pytest.raises(ValueError):
        validate_submission(value)


def test_token_accounting_and_tool_rejection():
    events = [
        {"type": "item.completed", "item": {"type": "agent_message", "text": "{}"}},
        {
            "type": "turn.completed",
            "usage": {"input_tokens": 100, "cached_input_tokens": 20, "output_tokens": 30},
        },
    ]
    assert parse_events("\n".join(map(json.dumps, events))) == (
        {"input_tokens": 100, "cached_input_tokens": 20, "output_tokens": 30},
        "{}",
    )
    events.insert(0, {"type": "item.started", "item": {"type": "command_execution"}})
    with pytest.raises(ValueError, match="Tool activity invalidates this no-tools trial"):
        parse_events("\n".join(map(json.dumps, events)))


def test_no_host_mounts_or_credentials_in_command():
    command = container_command("image", "internal", "trial")
    assert set(command) & {"-v", "--mount", "--privileged", "--network=host"} == set()
    assert command[command.index("--network") + 1] == "internal"
    assert command[-1] == "image"


def test_oracle_rejects_overwrite_and_is_deterministic(oracle, tmp_path):
    with pytest.raises(FileExistsError):
        prepare_oracle(oracle)
    other = tmp_path / "other"
    prepare_oracle(other)
    assert (oracle / "suite.json").read_bytes() == (other / "suite.json").read_bytes()


def test_score_run_keeps_failed_trials_in_summary(oracle, tmp_path):
    import csv

    from policy_authoring.harness import score_run

    failed = tmp_path / "dfc-001"
    failed.mkdir()
    write_json(
        failed / "metadata.json",
        {
            "approach": "dfc",
            "trial": 1,
            "status": "timeout",
            "wall_seconds_including_cli_startup": 900,
        },
    )
    submitted = tmp_path / "gprom-001"
    submitted.mkdir()
    write_json(
        submitted / "metadata.json",
        {
            "approach": "gprom",
            "trial": 1,
            "status": "submitted",
            "wall_seconds_including_cli_startup": 10,
            "usage": {"input_tokens": 100, "output_tokens": 50},
        },
    )
    write_json(submitted / "submission.json", reference_submission("gprom"))
    score_run(Namespace(run=tmp_path, oracle=oracle, postgres_image="postgres:16"))
    with (tmp_path / "summary.csv").open() as stream:
        rows = list(csv.DictReader(stream))
    assert [(row["approach"], row["status"], row["reference_agreement"]) for row in rows] == [
        ("dfc", "timeout", "False"),
        ("gprom", "submitted", "True"),
    ]
    assert rows[0]["input_tokens"] == ""
    assert rows[1]["input_tokens"] == "100"


def test_every_reference_policy_has_passing_and_failing_probes(oracle):
    suite = json.loads((oracle / "suite.json").read_text())
    assert {pid: sorted(set(values.values())) for pid, values in suite["expected"].items()} == {
        pid: [False, True] for pid in IDS
    }


def test_default_generation_settings(monkeypatch):
    from policy_authoring import harness

    captured = []
    monkeypatch.setattr("sys.argv", ["harness", "generate", "/unused"])
    monkeypatch.setattr(harness, "generate", captured.append)
    harness.main()
    args = captured[0]
    assert (args.model, args.effort, args.trials, args.timeout, args.parallelism) == (
        "gpt-6-luna",
        None,
        5,
        3600,
        15,
    )


def test_entrypoint_uses_model_default_reasoning():
    from policy_authoring.container.entrypoint import build_command

    for payload in ({"model": "gpt-6-astra"}, {"model": "gpt-6-astra", "effort": None}):
        assert not any(arg.startswith("model_reasoning_effort=") for arg in build_command(payload))
    command = build_command({"model": "gpt-6-astra", "effort": "high"})
    assert command[command.index('model_reasoning_effort="high"') - 1] == "-c"


def test_all_fifteen_trials_launch_concurrently(tmp_path, monkeypatch):
    from contextlib import nullcontext
    import subprocess
    from threading import Barrier

    from policy_authoring import harness

    auth = tmp_path / "auth.json"
    auth.write_text(json.dumps({"tokens": {"test": "fake"}}))
    barrier = Barrier(15, timeout=10)
    submission = {"policies": [{"id": pid, "code": "test"} for pid in IDS]}
    events = "\n".join(
        json.dumps(event)
        for event in [
            {
                "type": "item.completed",
                "item": {"type": "agent_message", "text": json.dumps(submission)},
            },
            {"type": "turn.completed", "usage": {"input_tokens": 10, "output_tokens": 20}},
        ]
    )

    def fake_run(command, **kwargs):
        if command[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(command, 0, "fake-image", "")
        assert kwargs["timeout"] == 3600
        assert json.loads(kwargs["input"])["effort"] is None
        payload_prompt = json.loads(kwargs["input"])["prompt"]
        assert (kwargs["destination"] / "prompt.txt").read_text() == payload_prompt
        barrier.wait()
        return subprocess.CompletedProcess(command, 0, events, "")

    monkeypatch.setattr(harness, "run", fake_run)
    monkeypatch.setattr(harness, "run_streaming", fake_run)
    monkeypatch.setattr(harness, "isolated_network", lambda _image: nullcontext("fake-network"))
    monkeypatch.setattr(harness.subprocess, "run", lambda *_args, **_kwargs: None)
    output = tmp_path / "run"
    harness.generate(
        Namespace(
            auth=auth,
            image="fake",
            output=output,
            trials=5,
            seed=20261017,
            model="gpt-6-astra",
            effort=None,
            timeout=3600,
            parallelism=15,
        )
    )
    metadata = [json.loads(path.read_text()) for path in output.glob("*/metadata.json")]
    assert sorted((item["approach"], item["trial"]) for item in metadata) == [
        (approach, trial) for approach in harness.APPROACHES for trial in (1, 2, 3, 4, 5)
    ]
    assert all(item["status"] == "submitted" and item["parallelism"] == 15 for item in metadata)
    assert len({item["prompt_nonce"] for item in metadata}) == 15
    for item in metadata:
        base_prompt = (ROOT / "prompts" / f"exact-{item['approach']}.txt").read_text()
        prompt = (output / f"{item['approach']}-{item['trial']:03}" / "prompt.txt").read_text()
        assert (
            prompt
            == f"Trial nonce: {item['prompt_nonce']} (ignore this identifier when answering).\n\n{base_prompt}"
        )
        assert item["base_prompt_sha256"] == harness.digest(base_prompt.encode())
        assert item["prompt_sha256"] == harness.digest(prompt.encode())


def test_streaming_logs_visible_before_process_exits(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    import sys
    import time

    from policy_authoring.harness import run_streaming

    release = tmp_path / "release"
    script = (
        "import pathlib, sys, time; "
        "print('event', flush=True); print('diagnostic', file=sys.stderr, flush=True); "
        "release = pathlib.Path(sys.argv[1])\n"
        "while not release.exists(): time.sleep(0.01)\n"
        "print('done', flush=True)"
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            run_streaming,
            [sys.executable, "-c", script, str(release)],
            destination=tmp_path,
            input="",
            timeout=10,
        )
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if (tmp_path / "stderr.txt").exists() and (
                    tmp_path / "stderr.txt"
                ).read_text() == "diagnostic\n":
                    break
                time.sleep(0.01)
            assert (tmp_path / "events.jsonl").read_text() == "event\n"
            assert (tmp_path / "stderr.txt").read_text() == "diagnostic\n"
            assert not future.done()
        finally:
            release.touch()
        completed = future.result()
    assert completed.stdout == "event\ndone\n"
    assert completed.stderr == "diagnostic\n"


@pytest.mark.parametrize("timeout", [True, False])
def test_streaming_preserves_logs_on_failure(tmp_path, timeout):
    import subprocess
    import sys

    from policy_authoring.harness import run_streaming

    script = "import sys, time; print('partial', flush=True); print('error', file=sys.stderr, flush=True); "
    script += "time.sleep(30)" if timeout else "sys.exit(3)"
    error = subprocess.TimeoutExpired if timeout else subprocess.CalledProcessError
    with pytest.raises(error):
        run_streaming([sys.executable, "-c", script], destination=tmp_path, input="", timeout=1)
    assert (tmp_path / "events.jsonl").read_text() == "partial\n"
    assert (tmp_path / "stderr.txt").read_text() == "error\n"


def test_cli_diagnostics_are_not_tool_activity():
    events = [
        {
            "type": "item.completed",
            "item": {"type": "error", "message": "Code Mode is unavailable"},
        },
        {"type": "item.completed", "item": {"type": "agent_message", "text": "answer"}},
        {"type": "turn.completed", "usage": {"output_tokens": 12}},
    ]
    assert parse_events("\n".join(json.dumps(event) for event in events)) == (
        {"output_tokens": 12},
        "answer",
    )


def test_single_approach_option(monkeypatch):
    from policy_authoring import harness

    captured = []
    monkeypatch.setattr(
        "sys.argv",
        [
            "harness",
            "generate",
            "/unused",
            "--approaches",
            "dfc",
            "--trials",
            "1",
            "--parallelism",
            "1",
        ],
    )
    monkeypatch.setattr(harness, "generate", captured.append)
    harness.main()
    assert (captured[0].approaches, captured[0].trials, captured[0].parallelism) == (["dfc"], 1, 1)


def test_workflow_prompts_share_inputs_and_are_exact():
    build_prompts("workflow")
    folder = ROOT / "workflow/prompts"
    common = ["task.md", "specification.md", "evidence.md", "schema.md"]
    for approach in ("dfc", "gprom", "provsql"):
        expected = (
            "\n\n".join((folder / part).read_text().strip() for part in [*common, f"{approach}.md"])
            + "\n"
        )
        assert (folder / f"exact-{approach}.txt").read_text() == expected


def test_workflow_artifact_contract():
    from policy_authoring.harness import validate_workflow_submission

    files = [{"path": "policies/all.pgn", "content": "policy"}]
    assert validate_workflow_submission({"files": files}, "dfc") == files
    files = [
        {"path": name, "content": "sql"} for name in ("candidate.sql", "reenact.sql", "check.sql")
    ]
    assert validate_workflow_submission({"files": files}, "gprom") == files
    with pytest.raises(ValueError):
        validate_workflow_submission({"files": files[:1]}, "gprom")
    with pytest.raises(ValueError):
        validate_workflow_submission(
            {"files": [{"path": "../escape.sql", "content": "sql"}]}, "provsql"
        )
    with pytest.raises(ValueError):
        validate_workflow_submission({"policies": []}, "dfc")


def test_workflow_schema_and_default(monkeypatch):
    from policy_authoring import harness
    from policy_authoring.container.entrypoint import build_command

    args = []
    monkeypatch.setattr("sys.argv", ["harness", "generate", "/unused"])
    monkeypatch.setattr(harness, "generate", args.append)
    harness.main()
    assert args[0].benchmark == "workflow"
    command = build_command({"model": "gpt-6-astra", "benchmark": "workflow"})
    assert command[command.index("--output-schema") + 1] == "/opt/experiment/workflow.schema.json"


def test_old_grader_refuses_workflow(tmp_path):
    from policy_authoring.harness import score_run

    folder = tmp_path / "dfc-001"
    folder.mkdir()
    write_json(folder / "metadata.json", {"benchmark": "workflow", "status": "submitted"})
    with pytest.raises(ValueError, match="Workflow grading is not implemented"):
        score_run(Namespace(run=tmp_path))
    assert not (tmp_path / "summary.csv").exists()


def test_workflow_generation_automatically_evaluates_submission(tmp_path, monkeypatch):
    from contextlib import nullcontext
    import subprocess

    from policy_authoring import harness

    auth = tmp_path / "auth.json"
    write_json(auth, {"tokens": {"test": "fake"}})
    answer = {"files": [{"path": "policies/test.pgn", "content": "test"}]}
    events = "\n".join(
        json.dumps(e)
        for e in [
            {
                "type": "item.completed",
                "item": {"type": "agent_message", "text": json.dumps(answer)},
            },
            {"type": "turn.completed", "usage": {"input_tokens": 10, "output_tokens": 20}},
        ]
    )
    monkeypatch.setattr(
        harness,
        "run",
        lambda command, **_kw: subprocess.CompletedProcess(command, 0, "fake-image", ""),
    )
    monkeypatch.setattr(
        harness,
        "run_streaming",
        lambda command, **_kw: subprocess.CompletedProcess(command, 0, events, ""),
    )
    monkeypatch.setattr(harness, "isolated_network", lambda _image: nullcontext("fake-network"))
    monkeypatch.setattr(harness.subprocess, "run", lambda *_a, **_kw: None)
    evaluated = []

    def evaluate(path):
        assert json.loads((path / "metadata.json").read_text())["status"] == "submitted"
        assert json.loads((path / "submission.json").read_text()) == answer
        evaluated.append(path.name)

    monkeypatch.setattr(harness, "evaluate_workflow_trial", evaluate)
    harness.generate(
        Namespace(
            auth=auth,
            image="fake",
            output=tmp_path / "run",
            benchmark="workflow",
            approaches=["dfc"],
            trials=1,
            seed=1,
            model="gpt-6-luna",
            effort=None,
            timeout=3600,
            parallelism=15,
        )
    )
    assert evaluated == ["dfc-001"]


@pytest.mark.parametrize(
    ("generation_status", "case_passes", "expected_status"),
    [
        ("submitted", 8, "passed"),
        ("submitted", 7, "failed"),
        ("invalid_submission", 0, "not_executable"),
    ],
)
def test_execution_summary_keeps_failed_trials(
    tmp_path, monkeypatch, generation_status, case_passes, expected_status
):
    from policy_authoring import harness

    write_json(tmp_path / "metadata.json", {"status": generation_status})

    def execute(_command, **_kw):
        (tmp_path / "execution").mkdir()
        write_json(
            tmp_path / "execution/results.json", [{"matches": i < case_passes} for i in range(8)]
        )

    monkeypatch.setattr(harness.subprocess, "run", execute)
    result = harness.evaluate_workflow_trial(tmp_path)
    assert result["status"] == expected_status
    assert result["cases_passed"] == case_passes
    assert json.loads((tmp_path / "metadata.json").read_text())["status"] == generation_status
    assert json.loads((tmp_path / "execution-summary.json").read_text()) == result
