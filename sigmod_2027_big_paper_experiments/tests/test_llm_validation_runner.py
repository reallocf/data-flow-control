"""Tests for incremental LLM benchmark execution."""

from __future__ import annotations

import csv
from datetime import datetime
from types import SimpleNamespace

from experiment_harness import ExperimentResult

from vldb_experiments.llm_validation_runner import (
    IncrementalResultWriter,
    build_tasks,
)


def test_build_tasks_assigns_stable_run_numbers() -> None:
    strategy = SimpleNamespace(
        settings=[(3, 1, "dfc_1phase"), (3, 1, "astra_query_results")],
        runs_per_setting=2,
    )

    tasks = build_tasks(strategy)

    assert [task.key for task in tasks] == [
        (3, 1, "dfc_1phase", 1),
        (3, 1, "dfc_1phase", 2),
        (3, 1, "astra_query_results", 1),
        (3, 1, "astra_query_results", 2),
    ]
    assert [task.execution_number for task in tasks] == [1, 2, 3, 4]


def test_incremental_writer_persists_and_resumes_successes(tmp_path) -> None:
    path = tmp_path / "results.csv"
    metric_names = ["run_num", "query_num", "policy_count", "approach"]
    task = build_tasks(
        SimpleNamespace(settings=[(3, 1, "astra_query_results")], runs_per_setting=1)
    )[0]
    writer = IncrementalResultWriter(path, metric_names, resume=True)
    writer.append(
        task,
        ExperimentResult(
            duration_ms=12.5,
            timestamp=datetime(2026, 10, 6, 12, 0, 0),
            custom_metrics={
                "run_num": 1,
                "query_num": 3,
                "policy_count": 1,
                "approach": "astra_query_results",
            },
        ),
    )

    with path.open(newline="") as source:
        rows = list(csv.DictReader(source))
    resumed = IncrementalResultWriter(path, metric_names, resume=True)

    assert rows == [
        {
            "execution_number": "1",
            "timestamp": "2026-10-06T12:00:00",
            "duration_ms": "12.5",
            "error": "",
            "run_num": "1",
            "query_num": "3",
            "policy_count": "1",
            "approach": "astra_query_results",
        }
    ]
    assert resumed.completed_keys == {(3, 1, "astra_query_results", 1)}


def test_incremental_writer_retries_failed_rows_on_resume(tmp_path) -> None:
    path = tmp_path / "results.csv"
    metric_names = ["run_num", "query_num", "policy_count", "approach"]
    task = build_tasks(
        SimpleNamespace(settings=[(3, 1, "astra_query_results")], runs_per_setting=1)
    )[0]
    writer = IncrementalResultWriter(path, metric_names, resume=True)
    writer.append(
        task,
        ExperimentResult(
            duration_ms=0.0,
            error="throttled",
            custom_metrics={
                "run_num": 1,
                "query_num": 3,
                "policy_count": 1,
                "approach": "astra_query_results",
            },
        ),
    )

    resumed = IncrementalResultWriter(path, metric_names, resume=True)

    assert resumed.completed_keys == set()
