from __future__ import annotations

import csv

from scripts.generate_regulation_z_v3_benchmark_chart import (
    ApproachMetrics,
    summarize_progress,
)


def test_summarize_progress_computes_accuracy_and_latency(tmp_path) -> None:
    path = tmp_path / "progress.csv"
    with path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=["approach", "expected_allow", "actual_allow", "elapsed_ms"],
        )
        writer.writeheader()
        writer.writerows(
            [
                {
                    "approach": "no_policy",
                    "expected_allow": "True",
                    "actual_allow": "True",
                    "elapsed_ms": "1",
                },
                {
                    "approach": "no_policy",
                    "expected_allow": "False",
                    "actual_allow": "True",
                    "elapsed_ms": "3",
                },
            ]
        )

    metrics = summarize_progress(path)

    assert metrics == {
        "no_policy": ApproachMetrics(
            accuracy=0.5,
            average_latency_seconds=0.002,
        )
    }


def test_unparseable_rejection_and_api_error_are_not_correct(tmp_path) -> None:
    path = tmp_path / "failures.csv"
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "approach",
                "expected_allow",
                "actual_allow",
                "elapsed_ms",
                "parse_failure",
                "error",
            ],
        )
        writer.writeheader()
        writer.writerows(
            [
                {
                    "approach": "luna",
                    "expected_allow": "False",
                    "actual_allow": "False",
                    "elapsed_ms": "100",
                    "parse_failure": "True",
                },
                {"approach": "luna", "expected_allow": "False", "error": "timeout"},
            ]
        )
    assert summarize_progress(path) == {"luna": ApproachMetrics(0.0, 0.1)}
