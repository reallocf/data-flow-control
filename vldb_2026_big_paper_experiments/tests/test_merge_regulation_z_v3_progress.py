from __future__ import annotations

import csv

import pytest

from scripts.merge_regulation_z_v3_progress import merge_progress, summarize


def _write(path, rows):
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "approach",
                "update_number",
                "expected_allow",
                "actual_allow",
                "parse_failure",
                "error",
                "elapsed_ms",
                "account_id",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def test_merge_preserves_parse_failure_and_recovers_only_api_errors(tmp_path):
    first, retry = tmp_path / "first.csv", tmp_path / "retry.csv"
    base = {
        "approach": "model",
        "expected_allow": "False",
        "actual_allow": "False",
        "elapsed_ms": "100",
        "account_id": "one",
    }
    _write(
        first,
        [
            {**base, "update_number": "1", "parse_failure": "True"},
            {**base, "update_number": "2", "error": "timeout"},
        ],
    )
    _write(retry, [{**base, "update_number": str(i)} for i in (1, 2)])
    rows = merge_progress([first, retry], 2)
    assert [r["correct"] for r in rows] == ["False", "True"]
    assert [r["source_run"] for r in rows] == ["first.csv", "retry.csv"]
    report = summarize(rows)["model"]
    assert report["accuracy"] == 0.5
    assert report["parse_failures"] == 1
    assert report["api_errors"] == 0
    assert report["false_positives"] == 0
    assert report["false_negatives"] == 0


def test_merge_rejects_incomplete_or_conflicting_workload(tmp_path):
    first, second = tmp_path / "first.csv", tmp_path / "second.csv"
    row = {
        "approach": "model",
        "update_number": "1",
        "expected_allow": "True",
        "actual_allow": "True",
        "account_id": "one",
    }
    _write(first, [row])
    _write(second, [{**row, "account_id": "different"}])
    with pytest.raises(ValueError, match="Incomplete case coverage"):
        merge_progress([first], 2)
    with pytest.raises(ValueError, match="Conflicting workload"):
        merge_progress([first, second], 1)
