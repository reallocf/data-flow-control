"""Merge continuation/recovery runs without rewarding parse or API failures."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def merge_progress(paths: list[Path], expected_cases: int) -> list[dict[str, str]]:
    selected = {}
    for path in paths:
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                if row.get("is_warmup") == "True":
                    continue
                key = (row["approach"], int(row["update_number"]))
                if key in selected:
                    previous = selected[key]
                    for field in (
                        "account_id",
                        "allocation_id",
                        "expected_allow",
                        "expected_selected_transaction_id",
                    ):
                        if previous.get(field) != row.get(field):
                            raise ValueError(f"Conflicting workload for {key}: {field}")
                    if not previous.get("error") or row.get("error"):
                        continue
                row["source_run"] = path.name
                row["correct"] = str(
                    not row.get("error")
                    and row.get("parse_failure") != "True"
                    and row.get("actual_allow") in {"True", "False"}
                    and row["actual_allow"] == row["expected_allow"]
                )
                row["num_updates"] = str(expected_cases)
                selected[key] = row
    for approach in {key[0] for key in selected}:
        actual = {number for name, number in selected if name == approach}
        if actual != set(range(1, expected_cases + 1)):
            raise ValueError(f"Incomplete case coverage for {approach}: {sorted(actual)}")
    return [selected[key] for key in sorted(selected)]


def summarize(rows: list[dict[str, str]]) -> dict:
    summary = {}
    for key in sorted({r["approach"] for r in rows}):
        group = [r for r in rows if r["approach"] == key]
        decided = [r for r in group if not r.get("error") and r.get("parse_failure") != "True"]
        times = [
            float(r["elapsed_ms"]) for r in group if r.get("elapsed_ms") and not r.get("error")
        ]
        summary[key] = {
            "requests": len(group),
            "correct": sum(r["correct"] == "True" for r in group),
            "accuracy": sum(r["correct"] == "True" for r in group) / len(group),
            "false_positives": sum(
                r["actual_allow"] == "False" and r["expected_allow"] == "True" for r in decided
            ),
            "false_negatives": sum(
                r["actual_allow"] == "True" and r["expected_allow"] == "False" for r in decided
            ),
            "parse_failures": sum(r.get("parse_failure") == "True" for r in group),
            "api_errors": sum(bool(r.get("error")) for r in group),
            "average_response_latency_seconds": sum(times) / len(times) / 1000 if times else None,
            "timed_responses": len(times),
            "cache_read_requests": sum(
                int(r.get("cache_read_input_tokens") or 0) > 0 for r in group
            ),
            "cache_read_tokens": sum(int(r.get("cache_read_input_tokens") or 0) for r in group),
        }
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", nargs="+", required=True, type=Path)
    parser.add_argument("--expected-cases", type=int, default=100)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    rows = merge_progress(args.inputs, args.expected_cases)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    report = summarize(rows)
    args.output.with_suffix(".summary.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
