#!/usr/bin/env python3
"""Aggregate LLM validation results into a compact summary table."""

from __future__ import annotations

import argparse
from collections import defaultdict
import csv
from pathlib import Path

from sklearn.metrics import f1_score

DEFAULT_INPUT = Path(
    "/Users/charliesummers/code/data-flow-control/vldb_2026_big_paper_experiments/final_results/llm_validation_results_full_gpt52_opus46.csv"
)
DEFAULT_OUTPUT = Path(
    "/Users/charliesummers/code/data-flow-control/vldb_2026_big_paper_experiments/final_results/llm_validation_table.csv"
)

# Standard rough derivation for English prose and code-ish text.
CHARS_PER_TOKEN = 5.0

# USD per 1M tokens.
# OpenAI pricing source: https://openai.com/api/pricing/
GPT_5_2_INPUT_PER_MILLION = 1.75
GPT_5_2_OUTPUT_PER_MILLION = 14.0

# Anthropic pricing sources:
# - https://www.anthropic.com/claude/opus
# - https://docs.claude.com/en/docs/about-claude/pricing
OPUS_4_6_INPUT_PER_MILLION = 5.0
OPUS_4_6_OUTPUT_PER_MILLION = 25.0
SERVER_COST_PER_HOUR = 0.31792


def _parse_bool(value: str) -> bool:
    return value.strip().lower() == "true"


def _parse_float(value: str) -> float:
    if not value:
        return 0.0
    return float(value)


def _estimate_tokens_from_chars(char_count: float) -> float:
    return char_count / CHARS_PER_TOKEN


def _estimate_output_tokens(raw_response: str) -> float:
    return _estimate_tokens_from_chars(float(len(raw_response or "")))


def _pricing_for_approach(approach: str) -> tuple[float, float]:
    if approach.startswith("gpt_"):
        return GPT_5_2_INPUT_PER_MILLION, GPT_5_2_OUTPUT_PER_MILLION
    if approach.startswith("opus_"):
        return OPUS_4_6_INPUT_PER_MILLION, OPUS_4_6_OUTPUT_PER_MILLION
    return 0.0, 0.0


def _display_provider(provider: str) -> str:
    normalized = provider.strip().lower()
    if normalized == "bedrock":
        return "Anthropic"
    if normalized == "openai":
        return "Open AI"
    return provider


def _display_model_name(model_name: str) -> str:
    normalized = model_name.strip()
    if normalized == "gpt-5.2":
        return "GPT-5.2"
    if normalized == "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/2zbh8y2el8aa":
        return "Opus 4.6"
    return model_name


def _column_prefix(approach: str) -> str:
    if approach == "dfc_1phase":
        return "dfc"
    if approach.startswith("gpt_"):
        return approach.replace("gpt_", "gpt_52_", 1)
    if approach.startswith("opus_"):
        return approach.replace("opus_", "opus_46_", 1)
    return approach


def generate_table(input_csv: Path, output_csv: Path) -> Path:
    grouped: dict[tuple[str, int], dict[str, object]] = defaultdict(
        lambda: {
            "provider": "",
            "model_name": "",
            "policy_count": 0,
            "runtime_sum": 0.0,
            "cost_sum": 0.0,
            "input_tokens_sum": 0.0,
            "output_tokens_sum": 0.0,
            "count": 0,
            "y_true": [],
            "y_pred": [],
        }
    )

    with input_csv.open(newline="") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            approach = row["approach"]
            policy_count = int(row["policy_count"])
            group = grouped[(approach, policy_count)]
            group["provider"] = row["provider"]
            group["model_name"] = row["model_name"]
            group["policy_count"] = policy_count
            group["runtime_sum"] += _parse_float(row["runtime_ms"])
            group["count"] += 1

            truth = _parse_bool(row["ground_truth_violation"])
            pred = _parse_bool(row["predicted_violation"])
            group["y_true"].append(truth)
            group["y_pred"].append(pred)

            input_tokens = _estimate_tokens_from_chars(_parse_float(row["llm_chars_sent"]))
            output_tokens = _estimate_output_tokens(row["raw_response"])
            group["input_tokens_sum"] += input_tokens
            group["output_tokens_sum"] += output_tokens

            input_price, output_price = _pricing_for_approach(approach)
            cost = (
                (input_tokens / 1_000_000.0) * input_price
                + (output_tokens / 1_000_000.0) * output_price
            )
            group["cost_sum"] += cost

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    sort_order = {
        "dfc_1phase": 0,
        "gpt_query_only": 1,
        "gpt_query_results": 2,
        "opus_query_only": 3,
        "opus_query_results": 4,
    }
    metric_names = ("f1", "avg_runtime_ms", "total_cost_usd")
    ordered_approaches = [
        approach for approach, _ in sorted(grouped.keys(), key=lambda item: (sort_order.get(item[0], 99), item[1]))
    ]
    unique_approaches: list[str] = []
    for approach in ordered_approaches:
        if approach not in unique_approaches:
            unique_approaches.append(approach)

    fieldnames = ["policy_count"]
    for approach in unique_approaches:
        prefix = _column_prefix(approach)
        for metric_name in metric_names:
            fieldnames.append(f"{prefix}_{metric_name}")

    rows_by_policy_count: dict[int, dict[str, str | int]] = {}
    with output_csv.open("w", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for (approach, policy_count), group in sorted(
            grouped.items(), key=lambda item: (sort_order.get(item[0][0], 99), item[0][1])
        ):
            total_runtime_ms = float(group["runtime_sum"])
            count = int(group["count"])
            infra_cost = (total_runtime_ms / 1000.0 / 3600.0) * SERVER_COST_PER_HOUR
            prefix = _column_prefix(approach)
            row = rows_by_policy_count.setdefault(policy_count, {"policy_count": policy_count})
            row[f"{prefix}_f1"] = f"{f1_score(group['y_true'], group['y_pred'], zero_division=0):.6f}"
            row[f"{prefix}_avg_runtime_ms"] = f"{(total_runtime_ms / count):.6f}"
            row[f"{prefix}_total_cost_usd"] = f"{(float(group['cost_sum']) + infra_cost):.8f}"

        for policy_count in sorted(rows_by_policy_count):
            writer.writerow(rows_by_policy_count[policy_count])
    return output_csv


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate an aggregated LLM validation summary table.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help=f"Input CSV (default: {DEFAULT_INPUT})")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help=f"Output CSV (default: {DEFAULT_OUTPUT})")
    args = parser.parse_args()

    output_path = generate_table(args.input, args.output)
    print(f"Wrote summary table to {output_path}")


if __name__ == "__main__":
    main()
