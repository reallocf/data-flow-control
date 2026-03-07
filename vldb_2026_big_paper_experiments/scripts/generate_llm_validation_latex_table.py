#!/usr/bin/env python3
"""Render the LLM validation summary CSV as a LaTeX table."""

from __future__ import annotations

import argparse
import csv
import math
from pathlib import Path

DEFAULT_INPUT = Path(
    "/Users/charliesummers/code/data-flow-control/vldb_2026_big_paper_experiments/final_results/llm_validation_table.csv"
)
DEFAULT_OUTPUT = Path(
    "/Users/charliesummers/code/data-flow-control/vldb_2026_big_paper_experiments/final_results/llm_validation_table.tex"
)


def _escape_latex(value: str) -> str:
    return (
        value.replace("\\", "\\textbackslash{}")
        .replace("&", "\\&")
        .replace("%", "\\%")
        .replace("$", "\\$")
        .replace("#", "\\#")
        .replace("_", "\\_")
        .replace("{", "\\{")
        .replace("}", "\\}")
    )


def _format_percent(value: str) -> str:
    return f"{int(float(value) * 100)}\\%"


def _format_ms_trunc(value: str) -> str:
    return str(int(float(value)))


def _format_seconds_trunc(value: str) -> str:
    return f"{float(value) / 1000.0:.1f}s"


def _format_cents(value: str) -> str:
    cents = float(value) * 100.0
    truncated = math.floor(cents * 1000.0) / 1000.0
    if truncated == 0:
        return ".000\\textcent"
    formatted = f"{truncated:.3f}".rstrip("0").rstrip(".")
    if not formatted:
        formatted = ".000"
    if formatted.startswith("0"):
        formatted = formatted[1:]
    return f"{formatted}\\textcent"


def generate_latex_table(input_csv: Path, output_tex: Path) -> Path:
    with input_csv.open(newline="") as infile:
        rows = list(csv.DictReader(infile))

    lines = [
        "\\begin{table}[t]",
        "\\centering",
        "\\resizebox{\\columnwidth}{!}{%",
        "\\begin{tabular}{lrrrrrrrrr}",
        "& \\multicolumn{3}{c}{\\textbf{DFC (us)}} & \\multicolumn{3}{c}{\\textbf{GPT-5.2}} & \\multicolumn{3}{c}{\\textbf{Opus 4.6}} \\\\",
        "\\arrayrulecolor{gray!80}",
        "\\cmidrule(lr){2-4} \\cmidrule(lr){5-7} \\cmidrule(lr){8-10}",
        "\\textbf{\\#} & \\textbf{F1} & \\textbf{ms} & \\textbf{\\textcent} & \\textbf{F1} & \\textbf{ms} & \\textbf{\\textcent} & \\textbf{F1} & \\textbf{sec} & \\textbf{\\textcent} \\\\",
    ]

    for row in rows:
        lines.append(
            " & ".join(
                [
                    _escape_latex(row["policy_count"]),
                    f"\\blue{{{_format_percent(row['dfc_f1'])}}}",
                    f"\\blue{{{_format_ms_trunc(row['dfc_avg_runtime_ms'])}}}",
                    f"\\blue{{{_format_cents(row['dfc_avg_cost_usd'])}}}",
                    _format_percent(row["gpt_52_query_results_f1"]),
                    _format_ms_trunc(row["gpt_52_query_results_avg_runtime_ms"]),
                    _format_cents(row["gpt_52_query_results_avg_cost_usd"]),
                    _format_percent(row["opus_46_query_results_f1"]),
                    _format_seconds_trunc(row["opus_46_query_results_avg_runtime_ms"]),
                    _format_cents(row["opus_46_query_results_avg_cost_usd"]),
                ]
            )
            + " \\\\"
        )

    lines.extend(
        [
            "\\end{tabular}%",
            "}",
            "\\caption{DFC vs LLM calls check 13 TPC-H queries against 1-32 trivial data flow control policies over \\code{lineitem} (5 runs per query). Policies check simple aggregate statistics e.g., {\\it ``Average quantity $\\leq$30''}. LLMs are given the query and first 100 result tuples\\tablefootnote{When given only the query, F1 is worse, latency is similar, and cost is roughly half.}. DFC is correct and orders of magnitude faster and cheaper.}",
            "\\label{tbl:llm}",
            "\\end{table}",
        ]
    )

    output_tex.parent.mkdir(parents=True, exist_ok=True)
    output_tex.write_text("\n".join(lines) + "\n")
    return output_tex


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a LaTeX table for LLM validation results.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help=f"Input CSV (default: {DEFAULT_INPUT})")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help=f"Output TeX (default: {DEFAULT_OUTPUT})")
    args = parser.parse_args()

    output_path = generate_latex_table(args.input, args.output)
    print(f"Wrote LaTeX table to {output_path}")


if __name__ == "__main__":
    main()
