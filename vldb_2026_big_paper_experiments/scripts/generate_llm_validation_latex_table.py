#!/usr/bin/env python3
"""Render the LLM validation summary CSV as a LaTeX table."""

from __future__ import annotations

import argparse
import csv
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


def _format_float(value: str, digits: int) -> str:
    return f"{float(value):.{digits}f}"


def generate_latex_table(input_csv: Path, output_tex: Path) -> Path:
    with input_csv.open(newline="") as infile:
        rows = list(csv.DictReader(infile))

    def render_single_table(title: str, prefix: str) -> list[str]:
        sub_lines = [
            "\\begin{minipage}[t]{0.48\\columnwidth}",
            "\\centering",
            f"\\textbf{{{title}}}",
            "\\vspace{0.25em}",
            "\\resizebox{\\columnwidth}{!}{%",
            "\\begin{tabular}{l|rrr}",
            "\\toprule",
            "Policy Cnt & F1 & Runtime (ms) & Cost (\\$) \\\\",
            "\\midrule",
        ]
        for row in rows:
            sub_lines.append(
                " & ".join(
                    [
                        _escape_latex(row["policy_count"]),
                        _format_float(row[f"{prefix}_f1"], 3),
                        _format_float(row[f"{prefix}_avg_runtime_ms"], 2),
                        _format_float(row[f"{prefix}_total_cost_usd"], 4),
                    ]
                )
                + " \\\\"
            )
        sub_lines.extend(
            [
                "\\bottomrule",
                "\\end{tabular}%",
                "}",
                "\\end{minipage}",
            ]
        )
        return sub_lines

    def render_pair_table(title: str, left_title: str, left_prefix: str, right_title: str, right_prefix: str) -> list[str]:
        sub_lines = [
            "\\begin{minipage}[t]{\\columnwidth}",
            "\\centering",
            f"\\textbf{{{title}}}",
            "\\vspace{0.25em}",
            "\\resizebox{\\columnwidth}{!}{%",
            "\\begin{tabular}{l|rrr|rrr}",
            "\\toprule",
            f"\\multirow{{2}}{{*}}{{Policy Cnt}} & \\multicolumn{{3}}{{c|}}{{\\textbf{{{left_title}}}}} & \\multicolumn{{3}}{{c}}{{\\textbf{{{right_title}}}}} \\\\",
            "& F1 & Runtime (ms) & Cost (\\$) & F1 & Runtime (ms) & Cost (\\$) \\\\",
            "\\midrule",
        ]
        for row in rows:
            sub_lines.append(
                " & ".join(
                    [
                        _escape_latex(row["policy_count"]),
                        _format_float(row[f"{left_prefix}_f1"], 3),
                        _format_float(row[f"{left_prefix}_avg_runtime_ms"], 2),
                        _format_float(row[f"{left_prefix}_total_cost_usd"], 4),
                        _format_float(row[f"{right_prefix}_f1"], 3),
                        _format_float(row[f"{right_prefix}_avg_runtime_ms"], 2),
                        _format_float(row[f"{right_prefix}_total_cost_usd"], 4),
                    ]
                )
                + " \\\\"
            )
        sub_lines.extend(
            [
                "\\bottomrule",
                "\\end{tabular}%",
                "}",
                "\\end{minipage}",
            ]
        )
        return sub_lines

    lines = [
        "\\begin{table}[t]",
        "\\centering",
    ]

    lines.extend(render_single_table("DFC Rewriter", "dfc"))
    lines.extend(
        [
            "\\vspace{0.75em}",
            "",
        ]
    )
    lines.extend(
        render_pair_table(
            "GPT 5.2",
            "Query Only",
            "gpt_52_query_only",
            "Query + Results",
            "gpt_52_query_results",
        )
    )
    lines.extend(
        [
            "\\vspace{0.75em}",
            "",
        ]
    )
    lines.extend(
        render_pair_table(
            "Opus 4.6",
            "Query Only",
            "opus_46_query_only",
            "Query + Results",
            "opus_46_query_results",
        )
    )

    lines.extend(
        [
            "\\caption{We compare \\code{DFC Rewriter} to LLM-based approaches to check whether TPC-H queries are allowed under 1-32 data flow control policies. Each policy checks aggregate qualities of the \\code{lineitem} table like \\code{Average quantity should remain at or below 30}. When given the query with or without the first 100 rows of query results, GPT-5.2 and Claude Opus 4.6 report an F1 of at most 53.7\\% but with orders of magnitude higher cost and latency.}",
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
