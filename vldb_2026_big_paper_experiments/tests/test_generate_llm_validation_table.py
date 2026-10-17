from __future__ import annotations

import csv
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "scripts"))

from generate_llm_validation_table import generate_table  # noqa: E402


def test_generate_table_excludes_failed_executions(tmp_path: Path) -> None:
    input_path = tmp_path / "raw.csv"
    output_path = tmp_path / "summary.csv"
    fieldnames = [
        "approach",
        "policy_count",
        "provider",
        "model_name",
        "runtime_ms",
        "dfc_1phase_exec_time_ms",
        "ground_truth_violation",
        "predicted_violation",
        "llm_chars_sent",
        "raw_response",
        "error",
    ]
    with input_path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "approach": "dfc_1phase",
                "policy_count": 1,
                "provider": "none",
                "model_name": "none",
                "runtime_ms": 10,
                "dfc_1phase_exec_time_ms": 10,
                "ground_truth_violation": "True",
                "predicted_violation": "True",
                "llm_chars_sent": 0,
                "raw_response": "",
                "error": "",
            }
        )
        writer.writerow(
            {
                "approach": "dfc_1phase",
                "policy_count": 1,
                "provider": "none",
                "model_name": "none",
                "runtime_ms": 10,
                "dfc_1phase_exec_time_ms": 10,
                "ground_truth_violation": "False",
                "predicted_violation": "False",
                "llm_chars_sent": 0,
                "raw_response": "",
                "error": "",
            }
        )
        writer.writerow(
            {
                "approach": "sol_6_1_query_results",
                "policy_count": 1,
                "provider": "bedrock",
                "model_name": "profile",
                "runtime_ms": "",
                "dfc_1phase_exec_time_ms": "",
                "ground_truth_violation": "True",
                "predicted_violation": "",
                "llm_chars_sent": -1,
                "raw_response": "",
                "error": "Access denied",
            }
        )

    generate_table(input_path, output_path)

    with output_path.open(newline="") as summary:
        reader = csv.DictReader(summary)
        assert reader.fieldnames == [
            "policy_count",
            "dfc_f1",
            "dfc_balanced_accuracy",
            "dfc_avg_runtime_ms",
            "dfc_avg_cost_usd",
        ]
        assert list(reader) == [
            {
                "policy_count": "1",
                "dfc_f1": "1.000000",
                "dfc_balanced_accuracy": "1.000000",
                "dfc_avg_runtime_ms": "10.000000",
                "dfc_avg_cost_usd": "0.00000088",
            }
        ]
