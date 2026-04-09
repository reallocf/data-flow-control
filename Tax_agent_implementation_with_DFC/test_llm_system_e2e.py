import json
import os
import sys
from pathlib import Path

import pytest

from llm_system_test_utils import (
    compute_tax_from_db,
    fetch_db_rows,
    read_process_output,
    run_llm_request_sync,
    start_server_process,
    stop_server_process,
)


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")


pytestmark = pytest.mark.skipif(
    not os.environ.get("OPENAI_API_KEY"),
    reason="OPENAI_API_KEY is required for live LLM end-to-end tests.",
)


def _log(title: str, payload):
    print(f"\n=== {title} ===")
    if isinstance(payload, str):
        print(payload)
    else:
        print(json.dumps(payload, indent=2, default=str))


def test_llm_full_flow_allows_meal_receipt_with_half_business_use():
    proc, db_path = start_server_process()
    try:
        image_path = str(Path("test_receipts/meal_receipt.png").resolve())
        prompt = (
            "Use the receipt image at this exact path: "
            f"{image_path}. "
            "OCR it first. Then record it as a business expense with business_use 0.5. "
            "Do not ask follow-up questions. If you succeed, briefly confirm what you recorded."
        )
        result = run_llm_request_sync(prompt)
        _log("LLM Final Message", result["final_message"])
        _log("LLM DFC Traces", result["traces"])
        _log("LLM Local Tool Tables", result["tool_tables"])

        expenses = fetch_db_rows(db_path, "expenses")
        receipts = fetch_db_rows(db_path, "receipts")
        _log("Server Receipts Table", receipts)
        _log("Server Expenses Table", expenses)

        tax_result = compute_tax_from_db(
            db_path,
            gross_receipts=1000.0,
            federal_withholding=0.0,
        )
        _log("Computed Tax Result", tax_result)

        assert expenses
        assert any(trace["result"]["status"] == "ok" and trace["result"]["tool"] == "get_receipt" for trace in result["traces"])
        assert any(trace["result"]["status"] == "ok" and trace["result"]["tool"] == "set_expense" for trace in result["traces"])
        assert tax_result["schedule_c_input"] == {
            "gross_receipts": 1000.0,
            "expenses": [
                {
                    "description": "The Fake Restaurant",
                    "amount": 37.12,
                    "category": "meals",
                    "receipt_present": True,
                }
            ],
        }
        assert tax_result["tax_return"]["Line 24: Total tax"] == 138.67
    finally:
        stop_server_process(proc)
        _log("Server Process Output", read_process_output(proc))


def test_llm_full_flow_blocks_meal_receipt_with_full_business_use():
    proc, db_path = start_server_process()
    try:
        image_path = str(Path("test_receipts/meal_receipt.png").resolve())
        prompt = (
            "Use the receipt image at this exact path: "
            f"{image_path}. "
            "OCR it first. Then try to record it as a business expense with business_use 1.0. "
            "If a policy blocks you, explain the policy violation."
        )
        result = run_llm_request_sync(prompt)
        _log("LLM Final Message", result["final_message"])
        _log("LLM DFC Traces", result["traces"])
        _log("LLM Local Tool Tables", result["tool_tables"])

        expenses = fetch_db_rows(db_path, "expenses")
        receipts = fetch_db_rows(db_path, "receipts")
        _log("Server Receipts Table", receipts)
        _log("Server Expenses Table", expenses)

        assert any(trace["result"]["status"] == "ok" and trace["result"]["tool"] == "get_receipt" for trace in result["traces"])
        assert any(trace["result"]["status"] == "blocked" for trace in result["traces"])
        assert len(expenses) == 0
        assert "policy" in result["final_message"].lower() or "100%" in result["final_message"]
    finally:
        stop_server_process(proc)
        _log("Server Process Output", read_process_output(proc))
