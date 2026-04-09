import os
import sys
import json
import duckdb
from pathlib import Path

os.environ.setdefault("OPENAI_API_KEY", "test-key")
sys.path.insert(0, str(Path("sql_rewriter/src").resolve()))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

import mcp_client_phase_18
import mcp_server_phase_18


class FakeOCRReader:
    def __init__(self, results):
        self._results = results

    def readtext(self, image_path):
        return self._results


def _log_step(title: str, payload=None):
    print(f"\n=== {title} ===")
    if payload is None:
        return
    if isinstance(payload, str):
        print(payload)
        return
    print(json.dumps(payload, indent=2, default=str))


def _init_test_con():
    con = duckdb.connect(":memory:")
    con.execute("CREATE SEQUENCE expenses_id_seq START 1")
    con.execute(
        """
        CREATE TABLE expenses (
            id INTEGER PRIMARY KEY DEFAULT nextval('expenses_id_seq'),
            date DATE NOT NULL DEFAULT current_date,
            amount DOUBLE NOT NULL,
            category VARCHAR NOT NULL,
            business_use DOUBLE DEFAULT 0.0,
            note VARCHAR DEFAULT '',
            valid BOOLEAN DEFAULT TRUE
        )
        """
    )
    con.execute(
        """
        CREATE TABLE receipts (
            id INTEGER PRIMARY KEY,
            vendor VARCHAR,
            amount DOUBLE,
            category VARCHAR,
            date DATE
        )
        """
    )
    return con


def _init_client_con():
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE get_receipt_out (
            call_id VARCHAR PRIMARY KEY,
            status VARCHAR,
            receipt_id INTEGER,
            vendor VARCHAR,
            amount DOUBLE,
            category VARCHAR,
            date VARCHAR,
            ocr_source BOOLEAN,
            ocr_lines VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE set_expense_in (
            call_id VARCHAR PRIMARY KEY,
            receipt_id INTEGER,
            amount DOUBLE,
            category VARCHAR,
            business_use DOUBLE
        )
        """
    )
    return con


def _store_receipt_output(client_con, receipt, *, call_id="receipt_call_1"):
    client_con.execute(
        """
        INSERT INTO get_receipt_out
        (call_id, status, receipt_id, vendor, amount, category, date, ocr_source, ocr_lines)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            call_id,
            receipt["status"],
            receipt["receipt_id"],
            receipt["vendor"],
            receipt["amount"],
            receipt["category"],
            receipt["date"],
            receipt.get("ocr_source", False),
            str(receipt.get("ocr_lines", [])),
        ],
    )


def _fetch_all_dicts(con, table: str):
    cursor = con.execute(f"SELECT * FROM {table}")
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in rows]


def _run_dfc_insert(client_con, sql):
    policy_deps = mcp_client_phase_18.parse_policy_deps(mcp_client_phase_18.DFC_POLICIES)
    _log_step("DFC Policy Dependencies", policy_deps)
    _log_step("Client Source Table Before DFC", _fetch_all_dicts(client_con, "get_receipt_out"))
    _log_step("Query Before Rewriting", sql.strip())
    gate1_error = mcp_client_phase_18.gate1_validate(sql, policy_deps)
    if gate1_error:
        result = {"status": "error", "stage": "gate1", "error": gate1_error}
        _log_step("Gate 1 Result", result)
        return result

    rewriter = mcp_client_phase_18.create_rewriter(client_con)
    assert mcp_client_phase_18.register_policies(rewriter, mcp_client_phase_18.DFC_POLICIES)

    sql_final = mcp_client_phase_18.inject_call_id(sql, "dfc_call_1")
    for source_table in mcp_client_phase_18.get_source_tables(sql_final):
        mcp_client_phase_18.deduplicate_source_table(client_con, source_table)

    sink_table = mcp_client_phase_18.get_sink_table(sql_final)
    count_before = client_con.execute(f"SELECT COUNT(*) FROM {sink_table}").fetchone()[0]
    rewritten = rewriter.transform_query(sql_final)
    _log_step("Query After call_id Injection", sql_final)
    _log_step("Query After Rewriting", rewritten)

    try:
        rewriter.execute(sql_final)
    except Exception as exc:
        if "KILLing due to dfc policy violation" in str(exc):
            result = {
                "status": "blocked",
                "stage": "dfc",
                "reason": str(exc),
                "sql_final": sql_final,
                "rewritten_sql": rewritten,
                "sink_count_before": count_before,
                "sink_count_after": client_con.execute(
                    f"SELECT COUNT(*) FROM {sink_table}"
                ).fetchone()[0],
            }
            _log_step("DFC Block Result", result)
            _log_step("Client Sink Table After Block", _fetch_all_dicts(client_con, sink_table))
            return result
        raise

    count_after = client_con.execute(f"SELECT COUNT(*) FROM {sink_table}").fetchone()[0]
    if count_after == count_before:
        result = {
            "status": "blocked",
            "stage": "dfc",
            "reason": "DFC policy prevented insert",
            "sql_final": sql_final,
            "rewritten_sql": rewritten,
            "sink_count_before": count_before,
            "sink_count_after": count_after,
        }
        _log_step("DFC Block Result", result)
        _log_step("Client Sink Table After Block", _fetch_all_dicts(client_con, sink_table))
        return result

    inserted = mcp_client_phase_18.fetch_row(client_con, sink_table, "dfc_call_1")
    result = {
        "status": "ok",
        "stage": "dfc",
        "row": inserted,
        "sql_final": sql_final,
        "rewritten_sql": rewritten,
        "sink_count_before": count_before,
        "sink_count_after": count_after,
    }
    _log_step("DFC Allow Result", result)
    _log_step("Client Sink Table After Allow", _fetch_all_dicts(client_con, sink_table))
    return result


def test_meal_receipt_flows_into_tax_engine(monkeypatch):
    test_con = _init_test_con()
    monkeypatch.setattr(mcp_server_phase_18, "CON", test_con)
    monkeypatch.setattr(
        mcp_server_phase_18,
        "_get_ocr_reader",
        lambda: FakeOCRReader(
            [
                (
                    [[0, 0], [10, 0], [10, 10], [0, 10]],
                    "THE FAKE RESTAURANT",
                    0.99,
                ),
                (
                    [[0, 20], [10, 20], [10, 30], [0, 30]],
                    "Date: 03/15/2026",
                    0.99,
                ),
                (
                    [[0, 40], [10, 40], [10, 50], [0, 50]],
                    "TOTAL: $74.25",
                    0.99,
                ),
            ]
        ),
    )

    image_path = Path("test_receipts") / "meal_receipt_integration.png"
    image_path.parent.mkdir(exist_ok=True)
    image_path.write_bytes(b"fake-image")

    receipt = mcp_server_phase_18.get_receipt(image_path=str(image_path))
    _log_step("OCR Receipt Output", receipt)
    assert receipt == {
        "status": "ok",
        "receipt_id": 1,
        "vendor": "The Fake Restaurant",
        "amount": 74.25,
        "category": "meal",
        "date": "2026-03-15",
        "ocr_source": True,
        "ocr_lines": [
            "THE FAKE RESTAURANT",
            "Date: 03/15/2026",
            "TOTAL: $74.25",
        ],
    }

    set_result = mcp_server_phase_18.set_expense(
        receipt_id=receipt["receipt_id"],
        amount=receipt["amount"],
        category=receipt["category"],
        business_use=0.5,
    )
    _log_step("set_expense Output", set_result)
    assert set_result["status"] == "ok"
    assert set_result["expense_id"] == 1

    tax_result = mcp_server_phase_18.compute_schedule_c_tax(
        gross_receipts=1000.0,
        federal_withholding=0.0,
    )
    _log_step("Tax Engine Output", tax_result)

    assert tax_result["status"] == "ok"
    assert tax_result["expense_count"] == 1
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
    assert tax_result["tax_return"] == {
        "Line 8: Schedule C net profit": 981.44,
        "Line 10: Adjustments (SE tax deduction)": 69.33,
        "Line 11: Adjusted gross income": 912.11,
        "Line 12: Standard deduction": 14600,
        "Line 15: Taxable income": 0.0,
        "Line 16: Income tax": 0.0,
        "Line 57: Self-employment tax (Schedule SE)": 138.67,
        "Line 24: Total tax": 138.67,
        "Line 25d: Federal tax withheld": 0.0,
        "Line 34: If line 33 is more than line 24, subtract line 24 from line 33. This is the amount you overpaid": 0.0,
        "Line 37: Subtract line 33 from line 24. This is the amount you owe": 138.67,
    }


def test_supplies_receipt_flows_without_meal_cap(monkeypatch):
    test_con = _init_test_con()
    monkeypatch.setattr(mcp_server_phase_18, "CON", test_con)
    monkeypatch.setattr(
        mcp_server_phase_18,
        "_get_ocr_reader",
        lambda: FakeOCRReader(
            [
                (
                    [[0, 0], [10, 0], [10, 10], [0, 10]],
                    "Office Depot",
                    0.99,
                ),
                (
                    [[0, 20], [10, 20], [10, 30], [0, 30]],
                    "2026-03-22",
                    0.99,
                ),
                (
                    [[0, 40], [10, 40], [10, 50], [0, 50]],
                    "TOTAL: $43.00",
                    0.99,
                ),
            ]
        ),
    )

    image_path = Path("test_receipts") / "supplies_receipt_integration.png"
    image_path.parent.mkdir(exist_ok=True)
    image_path.write_bytes(b"fake-image")

    receipt = mcp_server_phase_18.get_receipt(image_path=str(image_path))
    _log_step("OCR Receipt Output", receipt)
    assert receipt["status"] == "ok"
    assert receipt["category"] == "supplies"

    set_result = mcp_server_phase_18.set_expense(
        receipt_id=receipt["receipt_id"],
        amount=receipt["amount"],
        category=receipt["category"],
        business_use=1.0,
    )
    _log_step("set_expense Output", set_result)

    tax_result = mcp_server_phase_18.compute_schedule_c_tax(
        gross_receipts=1000.0,
        federal_withholding=50.0,
    )
    _log_step("Tax Engine Output", tax_result)

    assert tax_result["status"] == "ok"
    assert tax_result["expense_count"] == 1
    assert tax_result["schedule_c_input"] == {
        "gross_receipts": 1000.0,
        "expenses": [
            {
                "description": "Office Depot",
                "amount": 43.0,
                "category": "supplies",
                "receipt_present": True,
            }
        ],
    }
    assert tax_result["tax_return"] == {
        "Line 8: Schedule C net profit": 957.0,
        "Line 10: Adjustments (SE tax deduction)": 67.61,
        "Line 11: Adjusted gross income": 889.39,
        "Line 12: Standard deduction": 14600,
        "Line 15: Taxable income": 0.0,
        "Line 16: Income tax": 0.0,
        "Line 57: Self-employment tax (Schedule SE)": 135.22,
        "Line 24: Total tax": 135.22,
        "Line 25d: Federal tax withheld": 50.0,
        "Line 34: If line 33 is more than line 24, subtract line 24 from line 33. This is the amount you overpaid": 0.0,
        "Line 37: Subtract line 33 from line 24. This is the amount you owe": 85.22,
    }


def test_dfc_blocks_meal_receipt_with_100_percent_business_use(monkeypatch):
    server_con = _init_test_con()
    client_con = _init_client_con()
    monkeypatch.setattr(mcp_server_phase_18, "CON", server_con)
    monkeypatch.setattr(
        mcp_server_phase_18,
        "_get_ocr_reader",
        lambda: FakeOCRReader(
            [
                ([[0, 0], [10, 0], [10, 10], [0, 10]], "THE FAKE RESTAURANT", 0.99),
                ([[0, 20], [10, 20], [10, 30], [0, 30]], "Date: 03/15/2026", 0.99),
                ([[0, 40], [10, 40], [10, 50], [0, 50]], "TOTAL: $74.25", 0.99),
            ]
        ),
    )

    image_path = Path("test_receipts") / "meal_receipt_dfc_block.png"
    image_path.parent.mkdir(exist_ok=True)
    image_path.write_bytes(b"fake-image")

    receipt = mcp_server_phase_18.get_receipt(image_path=str(image_path))
    _log_step("OCR Receipt Output", receipt)
    _store_receipt_output(client_con, receipt)

    sql = """
        INSERT INTO set_expense_in (receipt_id, amount, category, business_use)
        SELECT receipt_id, amount, category, 1.0
        FROM get_receipt_out
        WHERE receipt_id = 1
    """
    result = _run_dfc_insert(client_con, sql)

    assert result["status"] == "blocked"
    assert client_con.execute("SELECT COUNT(*) FROM set_expense_in").fetchone()[0] == 0
    assert server_con.execute("SELECT COUNT(*) FROM expenses").fetchone()[0] == 0


def test_dfc_allows_non_full_meal_use_and_flows_into_tax_computation(monkeypatch):
    server_con = _init_test_con()
    client_con = _init_client_con()
    monkeypatch.setattr(mcp_server_phase_18, "CON", server_con)
    monkeypatch.setattr(
        mcp_server_phase_18,
        "_get_ocr_reader",
        lambda: FakeOCRReader(
            [
                ([[0, 0], [10, 0], [10, 10], [0, 10]], "THE FAKE RESTAURANT", 0.99),
                ([[0, 20], [10, 20], [10, 30], [0, 30]], "Date: 03/15/2026", 0.99),
                ([[0, 40], [10, 40], [10, 50], [0, 50]], "TOTAL: $74.25", 0.99),
            ]
        ),
    )

    image_path = Path("test_receipts") / "meal_receipt_dfc_allow.png"
    image_path.parent.mkdir(exist_ok=True)
    image_path.write_bytes(b"fake-image")

    receipt = mcp_server_phase_18.get_receipt(image_path=str(image_path))
    _log_step("OCR Receipt Output", receipt)
    _store_receipt_output(client_con, receipt)

    sql = """
        INSERT INTO set_expense_in (receipt_id, amount, category, business_use)
        SELECT receipt_id, amount, category, 0.5
        FROM get_receipt_out
        WHERE receipt_id = 1
    """
    result = _run_dfc_insert(client_con, sql)

    assert result["status"] == "ok"
    sink_row = result["row"]
    assert sink_row == {
        "call_id": "dfc_call_1",
        "receipt_id": 1,
        "amount": 74.25,
        "category": "meal",
        "business_use": 0.5,
    }

    set_result = mcp_server_phase_18.set_expense(
        receipt_id=sink_row["receipt_id"],
        amount=sink_row["amount"],
        category=sink_row["category"],
        business_use=sink_row["business_use"],
    )
    _log_step("set_expense Output", set_result)
    assert set_result["status"] == "ok"

    tax_result = mcp_server_phase_18.compute_schedule_c_tax(
        gross_receipts=1000.0,
        federal_withholding=0.0,
    )
    _log_step("Tax Engine Output", tax_result)
    assert tax_result["status"] == "ok"
    assert tax_result["expense_count"] == 1
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
