import argparse
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path("sql_rewriter/src").resolve()))

server = None
mcp_client_phase_18 = None


def init_temp_con():
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


def init_client_con():
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


def log_step(step: int, title: str, payload=None):
    print(f"\n=== Step {step}: {title} ===")
    if payload is not None:
        if isinstance(payload, str):
            print(payload)
        else:
            print(json.dumps(payload, indent=2, default=str))


def store_receipt_output(client_con, receipt, *, call_id="receipt_call_1"):
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
            json.dumps(receipt.get("ocr_lines", [])),
        ],
    )


def run_dfc_insert(client_con, sql: str):
    policy_deps = mcp_client_phase_18.parse_policy_deps(mcp_client_phase_18.DFC_POLICIES)
    gate1_error = mcp_client_phase_18.gate1_validate(sql, policy_deps)
    if gate1_error:
        return {
            "status": "error",
            "stage": "gate1",
            "error": gate1_error,
            "policy_deps": policy_deps,
        }

    rewriter = mcp_client_phase_18.create_rewriter(client_con)
    policies_registered = mcp_client_phase_18.register_policies(
        rewriter,
        mcp_client_phase_18.DFC_POLICIES,
    )
    sql_final = mcp_client_phase_18.inject_call_id(sql, "dfc_call_1")
    for source_table in mcp_client_phase_18.get_source_tables(sql_final):
        mcp_client_phase_18.deduplicate_source_table(client_con, source_table)

    sink_table = mcp_client_phase_18.get_sink_table(sql_final)
    count_before = client_con.execute(f"SELECT COUNT(*) FROM {sink_table}").fetchone()[0]

    try:
        rewritten = rewriter.transform_query(sql_final) if policies_registered else sql_final
        if policies_registered:
            rewriter.execute(sql_final)
        else:
            client_con.execute(sql_final)
    except Exception as exc:
        if "KILLing due to dfc policy violation" in str(exc):
            return {
                "status": "blocked",
                "stage": "dfc",
                "reason": str(exc),
                "policy_deps": policy_deps,
                "sql_final": sql_final,
                "rewritten_sql": rewritten,
            }
        raise

    count_after = client_con.execute(f"SELECT COUNT(*) FROM {sink_table}").fetchone()[0]
    if count_after == count_before:
        return {
            "status": "blocked",
            "stage": "dfc",
            "reason": "DFC policy prevented insert",
            "policy_deps": policy_deps,
            "sql_final": sql_final,
            "rewritten_sql": rewritten,
        }

    return {
        "status": "ok",
        "stage": "dfc",
        "policy_deps": policy_deps,
        "sql_final": sql_final,
        "rewritten_sql": rewritten,
        "row": mcp_client_phase_18.fetch_row(client_con, sink_table, "dfc_call_1"),
    }


def fetch_all_dicts(con, sql: str):
    cursor = con.execute(sql)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in rows]


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(
        description="Run a local OCR -> expense -> Schedule C sample flow."
    )
    parser.add_argument("--image-path", required=True, help="Path to a receipt image")
    parser.add_argument("--business-use", type=float, default=1.0)
    parser.add_argument("--gross-receipts", type=float, required=True)
    parser.add_argument("--federal-withholding", type=float, default=0.0)
    parser.add_argument(
        "--use-dfc",
        action="store_true",
        help="Run the set_expense intent through the local DFC SQL path before calling the server tool.",
    )
    args = parser.parse_args()

    os.environ.setdefault("KMP_DUPLICATE_LIB_OK", "TRUE")
    os.environ.setdefault(
        "EASYOCR_MODULE_PATH",
        os.path.join(os.path.dirname(__file__), ".easyocr"),
    )
    os.environ.setdefault("PYTHONUTF8", "1")
    os.environ.setdefault("OPENAI_API_KEY", "test-key")
    os.environ["EXPENSES_DB_PATH"] = os.path.join(
        tempfile.gettempdir(),
        f"tax_agent_manual_flow_{uuid.uuid4().hex[:8]}.duckdb",
    )

    global server, mcp_client_phase_18
    if server is None:
        import mcp_server_phase_18 as server_module

        server = server_module
    if mcp_client_phase_18 is None:
        import mcp_client_phase_18 as client_module

        mcp_client_phase_18 = client_module

    log_step(
        1,
        "Runtime Configuration",
        {
            "image_path": args.image_path,
            "business_use": args.business_use,
            "gross_receipts": args.gross_receipts,
            "federal_withholding": args.federal_withholding,
            "use_dfc": args.use_dfc,
            "easyocr_module_path": os.environ["EASYOCR_MODULE_PATH"],
            "expenses_db_path": os.environ["EXPENSES_DB_PATH"],
        },
    )

    server.CON = init_temp_con()
    log_step(2, "Server DB Initialized")

    receipt = server.get_receipt(image_path=args.image_path)
    if receipt.get("status") != "ok":
        raise SystemExit(f"Receipt OCR failed: {receipt}")
    log_step(3, "get_receipt(image_path=...)", receipt)

    expense_args = {
        "receipt_id": receipt["receipt_id"],
        "amount": receipt["amount"],
        "category": receipt["category"],
        "business_use": args.business_use,
    }

    if args.use_dfc:
        client_con = init_client_con()
        store_receipt_output(client_con, receipt)
        log_step(
            4,
            "Stored get_receipt output in local DFC source table",
            fetch_all_dicts(client_con, "SELECT * FROM get_receipt_out"),
        )
        sql = (
            "INSERT INTO set_expense_in (receipt_id, amount, category, business_use) "
            f"SELECT receipt_id, amount, category, {args.business_use} "
            "FROM get_receipt_out "
            f"WHERE receipt_id = {receipt['receipt_id']}"
        )
        log_step(5, "DFC Intent SQL", sql)
        dfc_result = run_dfc_insert(client_con, sql)
        log_step(6, "DFC Evaluation Result", dfc_result)
        if dfc_result.get("status") != "ok":
            raise SystemExit("Flow stopped before set_expense due to DFC block/error.")
        expense_args = dfc_result["row"]

    expense = server.set_expense(
        receipt_id=expense_args["receipt_id"],
        amount=expense_args["amount"],
        category=expense_args["category"],
        business_use=expense_args["business_use"],
    )
    if expense.get("status") != "ok":
        raise SystemExit(f"Expense creation failed: {expense}")
    log_step(7, "set_expense(...)", expense)

    result = server.compute_schedule_c_tax(
        gross_receipts=args.gross_receipts,
        federal_withholding=args.federal_withholding,
    )
    log_step(8, "compute_schedule_c_tax(...)", result)

    print("\n=== Summary ===")
    print("Flow completed successfully.")
    print("\nFormatted Return")
    print(result["formatted_return"])


if __name__ == "__main__":
    main()
