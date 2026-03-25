"""
Phase 6 — Tax Agent MCP Server (Vanilla)

Completely standard MCP server.
No DFC logic, no execute_sql, no tool_inputs, no chains.
Just get_receipt and set_expense as normal tools.

The client handles everything:
  - SQL intent parsing
  - DFC policy enforcement (SQLRewriter)
  - Per-tool input/output tables (local DuckDB)
  - JSON arg extraction and tool dispatch
"""

from mcp.server.fastmcp import FastMCP
import os
import duckdb

DB_PATH = os.path.join(os.path.dirname(__file__), "expenses.duckdb")


def init_db():
    con = duckdb.connect(DB_PATH)

    con.execute("CREATE SEQUENCE IF NOT EXISTS expenses_id_seq START 1")

    con.execute("""
        CREATE TABLE IF NOT EXISTS expenses (
            id           INTEGER PRIMARY KEY DEFAULT nextval('expenses_id_seq'),
            date         DATE NOT NULL DEFAULT current_date,
            amount       DOUBLE NOT NULL,
            category     VARCHAR NOT NULL,
            business_use DOUBLE DEFAULT 0.0,
            note         VARCHAR DEFAULT '',
            valid        BOOLEAN DEFAULT TRUE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS receipts (
            id       INTEGER PRIMARY KEY,
            vendor   VARCHAR,
            amount   DOUBLE,
            category VARCHAR,
            date     DATE
        )
    """)

    if con.execute("SELECT COUNT(*) FROM receipts").fetchone()[0] == 0:
        con.execute("""
            INSERT INTO receipts VALUES
                (1, 'Restaurant ABC',  50.0,  'meal',      current_date),
                (2, 'Uber',            25.0,  'transport', current_date),
                (3, 'Office Supplies', 100.0, 'supplies',  current_date),
                (4, 'Coffee Shop',     15.0,  'meal',      current_date)
        """)

    con.close()


init_db()

mcp = FastMCP("TaxAgent")
CON = duckdb.connect(DB_PATH)


@mcp.tool()
def get_receipt(receipt_id: int) -> dict:
    """
    Fetch a receipt by ID.
    Returns vendor, amount, category, date.
    """
    row = CON.execute(
        "SELECT id, vendor, amount, category, date FROM receipts WHERE id = ?",
        [receipt_id]
    ).fetchone()

    if not row:
        return {"status": "error", "error": f"Receipt {receipt_id} not found"}

    print(f"[get_receipt] receipt_id={receipt_id}, category={row[3]}")

    return {
        "status":   "ok",
        "receipt_id": row[0],
        "vendor":   row[1],
        "amount":   row[2],
        "category": row[3],
        "date":     str(row[4]),
    }


@mcp.tool()
def set_expense(
    receipt_id:   int,
    amount:       float,
    category:     str,
    business_use: float,
) -> dict:
    """
    Record a business expense.
    Returns status and the new expense id.
    """
    CON.execute("""
        INSERT INTO expenses (date, amount, category, business_use, note)
        VALUES (current_date, ?, ?, ?, ?)
    """, [amount, category, business_use, f"receipt_id={receipt_id}"])

    expense_id = CON.execute(
        "SELECT MAX(id) FROM expenses"
    ).fetchone()[0]

    print(
        f"[set_expense] receipt={receipt_id}, category={category}, "
        f"business_use={business_use} → expense_id={expense_id}"
    )

    return {
        "status":     "ok",
        "expense_id": expense_id,
        "message":    f"Expense recorded — category={category}, amount={amount}, business_use={business_use}",
    }


@mcp.tool()
def list_expenses(limit: int = 100) -> dict:
    """
    List recorded expenses in reverse chronological order.
    """
    rows = CON.execute(
        """
        SELECT id, date, amount, category, business_use, note, valid
        FROM expenses
        ORDER BY date DESC, id DESC
        LIMIT ?
        """,
        [min(max(int(limit), 1), 1000)]
    ).fetchall()

    result = [
        {
            "id": r[0],
            "date": str(r[1]),
            "amount": r[2],
            "category": r[3],
            "business_use": r[4],
            "note": r[5],
            "valid": r[6],
        }
        for r in rows
    ]

    print(f"[list_expenses] returned {len(result)} rows")

    return {
        "status": "ok",
        "count":  len(result),
        "rows":   result,
    }


if __name__ == "__main__":
    mcp.run(transport="sse")