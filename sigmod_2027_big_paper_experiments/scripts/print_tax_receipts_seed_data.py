#!/usr/bin/env python3
"""Print the tax-agent receipt seed rows and a CREATE TABLE ... VALUES statement."""

from __future__ import annotations

from datetime import date, datetime

import duckdb

QUERY = """
SELECT
  i AS receipt_id,
  DATE '2025-01-01' + CAST(((i - 1) % 365) AS INTEGER) AS tx_date,
  CASE
    WHEN i % 7 = 0 THEN 'Restaurant'
    WHEN i % 7 = 1 THEN 'Airline'
    WHEN i % 7 = 2 THEN 'Office Depot'
    WHEN i % 7 = 3 THEN 'Gas Station'
    WHEN i % 7 = 4 THEN 'Hotel'
    WHEN i % 7 = 5 THEN 'Cloud Vendor'
    ELSE 'Taxi'
  END AS merchant,
  ROUND(CAST((((i * 113) % 120000) / 100.0) + 1.0 AS DOUBLE), 2) AS amount,
  'USD' AS currency,
  CASE
    WHEN i % 6 = 0 THEN 'MEAL'
    WHEN i % 6 = 1 THEN 'TRAVEL'
    WHEN i % 6 = 2 THEN 'OFFICE'
    WHEN i % 6 = 3 THEN 'TRANSPORT'
    WHEN i % 6 = 4 THEN 'SOFTWARE'
    ELSE 'OTHER'
  END AS category,
  CASE WHEN i % 5 = 0 THEN 'CASH' ELSE 'CARD' END AS payment_method,
  CASE WHEN i % 2 = 0 THEN 'San Francisco' ELSE 'New York' END AS city,
  CASE WHEN i % 2 = 0 THEN 'CA' ELSE 'NY' END AS state,
  'US' AS country,
  (i % 2 = 0) AS client_billable,
  CASE WHEN i % 3 = 0 THEN 'PRJ-1001' WHEN i % 3 = 1 THEN NULL ELSE 'PRJ-2002' END
    AS project_code,
  CASE WHEN i % 10 = 0 THEN NULL ELSE 'VEND-' || CAST(i AS VARCHAR) END AS vendor_tax_id,
  CASE WHEN i % 9 = 0 THEN '' ELSE 'Business expense' END AS business_purpose,
  CAST((i * 3) % 500 AS INTEGER) AS miles,
  CAST((i % 8) + 1 AS INTEGER) AS attendee_count,
  (i % 4 <> 0) AS has_receipt_image,
  2025 AS tax_year,
  TIMESTAMP '2025-01-01 08:00:00' + ((i - 1) * INTERVAL 1 HOUR) AS created_at
FROM range(1, 101) t(i)
"""


def _sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, date) and not isinstance(value, datetime):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    return str(value)


def main() -> None:
    conn = duckdb.connect(database=":memory:")
    try:
        rows = conn.execute(QUERY).fetchall()
    finally:
        conn.close()

    for row in rows:
        print(row)

    print("\n--- CREATE TABLE VALUES SQL ---")
    print("CREATE TABLE receipts AS")
    print("SELECT *")
    print("FROM (VALUES")
    for idx, row in enumerate(rows):
        row_sql = ", ".join(_sql_literal(value) for value in row)
        suffix = "," if idx < len(rows) - 1 else ""
        print(f"  ({row_sql}){suffix}")
    print(") AS receipts(")
    print(
        "  receipt_id, tx_date, merchant, amount, currency, category, payment_method, city, state, country,"
    )
    print(
        "  client_billable, project_code, vendor_tax_id, business_purpose, miles, attendee_count, has_receipt_image, tax_year, created_at"
    )
    print(");")


if __name__ == "__main__":
    main()
