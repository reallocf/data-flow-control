"""
Reset and populate the finance DuckDB database.

Deletes all existing tables and creates 4 finance-related tables with sample data.
Run: python db_conn.py
"""

import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_DIR = BASE_DIR / "database"
DB_DIR.mkdir(exist_ok=True)
DB_PATH = DB_DIR / "finance.db"

conn = duckdb.connect(str(DB_PATH))

print("Connected to:", DB_PATH)

# -------------------------------
# Drop all existing tables
# -------------------------------
tables = conn.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'main'
""").fetchall()

for (table,) in tables:
    conn.execute(f"DROP TABLE IF EXISTS {table}")
    print(f"  Dropped: {table}")

if tables:
    print("Existing tables removed.\n")

# -------------------------------
# Create tables
# -------------------------------

# 1. accounts - bank/credit accounts
conn.execute("""
CREATE TABLE accounts (
    id INTEGER PRIMARY KEY,
    name VARCHAR,
    type VARCHAR,
    balance DOUBLE,
    currency VARCHAR
)
""")
print("Created: accounts")

# 2. categories - expense/income categories
conn.execute("""
CREATE TABLE categories (
    id INTEGER PRIMARY KEY,
    name VARCHAR,
    type VARCHAR
)
""")
print("Created: categories")

# 3. transactions - individual transactions
conn.execute("""
CREATE TABLE transactions (
    id INTEGER PRIMARY KEY,
    account_id INTEGER,
    category_id INTEGER,
    amount DOUBLE,
    type VARCHAR,
    date DATE,
    description VARCHAR
)
""")
print("Created: transactions")

# 4. budgets - budget per category per period
conn.execute("""
CREATE TABLE budgets (
    id INTEGER PRIMARY KEY,
    category_id INTEGER,
    amount DOUBLE,
    year INTEGER,
    month INTEGER
)
""")
print("Created: budgets")

# -------------------------------
# Insert sample data
# -------------------------------

conn.execute("""
INSERT INTO accounts VALUES
(1, 'Checking', 'checking', 5000.00, 'USD'),
(2, 'Savings', 'savings', 25000.00, 'USD'),
(3, 'Credit Card', 'credit', -1200.00, 'USD')
""")
print("Inserted: accounts")

conn.execute("""
INSERT INTO categories VALUES
(1, 'Groceries', 'expense'),
(2, 'Rent', 'expense'),
(3, 'Salary', 'income'),
(4, 'Utilities', 'expense'),
(5, 'Entertainment', 'expense')
""")
print("Inserted: categories")

conn.execute("""
INSERT INTO transactions VALUES
(1, 1, 1, -150.00, 'expense', '2024-01-15', 'Weekly groceries'),
(2, 1, 2, -1200.00, 'expense', '2024-01-01', 'Monthly rent'),
(3, 1, 3, 4500.00, 'income', '2024-01-31', 'Salary'),
(4, 1, 4, -85.00, 'expense', '2024-01-20', 'Electric bill'),
(5, 2, 5, -50.00, 'expense', '2024-01-10', 'Movie tickets'),
(6, 3, 1, -75.00, 'expense', '2024-01-18', 'Restaurant')
""")
print("Inserted: transactions")

conn.execute("""
INSERT INTO budgets VALUES
(1, 1, 600.00, 2024, 1),
(2, 2, 1200.00, 2024, 1),
(3, 4, 200.00, 2024, 1),
(4, 5, 100.00, 2024, 1)
""")
print("Inserted: budgets")

# -------------------------------
# Verify
# -------------------------------
print("\n--- Sample data ---")
for table in ["accounts", "categories", "transactions", "budgets"]:
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  {table}: {count} rows")

conn.close()
print("\nDone.")
