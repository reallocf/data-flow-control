import duckdb
from pathlib import Path

# -------------------------------
# Setup database path properly
# -------------------------------
BASE_DIR = Path(__file__).resolve().parent
DB_DIR = BASE_DIR / "database"
DB_DIR.mkdir(exist_ok=True)

DB_PATH = DB_DIR / "finance.db"

# -------------------------------
# Connect
# -------------------------------
conn = duckdb.connect(str(DB_PATH))

print("Connected to:", DB_PATH)

# -------------------------------
# Create table
# -------------------------------
conn.execute("""
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER,
    year INTEGER,
    revenue DOUBLE,
    expense DOUBLE
)
""")

print("Table created.")

# -------------------------------
# Insert data
# -------------------------------
conn.execute("""
INSERT INTO transactions VALUES
(1, 2020, 100000, 40000),
(2, 2021, 150000, 50000),
(3, 2022, 210000, 70000),
(4, 2023, 300000, 90000)
""")

print("Data inserted.")

# -------------------------------
# Verify data
# -------------------------------
result = conn.execute("SELECT * FROM transactions").fetchall()
print("\nCurrent Data:")
for row in result:
    print(row)

conn.close()
