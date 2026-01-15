#!/usr/bin/env python3
"""
Example script demonstrating violating row handling with bank transaction data.
"""

import duckdb
import os
import tempfile
from pathlib import Path

# Import local DuckDB setup (must be before duckdb import)
import use_local_duckdb
use_local_duckdb.setup_local_duckdb()

# Get project root to find extension and data files
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent

# Extension path
EXT_PATH = PROJECT_ROOT / "extended_duckdb" / "build" / "release" / "repository" / "v1.4.1" / "osx_arm64" / "external.duckdb_extension"
if not EXT_PATH.exists():
    alt_paths = [
        PROJECT_ROOT / "extended_duckdb" / "build" / "release" / "extension" / "external" / "external.duckdb_extension",
        PROJECT_ROOT / "extended_duckdb" / "build" / "release" / "external.duckdb_extension",
    ]
    for alt_path in alt_paths:
        if alt_path.exists():
            EXT_PATH = alt_path
            break
    else:
        raise FileNotFoundError(f"Extension not found. Tried: {EXT_PATH} and alternatives")

# CSV file path
CSV_PATH = PROJECT_ROOT / "data" / "simple_bank_txn.csv"

# Connect to DuckDB with unsigned extension loading
con = duckdb.connect(
    database=":memory:",
    config={"allow_unsigned_extensions": "true"},
)

# Load the external extension
con.execute(f"LOAD '{EXT_PATH}'")

# Load CSV data into DuckDB table
con.execute(f"""
    CREATE TABLE bank_txn AS
    SELECT description, amount
    FROM read_csv_auto('{CSV_PATH}');
""")

# Create a temporary file for the stream (initially empty)
stream_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt')
stream_path = stream_file.name
stream_file.close()

# Define & register the Python UDF
def address_violating_rows(description: str, amount: float, stream_endpoint: str) -> bool:
    """Handle violating rows where description=="Internet service". Writes row to stream file with amount=0.0."""
    # Write the violating row to the stream file with amount=0.0
    with open(stream_endpoint, 'a') as f:
        f.write(f"{description}\t0.0\n")
        f.flush()
    return False  # Filter out the violating row

con.create_function("address_violating_rows", address_violating_rows)

# Query with violation condition
query = f"""
SELECT *
FROM bank_txn
WHERE CASE
        WHEN LOWER(description) != 'internet service' THEN TRUE
        ELSE address_violating_rows(description, amount, '{stream_path}')
      END;
"""

# Execute the query
df = con.execute(query).df()

# Expected: 4 table rows (non-Internet service) + 1 stream row (Internet service with amount=0) = 5 rows
expected_rows = 5
passed = len(df) == expected_rows

# Verify stream data is unionized
stream_row_count = sum(1 for _, row in df.iterrows() 
                       if row['description'].lower() == 'internet service' and row['amount'] == 0.0)
table_row_count = sum(1 for _, row in df.iterrows() 
                       if not (row['description'].lower() == 'internet service' and row['amount'] == 0.0))

stream_correct = stream_row_count == 1
table_correct = table_row_count == 4

# Output results
print("Results:")
print(df)
print(f"\nTotal rows: {len(df)} (expected: {expected_rows})")
print(f"Stream rows: {stream_row_count} (expected: 1)")
print(f"Table rows: {table_row_count} (expected: 4)")

if passed and stream_correct and table_correct:
    print("\n✓ Test PASSED")
else:
    print("\n✗ Test FAILED")

# Cleanup
try:
    os.unlink(stream_path)
except:
    pass
