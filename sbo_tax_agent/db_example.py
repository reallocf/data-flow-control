#!/usr/bin/env python3
"""
Example script demonstrating violating row handling with bank transaction data.
"""

import os
from pathlib import Path
import tempfile

import duckdb

# Import local DuckDB setup (must be before duckdb import)
import use_local_duckdb

use_local_duckdb.setup_local_duckdb()

# Get project root to find extension and data files
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent

# Extension path
EXT_PATH = (
    PROJECT_ROOT
    / "extended_duckdb"
    / "build"
    / "release"
    / "repository"
    / "v1.4.1"
    / "osx_arm64"
    / "external.duckdb_extension"
)
if not EXT_PATH.exists():
    alt_paths = [
        PROJECT_ROOT
        / "extended_duckdb"
        / "build"
        / "release"
        / "extension"
        / "external"
        / "external.duckdb_extension",
        PROJECT_ROOT / "extended_duckdb" / "build" / "release" / "external.duckdb_extension",
    ]
    for alt_path in alt_paths:
        if alt_path.exists():
            EXT_PATH = alt_path
            break
    else:
        raise FileNotFoundError(f"Extension not found. Tried: {EXT_PATH} and alternatives")

# CSV file path (update filename if needed)
CSV_PATH = PROJECT_ROOT / "data" / "simple_bank_txn.csv"

# Connect to DuckDB with unsigned extension loading
con = duckdb.connect(
    database=":memory:",
    config={"allow_unsigned_extensions": "true"},
)

# Load the external extension
con.execute(f"LOAD '{EXT_PATH}'")

# Load CSV data into DuckDB table
# Keep a stable column order: VARCHAR first, then numeric, then int, then VARCHAR
# (This mirrors your "working pattern" comment, but adapted to new schema.)
con.execute(f"""
    CREATE TABLE bank_txn AS
    SELECT
        description::VARCHAR AS description,
        category::VARCHAR    AS category,
        amount::DOUBLE       AS amount,
        txn_id::INTEGER      AS txn_id
    FROM read_csv_auto('{CSV_PATH}');
""")

# Create a temporary file for the stream (initially empty)
stream_file = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt")
stream_path = stream_file.name
stream_file.close()

# Define & register the Python UDF
def address_violating_rows(
    description: str,
    category: str,
    amount: float,
    txn_id: int,
    stream_endpoint: str,
) -> bool:
    """
    Handle violating rows. Writes row to stream file with amount=0.0.
    Returns False to filter out the original violating row from the base table output.
    """
    with open(stream_endpoint, "a") as f:
        f.write(f"{description}\t{category}\t0.0\t{txn_id}\n")
        f.flush()
    return False

con.create_function("address_violating_rows", address_violating_rows)

# Violation condition:
# Treat category == "saber" as violating (so txn_id=1 "Excalibur" gets streamed with amount=0.0)
query = f"""
SELECT *
FROM bank_txn
WHERE CASE
        WHEN LOWER(category) != 'saber' THEN TRUE
        ELSE address_violating_rows(description, category, amount, txn_id, '{stream_path}')
      END;
"""

# Execute the query
df = con.execute(query).df()

expected_rows = 6
passed = len(df) == expected_rows

# Output results
print("Results:")
print(df)

# Cleanup
try:
    os.unlink(stream_path)
except:
    pass
