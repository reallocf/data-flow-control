"""Fixed test data setup for microbenchmark experiments."""


import duckdb


def setup_test_data(conn: duckdb.DuckDBPyConnection, num_rows: int = 1_000_000) -> None:
    """Set up fixed test data in the database.

    Creates a table 'test_data' with the following schema:
    - id (INTEGER): Primary key, sequential from 1 to num_rows
    - value (INTEGER): Numeric value for filtering/aggregation (1 to num_rows)
    - category (VARCHAR): Categorical data for grouping ('A', 'B', 'C', 'D', 'E')
    - amount (DOUBLE): Numeric data for calculations (value * 10.0)

    The data distribution ensures:
    - Values range from 1 to num_rows (so policy constraint max(value) > 100 will filter some rows)
    - Categories cycle through A-E for grouping tests
    - Amounts are proportional to values for aggregation tests

    Args:
        conn: DuckDB connection
        num_rows: Number of rows to insert (default: 1,000,000)
    """
    # Create table
    conn.execute("""
        CREATE TABLE test_data (
            id INTEGER,
            value INTEGER,
            category VARCHAR,
            amount DOUBLE
        )
    """)

    # Generate fixed data
    # Categories cycle: A, B, C, D, E
    categories = ["A", "B", "C", "D", "E"]

    # Insert data in batches for efficiency
    # Use larger batch size for better performance with large datasets
    batch_size = 10000 if num_rows > 10000 else 100
    for batch_start in range(0, num_rows, batch_size):
        batch_end = min(batch_start + batch_size, num_rows)
        values = []
        for i in range(batch_start + 1, batch_end + 1):
            category = categories[(i - 1) % len(categories)]
            values.append(f"({i}, {i}, '{category}', {i * 10.0})")

        insert_sql = f"""
            INSERT INTO test_data (id, value, category, amount)
            VALUES {', '.join(values)}
        """
        conn.execute(insert_sql)

    # Verify data was inserted
    result = conn.execute("SELECT COUNT(*) FROM test_data").fetchone()
    assert result[0] == num_rows, f"Expected {num_rows} rows, got {result[0]}"


def setup_test_data_with_groups(
    conn: duckdb.DuckDBPyConnection,
    num_rows: int = 1_000_000,
    num_groups: int = 5
) -> None:
    """Set up test data with specified number of groups for GROUP_BY tests.

    Args:
        conn: DuckDB connection
        num_rows: Number of rows to insert
        num_groups: Number of distinct category groups
    """
    # Create table
    conn.execute("""
        CREATE TABLE test_data (
            id INTEGER,
            value INTEGER,
            category VARCHAR,
            amount DOUBLE
        )
    """)

    # Generate categories for the specified number of groups
    categories = [f"CAT_{i}" for i in range(num_groups)]

    # Insert data in batches
    batch_size = 10000 if num_rows > 10000 else 100
    for batch_start in range(0, num_rows, batch_size):
        batch_end = min(batch_start + batch_size, num_rows)
        values = []
        for i in range(batch_start + 1, batch_end + 1):
            category = categories[(i - 1) % len(categories)]
            values.append(f"({i}, {i}, '{category}', {i * 10.0})")

        insert_sql = f"""
            INSERT INTO test_data (id, value, category, amount)
            VALUES {', '.join(values)}
        """
        conn.execute(insert_sql)

    # Verify data was inserted
    result = conn.execute("SELECT COUNT(*) FROM test_data").fetchone()
    assert result[0] == num_rows, f"Expected {num_rows} rows, got {result[0]}"


def setup_test_data_with_join_matches(
    conn: duckdb.DuckDBPyConnection,
    num_rows: int = 1_000_000,  # noqa: ARG001
    join_matches: int = 1_000_000
) -> None:
    """Set up test data with specified number of join matches for JOIN tests.

    The JOIN query does: test_data JOIN test_data other ON test_data.id = other.id
    This is a self-join where each row matches itself, so the number of matches equals
    the number of rows. To vary join matches, we vary the number of rows in the table.

    Args:
        conn: DuckDB connection
        num_rows: Maximum number of rows (not used, kept for compatibility)
        join_matches: Number of rows to create (determines join matches)
    """
    # Create table
    conn.execute("""
        CREATE TABLE test_data (
            id INTEGER,
            value INTEGER,
            category VARCHAR,
            amount DOUBLE
        )
    """)

    categories = ["A", "B", "C", "D", "E"]

    # Use join_matches as the actual number of rows to create
    # This directly controls the number of join matches since it's a self-join on id=id
    actual_rows = min(join_matches, 1_000_000)  # Cap at 1M for performance

    # Insert data in batches
    batch_size = 10000 if actual_rows > 10000 else 100
    for batch_start in range(0, actual_rows, batch_size):
        batch_end = min(batch_start + batch_size, actual_rows)
        values = []
        for i in range(batch_start + 1, batch_end + 1):
            category = categories[(i - 1) % len(categories)]
            values.append(f"({i}, {i}, '{category}', {i * 10.0})")

        insert_sql = f"""
            INSERT INTO test_data (id, value, category, amount)
            VALUES {', '.join(values)}
        """
        conn.execute(insert_sql)

    # Verify data was inserted
    result = conn.execute("SELECT COUNT(*) FROM test_data").fetchone()
    assert result[0] == actual_rows, f"Expected {actual_rows} rows, got {result[0]}"


def setup_test_data_with_join_group_by(
    conn: duckdb.DuckDBPyConnection,
    join_count: int,
    num_rows: int = 1_000,
) -> None:
    """Set up data for JOIN->GROUP_BY microbenchmarks.

    Creates one policy source table named ``test_data`` and ``join_count`` auxiliary
    tables named ``join_data_1`` ... ``join_data_{join_count}`` with matching schema/data.
    The policy targets only ``test_data``, so it is applied exactly once.

    Args:
        conn: DuckDB connection
        join_count: Number of joined auxiliary tables
        num_rows: Number of rows in each table
    """
    if join_count < 1:
        raise ValueError(f"join_count must be >= 1, got {join_count}")

    setup_test_data(conn, num_rows=num_rows)

    for idx in range(1, join_count + 1):
        conn.execute(
            f"""
            CREATE TABLE join_data_{idx} AS
            SELECT id, value, category, amount
            FROM test_data
            """
        )


def get_data_statistics(conn: duckdb.DuckDBPyConnection) -> dict:
    """Get statistics about the test data.

    Args:
        conn: DuckDB connection

    Returns:
        Dictionary with data statistics
    """
    stats = {}

    # Total rows
    result = conn.execute("SELECT COUNT(*) FROM test_data").fetchone()
    stats["total_rows"] = result[0]

    # Value range
    result = conn.execute("SELECT MIN(value), MAX(value) FROM test_data").fetchone()
    stats["min_value"] = result[0]
    stats["max_value"] = result[1]

    # Category distribution
    result = conn.execute("""
        SELECT category, COUNT(*)
        FROM test_data
        GROUP BY category
        ORDER BY category
    """).fetchall()
    stats["category_counts"] = dict(result)

    return stats
