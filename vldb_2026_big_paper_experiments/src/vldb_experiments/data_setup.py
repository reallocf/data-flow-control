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
    num_rows: int = 1_000_000,
    join_matches: int = 1_000_000
) -> None:
    """Set up test data with specified number of join matches for JOIN tests.

    The JOIN query does: test_data JOIN join_data other ON test_data.id = other.id.
    `test_data` is always created with `num_rows` rows (default 1,000,000).
    `join_data` is created with `join_matches` rows so that join cardinality is
    controlled by the second table size.

    Args:
        conn: DuckDB connection
        num_rows: Number of rows in primary table `test_data`
        join_matches: Number of rows in secondary table `join_data`
    """
    # Create primary table with fixed cardinality.
    conn.execute("""
        CREATE TABLE test_data (
            id INTEGER,
            value INTEGER,
            category VARCHAR,
            amount DOUBLE
        )
    """)

    categories = ["A", "B", "C", "D", "E"]

    # Insert primary table rows.
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

    # Create secondary join table.
    conn.execute("""
        CREATE TABLE join_data (
            id INTEGER,
            value INTEGER
        )
    """)

    secondary_rows = min(join_matches, num_rows)
    batch_size = 10000 if secondary_rows > 10000 else 100
    for batch_start in range(0, secondary_rows, batch_size):
        batch_end = min(batch_start + batch_size, secondary_rows)
        values = []
        for i in range(batch_start + 1, batch_end + 1):
            values.append(f"({i}, {i})")

        insert_sql = f"""
            INSERT INTO join_data (id, value)
            VALUES {', '.join(values)}
        """
        conn.execute(insert_sql)

    # Verify data was inserted.
    result = conn.execute("SELECT COUNT(*) FROM test_data").fetchone()
    assert result[0] == num_rows, f"Expected {num_rows} rows in test_data, got {result[0]}"
    result = conn.execute("SELECT COUNT(*) FROM join_data").fetchone()
    assert result[0] == secondary_rows, f"Expected {secondary_rows} rows in join_data, got {result[0]}"


def setup_join_data_only(
    conn: duckdb.DuckDBPyConnection,
    join_matches: int = 1_000_000,
    table_name: str = "join_data",
) -> None:
    """Create only the secondary join table used by JOIN microbenchmarks.

    Args:
        conn: DuckDB connection
        join_matches: Number of rows in secondary table `join_data`
        table_name: Name of secondary join table
    """
    conn.execute(
        f"""
        CREATE TABLE {table_name} (
            id INTEGER,
            value INTEGER
        )
        """
    )

    batch_size = 10000 if join_matches > 10000 else 100
    for batch_start in range(0, join_matches, batch_size):
        batch_end = min(batch_start + batch_size, join_matches)
        values = []
        for i in range(batch_start + 1, batch_end + 1):
            values.append(f"({i}, {i})")

        insert_sql = f"""
            INSERT INTO {table_name} (id, value)
            VALUES {', '.join(values)}
        """
        conn.execute(insert_sql)

    result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    assert result[0] == join_matches, f"Expected {join_matches} rows in {table_name}, got {result[0]}"


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
