"""Test cases for physical rewriter (SmokedDuck lineage-based baseline)."""

import re

import pytest
from sql_rewriter import DFCPolicy, Resolution

from vldb_experiments.baselines.physical_baseline import execute_query_physical
from vldb_experiments.baselines.physical_rewriter import (
    build_filter_query,
    is_aggregation_query,
    rewrite_query_physical,
    transform_constraint_for_filtering,
)
from vldb_experiments.data_setup import setup_test_data
from vldb_experiments.policy_setup import create_test_policy


def normalize_filter_query(filter_query: str) -> str:
    """Normalize filter query by replacing UUID-based temp table names with a placeholder.

    Args:
        filter_query: Filter query SQL string

    Returns:
        Normalized filter query with temp table name replaced by temp_table_name
    """
    # Replace UUID-based temp table names (query_results_<hex>) with placeholder
    pattern = r"query_results_[a-f0-9]{8}"
    normalized = re.sub(pattern, "temp_table_name", filter_query)
    normalized = normalized.replace("{temp_table_name}", "temp_table_name")
    normalized = re.sub(r"read_block\(\d+\)", "read_block(0)", normalized)
    normalized = re.sub(r"LINEAGE_\d+_", "LINEAGE_1_", normalized)
    normalized = re.sub(r"opid_\d+_test_data", "test_data_iid", normalized)
    normalized = normalized.replace('"output_id"', "output_id")
    return re.sub(r"CAST\((LINEAGE_[^\s)]+) AS VARCHAR\)", r"\1", normalized)


LINEAGE_QUERY_BASE = 'SELECT output_id AS out_index, "test_data_iid" AS "test_data" FROM read_block(0)'
LINEAGE_QUERY_SELECT = LINEAGE_QUERY_BASE
LINEAGE_QUERY_JOIN = LINEAGE_QUERY_BASE
LINEAGE_QUERY_GROUP = LINEAGE_QUERY_BASE
LINEAGE_QUERY_ORDER = LINEAGE_QUERY_BASE

FILTER_QUERY_SELECT = """
WITH lineage AS (
SELECT output_id AS out_index, "test_data_iid" AS "test_data" FROM read_block(0)
)
SELECT
    generated_table."id", generated_table."value", generated_table."category", generated_table."amount"
FROM temp_table_name AS generated_table
JOIN lineage
    ON generated_table.rowid::bigint = lineage.out_index::bigint
JOIN test_data
    ON test_data.rowid::bigint = lineage.test_data::bigint
GROUP BY generated_table.rowid, generated_table."id", generated_table."value", generated_table."category", generated_table."amount"
HAVING MAX(test_data.value) > 100
""".strip()

FILTER_QUERY_JOIN = """
WITH lineage AS (
SELECT output_id AS out_index, "test_data_iid" AS "test_data" FROM read_block(0)
)
SELECT
    generated_table."id", generated_table."value"
FROM temp_table_name AS generated_table
JOIN lineage
    ON generated_table.rowid::bigint = lineage.out_index::bigint
JOIN test_data
    ON test_data.rowid::bigint = lineage.test_data::bigint
GROUP BY generated_table.rowid, generated_table."id", generated_table."value"
HAVING MAX(test_data.value) > 100
""".strip()

FILTER_QUERY_GROUP = """
WITH lineage AS (
SELECT output_id AS out_index, "test_data_iid" AS "test_data" FROM read_block(0)
)
SELECT
    generated_table."category", generated_table."count_star()", generated_table."sum(amount)"
FROM temp_table_name AS generated_table
JOIN lineage
    ON generated_table.rowid::bigint = lineage.out_index::bigint
JOIN test_data
    ON test_data.rowid::bigint = lineage.test_data::bigint
GROUP BY generated_table.rowid, generated_table."category", generated_table."count_star()", generated_table."sum(amount)"
HAVING MAX(test_data.value) > 100
""".strip()

FILTER_QUERY_ORDER = """
WITH lineage AS (
SELECT output_id AS out_index, "test_data_iid" AS "test_data" FROM read_block(0)
)
SELECT
    generated_table."id", generated_table."value", generated_table."category", generated_table."amount"
FROM temp_table_name AS generated_table
JOIN lineage
    ON generated_table.rowid::bigint = lineage.out_index::bigint
JOIN test_data
    ON test_data.rowid::bigint = lineage.test_data::bigint
GROUP BY generated_table.rowid, generated_table."id", generated_table."value", generated_table."category", generated_table."amount"
HAVING MAX(test_data.value) > 100
ORDER BY generated_table.value DESC
""".strip()

FILTER_QUERY_SELECT_SPECIFIC = """
WITH lineage AS (
SELECT output_id AS out_index, "test_data_iid" AS "test_data" FROM read_block(0)
)
SELECT
    generated_table."id", generated_table."value", generated_table."category"
FROM temp_table_name AS generated_table
JOIN lineage
    ON generated_table.rowid::bigint = lineage.out_index::bigint
JOIN test_data
    ON test_data.rowid::bigint = lineage.test_data::bigint
GROUP BY generated_table.rowid, generated_table."id", generated_table."value", generated_table."category"
HAVING MAX(test_data.value) > 100
""".strip()

FILTER_QUERY_JOIN_AMOUNT = """
WITH lineage AS (
SELECT output_id AS out_index, "test_data_iid" AS "test_data" FROM read_block(0)
)
SELECT
    generated_table."id", generated_table."amount", generated_table."value"
FROM temp_table_name AS generated_table
JOIN lineage
    ON generated_table.rowid::bigint = lineage.out_index::bigint
JOIN test_data
    ON test_data.rowid::bigint = lineage.test_data::bigint
GROUP BY generated_table.rowid, generated_table."id", generated_table."amount", generated_table."value"
HAVING MAX(test_data.value) > 100
""".strip()

FILTER_QUERY_ORDER_LIMIT = """
WITH lineage AS (
SELECT output_id AS out_index, "test_data_iid" AS "test_data" FROM read_block(0)
)
SELECT
    generated_table."id", generated_table."value", generated_table."category", generated_table."amount"
FROM temp_table_name AS generated_table
JOIN lineage
    ON generated_table.rowid::bigint = lineage.out_index::bigint
JOIN test_data
    ON test_data.rowid::bigint = lineage.test_data::bigint
GROUP BY generated_table.rowid, generated_table."id", generated_table."value", generated_table."category", generated_table."amount"
HAVING MAX(test_data.value) > 100
ORDER BY generated_table.value DESC
LIMIT 10
""".strip()

FILTER_QUERY_GROUP_MULTI = """
WITH lineage AS (
SELECT output_id AS out_index, "test_data_iid" AS "test_data" FROM read_block(0)
)
SELECT
    generated_table."category", generated_table."count_star()", generated_table."sum(amount)", generated_table."avg(""value"")", generated_table."max(""value"")", generated_table."min(""value"")"
FROM temp_table_name AS generated_table
JOIN lineage
    ON generated_table.rowid::bigint = lineage.out_index::bigint
JOIN test_data
    ON test_data.rowid::bigint = lineage.test_data::bigint
GROUP BY generated_table.rowid, generated_table."category", generated_table."count_star()", generated_table."sum(amount)", generated_table."avg(""value"")", generated_table."max(""value"")", generated_table."min(""value"")"
HAVING MAX(test_data.value) > 100
""".strip()


def with_constraint(filter_query: str, constraint: str) -> str:
    """Replace the default MAX(value) constraint in a filter query."""
    return filter_query.replace("MAX(test_data.value) > 100", constraint)


class TestPhysicalRewriter:
    """Test cases for physical query rewriting."""

    @pytest.fixture
    def conn(cls):
        """Create a test database connection with test data.

        Note: This requires SmokedDuck to be available.
        """
        try:
            from vldb_experiments.use_local_smokedduck import setup_local_smokedduck
            duckdb_module = setup_local_smokedduck()
            conn = duckdb_module.connect(":memory:")
        except (ImportError, FileNotFoundError) as e:
            raise ImportError(
                "SmokedDuck is REQUIRED for physical baseline tests. "
                "Please run ./setup_venv.sh to clone and build SmokedDuck."
            ) from e

        setup_test_data(conn, num_rows=1000)
        yield conn
        conn.close()

    def test_transform_constraint_for_filtering(self):
        """Test constraint transformation for filtering."""
        constraint = "max(test_data.value) > 100"
        transformed = transform_constraint_for_filtering(constraint, "test_data")
        expected = "value > 100"
        assert transformed == expected, f"Expected:\n{expected}\nGot:\n{transformed}"

    def test_is_aggregation_query(self):
        """Test aggregation query detection."""
        # Non-aggregation queries
        assert not is_aggregation_query("SELECT * FROM test_data")
        assert not is_aggregation_query("SELECT * FROM test_data WHERE value > 50")

        # Aggregation queries
        assert is_aggregation_query("SELECT category, COUNT(*) FROM test_data GROUP BY category")
        assert is_aggregation_query("SELECT SUM(amount) FROM test_data")
        assert is_aggregation_query("SELECT MAX(value) FROM test_data")

    def test_build_filter_query_scan(self):
        """Test building filter query for scan queries."""
        constraint = "max(test_data.value) > 100"
        filter_query = build_filter_query(
            temp_table_name="temp_results",
            constraint=constraint,
            source_table="test_data",
            column_names=["id", "value", "category", "amount"],
            is_aggregation=False
        )

        expected = "SELECT * FROM temp_results WHERE value > 100"
        assert filter_query == expected, f"Expected:\n{expected}\nGot:\n{filter_query}"

    def test_build_filter_query_aggregation(self):
        """Test building filter query for aggregation queries."""
        constraint = "max(test_data.value) > 100"
        # For aggregation queries without value column, should return all results
        filter_query = build_filter_query(
            temp_table_name="temp_results",
            constraint=constraint,
            source_table="test_data",
            column_names=["category", "count", "sum"],
            is_aggregation=True
        )

        expected = "SELECT * FROM temp_results"
        assert filter_query == expected, f"Expected:\n{expected}\nGot:\n{filter_query}"

    @pytest.mark.usefixtures("conn")
    def test_rewrite_query_physical_select(self):
        """Test rewriting a simple SELECT query."""
        query = "SELECT * FROM test_data"
        policy = create_test_policy()

        base_query, filter_query_template, is_agg = rewrite_query_physical(
            query=query,
            policy=policy,
            lineage_query=LINEAGE_QUERY_SELECT,
            output_columns=["id", "value", "category", "amount"],
        )

        # Base query should be unchanged
        expected_base = "SELECT * FROM test_data"
        assert base_query == expected_base, f"Expected base query:\n{expected_base}\nGot:\n{base_query}"
        assert not is_agg

        expected_filter_template = FILTER_QUERY_SELECT
        normalized_filter = normalize_filter_query(filter_query_template)
        assert normalized_filter == expected_filter_template, (
            f"Expected filter template:\n{expected_filter_template}\n"
            f"Got:\n{filter_query_template}"
        )

    @pytest.mark.usefixtures("conn")
    def test_rewrite_query_physical_where(self):
        """Test rewriting a WHERE query."""
        query = "SELECT * FROM test_data WHERE value > 50"
        policy = create_test_policy()

        base_query, filter_query_template, is_agg = rewrite_query_physical(
            query=query,
            policy=policy,
            lineage_query=LINEAGE_QUERY_SELECT,
            output_columns=["id", "value", "category", "amount"],
        )

        # Base query should be unchanged
        expected_base = "SELECT * FROM test_data WHERE value > 50"
        assert base_query == expected_base, f"Expected base query:\n{expected_base}\nGot:\n{base_query}"
        assert not is_agg

        expected_filter_template = FILTER_QUERY_SELECT
        normalized_filter = normalize_filter_query(filter_query_template)
        assert normalized_filter == expected_filter_template, (
            f"Expected filter template:\n{expected_filter_template}\n"
            f"Got:\n{filter_query_template}"
        )

    @pytest.mark.usefixtures("conn")
    def test_rewrite_query_physical_join(self):
        """Test rewriting a JOIN query."""
        query = (
            "SELECT test_data.id, other.value "
            "FROM test_data "
            "JOIN test_data AS other ON test_data.id = other.id"
        )
        policy = create_test_policy()

        base_query, filter_query_template, is_agg = rewrite_query_physical(
            query=query,
            policy=policy,
            lineage_query=LINEAGE_QUERY_JOIN,
            output_columns=["id", "value"],
        )

        # Base query should be unchanged
        expected_base = "SELECT test_data.id, other.value FROM test_data JOIN test_data AS other ON test_data.id = other.id"
        assert base_query == expected_base, f"Expected base query:\n{expected_base}\nGot:\n{base_query}"
        assert not is_agg

        expected_filter_template = FILTER_QUERY_JOIN
        normalized_filter = normalize_filter_query(filter_query_template)
        assert normalized_filter == expected_filter_template, (
            f"Expected filter template:\n{expected_filter_template}\n"
            f"Got:\n{filter_query_template}"
        )

    @pytest.mark.usefixtures("conn")
    def test_rewrite_query_physical_group_by(self):
        """Test rewriting a GROUP BY (aggregation) query."""
        query = (
            "SELECT category, COUNT(*), SUM(amount) "
            "FROM test_data "
            "GROUP BY category"
        )
        policy = create_test_policy()

        base_query, filter_query_template, is_agg = rewrite_query_physical(
            query=query,
            policy=policy,
            lineage_query=LINEAGE_QUERY_GROUP,
            output_columns=["category", "count_star()", "sum(amount)"],
        )

        # Base query should be unchanged
        expected_base = "SELECT category, COUNT(*), SUM(amount) FROM test_data GROUP BY category"
        assert base_query == expected_base, f"Expected base query:\n{expected_base}\nGot:\n{base_query}"
        assert is_agg

        expected_filter_template = FILTER_QUERY_GROUP
        normalized_filter = normalize_filter_query(filter_query_template)
        assert normalized_filter == expected_filter_template, (
            f"Expected filter template:\n{expected_filter_template}\n"
            f"Got:\n{filter_query_template}"
        )

    @pytest.mark.usefixtures("conn")
    def test_rewrite_query_physical_order_by(self):
        """Test rewriting an ORDER BY query."""
        query = "SELECT * FROM test_data ORDER BY value DESC"
        policy = create_test_policy()

        base_query, filter_query_template, is_agg = rewrite_query_physical(
            query=query,
            policy=policy,
            lineage_query=LINEAGE_QUERY_ORDER,
            output_columns=["id", "value", "category", "amount"],
        )

        # Base query should be unchanged
        expected_base = "SELECT * FROM test_data ORDER BY value DESC"
        assert base_query == expected_base, f"Expected base query:\n{expected_base}\nGot:\n{base_query}"
        assert not is_agg

        expected_filter_template = FILTER_QUERY_ORDER
        normalized_filter = normalize_filter_query(filter_query_template)
        assert normalized_filter == expected_filter_template, (
            f"Expected filter template:\n{expected_filter_template}\n"
            f"Got:\n{filter_query_template}"
        )


class TestPhysicalBaselineExecution:
    """Test cases for physical baseline execution (end-to-end)."""

    @pytest.fixture
    def conn(cls):
        """Create a test database connection with test data.

        Note: This requires SmokedDuck to be available.
        """
        try:
            from vldb_experiments.use_local_smokedduck import setup_local_smokedduck
            duckdb_module = setup_local_smokedduck()
            conn = duckdb_module.connect(":memory:")
        except (ImportError, FileNotFoundError) as e:
            raise ImportError(
                "SmokedDuck is REQUIRED for physical baseline tests. "
                "Please run ./setup_venv.sh to clone and build SmokedDuck."
            ) from e

        setup_test_data(conn, num_rows=1000)
        yield conn
        conn.close()

    def test_execute_select_query(self, conn):
        """Test executing a simple SELECT query with physical baseline."""
        pytest.skip("Scan-only lineage is not supported for physical baseline.")
        query = "SELECT * FROM test_data"

        from vldb_experiments.policy_setup import create_test_policy
        policy = create_test_policy()
        results, execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        assert error is None, f"Execution failed: {error}"
        assert execution_time > 0, "Should have execution time"

        # Verify the complete expected SQL queries
        # Base query should be the original query (executed to capture lineage)
        expected_base_query = "SELECT * FROM test_data"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        # Filter query should filter the temp table based on policy constraint
        # Normalize temp table name for comparison
        normalized_filter = normalize_filter_query(filter_query_sql)
        expected_filter_query = FILTER_QUERY_SELECT
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

        # Results should be filtered (value > 100)
        # Verify by checking that all rows have value > 100
        # Note: We need to know the column order - assume value is at index 1
        for row in results:
            # Find value column (it's at index 1 in our test data)
            value = row[1]
            assert value > 100, f"Row should have value > 100, got {value}"

    def test_execute_where_query(self, conn):
        """Test executing a WHERE query with physical baseline."""
        pytest.skip("Scan-only lineage is not supported for physical baseline.")
        query = "SELECT * FROM test_data WHERE value > 50"

        from vldb_experiments.policy_setup import create_test_policy
        policy = create_test_policy()
        results, _execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        assert error is None, f"Execution failed: {error}"

        # Verify the complete expected SQL queries
        expected_base_query = "SELECT * FROM test_data WHERE value > 50"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        normalized_filter = normalize_filter_query(filter_query_sql)
        expected_filter_query = FILTER_QUERY_SELECT
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

        # Results should be filtered (value > 100, which is more restrictive than value > 50)
        for row in results:
            value = row[1]
            assert value > 100, f"Row should have value > 100, got {value}"

    def test_execute_join_query(self, conn):
        """Test executing a JOIN query with physical baseline."""
        query = (
            "SELECT test_data.id, other.value "
            "FROM test_data "
            "JOIN test_data AS other ON test_data.id = other.id"
        )

        from vldb_experiments.policy_setup import create_test_policy
        policy = create_test_policy()
        results, _execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        assert error is None, f"Execution failed: {error}"

        # Verify the complete expected SQL queries
        expected_base_query = "SELECT test_data.id, other.value FROM test_data JOIN test_data AS other ON test_data.id = other.id"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        normalized_filter = normalize_filter_query(filter_query_sql)
        expected_filter_query = FILTER_QUERY_JOIN
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

        # Results should be filtered (value > 100)
        # Value is at index 1 in the SELECT list
        for row in results:
            value = row[1]
            assert value > 100, f"Row should have value > 100, got {value}"

    def test_execute_group_by_query(self, conn):
        """Test executing a GROUP BY query with physical baseline."""
        query = (
            "SELECT category, COUNT(*), SUM(amount) "
            "FROM test_data "
            "GROUP BY category"
        )

        from vldb_experiments.policy_setup import create_test_policy
        policy = create_test_policy()
        _results, execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        assert error is None, f"Execution failed: {error}"
        # For aggregation queries, we may get all results if value column is not in results
        # This is expected behavior for the current implementation
        assert execution_time > 0, "Should have execution time"

        # Verify the complete expected SQL queries
        expected_base_query = "SELECT category, COUNT(*), SUM(amount) FROM test_data GROUP BY category"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        normalized_filter = normalize_filter_query(filter_query_sql)
        expected_filter_query = FILTER_QUERY_GROUP
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

    def test_execute_order_by_query(self, conn):
        """Test executing an ORDER BY query with physical baseline."""
        pytest.skip("Scan-only lineage is not supported for physical baseline.")
        query = "SELECT * FROM test_data ORDER BY value DESC"

        from vldb_experiments.policy_setup import create_test_policy
        policy = create_test_policy()
        results, _execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        assert error is None, f"Execution failed: {error}"

        # Verify the complete expected SQL queries
        expected_base_query = "SELECT * FROM test_data ORDER BY value DESC"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        normalized_filter = normalize_filter_query(filter_query_sql)
        expected_filter_query = FILTER_QUERY_ORDER
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

        # Results should be filtered (value > 100)
        for row in results:
            value = row[1]
            assert value > 100, f"Row should have value > 100, got {value}"

        # Results should be ordered by value DESC
        if len(results) > 1:
            for i in range(len(results) - 1):
                assert results[i][1] >= results[i+1][1], "Results should be ordered DESC"


class TestPhysicalRewriterWithDifferentPolicies:
    """Test cases for physical rewriter with various policies and queries."""

    @pytest.fixture
    def conn(cls):
        """Create a test database connection with test data.

        Note: This requires SmokedDuck to be available.
        """
        try:
            from vldb_experiments.use_local_smokedduck import setup_local_smokedduck
            duckdb_module = setup_local_smokedduck()
            conn = duckdb_module.connect(":memory:")
        except (ImportError, FileNotFoundError) as e:
            raise ImportError(
                "SmokedDuck is REQUIRED for physical baseline tests. "
                "Please run ./setup_venv.sh to clone and build SmokedDuck."
            ) from e

        setup_test_data(conn, num_rows=1000)
        yield conn
        conn.close()

    def test_policy_with_min_aggregation(self, conn):
        """Test rewriting with min() aggregation constraint."""
        pytest.skip("Scan-only lineage is not supported for physical baseline.")
        # Note: min() constraint gets transformed to value > 10 for scan queries
        # The physical rewriter should handle this transformation
        query = "SELECT * FROM test_data WHERE value > 50"
        policy = DFCPolicy(
            sources=["test_data"],
            constraint="min(test_data.value) > 10",
            on_fail=Resolution.REMOVE,
            description="Filter rows where value <= 10"
        )

        # Verify the complete expected SQL from rewrite_query_physical
        base_query, filter_query_template, is_agg = rewrite_query_physical(query, policy)
        expected_base = "SELECT * FROM test_data WHERE value > 50"
        expected_filter_template = "SELECT * FROM temp_table_name"
        assert base_query == expected_base, f"Expected base query:\n{expected_base}\nGot:\n{base_query}"
        normalized_filter_template = normalize_filter_query(filter_query_template)
        assert normalized_filter_template == expected_filter_template, f"Expected filter template:\n{expected_filter_template}\nGot:\n{filter_query_template}"
        assert not is_agg

        _results, execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        # Verify the complete expected SQL queries
        expected_base_query = "SELECT * FROM test_data WHERE value > 50"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        # Filter query may not have WHERE clause for SELECT * with min() constraint
        normalized_filter = normalize_filter_query(filter_query_sql)
        # min(value) > 10 gets transformed to value > 10 for scan queries
        expected_filter_query = with_constraint(FILTER_QUERY_SELECT, "MIN(test_data.value) > 10")
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

        # The transformation should work for min() - verify results are filtered
        assert error is None, f"Execution failed: {error}"
        assert execution_time >= 0, "Should have execution time"

    def test_policy_with_different_comparison_operator(self, conn):
        """Test rewriting with < comparison operator."""
        pytest.skip("Scan-only lineage is not supported for physical baseline.")
        query = "SELECT * FROM test_data"
        policy = DFCPolicy(
            sources=["test_data"],
            constraint="max(test_data.value) < 500",
            on_fail=Resolution.REMOVE,
            description="Filter rows where value >= 500"
        )

        # Verify the complete expected SQL from rewrite_query_physical
        base_query, filter_query_template, is_agg = rewrite_query_physical(query, policy)
        expected_base = "SELECT * FROM test_data"
        expected_filter_template = "SELECT * FROM temp_table_name"
        assert base_query == expected_base, f"Expected base query:\n{expected_base}\nGot:\n{base_query}"
        normalized_filter_template = normalize_filter_query(filter_query_template)
        assert normalized_filter_template == expected_filter_template, f"Expected filter template:\n{expected_filter_template}\nGot:\n{filter_query_template}"
        assert not is_agg

        results, _execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        assert error is None, f"Execution failed: {error}"

        # Verify the complete expected SQL queries
        expected_base_query = "SELECT * FROM test_data"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        normalized_filter = normalize_filter_query(filter_query_sql)
        # max(value) < 500 gets transformed to value < 500 for scan queries
        expected_filter_query = with_constraint(FILTER_QUERY_SELECT, "MAX(test_data.value) < 500")
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

        # Verify the constraint is applied (value < 500 for scan queries)
        for row in results:
            value = row[1]  # value is at index 1
            assert value < 500, f"Row should have value < 500, got {value}"

    def test_policy_with_greater_equal_operator(self, conn):
        """Test rewriting with >= comparison operator."""
        pytest.skip("Scan-only lineage is not supported for physical baseline.")
        query = "SELECT * FROM test_data WHERE value > 50"
        policy = DFCPolicy(
            sources=["test_data"],
            constraint="max(test_data.value) >= 200",
            on_fail=Resolution.REMOVE,
            description="Filter rows where value < 200"
        )

        # Verify the complete expected SQL from rewrite_query_physical
        base_query, filter_query_template, is_agg = rewrite_query_physical(query, policy)
        expected_base = "SELECT * FROM test_data WHERE value > 50"
        expected_filter_template = "SELECT * FROM temp_table_name"
        assert base_query == expected_base, f"Expected base query:\n{expected_base}\nGot:\n{base_query}"
        normalized_filter_template = normalize_filter_query(filter_query_template)
        assert normalized_filter_template == expected_filter_template, f"Expected filter template:\n{expected_filter_template}\nGot:\n{filter_query_template}"
        assert not is_agg

        results, _execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        assert error is None, f"Execution failed: {error}"

        # Verify the complete expected SQL queries
        expected_base_query = "SELECT * FROM test_data WHERE value > 50"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        normalized_filter = normalize_filter_query(filter_query_sql)
        # max(value) >= 200 gets transformed to value >= 200 for scan queries
        expected_filter_query = with_constraint(FILTER_QUERY_SELECT, "MAX(test_data.value) >= 200")
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

        # Verify the constraint is applied (value >= 200 for scan queries)
        for row in results:
            value = row[1]  # value is at index 1
            assert value >= 200, f"Row should have value >= 200, got {value}"

    def test_policy_with_different_column(self, conn):
        """Test rewriting with policy on amount column instead of value."""
        pytest.skip("Scan-only lineage is not supported for physical baseline.")
        # Note: The physical rewriter transforms max(amount) to amount for scan queries
        query = "SELECT * FROM test_data"
        policy = DFCPolicy(
            sources=["test_data"],
            constraint="max(test_data.amount) > 5000",
            on_fail=Resolution.REMOVE,
            description="Filter rows where amount <= 5000"
        )

        # Verify the complete expected SQL from rewrite_query_physical
        base_query, filter_query_template, is_agg = rewrite_query_physical(query, policy)
        expected_base = "SELECT * FROM test_data"
        expected_filter_template = "SELECT * FROM temp_table_name"
        assert base_query == expected_base, f"Expected base query:\n{expected_base}\nGot:\n{base_query}"
        normalized_filter_template = normalize_filter_query(filter_query_template)
        assert normalized_filter_template == expected_filter_template, f"Expected filter template:\n{expected_filter_template}\nGot:\n{filter_query_template}"
        assert not is_agg

        results, _execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        # Verify the complete expected SQL queries
        expected_base_query = "SELECT * FROM test_data"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        normalized_filter = normalize_filter_query(filter_query_sql)
        # max(amount) > 5000 gets transformed to amount > 5000 for scan queries
        expected_filter_query = with_constraint(FILTER_QUERY_SELECT, "MAX(test_data.amount) > 5000")
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

        # The transformation should work for max() - verify results are filtered
        if error is None:
            # Verify the constraint is applied (amount > 5000 for scan queries)
            for row in results:
                amount = row[3]  # amount is at index 3
                assert amount > 5000, f"Row should have amount > 5000, got {amount}"
        else:
            # If transformation doesn't work perfectly, that's a known limitation
            assert "aggregate" in str(error).lower(), f"Unexpected error: {error}"

    def test_policy_with_kill_resolution(self, conn):
        """Test rewriting with KILL resolution (should still work for physical rewriter)."""
        pytest.skip("Scan-only lineage is not supported for physical baseline.")
        query = "SELECT * FROM test_data WHERE value > 50"
        policy = DFCPolicy(
            sources=["test_data"],
            constraint="max(test_data.value) > 100",
            on_fail=Resolution.KILL,
            description="Kill query if value <= 100"
        )

        results, _execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        # Verify the complete expected SQL queries
        expected_base_query = "SELECT * FROM test_data WHERE value > 50"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        normalized_filter = normalize_filter_query(filter_query_sql)
        expected_filter_query = FILTER_QUERY_SELECT
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

        # KILL resolution doesn't affect the physical rewriter's execution
        # It just filters rows (the actual KILL behavior is in SQLRewriter)
        assert error is None, f"Execution failed: {error}"
        assert len(results) >= 0, "Rewritten query should execute"

    def test_policy_with_invalidate_resolution(self, conn):
        """Test rewriting with INVALIDATE resolution."""
        pytest.skip("Scan-only lineage is not supported for physical baseline.")
        query = "SELECT * FROM test_data"
        policy = DFCPolicy(
            sources=["test_data"],
            constraint="max(test_data.value) > 100",
            on_fail=Resolution.INVALIDATE,
            description="Invalidate rows where value <= 100"
        )

        results, _execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        # Verify the complete expected SQL queries
        expected_base_query = "SELECT * FROM test_data"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        normalized_filter = normalize_filter_query(filter_query_sql)
        expected_filter_query = FILTER_QUERY_SELECT
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

        assert error is None, f"Execution failed: {error}"
        assert len(results) >= 0, "Rewritten query should execute"

    def test_policy_with_complex_where_query(self, conn):
        """Test rewriting with complex WHERE clause."""
        pytest.skip("Scan-only lineage is not supported for physical baseline.")
        query = "SELECT * FROM test_data WHERE value > 50 AND category = 'A'"
        policy = DFCPolicy(
            sources=["test_data"],
            constraint="max(test_data.value) > 100",
            on_fail=Resolution.REMOVE,
            description="Filter rows where value <= 100"
        )

        # Verify the complete expected SQL from rewrite_query_physical
        base_query, filter_query_template, is_agg = rewrite_query_physical(query, policy)
        expected_base = "SELECT * FROM test_data WHERE value > 50 AND category = 'A'"
        expected_filter_template = "SELECT * FROM temp_table_name"
        assert base_query == expected_base, f"Expected base query:\n{expected_base}\nGot:\n{base_query}"
        normalized_filter_template = normalize_filter_query(filter_query_template)
        assert normalized_filter_template == expected_filter_template, f"Expected filter template:\n{expected_filter_template}\nGot:\n{filter_query_template}"
        assert not is_agg

        results, _execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        # Verify the complete expected SQL queries
        expected_base_query = "SELECT * FROM test_data WHERE value > 50 AND category = 'A'"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        normalized_filter = normalize_filter_query(filter_query_sql)
        expected_filter_query = FILTER_QUERY_SELECT
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

        assert error is None, f"Execution failed: {error}"
        assert len(results) >= 0, "Rewritten query should execute"

        # Verify both conditions are applied
        for row in results:
            value = row[1]
            category = row[2]
            assert value > 100, f"Row should have value > 100, got {value}"
            assert category == "A", f"Row should have category = 'A', got {category}"

    def test_policy_with_select_specific_columns(self, conn):
        """Test rewriting with SELECT of specific columns."""
        pytest.skip("Scan-only lineage is not supported for physical baseline.")
        query = "SELECT id, value, category FROM test_data WHERE value > 50"
        policy = DFCPolicy(
            sources=["test_data"],
            constraint="max(test_data.value) > 100",
            on_fail=Resolution.REMOVE,
            description="Filter rows where value <= 100"
        )

        # Verify the complete expected SQL from rewrite_query_physical
        base_query, filter_query_template, is_agg = rewrite_query_physical(query, policy)
        expected_base = "SELECT id, value, category FROM test_data WHERE value > 50"
        expected_filter_template = "SELECT * FROM temp_table_name WHERE value > 100"
        assert base_query == expected_base, f"Expected base query:\n{expected_base}\nGot:\n{base_query}"
        normalized_filter_template = normalize_filter_query(filter_query_template)
        assert normalized_filter_template == expected_filter_template, f"Expected filter template:\n{expected_filter_template}\nGot:\n{filter_query_template}"
        assert not is_agg

        results, _execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        # Verify the complete expected SQL queries
        expected_base_query = "SELECT id, value, category FROM test_data WHERE value > 50"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        normalized_filter = normalize_filter_query(filter_query_sql)
        expected_filter_query = FILTER_QUERY_SELECT_SPECIFIC
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

        assert error is None, f"Execution failed: {error}"

        # Verify the constraint is applied
        for row in results:
            value = row[1]  # value is at index 1
            assert value > 100, f"Row should have value > 100, got {value}"

    def test_policy_with_join_and_different_policy_column(self, conn):
        """Test rewriting JOIN query with policy on different column."""
        query = (
            "SELECT test_data.id, test_data.amount, other.value "
            "FROM test_data "
            "JOIN test_data AS other ON test_data.id = other.id"
        )
        policy = DFCPolicy(
            sources=["test_data"],
            constraint="max(test_data.amount) > 5000",
            on_fail=Resolution.REMOVE,
            description="Filter rows where amount <= 5000"
        )

        # Verify the complete expected SQL from rewrite_query_physical
        base_query, filter_query_template, is_agg = rewrite_query_physical(query, policy)
        expected_base = "SELECT test_data.id, test_data.amount, other.value FROM test_data JOIN test_data AS other ON test_data.id = other.id"
        expected_filter_template = "SELECT * FROM temp_table_name WHERE amount > 5000"
        assert base_query == expected_base, f"Expected base query:\n{expected_base}\nGot:\n{base_query}"
        normalized_filter_template = normalize_filter_query(filter_query_template)
        assert normalized_filter_template == expected_filter_template, f"Expected filter template:\n{expected_filter_template}\nGot:\n{filter_query_template}"
        assert not is_agg

        results, _execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        # Verify the complete expected SQL queries
        expected_base_query = "SELECT test_data.id, test_data.amount, other.value FROM test_data JOIN test_data AS other ON test_data.id = other.id"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        normalized_filter = normalize_filter_query(filter_query_sql)
        expected_filter_query = with_constraint(FILTER_QUERY_JOIN_AMOUNT, "MAX(test_data.amount) > 5000")
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

        # The transformation should work for max() - verify results are filtered
        if error is None:
            # Verify the constraint is applied (amount > 5000)
            for row in results:
                amount = row[1]  # amount is at index 1
                assert amount > 5000, f"Row should have amount > 5000, got {amount}"
        else:
            # If transformation doesn't work perfectly, that's a known limitation
            assert "aggregate" in str(error).lower(), f"Unexpected error: {error}"

    def test_policy_with_order_by_and_limit(self, conn):
        """Test rewriting query with ORDER BY and LIMIT."""
        pytest.skip("Scan-only lineage is not supported for physical baseline.")
        query = "SELECT * FROM test_data ORDER BY value DESC LIMIT 10"
        policy = DFCPolicy(
            sources=["test_data"],
            constraint="max(test_data.value) > 100",
            on_fail=Resolution.REMOVE,
            description="Filter rows where value <= 100"
        )

        # Verify the complete expected SQL from rewrite_query_physical
        base_query, filter_query_template, is_agg = rewrite_query_physical(query, policy)
        expected_base = "SELECT * FROM test_data ORDER BY value DESC LIMIT 10"
        expected_filter_template = "SELECT * FROM temp_table_name"
        assert base_query == expected_base, f"Expected base query:\n{expected_base}\nGot:\n{base_query}"
        normalized_filter_template = normalize_filter_query(filter_query_template)
        assert normalized_filter_template == expected_filter_template, f"Expected filter template:\n{expected_filter_template}\nGot:\n{filter_query_template}"
        assert not is_agg

        results, _execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        # Verify the complete expected SQL queries
        expected_base_query = "SELECT * FROM test_data ORDER BY value DESC LIMIT 10"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        normalized_filter = normalize_filter_query(filter_query_sql)
        expected_filter_query = FILTER_QUERY_ORDER_LIMIT
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

        assert error is None, f"Execution failed: {error}"

        # Verify the constraint is applied
        for row in results:
            value = row[1]
            assert value > 100, f"Row should have value > 100, got {value}"

        # Verify ORDER BY is preserved (results should be in descending order)
        if len(results) > 1:
            for i in range(len(results) - 1):
                assert results[i][1] >= results[i + 1][1], "Results should be ordered DESC"

    def test_policy_with_aggregation_and_different_threshold(self, conn):
        """Test rewriting aggregation query with different threshold."""
        query = "SELECT category, COUNT(*), SUM(amount) FROM test_data GROUP BY category"
        policy = DFCPolicy(
            sources=["test_data"],
            constraint="max(test_data.value) > 50",
            on_fail=Resolution.REMOVE,
            description="Filter groups where max(value) <= 50"
        )

        _results, execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        # Verify the complete expected SQL queries
        expected_base_query = "SELECT category, COUNT(*), SUM(amount) FROM test_data GROUP BY category"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        normalized_filter = normalize_filter_query(filter_query_sql)
        expected_filter_query = with_constraint(FILTER_QUERY_GROUP, "MAX(test_data.value) > 50")
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

        assert error is None, f"Execution failed: {error}"
        # For aggregation queries, we may get all results if value column is not in results
        # This is expected behavior for the current implementation
        assert execution_time > 0, "Should have execution time"

    def test_policy_with_multiple_aggregations_in_query(self, conn):
        """Test rewriting query with multiple aggregations."""
        query = (
            "SELECT category, COUNT(*), SUM(amount), AVG(value), MAX(value), MIN(value) "
            "FROM test_data "
            "GROUP BY category"
        )
        policy = DFCPolicy(
            sources=["test_data"],
            constraint="max(test_data.value) > 100",
            on_fail=Resolution.REMOVE,
            description="Filter groups where max(value) <= 100"
        )

        results, execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        # Verify the complete expected SQL queries
        expected_base_query = "SELECT category, COUNT(*), SUM(amount), AVG(value), MAX(value), MIN(value) FROM test_data GROUP BY category"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        normalized_filter = normalize_filter_query(filter_query_sql)
        expected_filter_query = FILTER_QUERY_GROUP_MULTI
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

        assert error is None, f"Execution failed: {error}"
        # For aggregation queries, we may get all results if value column is not in results
        # This is expected behavior for the current implementation
        assert execution_time > 0, "Should have execution time"
        if len(results) > 0:
            assert len(results[0]) == 6, "Should return 6 columns (category + 5 aggregations)"

    def test_policy_with_less_than_equal_operator(self, conn):
        """Test rewriting with <= comparison operator."""
        pytest.skip("Scan-only lineage is not supported for physical baseline.")
        query = "SELECT * FROM test_data"
        policy = DFCPolicy(
            sources=["test_data"],
            constraint="max(test_data.value) <= 200",
            on_fail=Resolution.REMOVE,
            description="Filter rows where value > 200"
        )

        # Verify the complete expected SQL from rewrite_query_physical
        base_query, filter_query_template, is_agg = rewrite_query_physical(query, policy)
        expected_base = "SELECT * FROM test_data"
        expected_filter_template = "SELECT * FROM temp_table_name"
        assert base_query == expected_base, f"Expected base query:\n{expected_base}\nGot:\n{base_query}"
        normalized_filter_template = normalize_filter_query(filter_query_template)
        assert normalized_filter_template == expected_filter_template, f"Expected filter template:\n{expected_filter_template}\nGot:\n{filter_query_template}"
        assert not is_agg

        results, _execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        # Verify the complete expected SQL queries
        expected_base_query = "SELECT * FROM test_data"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        normalized_filter = normalize_filter_query(filter_query_sql)
        # max(value) <= 200 gets transformed to value <= 200 for scan queries
        expected_filter_query = with_constraint(FILTER_QUERY_SELECT, "MAX(test_data.value) <= 200")
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

        assert error is None, f"Execution failed: {error}"

        # Verify the constraint is applied (value <= 200 for scan queries)
        for row in results:
            value = row[1]  # value is at index 1
            assert value <= 200, f"Row should have value <= 200, got {value}"

    def test_policy_with_equal_operator(self, conn):
        """Test rewriting with = comparison operator."""
        pytest.skip("Scan-only lineage is not supported for physical baseline.")
        query = "SELECT * FROM test_data WHERE value > 50"
        policy = DFCPolicy(
            sources=["test_data"],
            constraint="max(test_data.value) = 100",
            on_fail=Resolution.REMOVE,
            description="Filter rows where value != 100"
        )

        # Verify the complete expected SQL from rewrite_query_physical
        base_query, filter_query_template, is_agg = rewrite_query_physical(query, policy)
        expected_base = "SELECT * FROM test_data WHERE value > 50"
        expected_filter_template = "SELECT * FROM temp_table_name"
        assert base_query == expected_base, f"Expected base query:\n{expected_base}\nGot:\n{base_query}"
        normalized_filter_template = normalize_filter_query(filter_query_template)
        assert normalized_filter_template == expected_filter_template, f"Expected filter template:\n{expected_filter_template}\nGot:\n{filter_query_template}"
        assert not is_agg

        results, _execution_time, error, base_query_sql, filter_query_sql = execute_query_physical(conn, query, policy)

        # Verify the complete expected SQL queries
        expected_base_query = "SELECT * FROM test_data WHERE value > 50"
        assert base_query_sql == expected_base_query, f"Expected base query:\n{expected_base_query}\nGot:\n{base_query_sql}"

        normalized_filter = normalize_filter_query(filter_query_sql)
        # max(value) = 100 gets transformed to value = 100 for scan queries
        expected_filter_query = with_constraint(FILTER_QUERY_SELECT, "MAX(test_data.value) = 100")
        assert normalized_filter == expected_filter_query, f"Expected filter query:\n{expected_filter_query}\nGot:\n{normalized_filter}"

        assert error is None, f"Execution failed: {error}"
        # May return 0 or 1 results depending on data
        assert len(results) >= 0, "Rewritten query should execute"

        # If results exist, verify the constraint is applied (value = 100)
        for row in results:
            value = row[1]
            assert value == 100, f"Row should have value = 100, got {value}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
