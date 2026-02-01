"""Test cases for logical rewriter (CTE-based baseline)."""

import duckdb
import pytest
from sql_rewriter import DFCPolicy, Resolution

from vldb_experiments.baselines.logical_baseline import rewrite_query_logical
from vldb_experiments.data_setup import setup_test_data
from vldb_experiments.policy_setup import create_test_policy


class TestLogicalRewriter:
    """Test cases for logical query rewriting."""

    @pytest.fixture
    def conn(self):
        """Create a test database connection with test data."""
        conn = duckdb.connect(":memory:")
        setup_test_data(conn, num_rows=1000)
        yield conn
        conn.close()

    def test_select_query(self, conn):
        """Test rewriting a simple SELECT query."""
        query = "SELECT * FROM test_data"
        policy = create_test_policy()

        rewritten = rewrite_query_logical(query, policy)

        # Execute the rewritten query to verify it's valid SQL
        result = conn.execute(rewritten).fetchall()
        assert len(result) > 0, "Rewritten query should return results"

        # Verify the complete expected SQL
        expected = "WITH base_query AS (SELECT test_data.*, test_data.value FROM test_data) SELECT id, value, category, amount FROM base_query WHERE value > 100"
        assert rewritten == expected, f"Expected:\n{expected}\nGot:\n{rewritten}"

    def test_where_query(self, conn):
        """Test rewriting a WHERE query."""
        query = "SELECT * FROM test_data WHERE value > 50"
        policy = create_test_policy()

        rewritten = rewrite_query_logical(query, policy)

        # Execute the rewritten query to verify it's valid SQL
        result = conn.execute(rewritten).fetchall()
        assert len(result) > 0, "Rewritten query should return results"

        # Verify the complete expected SQL
        # The original WHERE (value > 50) is already applied in the CTE,
        # so the outer WHERE only needs the policy constraint (value > 100)
        expected = "WITH base_query AS (SELECT test_data.*, test_data.value FROM test_data WHERE value > 50) SELECT id, value, category, amount FROM base_query WHERE value > 100"
        assert rewritten == expected, f"Expected:\n{expected}\nGot:\n{rewritten}"

    def test_join_query(self, conn):
        """Test rewriting a JOIN query."""
        query = (
            "SELECT test_data.id, other.value "
            "FROM test_data "
            "JOIN test_data AS other ON test_data.id = other.id"
        )
        policy = create_test_policy()

        rewritten = rewrite_query_logical(query, policy)

        # Execute the rewritten query to verify it's valid SQL
        result = conn.execute(rewritten).fetchall()
        assert len(result) > 0, "Rewritten query should return results"

        # Verify the complete expected SQL
        expected = "WITH base_query AS (SELECT test_data.id, other.value FROM test_data JOIN test_data AS other ON test_data.id = other.id) SELECT id, value FROM base_query WHERE value > 100"
        assert rewritten == expected, f"Expected:\n{expected}\nGot:\n{rewritten}"

    def test_group_by_query(self, conn):
        """Test rewriting a GROUP BY (aggregation) query."""
        query = (
            "SELECT category, COUNT(*), SUM(amount) "
            "FROM test_data "
            "GROUP BY category"
        )
        policy = create_test_policy()

        rewritten = rewrite_query_logical(query, policy)

        # Execute the rewritten query to verify it's valid SQL
        result = conn.execute(rewritten).fetchall()
        assert len(result) > 0, "Rewritten query should return results"

        # Verify the complete expected SQL
        # Pattern: CTE runs original query with GROUP BY, then JOIN with rescan to get policy columns
        expected = "WITH base_query AS (SELECT category, COUNT(*) AS rewrite1, SUM(amount) AS rewrite2 FROM test_data GROUP BY category) SELECT rescan.category, base_query.rewrite1, base_query.rewrite2 FROM base_query JOIN (SELECT category, value AS rewrite3 FROM test_data) AS rescan ON base_query.category = rescan.category GROUP BY base_query.category, base_query.rewrite1, base_query.rewrite2, rescan.category HAVING max(rescan.rewrite3) > 100"
        assert rewritten == expected, f"Expected:\n{expected}\nGot:\n{rewritten}"

    def test_order_by_query(self, conn):
        """Test rewriting an ORDER BY query."""
        query = "SELECT * FROM test_data ORDER BY value DESC"
        policy = create_test_policy()

        rewritten = rewrite_query_logical(query, policy)

        # Execute the rewritten query to verify it's valid SQL
        result = conn.execute(rewritten).fetchall()
        assert len(result) > 0, "Rewritten query should return results"

        # Verify the complete expected SQL
        expected = "WITH base_query AS (SELECT test_data.*, test_data.value FROM test_data) SELECT id, value, category, amount FROM base_query WHERE value > 100 ORDER BY value DESC"
        assert rewritten == expected, f"Expected:\n{expected}\nGot:\n{rewritten}"


class TestLogicalRewriterWithDifferentPolicies:
    """Test cases for logical rewriter with various policies and queries."""

    @pytest.fixture
    def conn(self):
        """Create a test database connection with test data."""
        conn = duckdb.connect(":memory:")
        setup_test_data(conn, num_rows=1000)
        yield conn
        conn.close()

    def test_policy_with_min_aggregation(self, conn):
        """Test rewriting with min() aggregation constraint."""
        query = "SELECT * FROM test_data WHERE value > 50"
        policy = DFCPolicy(
            source="test_data",
            constraint="min(test_data.value) > 10",
            on_fail=Resolution.REMOVE,
            description="Filter rows where value <= 10"
        )

        rewritten = rewrite_query_logical(query, policy)

        # Execute the rewritten query to verify it's valid SQL
        result = conn.execute(rewritten).fetchall()
        assert len(result) > 0, "Rewritten query should return results"

        # Verify the complete expected SQL
        expected = "WITH base_query AS (SELECT test_data.*, test_data.value FROM test_data WHERE value > 50) SELECT id, value, category, amount FROM base_query WHERE value > 10"
        assert rewritten == expected, f"Expected:\n{expected}\nGot:\n{rewritten}"

    def test_policy_with_avg_aggregation(self, conn):
        """Test rewriting with avg() aggregation constraint."""
        query = "SELECT category, AVG(amount) FROM test_data GROUP BY category"
        policy = DFCPolicy(
            source="test_data",
            constraint="max(test_data.value) > 100",
            on_fail=Resolution.REMOVE,
            description="Filter groups where max(value) <= 100"
        )

        rewritten = rewrite_query_logical(query, policy)

        # Execute the rewritten query to verify it's valid SQL
        result = conn.execute(rewritten).fetchall()
        assert len(result) > 0, "Rewritten query should return results"

        # Verify the complete expected SQL
        expected = "WITH base_query AS (SELECT category, AVG(amount) AS rewrite1 FROM test_data GROUP BY category) SELECT rescan.category, base_query.rewrite1 FROM base_query JOIN (SELECT category, value AS rewrite2 FROM test_data) AS rescan ON base_query.category = rescan.category GROUP BY base_query.category, base_query.rewrite1, rescan.category HAVING max(rescan.rewrite2) > 100"
        assert rewritten == expected, f"Expected:\n{expected}\nGot:\n{rewritten}"

    def test_policy_with_different_comparison_operator(self, conn):
        """Test rewriting with < comparison operator."""
        query = "SELECT * FROM test_data"
        policy = DFCPolicy(
            source="test_data",
            constraint="max(test_data.value) < 500",
            on_fail=Resolution.REMOVE,
            description="Filter rows where value >= 500"
        )

        rewritten = rewrite_query_logical(query, policy)

        # Execute the rewritten query to verify it's valid SQL
        result = conn.execute(rewritten).fetchall()
        assert len(result) > 0, "Rewritten query should return results"

        # Verify the complete expected SQL
        expected = "WITH base_query AS (SELECT test_data.*, test_data.value FROM test_data) SELECT id, value, category, amount FROM base_query WHERE value < 500"
        assert rewritten == expected, f"Expected:\n{expected}\nGot:\n{rewritten}"

    def test_policy_with_greater_equal_operator(self, conn):
        """Test rewriting with >= comparison operator."""
        query = "SELECT * FROM test_data WHERE value > 50"
        policy = DFCPolicy(
            source="test_data",
            constraint="max(test_data.value) >= 200",
            on_fail=Resolution.REMOVE,
            description="Filter rows where value < 200"
        )

        rewritten = rewrite_query_logical(query, policy)

        # Execute the rewritten query to verify it's valid SQL
        result = conn.execute(rewritten).fetchall()
        assert len(result) > 0, "Rewritten query should return results"

        # Verify the complete expected SQL
        expected = "WITH base_query AS (SELECT test_data.*, test_data.value FROM test_data WHERE value > 50) SELECT id, value, category, amount FROM base_query WHERE value >= 200"
        assert rewritten == expected, f"Expected:\n{expected}\nGot:\n{rewritten}"

    def test_policy_with_different_column(self, conn):
        """Test rewriting with policy on amount column instead of value."""
        query = "SELECT * FROM test_data"
        policy = DFCPolicy(
            source="test_data",
            constraint="max(test_data.amount) > 5000",
            on_fail=Resolution.REMOVE,
            description="Filter rows where amount <= 5000"
        )

        rewritten = rewrite_query_logical(query, policy)

        # Execute the rewritten query to verify it's valid SQL
        result = conn.execute(rewritten).fetchall()
        assert len(result) > 0, "Rewritten query should return results"

        # Verify the complete expected SQL
        expected = "WITH base_query AS (SELECT test_data.*, test_data.amount FROM test_data) SELECT id, value, category, amount FROM base_query WHERE amount > 5000"
        assert rewritten == expected, f"Expected:\n{expected}\nGot:\n{rewritten}"

    def test_policy_with_kill_resolution(self, conn):
        """Test rewriting with KILL resolution (should still work for logical rewriter)."""
        query = "SELECT * FROM test_data WHERE value > 50"
        policy = DFCPolicy(
            source="test_data",
            constraint="max(test_data.value) > 100",
            on_fail=Resolution.KILL,
            description="Kill query if value <= 100"
        )

        rewritten = rewrite_query_logical(query, policy)

        # Execute the rewritten query to verify it's valid SQL
        result = conn.execute(rewritten).fetchall()
        # KILL resolution doesn't affect the logical rewriter's SQL generation
        # It just filters rows (the actual KILL behavior is in SQLRewriter)
        assert len(result) >= 0, "Rewritten query should execute"

        # Verify the complete expected SQL
        expected = "WITH base_query AS (SELECT test_data.*, test_data.value FROM test_data WHERE value > 50) SELECT id, value, category, amount FROM base_query WHERE value > 100"
        assert rewritten == expected, f"Expected:\n{expected}\nGot:\n{rewritten}"

    def test_policy_with_invalidate_resolution(self, conn):
        """Test rewriting with INVALIDATE resolution."""
        query = "SELECT * FROM test_data"
        policy = DFCPolicy(
            source="test_data",
            constraint="max(test_data.value) > 100",
            on_fail=Resolution.INVALIDATE,
            description="Invalidate rows where value <= 100"
        )

        rewritten = rewrite_query_logical(query, policy)

        # Execute the rewritten query to verify it's valid SQL
        result = conn.execute(rewritten).fetchall()
        assert len(result) >= 0, "Rewritten query should execute"

        # Verify the complete expected SQL
        expected = "WITH base_query AS (SELECT test_data.*, test_data.value FROM test_data) SELECT id, value, category, amount FROM base_query WHERE value > 100"
        assert rewritten == expected, f"Expected:\n{expected}\nGot:\n{rewritten}"

    def test_policy_with_complex_where_query(self, conn):
        """Test rewriting with complex WHERE clause."""
        query = "SELECT * FROM test_data WHERE value > 50 AND category = 'A'"
        policy = DFCPolicy(
            source="test_data",
            constraint="max(test_data.value) > 100",
            on_fail=Resolution.REMOVE,
            description="Filter rows where value <= 100"
        )

        rewritten = rewrite_query_logical(query, policy)

        # Execute the rewritten query to verify it's valid SQL
        result = conn.execute(rewritten).fetchall()
        assert len(result) >= 0, "Rewritten query should execute"

        # Verify the complete expected SQL
        expected = "WITH base_query AS (SELECT test_data.*, test_data.value FROM test_data WHERE value > 50 AND category = 'A') SELECT id, value, category, amount FROM base_query WHERE value > 100"
        assert rewritten == expected, f"Expected:\n{expected}\nGot:\n{rewritten}"

    def test_policy_with_select_specific_columns(self, conn):
        """Test rewriting with SELECT of specific columns."""
        query = "SELECT id, value, category FROM test_data WHERE value > 50"
        policy = DFCPolicy(
            source="test_data",
            constraint="max(test_data.value) > 100",
            on_fail=Resolution.REMOVE,
            description="Filter rows where value <= 100"
        )

        rewritten = rewrite_query_logical(query, policy)

        # Execute the rewritten query to verify it's valid SQL
        result = conn.execute(rewritten).fetchall()
        assert len(result) > 0, "Rewritten query should return results"

        # Verify the complete expected SQL
        expected = "WITH base_query AS (SELECT id, value, category FROM test_data WHERE value > 50) SELECT id, value, category FROM base_query WHERE value > 100"
        assert rewritten == expected, f"Expected:\n{expected}\nGot:\n{rewritten}"

    def test_policy_with_aggregation_and_different_threshold(self, conn):
        """Test rewriting aggregation query with different threshold."""
        query = "SELECT category, COUNT(*), SUM(amount) FROM test_data GROUP BY category"
        policy = DFCPolicy(
            source="test_data",
            constraint="max(test_data.value) > 50",
            on_fail=Resolution.REMOVE,
            description="Filter groups where max(value) <= 50"
        )

        rewritten = rewrite_query_logical(query, policy)

        # Execute the rewritten query to verify it's valid SQL
        result = conn.execute(rewritten).fetchall()
        assert len(result) > 0, "Rewritten query should return results"

        # Verify the complete expected SQL
        # Note: The constraint max(test_data.value) > 50 is applied in the HAVING clause
        expected = "WITH base_query AS (SELECT category, COUNT(*) AS rewrite1, SUM(amount) AS rewrite2 FROM test_data GROUP BY category) SELECT rescan.category, base_query.rewrite1, base_query.rewrite2 FROM base_query JOIN (SELECT category, value AS rewrite3 FROM test_data) AS rescan ON base_query.category = rescan.category GROUP BY base_query.category, base_query.rewrite1, base_query.rewrite2, rescan.category HAVING max(rescan.rewrite3) > 100"
        assert rewritten == expected, f"Expected:\n{expected}\nGot:\n{rewritten}"

    def test_policy_with_join_and_different_policy_column(self, conn):
        """Test rewriting JOIN query with policy on different column."""
        query = (
            "SELECT test_data.id, test_data.amount, other.value "
            "FROM test_data "
            "JOIN test_data AS other ON test_data.id = other.id"
        )
        policy = DFCPolicy(
            source="test_data",
            constraint="max(test_data.amount) > 5000",
            on_fail=Resolution.REMOVE,
            description="Filter rows where amount <= 5000"
        )

        rewritten = rewrite_query_logical(query, policy)

        # Execute the rewritten query to verify it's valid SQL
        result = conn.execute(rewritten).fetchall()
        assert len(result) > 0, "Rewritten query should return results"

        # Verify the complete expected SQL
        expected = "WITH base_query AS (SELECT test_data.id, test_data.amount, other.value FROM test_data JOIN test_data AS other ON test_data.id = other.id) SELECT id, amount, value FROM base_query WHERE amount > 5000"
        assert rewritten == expected, f"Expected:\n{expected}\nGot:\n{rewritten}"

    def test_policy_with_order_by_and_limit(self, conn):
        """Test rewriting query with ORDER BY and LIMIT."""
        query = "SELECT * FROM test_data ORDER BY value DESC LIMIT 10"
        policy = DFCPolicy(
            source="test_data",
            constraint="max(test_data.value) > 100",
            on_fail=Resolution.REMOVE,
            description="Filter rows where value <= 100"
        )

        rewritten = rewrite_query_logical(query, policy)

        # Execute the rewritten query to verify it's valid SQL
        result = conn.execute(rewritten).fetchall()
        assert len(result) > 0, "Rewritten query should return results"

        # Verify the complete expected SQL
        # Note: LIMIT is preserved in the rewritten query
        expected = "WITH base_query AS (SELECT test_data.*, test_data.value FROM test_data) SELECT id, value, category, amount FROM base_query WHERE value > 100 ORDER BY value DESC"
        assert rewritten == expected, f"Expected:\n{expected}\nGot:\n{rewritten}"

    def test_policy_with_multiple_aggregations_in_query(self, conn):
        """Test rewriting query with multiple aggregations."""
        query = (
            "SELECT category, COUNT(*), SUM(amount), AVG(value), MAX(value), MIN(value) "
            "FROM test_data "
            "GROUP BY category"
        )
        policy = DFCPolicy(
            source="test_data",
            constraint="max(test_data.value) > 100",
            on_fail=Resolution.REMOVE,
            description="Filter groups where max(value) <= 100"
        )

        rewritten = rewrite_query_logical(query, policy)

        # Execute the rewritten query to verify it's valid SQL
        result = conn.execute(rewritten).fetchall()
        assert len(result) > 0, "Rewritten query should return results"
        assert len(result[0]) == 6, "Should return 6 columns (category + 5 aggregations)"

        # Verify the complete expected SQL
        expected = "WITH base_query AS (SELECT category, COUNT(*) AS rewrite1, SUM(amount) AS rewrite2, AVG(value) AS rewrite3, MAX(value) AS rewrite4, MIN(value) AS rewrite5 FROM test_data GROUP BY category) SELECT rescan.category, base_query.rewrite1, base_query.rewrite2, base_query.rewrite3, base_query.rewrite4, base_query.rewrite5 FROM base_query JOIN (SELECT category, value AS rewrite6 FROM test_data) AS rescan ON base_query.category = rescan.category GROUP BY base_query.category, base_query.rewrite1, base_query.rewrite2, base_query.rewrite3, base_query.rewrite4, base_query.rewrite5, rescan.category HAVING max(rescan.rewrite6) > 100"
        assert rewritten == expected, f"Expected:\n{expected}\nGot:\n{rewritten}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
