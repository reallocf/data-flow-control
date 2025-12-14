"""Tests for the SQL rewriter."""

import pytest
from sql_rewriter import SQLRewriter


@pytest.fixture
def rewriter():
    """Create a SQLRewriter instance with test data."""
    rewriter = SQLRewriter()
    
    # Set up test table "foo" with data
    rewriter.execute("CREATE TABLE foo (id INTEGER, name VARCHAR)")
    rewriter.execute("INSERT INTO foo VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
    rewriter.execute("ALTER TABLE foo ADD COLUMN bar VARCHAR")
    rewriter.execute("UPDATE foo SET bar = 'value' || id::VARCHAR")
    
    # Set up test table "baz" for testing non-transformed queries
    rewriter.execute("CREATE TABLE baz (x INTEGER, y VARCHAR)")
    rewriter.execute("INSERT INTO baz VALUES (10, 'test')")
    
    yield rewriter
    
    rewriter.close()


def test_select_from_foo_adds_bar_column(rewriter):
    """Test that selecting from foo automatically adds the 'bar' column."""
    # Query that doesn't include 'bar'
    result = rewriter.fetchall("SELECT id, name FROM foo")
    
    # Should return 3 columns: id, name, bar
    assert len(result) == 3
    assert len(result[0]) == 3
    # Verify the data is correct
    assert result[0] == (1, 'Alice', 'value1')
    assert result[1] == (2, 'Bob', 'value2')
    assert result[2] == (3, 'Charlie', 'value3')


def test_transform_query_adds_bar_to_foo(rewriter):
    """Test that transform_query adds 'bar' column to queries on table 'foo'."""
    original_query = "SELECT id, name FROM foo"
    transformed = rewriter.transform_query(original_query)
    
    # The transformed query should include 'bar'
    assert "bar" in transformed.lower()
    assert "id" in transformed.lower()
    assert "name" in transformed.lower()
    
    # Verify it actually works when executed
    result = rewriter.fetchall(original_query)
    assert len(result[0]) == 3  # Should have 3 columns including bar


def test_query_different_table_not_transformed(rewriter):
    """Test that queries on tables other than 'foo' are not transformed."""
    original_query = "SELECT * FROM baz"
    transformed = rewriter.transform_query(original_query)
    
    # Should not be modified (no 'bar' added)
    # Check that 'bar' is not in the transformed query
    assert "bar" not in transformed.lower()
    # Should still contain the original table name
    assert "baz" in transformed.lower()
    
    # Should return correct results for baz table
    result = rewriter.fetchall(original_query)
    assert len(result) == 1
    assert result[0] == (10, 'test')


def test_bar_column_not_duplicated(rewriter):
    """Test that 'bar' column is not duplicated if already in SELECT."""
    original_query = "SELECT id, bar FROM foo"
    transformed = rewriter.transform_query(original_query)
    
    # Count occurrences of 'bar' in the transformed query
    bar_count = transformed.lower().count('bar')
    # Should only appear once (not duplicated)
    assert bar_count == 1
    
    # Should return correct results
    result = rewriter.fetchall(original_query)
    assert len(result) == 3
    assert len(result[0]) == 2  # id and bar only
    assert result[0] == (1, 'value1')


def test_select_star_from_foo_includes_bar(rewriter):
    """Test that SELECT * FROM foo includes the 'bar' column (via wildcard)."""
    result = rewriter.fetchall("SELECT * FROM foo")
    
    # SELECT * should return all columns including bar (id, name, bar)
    # Note: SELECT * already includes all columns, so we don't need to add bar explicitly
    assert len(result) == 3
    assert len(result[0]) == 3  # id, name, bar
    assert result[0] == (1, 'Alice', 'value1')


def test_execute_method_works(rewriter):
    """Test that the execute method works correctly."""
    # Should not raise an exception for non-aggregate queries
    cursor = rewriter.execute("SELECT id FROM foo LIMIT 1")
    result = cursor.fetchone()
    assert result is not None
    
    # Aggregate queries should work without transformation
    cursor = rewriter.execute("SELECT COUNT(*) FROM foo")
    result = cursor.fetchone()
    assert result[0] == 3


def test_fetchone_method_works(rewriter):
    """Test that the fetchone method works correctly."""
    result = rewriter.fetchone("SELECT id, name FROM foo WHERE id = 1")
    # Should return one row with 3 columns (id, name, bar)
    assert result is not None
    assert len(result) == 3
    assert result == (1, 'Alice', 'value1')


def test_aggregate_queries_not_transformed(rewriter):
    """Test that aggregate queries (like COUNT(*)) are not transformed."""
    # COUNT(*) should work without adding 'bar' column
    result = rewriter.fetchall("SELECT COUNT(*) FROM foo")
    assert result == [(3,)]
    
    # SUM query should also work
    result = rewriter.fetchall("SELECT SUM(id) FROM foo")
    assert result == [(6,)]  # 1 + 2 + 3 = 6


def test_context_manager(rewriter):
    """Test that SQLRewriter works as a context manager."""
    with SQLRewriter() as rw:
        rw.execute("CREATE TABLE test (x INTEGER)")
        rw.execute("INSERT INTO test VALUES (1)")
        result = rw.fetchall("SELECT * FROM test")
        assert result == [(1,)]
    # Connection should be closed after context exit

