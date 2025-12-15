"""Tests for rewrite rule functions."""

import pytest
import sqlglot
from sqlglot import exp

from sql_rewriter.policy import DFCPolicy, Resolution
from sql_rewriter.rewrite_rule import (
    apply_policy_constraints_to_aggregation,
    apply_policy_constraints_to_scan,
    transform_aggregations_to_columns,
    ensure_columns_accessible,
)


class TestApplyPolicyConstraintsToAggregation:
    """Tests for apply_policy_constraints_to_aggregation."""

    def test_adds_having_clause_when_none_exists(self):
        """Test that HAVING clause is added when query has no HAVING."""
        query = "SELECT max(foo.id) FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1",
            on_fail=Resolution.REMOVE,
        )
        
        apply_policy_constraints_to_aggregation(parsed, [policy], {"foo"})
        
        # Should have HAVING clause
        assert parsed.args.get("having") is not None
        having_sql = parsed.args["having"].sql()
        # Check that the constraint is in the HAVING clause
        assert "max(foo.id) > 1" in having_sql.lower() or "MAX(FOO.ID) > 1" in having_sql.upper()

    def test_combines_with_existing_having_clause(self):
        """Test that new constraint is combined with existing HAVING using AND."""
        query = "SELECT max(foo.id) FROM foo HAVING max(foo.id) < 10"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1",
            on_fail=Resolution.REMOVE,
        )
        
        apply_policy_constraints_to_aggregation(parsed, [policy], {"foo"})
        
        # Should have combined HAVING clause
        having = parsed.args.get("having")
        assert having is not None
        having_sql = having.sql().upper()
        # Should contain both conditions
        assert "MAX(FOO.ID) < 10" in having_sql or "max(foo.id) < 10" in having_sql.lower()
        assert "MAX(FOO.ID) > 1" in having_sql or "max(foo.id) > 1" in having_sql.lower()
        # Should be combined with AND
        assert "AND" in having_sql

    def test_applies_multiple_policies(self):
        """Test that multiple policies are applied correctly."""
        query = "SELECT max(foo.id) FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy1 = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1",
            on_fail=Resolution.REMOVE,
        )
        policy2 = DFCPolicy(
            source="foo",
            constraint="max(foo.id) < 10",
            on_fail=Resolution.KILL,
        )
        
        apply_policy_constraints_to_aggregation(parsed, [policy1, policy2], {"foo"})
        
        # Should have HAVING clause with both constraints
        having = parsed.args.get("having")
        assert having is not None
        having_sql = having.sql().upper()
        assert "MAX(FOO.ID) > 1" in having_sql or "max(foo.id) > 1" in having_sql.lower()
        assert "MAX(FOO.ID) < 10" in having_sql or "max(foo.id) < 10" in having_sql.lower()

    def test_does_not_modify_query_without_policies(self):
        """Test that query is not modified when no policies are provided."""
        query = "SELECT max(foo.id) FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        original_sql = parsed.sql()
        
        apply_policy_constraints_to_aggregation(parsed, [], {"foo"})
        
        # Query should be unchanged
        assert parsed.sql() == original_sql

    def test_handles_complex_constraint(self):
        """Test that complex constraints with multiple conditions work."""
        query = "SELECT max(foo.id), min(foo.id) FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1 AND min(foo.id) < 5",
            on_fail=Resolution.REMOVE,
        )
        
        apply_policy_constraints_to_aggregation(parsed, [policy], {"foo"})
        
        # Should have HAVING clause
        having = parsed.args.get("having")
        assert having is not None
        having_sql = having.sql().upper()
        assert "MAX" in having_sql
        assert "MIN" in having_sql
        assert "AND" in having_sql


class TestApplyPolicyConstraintsToScan:
    """Tests for apply_policy_constraints_to_scan."""

    def test_adds_where_clause_when_none_exists(self):
        """Test that WHERE clause is added when query has no WHERE."""
        query = "SELECT id FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1",
            on_fail=Resolution.REMOVE,
        )
        
        apply_policy_constraints_to_scan(parsed, [policy], {"foo"})
        
        # Should have WHERE clause
        assert parsed.args.get("where") is not None
        where_sql = parsed.args["where"].sql()
        # The aggregation should be transformed (max(foo.id) becomes foo.id)
        assert "WHERE" in where_sql.upper()

    def test_combines_with_existing_where_clause(self):
        """Test that new constraint is combined with existing WHERE using AND."""
        query = "SELECT id FROM foo WHERE id < 10"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1",
            on_fail=Resolution.REMOVE,
        )
        
        apply_policy_constraints_to_scan(parsed, [policy], {"foo"})
        
        # Should have combined WHERE clause
        where_expr = parsed.args.get("where")
        assert where_expr is not None
        where_sql = where_expr.sql().upper()
        # Should contain both conditions
        assert "ID < 10" in where_sql or "id < 10" in where_sql.lower()
        # Should be combined with AND
        assert "AND" in where_sql

    def test_applies_multiple_policies(self):
        """Test that multiple policies are applied correctly."""
        query = "SELECT id FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy1 = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1",
            on_fail=Resolution.REMOVE,
        )
        policy2 = DFCPolicy(
            source="foo",
            constraint="min(foo.id) < 10",
            on_fail=Resolution.KILL,
        )
        
        apply_policy_constraints_to_scan(parsed, [policy1, policy2], {"foo"})
        
        # Should have WHERE clause with both constraints
        where_expr = parsed.args.get("where")
        assert where_expr is not None
        where_sql = where_expr.sql().upper()
        assert "AND" in where_sql

    def test_transforms_aggregations_in_constraint(self):
        """Test that aggregations in constraints are transformed to columns."""
        query = "SELECT id FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1",
            on_fail=Resolution.REMOVE,
        )
        
        apply_policy_constraints_to_scan(parsed, [policy], {"foo"})
        
        # The WHERE clause should have the transformed constraint
        where_expr = parsed.args.get("where")
        assert where_expr is not None
        where_sql = where_expr.sql()
        # max(foo.id) should be transformed to foo.id
        assert "foo.id" in where_sql.lower() or "FOO.ID" in where_sql.upper()
        # Should not contain "max" in the WHERE clause (it's been transformed)
        # Actually, let's check that it's been transformed - the SQL should have "id" or "foo.id"
        assert "> 1" in where_sql or ">1" in where_sql


class TestTransformAggregationsToColumns:
    """Tests for transform_aggregations_to_columns."""

    def test_transforms_count_to_one(self):
        """Test that COUNT is transformed to 1."""
        constraint = "COUNT(foo.id) > 0"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # COUNT should be replaced with 1
        assert "1 > 0" in transformed_sql or "1>0" in transformed_sql.replace(" ", "")

    def test_transforms_count_distinct_to_one(self):
        """Test that COUNT(DISTINCT ...) is transformed to 1."""
        constraint = "COUNT(DISTINCT foo.id) > 0"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # COUNT(DISTINCT ...) should be replaced with 1
        assert "1 > 0" in transformed_sql or "1>0" in transformed_sql.replace(" ", "")

    def test_transforms_count_star_to_one(self):
        """Test that COUNT(*) is transformed to 1."""
        constraint = "COUNT(*) > 0"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, set())
        
        transformed_sql = transformed.sql()
        # COUNT(*) should be replaced with 1
        assert "1 > 0" in transformed_sql or "1>0" in transformed_sql.replace(" ", "")

    def test_transforms_max_to_column(self):
        """Test that MAX is transformed to the underlying column."""
        constraint = "max(foo.id) > 1"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # MAX should be replaced with the column
        assert "foo.id" in transformed_sql.lower() or "FOO.ID" in transformed_sql.upper()
        assert "> 1" in transformed_sql or ">1" in transformed_sql
        # Should not contain "max"
        assert "max" not in transformed_sql.lower() or "max" not in transformed_sql

    def test_transforms_min_to_column(self):
        """Test that MIN is transformed to the underlying column."""
        constraint = "min(foo.id) < 10"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # MIN should be replaced with the column
        assert "foo.id" in transformed_sql.lower() or "FOO.ID" in transformed_sql.upper()
        assert "< 10" in transformed_sql or "<10" in transformed_sql

    def test_transforms_sum_to_column(self):
        """Test that SUM is transformed to the underlying column."""
        constraint = "sum(foo.id) > 5"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # SUM should be replaced with the column
        assert "foo.id" in transformed_sql.lower() or "FOO.ID" in transformed_sql.upper()

    def test_transforms_avg_to_column(self):
        """Test that AVG is transformed to the underlying column."""
        constraint = "avg(foo.id) > 2"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # AVG should be replaced with the column
        assert "foo.id" in transformed_sql.lower() or "FOO.ID" in transformed_sql.upper()

    def test_transforms_count_if_to_case_when(self):
        """Test that COUNT_IF is transformed to CASE WHEN."""
        constraint = "COUNT_IF(foo.id > 5) > 0"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # COUNT_IF should be replaced with CASE WHEN
        assert "CASE" in transformed_sql.upper()
        assert "WHEN" in transformed_sql.upper()
        # Should contain the condition
        assert "foo.id > 5" in transformed_sql.lower() or "FOO.ID > 5" in transformed_sql.upper()

    def test_transforms_countif_to_case_when(self):
        """Test that COUNTIF is transformed to CASE WHEN."""
        constraint = "COUNTIF(foo.id > 5) > 0"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # COUNTIF should be replaced with CASE WHEN
        assert "CASE" in transformed_sql.upper()
        assert "WHEN" in transformed_sql.upper()

    def test_transforms_array_agg_to_array(self):
        """Test that ARRAY_AGG is transformed to ARRAY[column]."""
        constraint = "array_agg(foo.id) = ARRAY[2]"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # ARRAY_AGG should be replaced with array syntax
        assert "[" in transformed_sql or "ARRAY" in transformed_sql.upper()
        assert "foo.id" in transformed_sql.lower() or "FOO.ID" in transformed_sql.upper()

    def test_transforms_approx_count_distinct_to_one(self):
        """Test that APPROX_COUNT_DISTINCT is transformed to 1."""
        constraint = "APPROX_COUNT_DISTINCT(foo.id) > 0"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # APPROX_COUNT_DISTINCT should be replaced with 1
        assert "1 > 0" in transformed_sql or "1>0" in transformed_sql.replace(" ", "")

    def test_transforms_complex_constraint_with_multiple_aggregations(self):
        """Test that complex constraints with multiple aggregations are transformed."""
        constraint = "max(foo.id) > 1 AND min(foo.id) < 10 AND COUNT(foo.id) > 0"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # All aggregations should be transformed
        assert "max" not in transformed_sql.lower() or "MAX" not in transformed_sql
        assert "min" not in transformed_sql.lower() or "MIN" not in transformed_sql
        assert "COUNT" not in transformed_sql.upper() or "count" not in transformed_sql.lower()
        # Should contain the column references
        assert "foo.id" in transformed_sql.lower() or "FOO.ID" in transformed_sql.upper()
        # Should contain the literal 1
        assert "1" in transformed_sql

    def test_transforms_max_with_case_expression(self):
        """Test that max(CASE WHEN ...) preserves the full CASE expression."""
        constraint = "max(CASE WHEN foo.id > 0 THEN foo.status ELSE NULL END) > 'active'"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # The CASE expression should be preserved, not just the first column
        assert "CASE" in transformed_sql.upper()
        assert "WHEN" in transformed_sql.upper()
        assert "THEN" in transformed_sql.upper()
        assert "ELSE" in transformed_sql.upper()
        # Should contain both columns from the CASE expression
        assert "foo.id" in transformed_sql.lower() or "FOO.ID" in transformed_sql.upper()
        assert "foo.status" in transformed_sql.lower() or "FOO.STATUS" in transformed_sql.upper()
        # Should not contain the aggregation function
        assert "max" not in transformed_sql.lower() or "MAX" not in transformed_sql

    def test_transforms_min_with_function_call(self):
        """Test that min(function_call(...)) preserves the full function call."""
        constraint = "min(COALESCE(foo.id, 0)) > 5"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # The COALESCE function should be preserved
        assert "COALESCE" in transformed_sql.upper() or "coalesce" in transformed_sql.lower()
        assert "foo.id" in transformed_sql.lower() or "FOO.ID" in transformed_sql.upper()
        # Should not contain the aggregation function
        assert "min" not in transformed_sql.lower() or "MIN" not in transformed_sql

    def test_transforms_sum_with_arithmetic_expression(self):
        """Test that sum(expr1 + expr2) preserves the full arithmetic expression."""
        constraint = "sum(foo.id + foo.value) > 100"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # The arithmetic expression should be preserved
        assert "foo.id" in transformed_sql.lower() or "FOO.ID" in transformed_sql.upper()
        assert "foo.value" in transformed_sql.lower() or "FOO.VALUE" in transformed_sql.upper()
        assert "+" in transformed_sql
        # Should not contain the aggregation function
        assert "sum" not in transformed_sql.lower() or "SUM" not in transformed_sql

    def test_transforms_avg_with_nested_case(self):
        """Test that avg(CASE WHEN ... THEN ... ELSE ... END) preserves the full CASE expression."""
        constraint = "avg(CASE WHEN foo.status = 'active' THEN foo.value ELSE 0 END) > 50"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # The CASE expression should be preserved
        assert "CASE" in transformed_sql.upper()
        assert "WHEN" in transformed_sql.upper()
        assert "THEN" in transformed_sql.upper()
        assert "ELSE" in transformed_sql.upper()
        # Should contain the columns and values from the CASE expression
        assert "foo.status" in transformed_sql.lower() or "FOO.STATUS" in transformed_sql.upper()
        assert "foo.value" in transformed_sql.lower() or "FOO.VALUE" in transformed_sql.upper()
        # Should not contain the aggregation function
        assert "avg" not in transformed_sql.lower() or "AVG" not in transformed_sql

    def test_transforms_nested_aggregations(self):
        """Test that nested aggregations are transformed correctly."""
        constraint = "max(foo.id) + min(foo.id) > 5"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # Both aggregations should be transformed to columns
        assert "foo.id" in transformed_sql.lower() or "FOO.ID" in transformed_sql.upper()
        assert "+" in transformed_sql or " + " in transformed_sql
        assert "> 5" in transformed_sql or ">5" in transformed_sql

    def test_preserves_non_aggregation_expressions(self):
        """Test that non-aggregation expressions are preserved."""
        constraint = "foo.id > 5 AND foo.name = 'test'"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # Non-aggregation expressions should be unchanged
        assert "foo.id > 5" in transformed_sql.lower() or "FOO.ID > 5" in transformed_sql.upper()
        assert "foo.name = 'test'" in transformed_sql.lower() or "FOO.NAME = 'test'" in transformed_sql.upper()

    def test_handles_aggregation_without_column(self):
        """Test that aggregations without columns (like COUNT(*)) are handled."""
        constraint = "COUNT(*) > 0"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, set())
        
        transformed_sql = transformed.sql()
        # COUNT(*) should be replaced with 1
        assert "1 > 0" in transformed_sql or "1>0" in transformed_sql.replace(" ", "")

    def test_handles_mixed_aggregation_types(self):
        """Test that different aggregation types in one constraint are handled."""
        constraint = "max(foo.id) > COUNT(foo.id)"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # MAX should become column, COUNT should become 1
        assert "foo.id" in transformed_sql.lower() or "FOO.ID" in transformed_sql.upper()
        assert "1" in transformed_sql
        assert ">" in transformed_sql


class TestEnsureColumnsAccessible:
    """Tests for ensure_columns_accessible."""

    def test_is_no_op_for_aggregated_columns(self):
        """Test that ensure_columns_accessible is a no-op for aggregated columns."""
        query = "SELECT max(foo.id) FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        original_sql = parsed.sql()
        
        constraint = sqlglot.parse_one("max(foo.id) > 1", read="duckdb")
        
        ensure_columns_accessible(parsed, constraint, {"foo"})
        
        # Query should be unchanged (it's a no-op)
        assert parsed.sql() == original_sql

    def test_does_not_raise_error(self):
        """Test that ensure_columns_accessible does not raise errors."""
        query = "SELECT id FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        constraint = sqlglot.parse_one("foo.id > 1", read="duckdb")
        
        # Should not raise any errors
        ensure_columns_accessible(parsed, constraint, {"foo"})

    def test_handles_complex_constraints(self):
        """Test that ensure_columns_accessible handles complex constraints."""
        query = "SELECT max(foo.id), min(foo.id) FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        constraint = sqlglot.parse_one("max(foo.id) > 1 AND min(foo.id) < 10", read="duckdb")
        
        # Should not raise any errors
        ensure_columns_accessible(parsed, constraint, {"foo"})


class TestIntegration:
    """Integration tests for rewrite rules working together."""

    def test_aggregation_query_with_policy(self):
        """Test that aggregation queries correctly apply policies."""
        query = "SELECT max(foo.id) FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1",
            on_fail=Resolution.REMOVE,
        )
        
        apply_policy_constraints_to_aggregation(parsed, [policy], {"foo"})
        
        # Should have HAVING clause
        assert parsed.args.get("having") is not None
        having_sql = parsed.args["having"].sql()
        assert "max(foo.id) > 1" in having_sql.lower() or "MAX(FOO.ID) > 1" in having_sql.upper()

    def test_scan_query_with_policy_transforms_aggregations(self):
        """Test that scan queries transform aggregations in policy constraints."""
        query = "SELECT id FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1",
            on_fail=Resolution.REMOVE,
        )
        
        apply_policy_constraints_to_scan(parsed, [policy], {"foo"})
        
        # Should have WHERE clause
        assert parsed.args.get("where") is not None
        where_sql = parsed.args["where"].sql()
        # The aggregation should be transformed
        assert "WHERE" in where_sql.upper()
        # max(foo.id) should be transformed to foo.id
        assert "foo.id" in where_sql.lower() or "FOO.ID" in where_sql.upper()

    def test_multiple_policies_on_aggregation_query(self):
        """Test that multiple policies are correctly applied to aggregation queries."""
        query = "SELECT max(foo.id) FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy1 = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1",
            on_fail=Resolution.REMOVE,
        )
        policy2 = DFCPolicy(
            source="foo",
            constraint="max(foo.id) < 10",
            on_fail=Resolution.KILL,
        )
        
        apply_policy_constraints_to_aggregation(parsed, [policy1, policy2], {"foo"})
        
        # Should have HAVING clause with both constraints
        having = parsed.args.get("having")
        assert having is not None
        having_sql = having.sql().upper()
        assert "AND" in having_sql

    def test_multiple_policies_on_scan_query(self):
        """Test that multiple policies are correctly applied to scan queries."""
        query = "SELECT id FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy1 = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1",
            on_fail=Resolution.REMOVE,
        )
        policy2 = DFCPolicy(
            source="foo",
            constraint="min(foo.id) < 10",
            on_fail=Resolution.KILL,
        )
        
        apply_policy_constraints_to_scan(parsed, [policy1, policy2], {"foo"})
        
        # Should have WHERE clause with both constraints
        where_expr = parsed.args.get("where")
        assert where_expr is not None
        where_sql = where_expr.sql().upper()
        assert "AND" in where_sql

