"""Tests for rewrite rule functions."""

import pytest
import sqlglot
from sqlglot import exp

from sql_rewriter.policy import DFCPolicy, AggregateDFCPolicy, Resolution
from sql_rewriter.rewrite_rule import (
    apply_policy_constraints_to_aggregation,
    apply_policy_constraints_to_scan,
    apply_aggregate_policy_constraints_to_aggregation,
    apply_aggregate_policy_constraints_to_scan,
    transform_aggregations_to_columns,
    ensure_columns_accessible,
    get_policy_identifier,
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
        # Each policy addition is wrapped in parentheses (includes HAVING keyword)
        assert having_sql == "HAVING (MAX(foo.id) > 1)"

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
        having_sql = having.sql()
        # Should contain both conditions combined with AND, wrapped in parentheses (includes HAVING keyword)
        assert having_sql == "HAVING (MAX(foo.id) < 10) AND (MAX(foo.id) > 1)"

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
        having_sql = having.sql()
        # KILL policy wraps constraint in CASE WHEN, REMOVE policy adds constraint directly
        # The order depends on policy order, and KILL wraps in CASE
        # Both constraints should be wrapped in parentheses (includes HAVING keyword)
        assert having_sql == "HAVING (MAX(foo.id) > 1) AND (CASE WHEN MAX(foo.id) < 10 THEN true ELSE KILL() END)"

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
        having_sql = having.sql()
        # Each policy addition is wrapped in parentheses
        assert having_sql == "HAVING (MAX(foo.id) > 1 AND MIN(foo.id) < 5)"

    def test_combines_with_existing_having_with_or_clause(self):
        """Test that new constraint is combined correctly with existing HAVING that has OR."""
        query = "SELECT max(foo.id) FROM foo HAVING max(foo.id) < 5 OR max(foo.id) > 10"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1",
            on_fail=Resolution.REMOVE,
        )
        
        apply_policy_constraints_to_aggregation(parsed, [policy], {"foo"})
        
        # Should have combined HAVING clause with parentheses to ensure proper precedence
        having = parsed.args.get("having")
        assert having is not None
        having_sql = having.sql()
        # The OR clause should be wrapped in parentheses, and the new constraint too
        assert having_sql == "HAVING (MAX(foo.id) < 5 OR MAX(foo.id) > 10) AND (MAX(foo.id) > 1)"

    def test_combines_policy_with_or_constraint_with_existing_having(self):
        """Test that policy constraint with OR is combined correctly with existing HAVING."""
        query = "SELECT max(foo.id) FROM foo HAVING max(foo.id) < 10"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy = DFCPolicy(
            source="foo",
            constraint="max(foo.id) = 1 OR max(foo.id) = 3",
            on_fail=Resolution.REMOVE,
        )
        
        apply_policy_constraints_to_aggregation(parsed, [policy], {"foo"})
        
        # Should have combined HAVING clause with parentheses
        having = parsed.args.get("having")
        assert having is not None
        having_sql = having.sql()
        # Both expressions should be wrapped in parentheses
        assert having_sql == "HAVING (MAX(foo.id) < 10) AND (MAX(foo.id) = 1 OR MAX(foo.id) = 3)"

    def test_invalidate_resolution_adds_column_to_aggregation(self):
        """Test that INVALIDATE resolution adds a 'valid' column to aggregation queries."""
        query = "SELECT max(foo.id) FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1",
            on_fail=Resolution.INVALIDATE,
        )
        
        apply_policy_constraints_to_aggregation(parsed, [policy], {"foo"})
        
        # Should have 'valid' column in SELECT, not HAVING clause
        assert parsed.args.get("having") is None
        
        # Check the full SQL string
        # valid = constraint, so when constraint is MAX(foo.id) > 1,
        # valid = (MAX(foo.id) > 1) (wrapped in parentheses like REMOVE)
        full_sql = parsed.sql()
        assert full_sql == "SELECT MAX(foo.id), (MAX(foo.id) > 1) AS valid FROM foo"

    def test_invalidate_resolution_combines_multiple_policies(self):
        """Test that multiple INVALIDATE policies are combined with AND in the 'valid' column."""
        query = "SELECT max(foo.id) FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy1 = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1",
            on_fail=Resolution.INVALIDATE,
        )
        policy2 = DFCPolicy(
            source="foo",
            constraint="max(foo.id) < 10",
            on_fail=Resolution.INVALIDATE,
        )
        
        apply_policy_constraints_to_aggregation(parsed, [policy1, policy2], {"foo"})
        
        # Should have 'valid' column with combined constraints
        # Both constraints are combined with AND
        # valid = MAX(foo.id) > 1 AND MAX(foo.id) < 10
        # Check the full SQL string
        full_sql = parsed.sql()
        assert full_sql == "SELECT MAX(foo.id), (MAX(foo.id) > 1) AND (MAX(foo.id) < 10) AS valid FROM foo"

    def test_invalidate_resolution_with_other_resolutions(self):
        """Test that INVALIDATE resolution works alongside REMOVE/KILL policies."""
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
            on_fail=Resolution.INVALIDATE,
        )
        
        apply_policy_constraints_to_aggregation(parsed, [policy1, policy2], {"foo"})
        
        # Should have both HAVING clause (from REMOVE) and 'valid' column (from INVALIDATE)
        # valid = (MAX(foo.id) < 10) (wrapped in parentheses)
        # Check the full SQL string
        full_sql = parsed.sql()
        assert full_sql == "SELECT MAX(foo.id), (MAX(foo.id) < 10) AS valid FROM foo HAVING (MAX(foo.id) > 1)"


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
        # Each policy addition is wrapped in parentheses (includes WHERE keyword)
        assert where_sql == "WHERE (foo.id > 1)"

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
        where_sql = where_expr.sql()
        # Should contain both conditions combined with AND, wrapped in parentheses (includes WHERE keyword)
        assert where_sql == "WHERE (id < 10) AND (foo.id > 1)"

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
        where_sql = where_expr.sql()
        # KILL policy wraps constraint in CASE WHEN, REMOVE policy adds constraint directly
        # Both constraints should be wrapped in parentheses (includes WHERE keyword)
        assert where_sql == "WHERE (foo.id > 1) AND (CASE WHEN foo.id < 10 THEN true ELSE KILL() END)"

    def test_combines_with_existing_where_with_or_clause(self):
        """Test that new constraint is combined correctly with existing WHERE that has OR."""
        query = "SELECT id FROM foo WHERE id < 5 OR id > 10"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1",
            on_fail=Resolution.REMOVE,
        )
        
        apply_policy_constraints_to_scan(parsed, [policy], {"foo"})
        
        # Should have combined WHERE clause with parentheses to ensure proper precedence
        where_expr = parsed.args.get("where")
        assert where_expr is not None
        where_sql = where_expr.sql()
        # The OR clause should be wrapped in parentheses, and the new constraint too
        assert where_sql == "WHERE (id < 5 OR id > 10) AND (foo.id > 1)"

    def test_combines_policy_with_or_constraint_with_existing_where(self):
        """Test that policy constraint with OR is combined correctly with existing WHERE."""
        query = "SELECT id FROM foo WHERE id < 10"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy = DFCPolicy(
            source="foo",
            constraint="max(foo.id) = 1 OR max(foo.id) = 3",
            on_fail=Resolution.REMOVE,
        )
        
        apply_policy_constraints_to_scan(parsed, [policy], {"foo"})
        
        # Should have combined WHERE clause with parentheses
        where_expr = parsed.args.get("where")
        assert where_expr is not None
        where_sql = where_expr.sql()
        # Both expressions should be wrapped in parentheses
        assert where_sql == "WHERE (id < 10) AND (foo.id = 1 OR foo.id = 3)"

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
        # Each policy addition is wrapped in parentheses (includes WHERE keyword)
        assert where_sql == "WHERE (foo.id > 1)"

    def test_invalidate_resolution_adds_column_to_scan(self):
        """Test that INVALIDATE resolution adds a 'valid' column to scan queries."""
        query = "SELECT id FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1",
            on_fail=Resolution.INVALIDATE,
        )
        
        apply_policy_constraints_to_scan(parsed, [policy], {"foo"})
        
        # Should have 'valid' column in SELECT, not WHERE clause
        assert parsed.args.get("where") is None
        
        # Check the full SQL string
        # valid = (foo.id > 1) (wrapped in parentheses like REMOVE)
        full_sql = parsed.sql()
        assert full_sql == "SELECT id, (foo.id > 1) AS valid FROM foo"

    def test_invalidate_resolution_combines_multiple_policies_in_scan(self):
        """Test that multiple INVALIDATE policies are combined with AND in scan queries."""
        query = "SELECT id FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy1 = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1",
            on_fail=Resolution.INVALIDATE,
        )
        policy2 = DFCPolicy(
            source="foo",
            constraint="min(foo.id) < 10",
            on_fail=Resolution.INVALIDATE,
        )
        
        apply_policy_constraints_to_scan(parsed, [policy1, policy2], {"foo"})
        
        # Should have 'valid' column with combined constraints
        # Both constraints are combined with AND
        # valid = foo.id > 1 AND foo.id < 10
        # Check the full SQL string
        full_sql = parsed.sql()
        assert full_sql == "SELECT id, (foo.id > 1) AND (foo.id < 10) AS valid FROM foo"

    def test_invalidate_resolution_with_other_resolutions_in_scan(self):
        """Test that INVALIDATE resolution works alongside REMOVE/KILL policies in scan queries."""
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
            on_fail=Resolution.INVALIDATE,
        )
        
        apply_policy_constraints_to_scan(parsed, [policy1, policy2], {"foo"})
        
        # Should have both WHERE clause (from REMOVE) and 'valid' column (from INVALIDATE)
        # valid = (foo.id < 10) (wrapped in parentheses)
        # Check the full SQL string
        full_sql = parsed.sql()
        assert full_sql == "SELECT id, (foo.id < 10) AS valid FROM foo WHERE (foo.id > 1)"


class TestTransformAggregationsToColumns:
    """Tests for transform_aggregations_to_columns."""

    def test_transforms_count_to_one(self):
        """Test that COUNT is transformed to 1."""
        constraint = "COUNT(foo.id) > 0"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # COUNT should be replaced with 1
        assert transformed_sql == "1 > 0"

    def test_transforms_count_distinct_to_one(self):
        """Test that COUNT(DISTINCT ...) is transformed to 1."""
        constraint = "COUNT(DISTINCT foo.id) > 0"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # COUNT(DISTINCT ...) should be replaced with 1
        assert transformed_sql == "1 > 0"

    def test_transforms_count_star_to_one(self):
        """Test that COUNT(*) is transformed to 1."""
        constraint = "COUNT(*) > 0"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, set())
        
        transformed_sql = transformed.sql()
        # COUNT(*) should be replaced with 1
        assert transformed_sql == "1 > 0"

    def test_transforms_max_to_column(self):
        """Test that MAX is transformed to the underlying column."""
        constraint = "max(foo.id) > 1"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # MAX should be replaced with the column
        assert transformed_sql == "foo.id > 1"

    def test_transforms_min_to_column(self):
        """Test that MIN is transformed to the underlying column."""
        constraint = "min(foo.id) < 10"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # MIN should be replaced with the column
        assert transformed_sql == "foo.id < 10"

    def test_transforms_sum_to_column(self):
        """Test that SUM is transformed to the underlying column."""
        constraint = "sum(foo.id) > 5"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # SUM should be replaced with the column
        assert transformed_sql == "foo.id > 5"

    def test_transforms_avg_to_column(self):
        """Test that AVG is transformed to the underlying column."""
        constraint = "avg(foo.id) > 2"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # AVG should be replaced with the column
        assert transformed_sql == "foo.id > 2"

    def test_transforms_count_if_to_case_when(self):
        """Test that COUNT_IF is transformed to CASE WHEN."""
        constraint = "COUNT_IF(foo.id > 5) > 0"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # COUNT_IF should be replaced with CASE WHEN ... THEN 1 ELSE 0 END
        assert transformed_sql == "CASE WHEN foo.id > 5 THEN 1 ELSE 0 END > 0"

    def test_transforms_countif_to_case_when(self):
        """Test that COUNTIF is transformed to CASE WHEN."""
        constraint = "COUNTIF(foo.id > 5) > 0"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # COUNTIF should be replaced with CASE WHEN ... THEN 1 ELSE 0 END
        assert transformed_sql == "CASE WHEN foo.id > 5 THEN 1 ELSE 0 END > 0"

    def test_transforms_array_agg_to_array(self):
        """Test that ARRAY_AGG is transformed to ARRAY[column]."""
        constraint = "array_agg(foo.id) = ARRAY[2]"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # ARRAY_AGG should be replaced with ARRAY(column) syntax
        assert transformed_sql == "ARRAY(foo.id) = ARRAY(2)"

    def test_transforms_approx_count_distinct_to_one(self):
        """Test that APPROX_COUNT_DISTINCT is transformed to 1."""
        constraint = "APPROX_COUNT_DISTINCT(foo.id) > 0"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # APPROX_COUNT_DISTINCT should be replaced with 1
        assert transformed_sql == "1 > 0"

    def test_transforms_complex_constraint_with_multiple_aggregations(self):
        """Test that complex constraints with multiple aggregations are transformed."""
        constraint = "max(foo.id) > 1 AND min(foo.id) < 10 AND COUNT(foo.id) > 0"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # All aggregations should be transformed: max/min to column, COUNT to 1
        assert transformed_sql == "foo.id > 1 AND foo.id < 10 AND 1 > 0"

    def test_transforms_max_with_case_expression(self):
        """Test that max(CASE WHEN ...) preserves the full CASE expression."""
        constraint = "max(CASE WHEN foo.id > 0 THEN foo.status ELSE NULL END) > 'active'"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # The CASE expression should be preserved, not just the first column
        assert transformed_sql == "CASE WHEN foo.id > 0 THEN foo.status ELSE NULL END > 'active'"

    def test_transforms_min_with_function_call(self):
        """Test that min(function_call(...)) preserves the full function call."""
        constraint = "min(COALESCE(foo.id, 0)) > 5"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # The COALESCE function should be preserved
        assert transformed_sql == "COALESCE(foo.id, 0) > 5"

    def test_transforms_sum_with_arithmetic_expression(self):
        """Test that sum(expr1 + expr2) preserves the full arithmetic expression."""
        constraint = "sum(foo.id + foo.value) > 100"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # The arithmetic expression should be preserved
        assert transformed_sql == "foo.id + foo.value > 100"

    def test_transforms_avg_with_nested_case(self):
        """Test that avg(CASE WHEN ... THEN ... ELSE ... END) preserves the full CASE expression."""
        constraint = "avg(CASE WHEN foo.status = 'active' THEN foo.value ELSE 0 END) > 50"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # The CASE expression should be preserved
        assert transformed_sql == "CASE WHEN foo.status = 'active' THEN foo.value ELSE 0 END > 50"

    def test_transforms_nested_aggregations(self):
        """Test that nested aggregations are transformed correctly."""
        constraint = "max(foo.id) + min(foo.id) > 5"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # Both aggregations should be transformed to columns
        assert transformed_sql == "foo.id + foo.id > 5"

    def test_preserves_non_aggregation_expressions(self):
        """Test that non-aggregation expressions are preserved."""
        constraint = "foo.id > 5 AND foo.name = 'test'"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # Non-aggregation expressions should be unchanged
        assert transformed_sql == "foo.id > 5 AND foo.name = 'test'"

    def test_handles_aggregation_without_column(self):
        """Test that aggregations without columns (like COUNT(*)) are handled."""
        constraint = "COUNT(*) > 0"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, set())
        
        transformed_sql = transformed.sql()
        # COUNT(*) should be replaced with 1
        assert transformed_sql == "1 > 0"

    def test_handles_mixed_aggregation_types(self):
        """Test that different aggregation types in one constraint are handled."""
        constraint = "max(foo.id) > COUNT(foo.id)"
        parsed = sqlglot.parse_one(constraint, read="duckdb")
        
        transformed = transform_aggregations_to_columns(parsed, {"foo"})
        
        transformed_sql = transformed.sql()
        # MAX should become column, COUNT should become 1
        assert transformed_sql == "foo.id > 1"


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
        # Each policy addition is wrapped in parentheses
        assert having_sql == "HAVING (MAX(foo.id) > 1)"

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
        # max(foo.id) should be transformed to foo.id
        # Each policy addition is wrapped in parentheses (includes WHERE keyword)
        assert where_sql == "WHERE (foo.id > 1)"

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
        having_sql = having.sql()
        # KILL policy wraps constraint in CASE WHEN, REMOVE policy adds constraint directly
        # Both constraints should be wrapped in parentheses (includes HAVING keyword)
        assert having_sql == "HAVING (MAX(foo.id) > 1) AND (CASE WHEN MAX(foo.id) < 10 THEN true ELSE KILL() END)"

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
        where_sql = where_expr.sql()
        # KILL policy wraps constraint in CASE WHEN, REMOVE policy adds constraint directly
        # Both constraints should be wrapped in parentheses (includes WHERE keyword)
        assert where_sql == "WHERE (foo.id > 1) AND (CASE WHEN foo.id < 10 THEN true ELSE KILL() END)"



class TestApplyAggregatePolicyConstraintsToAggregation:
    """Tests for apply_aggregate_policy_constraints_to_aggregation."""

    def test_adds_temp_column_for_source_aggregate(self):
        """Test that temp column is added for source aggregate."""
        query = "SELECT sum(foo.amount) FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy = AggregateDFCPolicy(
            source="foo",
            constraint="sum(foo.amount) > 100",
            on_fail=Resolution.INVALIDATE,
        )
        
        apply_aggregate_policy_constraints_to_aggregation(parsed, [policy], {"foo"})
        
        # Should have temp column added
        expressions = parsed.expressions
        assert len(expressions) >= 2  # Original + temp column
        
        # Find temp column
        temp_col_found = False
        for expr in expressions:
            if isinstance(expr, exp.Alias):
                alias_name = expr.alias.this if hasattr(expr.alias, 'this') else str(expr.alias)
                if alias_name and alias_name.startswith("_policy_") and "_tmp" in alias_name:
                    temp_col_found = True
                    break
        
        assert temp_col_found, "Temp column not found in SELECT list"

    def test_adds_multiple_temp_columns_for_multiple_aggregates(self):
        """Test that multiple temp columns are added for multiple aggregates."""
        query = "SELECT sum(foo.amount), max(foo.id) FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        
        policy = AggregateDFCPolicy(
            source="foo",
            constraint="sum(foo.amount) > 100 AND max(foo.id) > 10",
            on_fail=Resolution.INVALIDATE,
        )
        
        apply_aggregate_policy_constraints_to_aggregation(parsed, [policy], {"foo"})
        
        # Should have temp columns for both aggregates
        expressions = parsed.expressions
        temp_cols = []
        for expr in expressions:
            if isinstance(expr, exp.Alias):
                alias_name = expr.alias.this if hasattr(expr.alias, 'this') else str(expr.alias)
                if alias_name and alias_name.startswith("_policy_") and "_tmp" in alias_name:
                    temp_cols.append(alias_name)
        
        assert len(temp_cols) == 2, f"Expected 2 temp columns, found {len(temp_cols)}"

    def test_does_not_modify_query_without_policies(self):
        """Test that query is not modified when no aggregate policies are provided."""
        query = "SELECT sum(foo.amount) FROM foo"
        parsed = sqlglot.parse_one(query, read="duckdb")
        original_sql = parsed.sql()
        
        apply_aggregate_policy_constraints_to_aggregation(parsed, [], {"foo"})
        
        # Query should be unchanged
        assert parsed.sql() == original_sql


class TestGetPolicyIdentifier:
    """Tests for get_policy_identifier function."""

    def test_same_policy_same_identifier(self):
        """Test that same policy produces same identifier."""
        policy1 = AggregateDFCPolicy(
            source="users",
            sink="reports",
            constraint="sum(users.amount) > 100",
            on_fail=Resolution.INVALIDATE,
        )
        policy2 = AggregateDFCPolicy(
            source="users",
            sink="reports",
            constraint="sum(users.amount) > 100",
            on_fail=Resolution.INVALIDATE,
        )
        
        id1 = get_policy_identifier(policy1)
        id2 = get_policy_identifier(policy2)
        
        assert id1 == id2
        assert id1.startswith("policy_")

    def test_different_policies_different_identifiers(self):
        """Test that different policies produce different identifiers."""
        policy1 = AggregateDFCPolicy(
            source="users",
            constraint="sum(users.amount) > 100",
            on_fail=Resolution.INVALIDATE,
        )
        policy2 = AggregateDFCPolicy(
            source="users",
            constraint="sum(users.amount) > 200",
            on_fail=Resolution.INVALIDATE,
        )
        
        id1 = get_policy_identifier(policy1)
        id2 = get_policy_identifier(policy2)
        
        assert id1 != id2
