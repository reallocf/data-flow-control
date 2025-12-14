"""Tests for DFCPolicy."""

import pytest
from sql_rewriter.policy import DFCPolicy, Resolution


def test_policy_with_source_only_rejects_unaggregated():
    """Test that policies with source table reject unaggregated source columns."""
    with pytest.raises(ValueError, match="All columns from source table.*must be aggregated"):
        DFCPolicy(
            source="users",
            constraint="users.age >= 18",
            on_fail=Resolution.REMOVE,
        )


def test_policy_with_source_only_accepts_aggregated():
    """Test creating a policy with only a source table and aggregated columns."""
    policy = DFCPolicy(
        source="users",
        constraint="max(users.age) >= 18",
        on_fail=Resolution.REMOVE,
    )
    assert policy.source == "users"
    assert policy.sink is None
    assert policy.constraint == "max(users.age) >= 18"
    assert policy.on_fail == Resolution.REMOVE


def test_policy_with_sink_only():
    """Test creating a policy with only a sink table."""
    policy = DFCPolicy(
        sink="reports",
        constraint="reports.status = 'approved'",
        on_fail=Resolution.KILL,
    )
    assert policy.source is None
    assert policy.sink == "reports"
    assert policy.constraint == "reports.status = 'approved'"
    assert policy.on_fail == Resolution.KILL


def test_policy_with_both_source_and_sink():
    """Test creating a policy with both source and sink."""
    policy = DFCPolicy(
        source="users",
        sink="analytics",
        constraint="max(users.id) = analytics.user_id",
        on_fail=Resolution.REMOVE,
    )
    assert policy.source == "users"
    assert policy.sink == "analytics"
    assert policy.constraint == "max(users.id) = analytics.user_id"
    assert policy.on_fail == Resolution.REMOVE


def test_policy_requires_source_or_sink():
    """Test that a policy must have either source or sink."""
    with pytest.raises(ValueError, match="Either source or sink must be provided"):
        DFCPolicy(
            constraint="users.age >= 18",
            on_fail=Resolution.REMOVE,
        )


def test_policy_validation_invalid_source():
    """Test that invalid source table names are rejected."""
    # Invalid SQL syntax
    with pytest.raises(ValueError, match="Invalid source table"):
        DFCPolicy(
            source="invalid sql syntax!!!",
            constraint="users.age >= 18",
            on_fail=Resolution.REMOVE,
        )


def test_policy_validation_invalid_sink():
    """Test that invalid sink table names are rejected."""
    with pytest.raises(ValueError, match="Invalid sink table"):
        DFCPolicy(
            sink="invalid sql syntax!!!",
            constraint="users.age >= 18",
            on_fail=Resolution.REMOVE,
        )


def test_policy_validation_invalid_constraint():
    """Test that invalid constraint expressions are rejected."""
    # Invalid SQL syntax
    with pytest.raises(ValueError, match="Invalid constraint SQL expression"):
        DFCPolicy(
            source="users",
            constraint="this is not valid SQL!!!",
            on_fail=Resolution.REMOVE,
        )


def test_policy_validation_constraint_cannot_be_select():
    """Test that constraint cannot be a SELECT statement."""
    with pytest.raises(ValueError, match="Constraint must be an expression, not a SELECT statement"):
        DFCPolicy(
            source="users",
            constraint="SELECT * FROM users",
            on_fail=Resolution.REMOVE,
        )


def test_policy_validation_constraint_with_source():
    """Test that constraint can reference source table columns (must be aggregated)."""
    policy = DFCPolicy(
        source="users",
        constraint="max(users.age) >= 18 AND min(users.status) = 'active'",
        on_fail=Resolution.REMOVE,
    )
    assert policy.constraint == "max(users.age) >= 18 AND min(users.status) = 'active'"


def test_policy_validation_constraint_with_sink():
    """Test that constraint can reference sink table columns."""
    policy = DFCPolicy(
        sink="reports",
        constraint="reports.created_at > '2024-01-01'",
        on_fail=Resolution.KILL,
    )
    assert policy.constraint == "reports.created_at > '2024-01-01'"


def test_policy_validation_constraint_with_both_tables():
    """Test that constraint can reference both source and sink tables."""
    policy = DFCPolicy(
        source="users",
        sink="orders",
        constraint="max(users.id) = orders.user_id AND min(users.status) = 'active'",
        on_fail=Resolution.REMOVE,
    )
    assert policy.constraint == "max(users.id) = orders.user_id AND min(users.status) = 'active'"


def test_policy_repr():
    """Test string representation of policy."""
    policy = DFCPolicy(
        source="users",
        sink="analytics",
        constraint="max(users.age) >= 18",
        on_fail=Resolution.REMOVE,
    )
    repr_str = repr(policy)
    assert "source='users'" in repr_str
    assert "sink='analytics'" in repr_str
    assert "constraint='max(users.age) >= 18'" in repr_str
    assert "on_fail=REMOVE" in repr_str


def test_policy_repr_source_only():
    """Test string representation of policy with only source."""
    policy = DFCPolicy(
        source="users",
        constraint="max(users.age) >= 18",
        on_fail=Resolution.KILL,
    )
    repr_str = repr(policy)
    assert "source='users'" in repr_str
    assert "sink=" not in repr_str or "sink=None" in repr_str


def test_policy_equality():
    """Test that two policies with the same values are equal."""
    policy1 = DFCPolicy(
        source="users",
        sink="analytics",
        constraint="max(users.age) >= 18",
        on_fail=Resolution.REMOVE,
    )
    policy2 = DFCPolicy(
        source="users",
        sink="analytics",
        constraint="max(users.age) >= 18",
        on_fail=Resolution.REMOVE,
    )
    assert policy1 == policy2


def test_policy_inequality():
    """Test that two policies with different values are not equal."""
    policy1 = DFCPolicy(
        source="users",
        constraint="max(users.age) >= 18",
        on_fail=Resolution.REMOVE,
    )
    policy2 = DFCPolicy(
        source="users",
        constraint="max(users.age) >= 21",
        on_fail=Resolution.REMOVE,
    )
    assert policy1 != policy2


def test_policy_inequality_different_on_fail():
    """Test that policies with different on_fail actions are not equal."""
    policy1 = DFCPolicy(
        source="users",
        constraint="max(users.age) >= 18",
        on_fail=Resolution.REMOVE,
    )
    policy2 = DFCPolicy(
        source="users",
        constraint="max(users.age) >= 18",
        on_fail=Resolution.KILL,
    )
    assert policy1 != policy2


def test_resolution_enum():
    """Test Resolution enum values."""
    assert Resolution.REMOVE.value == "REMOVE"
    assert Resolution.KILL.value == "KILL"


def test_policy_complex_constraint():
    """Test policy with a complex constraint expression."""
    policy = DFCPolicy(
        source="users",
        constraint="(max(users.age) >= 18 AND min(users.status) = 'active') OR (max(users.age) >= 21 AND min(users.status) = 'pending')",
        on_fail=Resolution.REMOVE,
    )
    assert "max(users.age) >= 18" in policy.constraint
    assert "OR" in policy.constraint


def test_policy_with_table_qualification():
    """Test policy with qualified table names in constraint."""
    policy = DFCPolicy(
        source="users",
        sink="orders",
        constraint="max(users.id) = orders.user_id AND min(users.created_at) < orders.created_at",
        on_fail=Resolution.REMOVE,
    )
    assert "max(users.id)" in policy.constraint
    assert "orders.user_id" in policy.constraint


def test_policy_aggregation_over_source():
    """Test that aggregations over source table are allowed."""
    policy = DFCPolicy(
        source="users",
        sink="reports",
        constraint="max(users.age) > 18 AND reports.status = 'active'",
        on_fail=Resolution.REMOVE,
    )
    assert "max(users.age)" in policy.constraint


def test_policy_aggregation_over_source_only():
    """Test that aggregations over source table are allowed when only source is provided."""
    policy = DFCPolicy(
        source="users",
        constraint="max(users.age) > 18",
        on_fail=Resolution.REMOVE,
    )
    assert "max(users.age)" in policy.constraint


def test_policy_aggregation_rejects_sink():
    """Test that aggregations over sink table are rejected."""
    with pytest.raises(ValueError, match="Aggregation.*references sink table"):
        DFCPolicy(
            source="users",
            sink="reports",
            constraint="max(reports.value) > 10",
            on_fail=Resolution.REMOVE,
        )


def test_policy_aggregation_with_unqualified_column_rejected():
    """Test that aggregations with unqualified columns are rejected."""
    with pytest.raises(ValueError, match="All columns in constraints must be qualified"):
        DFCPolicy(
            sink="reports",
            constraint="max(value) > 10",
            on_fail=Resolution.REMOVE,
        )


def test_policy_aggregation_requires_source():
    """Test that aggregations require a source table."""
    with pytest.raises(ValueError, match="Aggregations in constraints can only reference the source table"):
        DFCPolicy(
            sink="reports",
            constraint="max(reports.value) > 10",
            on_fail=Resolution.REMOVE,
        )


def test_policy_aggregation_mixed_constraint():
    """Test constraint with aggregation over source and regular column from sink."""
    policy = DFCPolicy(
        source="users",
        sink="reports",
        constraint="max(users.age) > 10 AND reports.status = 'active'",
        on_fail=Resolution.REMOVE,
    )
    assert "max(users.age)" in policy.constraint
    assert "reports.status" in policy.constraint


def test_policy_multiple_aggregations_over_source():
    """Test that multiple aggregations over source are allowed."""
    policy = DFCPolicy(
        source="users",
        sink="reports",
        constraint="max(users.age) > 18 AND min(users.age) < 100 AND reports.status = 'active'",
        on_fail=Resolution.REMOVE,
    )
    assert "max(users.age)" in policy.constraint
    assert "min(users.age)" in policy.constraint


def test_policy_aggregation_source_with_sink_column():
    """Test constraint like max(source.foo) > 10 and sink.bar = 'cat'."""
    policy = DFCPolicy(
        source="users",
        sink="reports",
        constraint="max(users.foo) > 10 AND reports.bar = 'cat'",
        on_fail=Resolution.REMOVE,
    )
    assert "max(users.foo)" in policy.constraint
    assert "reports.bar" in policy.constraint


def test_policy_rejects_unqualified_columns_with_source_only():
    """Test that unqualified columns in constraints are rejected when only source is provided."""
    with pytest.raises(ValueError, match="All columns in constraints must be qualified"):
        DFCPolicy(
            source="users",
            constraint="age >= 18",
            on_fail=Resolution.REMOVE,
        )


def test_policy_rejects_unqualified_columns_with_source_and_sink():
    """Test that unqualified columns in constraints are rejected when both source and sink are provided."""
    with pytest.raises(ValueError, match="All columns in constraints must be qualified"):
        DFCPolicy(
            source="users",
            sink="reports",
            constraint="users.age >= 18 AND status = 'active'",
            on_fail=Resolution.REMOVE,
        )


def test_policy_table_name_extraction_with_source_only():
    """Test that table name extraction works correctly with source table only.
    
    This validates that column.table is correctly extracted when it's a string
    or an Identifier object for source-only policies.
    """
    policy = DFCPolicy(
        source="users",
        constraint="max(users.age) > 18",
        on_fail=Resolution.REMOVE,
    )
    assert policy.source == "users"


def test_policy_table_name_extraction_with_source_and_sink():
    """Test that table name extraction works correctly with both source and sink.
    
    This validates that column.table is correctly extracted for policies with
    both source and sink tables.
    """
    policy = DFCPolicy(
        source="users",
        sink="reports",
        constraint="max(users.age) > 18 AND reports.status = 'active'",
        on_fail=Resolution.REMOVE,
    )
    assert policy.source == "users"
    assert policy.sink == "reports"


def test_policy_table_name_extraction_rejects_aggregation_over_sink():
    """Test that table name extraction correctly identifies sink table in aggregations.
    
    This validates that column.table is correctly extracted in aggregation checks,
    ensuring aggregations over sink tables are rejected.
    """
    with pytest.raises(ValueError, match="Aggregation.*references sink table"):
        DFCPolicy(
            source="users",
            sink="reports",
            constraint="max(reports.value) > 10",
            on_fail=Resolution.REMOVE,
        )


def test_policy_table_name_extraction_rejects_unaggregated_source_with_sink():
    """Test that table name extraction correctly identifies unaggregated source columns.
    
    This validates that column.table is correctly extracted in source column checks,
    ensuring unaggregated source columns are rejected when sink is also present.
    """
    with pytest.raises(ValueError, match="All columns from source table.*must be aggregated"):
        DFCPolicy(
            source="users",
            sink="reports",
            constraint="users.age > 18 AND reports.status = 'active'",
            on_fail=Resolution.REMOVE,
        )


def test_policy_table_name_extraction_rejects_multiple_unaggregated_source_columns():
    """Test that table name extraction correctly identifies multiple unaggregated source columns.
    
    This validates that column.table is correctly extracted for all source columns,
    ensuring all unaggregated source columns are identified.
    """
    with pytest.raises(ValueError, match="All columns from source table.*must be aggregated"):
        DFCPolicy(
            source="users",
            constraint="users.age > 18 AND users.status = 'active'",
            on_fail=Resolution.REMOVE,
        )

