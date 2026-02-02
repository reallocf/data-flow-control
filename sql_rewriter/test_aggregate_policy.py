"""Tests for AggregateDFCPolicy."""

import pytest
import sqlglot
from sqlglot import exp

from sql_rewriter.policy import AggregateDFCPolicy, DFCPolicy, Resolution
from sql_rewriter.rewrite_rule import (
    apply_aggregate_policy_constraints_to_aggregation,
    apply_aggregate_policy_constraints_to_scan,
    get_policy_identifier,
)


class TestAggregateDFCPolicyCreation:
    """Tests for AggregateDFCPolicy creation and validation."""

    def test_aggregate_policy_requires_source_or_sink(self):
        """Test that aggregate policy must have either source or sink."""
        with pytest.raises(ValueError, match="Either source or sink must be provided"):
            AggregateDFCPolicy(
                constraint="max(users.age) >= 18",
                on_fail=Resolution.INVALIDATE,
            )

    def test_aggregate_policy_only_supports_invalidate(self):
        """Test that aggregate policy only supports INVALIDATE resolution initially."""
        with pytest.raises(ValueError, match="currently only supports INVALIDATE resolution"):
            AggregateDFCPolicy(
                source="users",
                constraint="sum(users.amount) > 100",
                on_fail=Resolution.REMOVE,
            )

    def test_aggregate_policy_with_source_only(self):
        """Test creating aggregate policy with only source table."""
        policy = AggregateDFCPolicy(
            source="users",
            constraint="sum(users.amount) > 100",
            on_fail=Resolution.INVALIDATE,
        )
        assert policy.source == "users"
        assert policy.sink is None
        assert policy.constraint == "sum(users.amount) > 100"
        assert policy.on_fail == Resolution.INVALIDATE

    def test_aggregate_policy_with_sink_only(self):
        """Test creating aggregate policy with only sink table."""
        policy = AggregateDFCPolicy(
            sink="reports",
            constraint="sum(reports.value) > 100",
            on_fail=Resolution.INVALIDATE,
        )
        assert policy.source is None
        assert policy.sink == "reports"
        assert policy.constraint == "sum(reports.value) > 100"
        assert policy.on_fail == Resolution.INVALIDATE

    def test_aggregate_policy_with_filter_clause(self):
        """Test creating aggregate policy with FILTER clause in aggregate function."""
        policy = AggregateDFCPolicy(
            source="bank_txn",
            sink="irs_form",
            constraint="sum(irs_form.amount) filter (where irs_form.kind = 'Income') > 4000",
            on_fail=Resolution.INVALIDATE,
        )
        assert policy.source == "bank_txn"
        assert policy.sink == "irs_form"
        assert policy.constraint == "sum(irs_form.amount) filter (where irs_form.kind = 'Income') > 4000"
        assert policy.on_fail == Resolution.INVALIDATE

    def test_aggregate_policy_with_filter_clause_unqualified_in_aggregate(self):
        """Test aggregate policy with FILTER clause where aggregate argument is unqualified table name."""
        # This tests the case: sum(irs_form) filter (where irs_form.kind = 'Income')
        # where irs_form in the aggregate argument is unqualified but should be allowed
        policy = AggregateDFCPolicy(
            source="bank_txn",
            sink="irs_form",
            constraint="sum(irs_form) filter (where irs_form.kind = 'Income') > 4000",
            on_fail=Resolution.INVALIDATE,
        )
        assert policy.source == "bank_txn"
        assert policy.sink == "irs_form"
        assert "sum(irs_form)" in policy.constraint.lower()
        assert "filter" in policy.constraint.lower()
        assert policy.on_fail == Resolution.INVALIDATE

    def test_aggregate_policy_with_filter_clause_parsing(self):
        """Test that aggregate policy with FILTER clause can be parsed from string."""
        policy_str = "AGGREGATE SOURCE bank_txn SINK irs_form CONSTRAINT sum(irs_form) filter (where irs_form.kind = 'Income') > 4000 ON FAIL INVALIDATE"
        policy = AggregateDFCPolicy.from_policy_str(policy_str)
        assert policy.source == "bank_txn"
        assert policy.sink == "irs_form"
        assert "sum(irs_form)" in policy.constraint.lower()
        assert "filter" in policy.constraint.lower()
        assert policy.on_fail == Resolution.INVALIDATE

    def test_aggregate_policy_with_both_source_and_sink(self):
        """Test creating aggregate policy with both source and sink."""
        policy = AggregateDFCPolicy(
            source="users",
            sink="reports",
            constraint="sum(sum(users.amount)) > sum(reports.total)",
            on_fail=Resolution.INVALIDATE,
        )
        assert policy.source == "users"
        assert policy.sink == "reports"
        assert policy.constraint == "sum(sum(users.amount)) > sum(reports.total)"
        assert policy.on_fail == Resolution.INVALIDATE

    def test_aggregate_policy_source_must_be_aggregated(self):
        """Test that source columns must be aggregated in aggregate policies."""
    with pytest.raises(ValueError, match=r"All columns from source table.*must be aggregated"):
            AggregateDFCPolicy(
                source="users",
                constraint="users.amount > 100",
                on_fail=Resolution.INVALIDATE,
            )

    def test_aggregate_policy_allows_sink_aggregation(self):
        """Test that aggregate policies allow sink aggregations (unlike regular policies)."""
        policy = AggregateDFCPolicy(
            source="users",
            sink="reports",
            constraint="sum(users.amount) > sum(reports.total)",
            on_fail=Resolution.INVALIDATE,
        )
        assert policy.constraint == "sum(users.amount) > sum(reports.total)"

    def test_aggregate_policy_allows_unaggregated_sink(self):
        """Test that aggregate policies allow unaggregated sink columns."""
        policy = AggregateDFCPolicy(
            source="users",
            sink="reports",
            constraint="sum(users.amount) > reports.threshold",
            on_fail=Resolution.INVALIDATE,
        )
        assert policy.constraint == "sum(users.amount) > reports.threshold"

    def test_aggregate_policy_requires_column_qualification(self):
        """Test that aggregate policies require column qualification."""
        with pytest.raises(ValueError, match="All columns in constraints must be qualified"):
            AggregateDFCPolicy(
                source="users",
                constraint="sum(amount) > 100",
                on_fail=Resolution.INVALIDATE,
            )

    def test_aggregate_policy_repr(self):
        """Test string representation of aggregate policy."""
        policy = AggregateDFCPolicy(
            source="users",
            sink="reports",
            constraint="sum(users.amount) > 100",
            on_fail=Resolution.INVALIDATE,
            description="Test policy",
        )
        repr_str = repr(policy)
        assert "AggregateDFCPolicy" in repr_str
        assert "source='users'" in repr_str
        assert "sink='reports'" in repr_str
        assert "constraint='sum(users.amount) > 100'" in repr_str
        assert "on_fail=INVALIDATE" in repr_str
        assert "description='Test policy'" in repr_str

    def test_aggregate_policy_equality(self):
        """Test that two aggregate policies with same values are equal."""
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
        assert policy1 == policy2

    def test_aggregate_policy_inequality(self):
        """Test that two aggregate policies with different values are not equal."""
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
        assert policy1 != policy2


class TestAggregateDFCPolicyParsing:
    """Tests for AggregateDFCPolicy.from_policy_str."""

    def test_parse_aggregate_policy_with_source_only(self):
        """Test parsing aggregate policy with only source."""
        policy_str = "AGGREGATE SOURCE users CONSTRAINT sum(users.amount) > 100 ON FAIL INVALIDATE"
        policy = AggregateDFCPolicy.from_policy_str(policy_str)
        assert policy.source == "users"
        assert policy.sink is None
        assert policy.constraint == "sum(users.amount) > 100"
        assert policy.on_fail == Resolution.INVALIDATE

    def test_parse_aggregate_policy_with_sink_only(self):
        """Test parsing aggregate policy with only sink."""
        policy_str = "AGGREGATE SINK reports CONSTRAINT sum(reports.value) > 100 ON FAIL INVALIDATE"
        policy = AggregateDFCPolicy.from_policy_str(policy_str)
        assert policy.source is None
        assert policy.sink == "reports"
        assert policy.constraint == "sum(reports.value) > 100"
        assert policy.on_fail == Resolution.INVALIDATE

    def test_parse_aggregate_policy_with_both(self):
        """Test parsing aggregate policy with both source and sink."""
        policy_str = "AGGREGATE SOURCE users SINK reports CONSTRAINT sum(users.amount) > sum(reports.total) ON FAIL INVALIDATE"
        policy = AggregateDFCPolicy.from_policy_str(policy_str)
        assert policy.source == "users"
        assert policy.sink == "reports"
        assert policy.constraint == "sum(users.amount) > sum(reports.total)"
        assert policy.on_fail == Resolution.INVALIDATE

    def test_parse_aggregate_policy_with_description(self):
        """Test parsing aggregate policy with description."""
        policy_str = "AGGREGATE SOURCE users CONSTRAINT sum(users.amount) > 100 ON FAIL INVALIDATE DESCRIPTION Test aggregate policy"
        policy = AggregateDFCPolicy.from_policy_str(policy_str)
        assert policy.source == "users"
        assert policy.constraint == "sum(users.amount) > 100"
        assert policy.description == "Test aggregate policy"

    def test_parse_aggregate_policy_requires_aggregate_keyword(self):
        """Test that parsing requires AGGREGATE keyword."""
        policy_str = "SOURCE users CONSTRAINT sum(users.amount) > 100 ON FAIL INVALIDATE"
        with pytest.raises(ValueError, match="requires 'AGGREGATE' keyword"):
            AggregateDFCPolicy.from_policy_str(policy_str)

    def test_parse_aggregate_policy_case_insensitive(self):
        """Test that AGGREGATE keyword is case-insensitive."""
        policy_str = "aggregate SOURCE users CONSTRAINT sum(users.amount) > 100 ON FAIL INVALIDATE"
        policy = AggregateDFCPolicy.from_policy_str(policy_str)
        assert policy.source == "users"
        assert policy.constraint == "sum(users.amount) > 100"

    def test_parse_aggregate_policy_with_whitespace(self):
        """Test parsing aggregate policy with various whitespace."""
        policy_str = "AGGREGATE\nSOURCE\tusers\nSINK\treports\nCONSTRAINT sum(users.amount) > 100\nON FAIL INVALIDATE"
        policy = AggregateDFCPolicy.from_policy_str(policy_str)
        assert policy.source == "users"
        assert policy.sink == "reports"
        assert policy.constraint == "sum(users.amount) > 100"


class TestAggregatePolicyRewriting:
    """Tests for aggregate policy query rewriting."""

    def test_adds_temp_columns_for_source_aggregate(self):
        """Test that temp columns are added for source aggregates."""
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

        # Find temp column and assert full SQL
        temp_col_found = False
        for expr in expressions:
            if isinstance(expr, exp.Alias):
                alias_name = expr.alias.this if hasattr(expr.alias, "this") else str(expr.alias)
                if alias_name and alias_name.startswith("_policy_") and "_tmp" in alias_name:
                    temp_col_found = True
                    # Assert full SQL of the temp column expression
                    temp_sql = expr.this.sql()
                    assert temp_sql == "SUM(foo.amount)"
                    break

        assert temp_col_found, "Temp column not found in SELECT list"

    def test_adds_temp_columns_for_sink_expression(self):
        """Test that temp columns are added for sink expressions."""
        query = "SELECT bar.value FROM bar"
        parsed = sqlglot.parse_one(query, read="duckdb")

        policy = AggregateDFCPolicy(
            sink="bar",
            constraint="bar.value > 100",
            on_fail=Resolution.INVALIDATE,
        )

        apply_aggregate_policy_constraints_to_scan(parsed, [policy], set(), sink_table="bar")

        # Should have temp column added
        expressions = parsed.expressions
        assert len(expressions) >= 2  # Original + temp column

        # Find temp column
        temp_col_found = False
        for expr in expressions:
            if isinstance(expr, exp.Alias):
                alias_name = expr.alias.this if hasattr(expr.alias, "this") else str(expr.alias)
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
                alias_name = expr.alias.this if hasattr(expr.alias, "this") else str(expr.alias)
                if alias_name and alias_name.startswith("_policy_") and "_tmp" in alias_name:
                    temp_cols.append(alias_name)

        assert len(temp_cols) == 2, f"Expected 2 temp columns, found {len(temp_cols)}"

    def test_policy_identifier_generation(self):
        """Test that policy identifiers are generated correctly."""
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

        # Same policy should have same identifier
        assert id1 == id2
        assert id1.startswith("policy_")
        assert len(id1) > len("policy_")

    def test_different_policies_have_different_identifiers(self):
        """Test that different policies have different identifiers."""
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

        # Different constraints should have different identifiers
        assert id1 != id2


class TestAggregatePolicyFinalize:
    """Tests for finalize_aggregate_policies method."""

    @pytest.fixture
    def rewriter_with_data(cls):
        """Create a SQLRewriter with test data for finalize tests."""
        from sql_rewriter import SQLRewriter
        rewriter = SQLRewriter()

        # Create source table
        rewriter.execute("CREATE TABLE bank_txn (txn_id INTEGER, amount DOUBLE)")
        rewriter.execute("INSERT INTO bank_txn VALUES (1, 100.0), (2, 200.0), (3, 300.0)")

        # Create sink table with temp columns
        rewriter.execute("""
            CREATE TABLE irs_form (
                txn_id INTEGER,
                amount DOUBLE,
                _policy_test_tmp1 DOUBLE,
                _policy_test_tmp2 DOUBLE
            )
        """)

        # Insert data with temp columns (inner aggregates and sink values)
        rewriter.execute("""
            INSERT INTO irs_form (txn_id, amount, _policy_test_tmp1, _policy_test_tmp2)
            VALUES
                (1, 100.0, 100.0, 50.0),
                (2, 200.0, 200.0, 75.0),
                (3, 300.0, 300.0, 100.0)
        """)

        yield rewriter
        rewriter.close()

    def test_finalize_with_no_policies(self, rewriter_with_data):
        """Test finalize with no aggregate policies."""
        violations = rewriter_with_data.finalize_aggregate_policies("irs_form")
        assert violations == {}

    def test_finalize_with_passing_constraint(self, rewriter_with_data):
        """Test finalize with constraint that passes."""
        # Note: In real usage, constraints reference source/sink tables, not temp columns
        # Temp columns are only used internally during finalize evaluation
        # This test just verifies finalize runs without error
        policy = AggregateDFCPolicy(
            source="bank_txn",
            sink="irs_form",
            constraint="sum(bank_txn.amount) > 500",
            on_fail=Resolution.INVALIDATE,
        )
        rewriter_with_data.register_policy(policy)

        # Finalize should run without error
        violations = rewriter_with_data.finalize_aggregate_policies("irs_form")
        assert isinstance(violations, dict)

    def test_finalize_with_nonexistent_table(self, rewriter_with_data):
        """Test finalize with nonexistent sink table."""
        # Create the table first so we can register the policy
        rewriter_with_data.execute("CREATE TABLE IF NOT EXISTS nonexistent (id INTEGER)")

        policy = AggregateDFCPolicy(
            source="bank_txn",
            sink="nonexistent",
            constraint="sum(bank_txn.amount) > 100",
            on_fail=Resolution.INVALIDATE,
        )
        rewriter_with_data.register_policy(policy)

        # Drop the table to simulate nonexistent table
        rewriter_with_data.execute("DROP TABLE IF EXISTS nonexistent")

        violations = rewriter_with_data.finalize_aggregate_policies("nonexistent")
        # Should return empty dict or dict with None values for policies with no data
        assert isinstance(violations, dict)

    def test_two_stage_aggregation_source_columns(self, rewriter_with_data):
        """Test that source columns are aggregated twice: inner during rewriting, outer during finalize."""
        # Policy with nested aggregate: max(sum(bank_txn.amount)) > 500
        policy = AggregateDFCPolicy(
            source="bank_txn",
            sink="reports",
            constraint="max(sum(bank_txn.amount)) > 500",
            on_fail=Resolution.INVALIDATE,
        )
        policy_id = get_policy_identifier(policy)
        temp_col_name = f"_{policy_id}_tmp1"

        # Create sink table with temp column for inner aggregate
        rewriter_with_data.execute("DROP TABLE IF EXISTS reports")
        rewriter_with_data.execute(f"""
            CREATE TABLE reports (
                id INTEGER,
                value DOUBLE,
                {temp_col_name} DOUBLE
            )
        """)

        # Insert data with inner aggregate values in temp column
        # For constraint max(sum(bank_txn.amount)) > 500:
        # - Inner aggregate: sum(bank_txn.amount) per group -> stored in temp column
        # - Outer aggregate: max(temp_column) -> computed during finalize
        rewriter_with_data.execute(f"""
            INSERT INTO reports (id, value, {temp_col_name})
            VALUES
                (1, 100.0, 150.0),  -- group 1: sum = 150
                (2, 200.0, 250.0),  -- group 2: sum = 250
                (3, 300.0, 350.0)    -- group 3: sum = 350
        """)

        # This should fail because max(150, 250, 350) = 350, which is not > 500
        rewriter_with_data.register_policy(policy)

        violations = rewriter_with_data.finalize_aggregate_policies("reports")
        assert policy_id in violations
        assert violations[policy_id] is not None  # Should have violation
        # Verify the error message format
        assert "Aggregate policy constraint violated" in violations[policy_id]
        assert "max(sum(bank_txn.amount)) > 500" in violations[policy_id]

        # Now test with data that passes
        rewriter_with_data.execute("DROP TABLE IF EXISTS reports")
        rewriter_with_data.execute(f"""
            CREATE TABLE reports (
                id INTEGER,
                value DOUBLE,
                {temp_col_name} DOUBLE
            )
        """)
        rewriter_with_data.execute(f"""
            INSERT INTO reports (id, value, {temp_col_name})
            VALUES
                (1, 100.0, 200.0),
                (2, 200.0, 300.0),
                (3, 300.0, 600.0)  -- max(200, 300, 600) = 600 > 500, should pass
        """)

        violations = rewriter_with_data.finalize_aggregate_policies("reports")
        assert violations[policy_id] is None  # Should pass

    def test_sink_columns_aggregated_once(self, rewriter_with_data):
        """Test that sink columns are aggregated once during finalize."""
        policy = AggregateDFCPolicy(
            source="bank_txn",
            sink="reports",
            constraint="sum(reports.value) > 500",
            on_fail=Resolution.INVALIDATE,
        )
        policy_id = get_policy_identifier(policy)
        temp_col_name = f"_{policy_id}_tmp1"

        # Create sink table with temp column for sink values
        rewriter_with_data.execute("DROP TABLE IF EXISTS reports")
        rewriter_with_data.execute(f"""
            CREATE TABLE reports (
                id INTEGER,
                value DOUBLE,
                {temp_col_name} DOUBLE
            )
        """)

        # Insert data with sink values in temp column
        # For constraint sum(reports.value) > 500:
        # - Sink values stored unaggregated in temp column
        # - Finalize computes: sum(temp_column)
        rewriter_with_data.execute(f"""
            INSERT INTO reports (id, value, {temp_col_name})
            VALUES
                (1, 100.0, 100.0),
                (2, 200.0, 200.0),
                (3, 300.0, 300.0)  -- sum(100, 200, 300) = 600 > 500, should pass
        """)

        rewriter_with_data.register_policy(policy)

        violations = rewriter_with_data.finalize_aggregate_policies("reports")
        assert policy_id in violations
        assert violations[policy_id] is None  # Should pass (600 > 500)

        # Test with data that fails
        rewriter_with_data.execute("DROP TABLE IF EXISTS reports")
        rewriter_with_data.execute(f"""
            CREATE TABLE reports (
                id INTEGER,
                value DOUBLE,
                {temp_col_name} DOUBLE
            )
        """)
        rewriter_with_data.execute(f"""
            INSERT INTO reports (id, value, {temp_col_name})
            VALUES
                (1, 100.0, 100.0),
                (2, 150.0, 150.0),
                (3, 200.0, 200.0)  -- sum(100, 150, 200) = 450 < 500, should fail
        """)

        violations = rewriter_with_data.finalize_aggregate_policies("reports")
        assert violations[policy_id] is not None  # Should have violation
        # Verify the error message format
        assert "Aggregate policy constraint violated" in violations[policy_id]
        assert "sum(reports.value) > 500" in violations[policy_id]

    def test_combined_source_and_sink_aggregation(self, rewriter_with_data):
        """Test policy with both source (two-stage) and sink (one-stage) aggregation."""
        policy = AggregateDFCPolicy(
            source="bank_txn",
            sink="reports",
            constraint="max(sum(bank_txn.amount)) > sum(reports.value)",
            on_fail=Resolution.INVALIDATE,
        )
        policy_id = get_policy_identifier(policy)
        source_temp_col = f"_{policy_id}_tmp1"  # source inner aggregate
        sink_temp_col = f"_{policy_id}_tmp2"     # sink values

        rewriter_with_data.execute("DROP TABLE IF EXISTS reports")
        rewriter_with_data.execute(f"""
            CREATE TABLE reports (
                id INTEGER,
                value DOUBLE,
                {source_temp_col} DOUBLE,  -- source inner aggregate
                {sink_temp_col} DOUBLE      -- sink values
            )
        """)

        # Insert data
        # Constraint: max(sum(bank_txn.amount)) > sum(reports.value)
        # - Source: max(sum(bank_txn.amount)) -> max of inner aggregates
        # - Sink: sum(reports.value) -> sum of sink values
        rewriter_with_data.execute(f"""
            INSERT INTO reports (id, value, {source_temp_col}, {sink_temp_col})
            VALUES
                (1, 100.0, 150.0, 100.0),  -- group 1: sum(bank_txn.amount) = 150
                (2, 200.0, 250.0, 200.0),  -- group 2: sum(bank_txn.amount) = 250
                (3, 300.0, 350.0, 300.0)    -- group 3: sum(bank_txn.amount) = 350
        """)
        # max(150, 250, 350) = 350, sum(100, 200, 300) = 600
        # 350 > 600 is False, should fail

        rewriter_with_data.register_policy(policy)

        violations = rewriter_with_data.finalize_aggregate_policies("reports")
        assert policy_id in violations
        assert violations[policy_id] is not None  # Should have violation (350 not > 600)
        # Verify the error message format
        assert "Aggregate policy constraint violated" in violations[policy_id]
        assert "max(sum(bank_txn.amount)) > sum(reports.value)" in violations[policy_id]

    def test_finalize_returns_error_message_when_constraint_fails(self, rewriter_with_data):
        """Test that finalize returns the correct error message format when constraint fails."""
        policy = AggregateDFCPolicy(
            source="bank_txn",
            sink="reports",
            constraint="sum(bank_txn.amount) > 1000",
            on_fail=Resolution.INVALIDATE,
            description="Test policy description",
        )
        policy_id = get_policy_identifier(policy)
        temp_col_name = f"_{policy_id}_tmp1"

        # Create sink table with temp column
        rewriter_with_data.execute("DROP TABLE IF EXISTS reports")
        rewriter_with_data.execute(f"""
            CREATE TABLE reports (
                id INTEGER,
                value DOUBLE,
                {temp_col_name} DOUBLE
            )
        """)

        # Insert data that will fail the constraint
        # sum(100, 200, 300) = 600, which is not > 1000
        rewriter_with_data.execute(f"""
            INSERT INTO reports (id, value, {temp_col_name})
            VALUES
                (1, 100.0, 100.0),
                (2, 200.0, 200.0),
                (3, 300.0, 300.0)
        """)

        rewriter_with_data.register_policy(policy)

        violations = rewriter_with_data.finalize_aggregate_policies("reports")
        assert policy_id in violations
        assert violations[policy_id] is not None

        # Verify the exact error message format
        violation_message = violations[policy_id]
        assert "Test policy description" in violation_message
        assert "Aggregate policy constraint violated" in violation_message
        assert "sum(bank_txn.amount) > 1000" in violation_message
        # Verify the format: "description: Aggregate policy constraint violated: constraint"
        assert violation_message.startswith("Test policy description: Aggregate policy constraint violated: sum(bank_txn.amount) > 1000")

        # Test without description
        policy_no_desc = AggregateDFCPolicy(
            source="bank_txn",
            sink="reports",
            constraint="sum(bank_txn.amount) > 2000",
            on_fail=Resolution.INVALIDATE,
        )
        policy_no_desc_id = get_policy_identifier(policy_no_desc)
        temp_col_name_no_desc = f"_{policy_no_desc_id}_tmp1"

        # Add the temp column for the second policy
        rewriter_with_data.execute(f"ALTER TABLE reports ADD COLUMN {temp_col_name_no_desc} DOUBLE")
        # Update the temp column values (same as first policy for this test)
        rewriter_with_data.execute(f"UPDATE reports SET {temp_col_name_no_desc} = {temp_col_name}")

        rewriter_with_data.register_policy(policy_no_desc)

        violations = rewriter_with_data.finalize_aggregate_policies("reports")
        assert policy_no_desc_id in violations
        assert violations[policy_no_desc_id] is not None

        # Without description, should just be: "Aggregate policy constraint violated: constraint"
        violation_message_no_desc = violations[policy_no_desc_id]
        assert violation_message_no_desc == "Aggregate policy constraint violated: sum(bank_txn.amount) > 2000"

    def test_finalize_preserves_filter_clause(self, rewriter_with_data):
        """Test that FILTER clauses are preserved when replacing sink expressions during finalize."""
        # Policy with FILTER clause: sum(irs_form) filter (where irs_form.kind = 'Income') > 4000
        policy = AggregateDFCPolicy(
            source="bank_txn",
            sink="irs_form",
            constraint="sum(irs_form) filter (where irs_form.kind = 'Income') > 4000",
            on_fail=Resolution.INVALIDATE,
            description="Ensure sum of income entries exceeds threshold",
        )
        policy_id = get_policy_identifier(policy)
        temp_col_name = f"_{policy_id}_tmp1"

        # Create sink table with temp column
        rewriter_with_data.execute("DROP TABLE IF EXISTS irs_form")
        rewriter_with_data.execute(f"""
            CREATE TABLE irs_form (
                txn_id INTEGER,
                amount DOUBLE,
                kind VARCHAR,
                {temp_col_name} DOUBLE
            )
        """)

        # Insert data: Income entries are 1500 + 2000 = 3500, which is < 4000, so should fail
        # The FILTER clause should ensure only Income entries are summed
        rewriter_with_data.execute(f"""
            INSERT INTO irs_form (txn_id, amount, kind, {temp_col_name})
            VALUES
                (1, 1500.0, 'Income', 1500.0),
                (2, 250.0, 'Expense', 250.0),
                (3, 2000.0, 'Income', 2000.0),
                (4, 500.0, 'Expense', 500.0)
        """)

        rewriter_with_data.register_policy(policy)

        violations = rewriter_with_data.finalize_aggregate_policies("irs_form")
        assert policy_id in violations
        assert violations[policy_id] is not None  # Should have violation (3500 < 4000)

        # Verify the violation message includes the FILTER clause
        violation_message = violations[policy_id]
        assert "filter" in violation_message.lower()
        assert "irs_form.kind = 'Income'" in violation_message or "irs_form.kind = 'income'" in violation_message.lower()

        # Now test with data that passes (sum > 4000)
        rewriter_with_data.execute("DROP TABLE IF EXISTS irs_form")
        rewriter_with_data.execute(f"""
            CREATE TABLE irs_form (
                txn_id INTEGER,
                amount DOUBLE,
                kind VARCHAR,
                {temp_col_name} DOUBLE
            )
        """)

        # Insert data: Income entries are 2000 + 2500 = 4500, which is > 4000, so should pass
        rewriter_with_data.execute(f"""
            INSERT INTO irs_form (txn_id, amount, kind, {temp_col_name})
            VALUES
                (1, 2000.0, 'Income', 2000.0),
                (2, 250.0, 'Expense', 250.0),
                (3, 2500.0, 'Income', 2500.0),
                (4, 500.0, 'Expense', 500.0)
        """)

        violations = rewriter_with_data.finalize_aggregate_policies("irs_form")
        assert violations[policy_id] is None  # Should pass (4500 > 4000)

        # Test that FILTER actually filters: if we have enough total but not enough Income, it should fail
        rewriter_with_data.execute("DROP TABLE IF EXISTS irs_form")
        rewriter_with_data.execute(f"""
            CREATE TABLE irs_form (
                txn_id INTEGER,
                amount DOUBLE,
                kind VARCHAR,
                {temp_col_name} DOUBLE
            )
        """)

        # Insert data: Total is 5000, but Income is only 1000 + 2000 = 3000, which is < 4000
        # The FILTER should ensure only Income entries are considered
        rewriter_with_data.execute(f"""
            INSERT INTO irs_form (txn_id, amount, kind, {temp_col_name})
            VALUES
                (1, 1000.0, 'Income', 1000.0),
                (2, 1500.0, 'Expense', 1500.0),
                (3, 2000.0, 'Income', 2000.0),
                (4, 500.0, 'Expense', 500.0)
        """)

        violations = rewriter_with_data.finalize_aggregate_policies("irs_form")
        assert violations[policy_id] is not None  # Should fail (3000 < 4000, FILTER ensures only Income is summed)

    def test_aggregate_policy_temp_columns_in_insert_column_list(self, rewriter_with_data):
        """Test that aggregate policy temp columns are added to INSERT column list."""
        # Create sink table
        rewriter_with_data.execute("DROP TABLE IF EXISTS irs_form")
        rewriter_with_data.execute("CREATE TABLE irs_form (txn_id INTEGER, amount DOUBLE, kind VARCHAR)")

        policy = AggregateDFCPolicy(
            source="bank_txn",
            sink="irs_form",
            constraint="sum(irs_form.amount) filter (where irs_form.kind = 'Income') > 4000",
            on_fail=Resolution.INVALIDATE,
        )
        policy_id = get_policy_identifier(policy)
        temp_col_name = f"_{policy_id}_tmp1"
        rewriter_with_data.register_policy(policy)

        # Create source table
        rewriter_with_data.execute("CREATE TABLE IF NOT EXISTS bank_txn (txn_id INTEGER, amount DOUBLE)")

        # Test INSERT query
        query = "INSERT INTO irs_form (txn_id, amount, kind) SELECT txn_id, ABS(amount), 'Expense' FROM bank_txn WHERE txn_id = 1"
        transformed = rewriter_with_data.transform_query(query)

        # Verify temp column is in SELECT
        assert temp_col_name in transformed

        # Verify temp column is in INSERT column list
        # Should be: INSERT INTO irs_form (txn_id, amount, kind, _policy_xxx_tmp1)
        # Check that temp column appears after the explicit columns
        insert_part = transformed.split("SELECT")[0]
        assert temp_col_name in insert_part

        # Verify the temp column expression is in SELECT
        # For scan queries, FILTER clauses that reference SELECT output columns are transformed
        # into CASE expressions. The condition 'kind = 'Income'' is replaced with the actual
        # value from the SELECT ('Expense'), so we should see a CASE expression.
        assert "CASE" in transformed or "case" in transformed.lower()
        assert "amount" in transformed.lower() or "AMOUNT" in transformed
        # The condition should be replaced with the actual value ('Expense' = 'Income')
        assert "'Expense'" in transformed or '"Expense"' in transformed or "Expense" in transformed

        # Note: For scan queries, aggregates with FILTER clauses that reference SELECT output
        # columns are transformed into CASE expressions because:
        # 1. Scan queries can't use aggregates in SELECT without GROUP BY
        # 2. FILTER conditions referencing SELECT output columns need to be evaluated per row
        # The important part is that the temp column is correctly added to both
        # the SELECT and INSERT column lists, which we've already verified above.

    def test_aggregate_policy_filter_replaces_output_column_with_value_expense(self, rewriter_with_data):
        """Test that FILTER conditions referencing output columns are replaced with actual values (Expense case)."""
        # Create sink table (we'll add the temp column after we know the policy ID)
        rewriter_with_data.execute("DROP TABLE IF EXISTS irs_form")
        rewriter_with_data.execute("CREATE TABLE irs_form (txn_id INTEGER, amount DOUBLE, kind VARCHAR)")

        policy = AggregateDFCPolicy(
            source="bank_txn",
            sink="irs_form",
            constraint="sum(irs_form.amount) filter (where irs_form.kind = 'Income') > 4000",
            on_fail=Resolution.INVALIDATE,
        )
        policy_id = get_policy_identifier(policy)
        temp_col_name = f"_{policy_id}_tmp1"
        rewriter_with_data.register_policy(policy)

        # Add the temp column to the table
        rewriter_with_data.execute(f"ALTER TABLE irs_form ADD COLUMN {temp_col_name} DOUBLE")

        # Create source table
        rewriter_with_data.execute("CREATE TABLE IF NOT EXISTS bank_txn (txn_id INTEGER, amount DOUBLE)")

        # Test INSERT query with 'Expense' - should result in 0 (Expense != Income)
        query = "INSERT INTO irs_form (txn_id, amount, kind) SELECT txn_id, ABS(amount), 'Expense' FROM bank_txn WHERE txn_id = 1"
        transformed = rewriter_with_data.transform_query(query)

        # Verify temp column is in SELECT and INSERT
        assert temp_col_name in transformed
        insert_part = transformed.split("SELECT")[0]
        assert temp_col_name in insert_part

        # Verify the condition was replaced: 'Expense' = 'Income' should be in the CASE
        assert "'Expense' = 'Income'" in transformed or '"Expense" = "Income"' in transformed
        assert "CASE WHEN" in transformed or "case when" in transformed.lower()
        assert "ELSE 0" in transformed or "else 0" in transformed.lower()

        # Verify we can execute this query (use original query, not transformed, to avoid double transformation)
        rewriter_with_data.execute("INSERT INTO bank_txn VALUES (1, 100.0)")
        rewriter_with_data.execute(query)

        # Verify the temp column has value 0 (since Expense != Income)
        result = rewriter_with_data.execute(f"SELECT {temp_col_name} FROM irs_form").fetchone()
        assert result is not None
        assert result[0] == 0.0, f"Expected 0.0 for Expense, got {result[0]}"

    def test_aggregate_policy_filter_replaces_output_column_with_value_income(self, rewriter_with_data):
        """Test that FILTER conditions referencing output columns are replaced with actual values (Income case)."""
        # Create sink table (we'll add the temp column after we know the policy ID)
        rewriter_with_data.execute("DROP TABLE IF EXISTS irs_form")
        rewriter_with_data.execute("CREATE TABLE irs_form (txn_id INTEGER, amount DOUBLE, kind VARCHAR)")

        policy = AggregateDFCPolicy(
            source="bank_txn",
            sink="irs_form",
            constraint="sum(irs_form.amount) filter (where irs_form.kind = 'Income') > 4000",
            on_fail=Resolution.INVALIDATE,
        )
        policy_id = get_policy_identifier(policy)
        temp_col_name = f"_{policy_id}_tmp1"
        rewriter_with_data.register_policy(policy)

        # Add the temp column to the table
        rewriter_with_data.execute(f"ALTER TABLE irs_form ADD COLUMN {temp_col_name} DOUBLE")

        # Create source table
        rewriter_with_data.execute("CREATE TABLE IF NOT EXISTS bank_txn (txn_id INTEGER, amount DOUBLE)")

        # Test INSERT query with 'Income' - should result in amount (Income == Income)
        query = "INSERT INTO irs_form (txn_id, amount, kind) SELECT txn_id, ABS(amount), 'Income' FROM bank_txn WHERE txn_id = 1"
        transformed = rewriter_with_data.transform_query(query)

        # Verify temp column is in SELECT and INSERT
        assert temp_col_name in transformed
        insert_part = transformed.split("SELECT")[0]
        assert temp_col_name in insert_part

        # Verify the condition was replaced: 'Income' = 'Income' should be in the CASE
        assert "'Income' = 'Income'" in transformed or '"Income" = "Income"' in transformed
        assert "CASE WHEN" in transformed or "case when" in transformed.lower()
        assert "THEN amount" in transformed or "then amount" in transformed.lower()

        # Verify we can execute this query (use original query, not transformed, to avoid double transformation)
        rewriter_with_data.execute("INSERT INTO bank_txn VALUES (1, 100.0)")
        rewriter_with_data.execute(query)

        # Verify the temp column has the amount value (since Income == Income)
        result = rewriter_with_data.execute(f"SELECT {temp_col_name} FROM irs_form").fetchone()
        assert result is not None
        assert result[0] == 100.0, f"Expected 100.0 for Income, got {result[0]}"

    def test_aggregate_policy_filter_replaces_output_column_with_complex_expression(self, rewriter_with_data):
        """Test that FILTER conditions work with complex expressions in output columns."""
        # Create sink table (we'll add the temp column after we know the policy ID)
        rewriter_with_data.execute("DROP TABLE IF EXISTS irs_form")
        rewriter_with_data.execute("CREATE TABLE irs_form (txn_id INTEGER, amount DOUBLE, kind VARCHAR)")

        policy = AggregateDFCPolicy(
            source="bank_txn",
            sink="irs_form",
            constraint="sum(irs_form.amount) filter (where irs_form.kind = 'Income') > 4000",
            on_fail=Resolution.INVALIDATE,
        )
        policy_id = get_policy_identifier(policy)
        temp_col_name = f"_{policy_id}_tmp1"
        rewriter_with_data.register_policy(policy)

        # Add the temp column to the table
        rewriter_with_data.execute(f"ALTER TABLE irs_form ADD COLUMN {temp_col_name} DOUBLE")

        # Create source table with category column
        rewriter_with_data.execute("DROP TABLE IF EXISTS bank_txn")
        rewriter_with_data.execute("CREATE TABLE bank_txn (txn_id INTEGER, amount DOUBLE, category VARCHAR)")
        rewriter_with_data.execute("INSERT INTO bank_txn VALUES (1, 100.0, 'meal')")

        # Test INSERT query with a CASE expression for kind
        # Note: Use single quotes for string literals in SQL
        query = "INSERT INTO irs_form (txn_id, amount, kind) SELECT txn_id, ABS(amount), CASE WHEN category = 'meal' THEN 'Expense' ELSE 'Income' END FROM bank_txn WHERE txn_id = 1"
        transformed = rewriter_with_data.transform_query(query)

        # Verify temp column is in SELECT and INSERT
        assert temp_col_name in transformed
        insert_part = transformed.split("SELECT")[0]
        assert temp_col_name in insert_part

        # Verify the condition was replaced with the CASE expression
        # The CASE expression should be in the FILTER condition replacement
        assert "CASE WHEN" in transformed or "case when" in transformed.lower()

        # Verify we can execute this query (use original query, not transformed, to avoid double transformation)
        rewriter_with_data.execute(query)

        # Verify the temp column exists and has a value
        # For 'meal' category, kind will be 'Expense', so the condition 'Expense' = 'Income' is false, value should be 0
        result = rewriter_with_data.execute(f"SELECT {temp_col_name} FROM irs_form").fetchone()
        assert result is not None
        assert result[0] == 0.0, f"Expected 0.0 for Expense (from meal category), got {result[0]}"


class TestAggregatePolicyIntegration:
    """Integration tests for aggregate policies with rewriter."""

    @pytest.fixture
    def rewriter(cls):
        """Create a SQLRewriter instance with test data."""
        from sql_rewriter import SQLRewriter
        rewriter = SQLRewriter()

        rewriter.execute("CREATE TABLE foo (id INTEGER, amount DOUBLE)")
        rewriter.execute("INSERT INTO foo VALUES (1, 100.0), (2, 200.0), (3, 300.0)")

        rewriter.execute("CREATE TABLE bar (id INTEGER, total DOUBLE)")

        yield rewriter
        rewriter.close()

    def test_register_aggregate_policy(self, rewriter):
        """Test registering an aggregate policy."""
        # Create sink table (aggregate policies don't require 'valid' column)
        rewriter.execute("CREATE TABLE IF NOT EXISTS bar (id INTEGER, value DOUBLE)")

        policy = AggregateDFCPolicy(
            source="foo",
            sink="bar",
            constraint="sum(foo.amount) > 100",
            on_fail=Resolution.INVALIDATE,
        )
        rewriter.register_policy(policy)

        aggregate_policies = rewriter.get_aggregate_policies()
        assert len(aggregate_policies) == 1
        assert aggregate_policies[0] == policy

    def test_aggregate_policy_separate_from_regular_policy(self, rewriter):
        """Test that aggregate policies are stored separately from regular policies."""
        # Create sink table
        rewriter.execute("CREATE TABLE IF NOT EXISTS bar (id INTEGER, value DOUBLE)")

        regular_policy = DFCPolicy(
            source="foo",
            constraint="max(foo.id) > 1",
            on_fail=Resolution.REMOVE,
        )
        aggregate_policy = AggregateDFCPolicy(
            source="foo",
            sink="bar",
            constraint="sum(foo.amount) > 100",
            on_fail=Resolution.INVALIDATE,
        )

        rewriter.register_policy(regular_policy)
        rewriter.register_policy(aggregate_policy)

        regular_policies = rewriter.get_dfc_policies()
        aggregate_policies = rewriter.get_aggregate_policies()

        assert len(regular_policies) == 1
        assert len(aggregate_policies) == 1
        assert regular_policies[0] == regular_policy
        assert aggregate_policies[0] == aggregate_policy

    def test_transform_query_adds_temp_columns_for_aggregate_policy(self, rewriter):
        """Test that transform_query adds temp columns for aggregate policies."""
        # Create sink table (aggregate policies don't require 'valid' column)
        rewriter.execute("CREATE TABLE IF NOT EXISTS bar (id INTEGER, total DOUBLE)")

        # Add amount column to foo for the aggregate
        rewriter.execute("ALTER TABLE foo ADD COLUMN IF NOT EXISTS amount DOUBLE")
        rewriter.execute("UPDATE foo SET amount = id * 10.0")

        # Use a constraint with a sink expression to ensure temp columns are added
        policy = AggregateDFCPolicy(
            source="foo",
            sink="bar",
            constraint="sum(bar.total) > 100",
            on_fail=Resolution.INVALIDATE,
        )
        policy_id = get_policy_identifier(policy)
        rewriter.register_policy(policy)

        # Use INSERT query with aggregation to test temp column addition
        query = "INSERT INTO bar (id, total) SELECT id, sum(amount) FROM foo GROUP BY id"
        transformed = rewriter.transform_query(query)

        # Verify temp column is added to SELECT (for sink expression)
        temp_col_name = f"_{policy_id}_tmp1"
        assert temp_col_name in transformed, f"Temp column {temp_col_name} not found in:\n{transformed}"

        # Verify temp column is added to INSERT column list
        # Should be: INSERT INTO bar (id, total, _policy_xxx_tmp1)
        insert_part = transformed.split("SELECT")[0]
        assert temp_col_name in insert_part, f"Temp column {temp_col_name} not in INSERT column list:\n{insert_part}"

        # Verify the temp column expression (SUM) is in SELECT
        assert "SUM(total)" in transformed or "SUM(TOTAL)" in transformed

    def test_finalize_returns_violations_dict(self, rewriter):
        """Test that finalize returns a dictionary of violations."""
        # Create sink table (aggregate policies don't require 'valid' column)
        # Use DROP IF EXISTS to avoid conflicts with other tests
        rewriter.execute("DROP TABLE IF EXISTS bar")
        rewriter.execute("CREATE TABLE bar (id INTEGER, value DOUBLE)")

        policy = AggregateDFCPolicy(
            source="foo",
            sink="bar",
            constraint="sum(foo.amount) > 1000",
            on_fail=Resolution.INVALIDATE,
        )
        rewriter.register_policy(policy)

        violations = rewriter.finalize_aggregate_policies("bar")
        assert isinstance(violations, dict)

    def test_multiple_aggregate_policies(self, rewriter):
        """Test handling multiple aggregate policies."""
        # Create sink table (aggregate policies don't require 'valid' column)
        rewriter.execute("CREATE TABLE IF NOT EXISTS bar (id INTEGER, total DOUBLE)")

        policy1 = AggregateDFCPolicy(
            source="foo",
            sink="bar",
            constraint="sum(foo.amount) > 100",
            on_fail=Resolution.INVALIDATE,
        )
        policy2 = AggregateDFCPolicy(
            source="foo",
            sink="bar",
            constraint="max(foo.id) > 5",
            on_fail=Resolution.INVALIDATE,
        )

        rewriter.register_policy(policy1)
        rewriter.register_policy(policy2)

        aggregate_policies = rewriter.get_aggregate_policies()
        assert len(aggregate_policies) == 2
