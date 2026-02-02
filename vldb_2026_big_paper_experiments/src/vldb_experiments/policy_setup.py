"""Policy configuration for microbenchmark experiments."""

from sql_rewriter import DFCPolicy, Resolution


def create_test_policy(threshold: int = 100) -> DFCPolicy:
    """Create a single source-only DFC policy for testing.

    Policy: SOURCE test_data CONSTRAINT max(test_data.value) > threshold ON FAIL REMOVE

    This policy will filter rows where value <= threshold when applied to queries.
    The constraint uses max(value) > threshold, which for scan queries gets transformed
    to value > threshold, filtering out rows with value <= threshold.

    Args:
        threshold: Policy threshold value (default: 100)

    Returns:
        DFCPolicy instance configured for test_data table
    """
    return DFCPolicy(
        source="test_data",
        constraint=f"max(test_data.value) > {threshold}",
        on_fail=Resolution.REMOVE,
        description=f"Filter rows where value <= {threshold}"
    )
