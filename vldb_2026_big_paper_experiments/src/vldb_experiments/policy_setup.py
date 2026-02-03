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


def create_test_policies(threshold: int = 100, policy_count: int = 1) -> list[DFCPolicy]:
    """Create multiple source-only DFC policies for testing.

    Policies are created with distinct threshold values to ensure uniqueness.

    Args:
        threshold: Base policy threshold value (default: 100)
        policy_count: Number of policies to create (default: 1)

    Returns:
        List of DFCPolicy instances configured for test_data table
    """
    if policy_count <= 1:
        return [create_test_policy(threshold=threshold)]

    policies = []
    # Keep thresholds within a sane range while ensuring uniqueness.
    min_threshold = 1
    max_threshold = 999
    threshold_span = max_threshold - min_threshold + 1

    for idx in range(policy_count):
        offset = idx % threshold_span
        policy_threshold = min_threshold + ((threshold - min_threshold + offset) % threshold_span)
        policies.append(
            DFCPolicy(
                source="test_data",
                constraint=f"max(test_data.value) > {policy_threshold}",
                on_fail=Resolution.REMOVE,
                description=f"Filter rows where value <= {policy_threshold}",
            )
        )

    return policies
