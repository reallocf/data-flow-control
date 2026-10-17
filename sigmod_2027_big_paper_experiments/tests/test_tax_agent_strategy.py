"""Tests for tax agent strategy policy configuration."""

from vldb_experiments.strategies.tax_agent_strategy import _policy_catalog


def test_tax_agent_policy_catalog_size_is_fixed() -> None:
    """Keep policy count fixed while iterating on policy content."""
    assert len(_policy_catalog()) == 32
