"""Tests for llm_validation strategy policy configuration."""

from vldb_experiments.strategies.llm_validation_strategy import _policy_catalog


def test_llm_validation_policy_catalog_size_is_fixed() -> None:
    """Keep policy count fixed while iterating on policy content."""
    assert len(_policy_catalog()) == 32
