"""Tests for llm_validation_grid strategy configuration helpers."""

from vldb_experiments.strategies.llm_validation_common import (
    default_database_specs,
    normalize_database_specs,
)


def test_llm_validation_grid_default_database_specs_are_stable() -> None:
    specs = default_database_specs([0.1, 0.5, 1.0, 5.0, 10.0], "llm_validation_grid")

    assert specs == [
        {
            "label": "sf0.1",
            "tpch_sf": 0.1,
            "db_path": "./results/llm_validation_grid_sf0.1.db",
        },
        {
            "label": "sf0.5",
            "tpch_sf": 0.5,
            "db_path": "./results/llm_validation_grid_sf0.5.db",
        },
        {
            "label": "sf1",
            "tpch_sf": 1.0,
            "db_path": "./results/llm_validation_grid_sf1.db",
        },
        {
            "label": "sf5",
            "tpch_sf": 5.0,
            "db_path": "./results/llm_validation_grid_sf5.db",
        },
        {
            "label": "sf10",
            "tpch_sf": 10.0,
            "db_path": "./results/llm_validation_grid_sf10.db",
        },
    ]


def test_llm_validation_grid_normalize_database_specs_respects_explicit_values() -> None:
    specs = normalize_database_specs(
        database_specs=[
            {"label": "small", "tpch_sf": 0.1, "db_path": "/tmp/small.db"},
            {"tpch_sf": 2.0},
        ],
        database_sfs=None,
        base_filename_prefix="ignored",
    )

    assert specs == [
        {
            "label": "small",
            "tpch_sf": 0.1,
            "db_path": "/tmp/small.db",
        },
        {
            "label": "sf2",
            "tpch_sf": 2.0,
            "db_path": "./results/ignored_sf2.db",
        },
    ]
