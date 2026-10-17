from __future__ import annotations

import pandas as pd
import pytest

from scripts.generate_regulation_z_v2_benchmark_chart import _merge_model_metrics


def test_merge_model_metrics_preserves_main_run_baselines() -> None:
    main = pd.DataFrame(
        {
            "num_updates": [100],
            "dfc_time_ms": [25.0],
            "no_policy_time_ms": [1090.92],
        }
    )
    additional = pd.DataFrame(
        {
            "num_updates": [100],
            "dfc_time_ms": [30.0],
            "no_policy_time_ms": [242.34],
            "gpt_oss_120b_time_ms": [1255700.0],
            "gpt_oss_120b_accuracy": [1.0],
        }
    )

    merged = _merge_model_metrics(main, additional)

    assert merged.to_dict(orient="records") == [
        {
            "num_updates": 100,
            "dfc_time_ms": 25.0,
            "no_policy_time_ms": 1090.92,
            "gpt_oss_120b_time_ms": 1255700.0,
            "gpt_oss_120b_accuracy": 1.0,
        }
    ]


def test_merge_model_metrics_rejects_model_overwrite() -> None:
    main = pd.DataFrame({"gpt_oss_120b_time_ms": [100.0]})
    additional = pd.DataFrame({"gpt_oss_120b_time_ms": [200.0]})

    with pytest.raises(
        ValueError,
        match=("Merged model metrics already exist in the main input: gpt_oss_120b_time_ms"),
    ):
        _merge_model_metrics(main, additional)
