from datetime import datetime

import pandas as pd

from src.ingestion.producer.datasets.susep_loader import generate_synthetic_claims


def test_generates_one_row_per_claim_in_frequency():
    aggregates_df = pd.DataFrame(
        [
            {
                "REGIAO": "SP",
                "FREQ_SIN1": 3,
                "INDENIZ1": 3000.0,
                "FREQ_SIN2": 0,
                "INDENIZ2": 0.0,
            }
        ]
    )

    result = generate_synthetic_claims(
        aggregates_df,
        reference_period_start=datetime(2024, 1, 1),
        reference_period_end=datetime(2024, 6, 30),
    )

    assert len(result) == 3
    assert (result["region"] == "SP").all()
    assert (result["vehicle_type"] == "roubo_furto").all()
    assert (result["amount"] > 0).all()


def test_skips_zero_frequency_coverages():
    aggregates_df = pd.DataFrame(
        [{"REGIAO": "RJ", "FREQ_SIN1": 0, "INDENIZ1": 0.0, "FREQ_SIN4": 2, "INDENIZ4": 8000.0}]
    )

    result = generate_synthetic_claims(
        aggregates_df,
        reference_period_start=datetime(2024, 1, 1),
        reference_period_end=datetime(2024, 6, 30),
    )

    assert len(result) == 2
    assert (result["vehicle_type"] == "incendio").all()


def test_timestamps_fall_within_reference_period():
    aggregates_df = pd.DataFrame([{"REGIAO": "MG", "FREQ_SIN9": 5, "INDENIZ9": 500.0}])
    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 31)

    result = generate_synthetic_claims(aggregates_df, start, end)

    assert (result["event_timestamp"] >= start).all()
    assert (result["event_timestamp"] <= end).all()


def test_unknown_region_falls_back_when_column_missing():
    aggregates_df = pd.DataFrame([{"FREQ_SIN1": 1, "INDENIZ1": 100.0}])

    result = generate_synthetic_claims(
        aggregates_df,
        reference_period_start=datetime(2024, 1, 1),
        reference_period_end=datetime(2024, 1, 2),
    )

    assert result.iloc[0]["region"] == "UNKNOWN"
