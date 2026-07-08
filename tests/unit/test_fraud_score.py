from datetime import datetime

from src.fraud.streaming_score import (
    add_frequency_feature,
    add_night_claim_feature,
    compute_fraud_score,
)


def test_add_night_claim_feature_flags_night_events(spark):
    df = spark.createDataFrame(
        [
            ("c1", datetime(2026, 1, 1, 2, 0, 0)),
            ("c2", datetime(2026, 1, 1, 14, 0, 0)),
        ],
        ["claim_id", "event_timestamp"],
    )

    result = {
        row["claim_id"]: row["feature_night_claim"]
        for row in add_night_claim_feature(df).collect()
    }

    assert result["c1"] == 1.0
    assert result["c2"] == 0.0


def test_add_frequency_feature_flags_high_frequency_customers(spark):
    df = spark.createDataFrame(
        [
            ("cl1", "cust1", datetime(2026, 1, 1, 8, 0, 0)),
            ("cl2", "cust1", datetime(2026, 1, 1, 9, 0, 0)),
            ("cl3", "cust1", datetime(2026, 1, 1, 10, 0, 0)),
            ("cl4", "cust2", datetime(2026, 1, 1, 8, 0, 0)),
        ],
        ["claim_id", "customer_id", "event_timestamp"],
    )

    result = add_frequency_feature(df, max_expected_claims=3).collect()
    by_claim = {row["claim_id"]: row["feature_high_frequency"] for row in result}

    assert by_claim["cl3"] == 1.0
    assert by_claim["cl4"] < by_claim["cl3"]


def test_compute_fraud_score_produces_bounded_scores(spark):
    df = spark.createDataFrame(
        [
            ("cl1", "cust1", datetime(2026, 1, 1, 2, 0, 0), 1000.0, "SP"),
            ("cl2", "cust2", datetime(2026, 1, 1, 14, 0, 0), 1200.0, "SP"),
        ],
        ["claim_id", "customer_id", "event_timestamp", "amount", "region"],
    )

    scored = compute_fraud_score(df).collect()

    for row in scored:
        assert 0.0 <= row["fraud_score"] <= 1.0
