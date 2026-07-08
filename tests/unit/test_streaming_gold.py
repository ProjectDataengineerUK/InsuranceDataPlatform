from datetime import datetime

from src.fraud.streaming_gold import build_claims_gold, process_batch


def _silver_claims_df(spark):
    return spark.createDataFrame(
        [
            (
                "cl1",
                "p1",
                "cust1",
                datetime(2026, 1, 1, 2, 0, 0),
                1000.0,
                "SP",
                "carro",
                "2026-01-01",
                datetime(2026, 1, 1, 2, 0, 5),
            ),
            (
                "cl2",
                "p2",
                "cust2",
                datetime(2026, 1, 1, 14, 0, 0),
                50000.0,
                "RJ",
                "carro",
                "2026-01-01",
                datetime(2026, 1, 1, 14, 0, 5),
            ),
        ],
        [
            "claim_id",
            "policy_id",
            "customer_id",
            "event_timestamp",
            "amount",
            "region",
            "vehicle_type",
            "event_date",
            "_ingested_at",
        ],
    )


def test_build_claims_gold_scores_and_flags_claims(spark):
    df = _silver_claims_df(spark)

    result = {row["claim_id"]: row for row in build_claims_gold(df).collect()}

    assert result["cl1"]["auto_approved"] is True
    assert result["cl2"]["auto_approved"] is False
    assert result["cl1"]["_ingested_at"] is not None
    assert result["cl1"]["_scored_at"] is not None


def test_process_batch_persists_quality_results_and_merges(spark):
    df = _silver_claims_df(spark)

    process_batch(
        df,
        batch_id=0,
        gold_claims_table="test_fraud_gold_claims",
        results_table="test_fraud_dq_results",
    )

    gold_rows = spark.read.table("test_fraud_gold_claims").collect()
    assert len(gold_rows) == 2

    dq_rows = {
        row["check_name"]: row["passed"]
        for row in spark.read.table("test_fraud_dq_results").collect()
    }
    assert dq_rows["unique:claim_id"] is True
    assert dq_rows["not_null:claim_id"] is True
