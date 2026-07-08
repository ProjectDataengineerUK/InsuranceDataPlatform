from datetime import datetime

from src.streaming.gold_aggregate import build_claims_gold, build_region_aggregates
from src.streaming.silver_transform import _deduplicate


def _build_bronze_df(spark):
    return spark.createDataFrame(
        [
            ("cl1", "p1", "cust1", "claim-opened", datetime(2026, 1, 1, 2, 0, 0), 1000.0, "SP", "carro", 1),
            ("cl1", "p1", "cust1", "claim-opened", datetime(2026, 1, 1, 2, 0, 0), 1000.0, "SP", "carro", 2),
            ("cl2", "p2", "cust2", "claim-opened", datetime(2026, 1, 1, 14, 0, 0), 4000.0, "SP", "moto", 1),
            ("cl3", "p3", "cust3", "claim-opened", datetime(2026, 1, 1, 15, 0, 0), 50000.0, "RJ", "carro", 1),
        ],
        [
            "claim_id",
            "policy_id",
            "customer_id",
            "event_type",
            "event_timestamp",
            "amount",
            "region",
            "vehicle_type",
            "_ingested_at",
        ],
    )


def test_bronze_to_gold_pipeline_end_to_end(spark):
    bronze_df = _build_bronze_df(spark)

    deduped = _deduplicate(bronze_df, key_column="claim_id", order_column="_ingested_at")
    assert deduped.count() == 3

    silver_df = deduped.withColumn("event_date", deduped["event_timestamp"].cast("date"))

    claims_gold_df = build_claims_gold(silver_df)
    claims = {row["claim_id"]: row for row in claims_gold_df.collect()}

    assert claims["cl1"]["auto_approved"] is True
    assert claims["cl2"]["fraud_score"] > 0
    assert claims["cl3"]["auto_approved"] is False

    region_agg_df = build_region_aggregates(claims_gold_df)
    region_agg = {row["region"]: row for row in region_agg_df.collect()}

    assert region_agg["SP"]["claim_count"] == 2
    assert region_agg["RJ"]["claim_count"] == 1
