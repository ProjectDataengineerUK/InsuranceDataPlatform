from src.streaming.silver_transform import _deduplicate


def test_deduplicate_keeps_latest_by_order_column(spark):
    df = spark.createDataFrame(
        [
            ("c1", 1),
            ("c1", 3),
            ("c1", 2),
            ("c2", 5),
        ],
        ["claim_id", "_ingested_at"],
    )

    result = _deduplicate(df, key_column="claim_id", order_column="_ingested_at").collect()

    by_claim = {row["claim_id"]: row["_ingested_at"] for row in result}
    assert by_claim == {"c1": 3, "c2": 5}
    assert len(result) == 2


def test_deduplicate_is_noop_when_no_duplicates(spark):
    df = spark.createDataFrame(
        [("c1", 1), ("c2", 2), ("c3", 3)],
        ["claim_id", "_ingested_at"],
    )

    result = _deduplicate(df, key_column="claim_id", order_column="_ingested_at")

    assert result.count() == 3
