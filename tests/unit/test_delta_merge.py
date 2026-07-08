from src.common.delta_merge import merge_into_delta


def test_merge_into_delta_creates_table_when_absent(spark):
    df = spark.createDataFrame(
        [("c1", "2026-01-01", 10)],
        ["claim_id", "event_date", "amount"],
    )

    merge_into_delta(df, "merge_test_create", key_column="claim_id")

    result = spark.read.table("merge_test_create").collect()
    assert len(result) == 1
    assert result[0]["amount"] == 10


def test_merge_into_delta_upserts_existing_rows(spark):
    initial = spark.createDataFrame(
        [("c1", "2026-01-01", 10), ("c2", "2026-01-01", 20)],
        ["claim_id", "event_date", "amount"],
    )
    merge_into_delta(initial, "merge_test_upsert", key_column="claim_id")

    updates = spark.createDataFrame(
        [("c1", "2026-01-01", 99), ("c3", "2026-01-01", 30)],
        ["claim_id", "event_date", "amount"],
    )
    merge_into_delta(updates, "merge_test_upsert", key_column="claim_id")

    by_claim = {
        row["claim_id"]: row["amount"] for row in spark.read.table("merge_test_upsert").collect()
    }
    assert by_claim == {"c1": 99, "c2": 20, "c3": 30}
