from datetime import datetime, timedelta

from src.common.scd2_merge import merge_scd2_into_delta


def test_merge_scd2_creates_table_on_first_batch(spark):
    df = spark.createDataFrame(
        [("p1", "GRANTED", datetime(2026, 1, 1, 10, 0, 0))],
        ["policy_id", "consent_status", "event_timestamp"],
    )

    merge_scd2_into_delta(
        df, "scd2_test_create", key_column="policy_id", effective_column="event_timestamp"
    )

    result = spark.read.table("scd2_test_create").collect()
    assert len(result) == 1
    assert result[0]["is_current"] is True
    assert result[0]["valid_to"] is None


def test_merge_scd2_closes_previous_version_on_newer_event(spark):
    t1 = datetime(2026, 1, 1, 10, 0, 0)
    t2 = t1 + timedelta(days=1)

    granted = spark.createDataFrame(
        [("p2", "GRANTED", t1)], ["policy_id", "consent_status", "event_timestamp"]
    )
    merge_scd2_into_delta(
        granted, "scd2_test_history", key_column="policy_id", effective_column="event_timestamp"
    )

    revoked = spark.createDataFrame(
        [("p2", "REVOKED", t2)], ["policy_id", "consent_status", "event_timestamp"]
    )
    merge_scd2_into_delta(
        revoked, "scd2_test_history", key_column="policy_id", effective_column="event_timestamp"
    )

    rows = spark.read.table("scd2_test_history").orderBy("event_timestamp").collect()
    assert len(rows) == 2
    assert rows[0]["consent_status"] == "GRANTED"
    assert rows[0]["is_current"] is False
    assert rows[0]["valid_to"] == t2
    assert rows[1]["consent_status"] == "REVOKED"
    assert rows[1]["is_current"] is True
    assert rows[1]["valid_to"] is None


def test_merge_scd2_ignores_out_of_order_event(spark):
    t1 = datetime(2026, 1, 1, 10, 0, 0)
    t0 = t1 - timedelta(days=1)

    granted = spark.createDataFrame(
        [("p3", "GRANTED", t1)], ["policy_id", "consent_status", "event_timestamp"]
    )
    merge_scd2_into_delta(
        granted,
        "scd2_test_out_of_order",
        key_column="policy_id",
        effective_column="event_timestamp",
    )

    stale = spark.createDataFrame(
        [("p3", "REVOKED", t0)], ["policy_id", "consent_status", "event_timestamp"]
    )
    merge_scd2_into_delta(
        stale,
        "scd2_test_out_of_order",
        key_column="policy_id",
        effective_column="event_timestamp",
    )

    rows = spark.read.table("scd2_test_out_of_order").collect()
    assert len(rows) == 1
    assert rows[0]["consent_status"] == "GRANTED"
    assert rows[0]["is_current"] is True


def test_merge_scd2_reprocessing_same_batch_is_idempotent(spark):
    t1 = datetime(2026, 1, 1, 10, 0, 0)
    df = spark.createDataFrame(
        [("p4", "GRANTED", t1)], ["policy_id", "consent_status", "event_timestamp"]
    )
    merge_scd2_into_delta(
        df, "scd2_test_idempotent", key_column="policy_id", effective_column="event_timestamp"
    )
    merge_scd2_into_delta(
        df, "scd2_test_idempotent", key_column="policy_id", effective_column="event_timestamp"
    )

    rows = spark.read.table("scd2_test_idempotent").collect()
    assert len(rows) == 1
