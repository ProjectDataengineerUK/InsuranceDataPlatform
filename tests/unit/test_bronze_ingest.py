from src.common.schemas import CLAIM_EVENT_SCHEMA
from src.streaming.bronze_ingest import _is_malformed, _parse_events


def test_malformed_json_is_flagged(spark):
    raw_df = spark.createDataFrame(
        [("k1", "not valid json", 1), ("k2", '{"claim_id": null}', 2)],
        ["key", "value", "timestamp"],
    )

    parsed = _parse_events(raw_df, CLAIM_EVENT_SCHEMA)
    flags = {
        row["event_key"]: row["is_malformed"]
        for row in parsed.withColumn("is_malformed", _is_malformed(parsed)).collect()
    }

    assert flags["k1"] is True
    assert flags["k2"] is True


def test_valid_event_is_not_flagged(spark):
    valid_json = (
        '{"claim_id": "c1", "policy_id": "p1", "customer_id": "cust1", '
        '"event_type": "claim-opened", "event_timestamp": "2026-01-01T10:00:00", '
        '"amount": 100.0, "region": "SP", "vehicle_type": "carro", "source": "susep"}'
    )
    raw_df = spark.createDataFrame([("k1", valid_json, 1)], ["key", "value", "timestamp"])

    parsed = _parse_events(raw_df, CLAIM_EVENT_SCHEMA)
    result = parsed.withColumn("is_malformed", _is_malformed(parsed)).collect()

    assert result[0]["is_malformed"] is False
