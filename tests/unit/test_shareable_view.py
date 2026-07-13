from datetime import datetime

from src.open_insurance.shareable_view import run_shareable_view


def _write_table(spark, rows, schema, table_name):
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").saveAsTable(
        table_name
    )


def test_shareable_view_includes_granted_with_and_without_claims(spark):
    _write_table(
        spark,
        [
            ("p1", "GRANTED", "insurer_a", "claims,profile", True, datetime(2026, 1, 1), None),
            ("p2", "GRANTED", "insurer_b", "claims,profile", True, datetime(2026, 1, 1), None),
            ("p3", "REVOKED", "insurer_a", "claims,profile", True, datetime(2026, 1, 1), None),
        ],
        [
            "policy_id",
            "consent_status",
            "target_institution",
            "scope",
            "is_current",
            "valid_from",
            "valid_to",
        ],
        "shareable_test_consent_silver",
    )
    _write_table(
        spark,
        [
            ("p1", "Ana Silva", 30, 0.5),
            ("p2", "Bruno Souza", 40, 0.2),
            ("p3", "Carla Costa", 50, 0.9),
        ],
        ["policy_id", "synthetic_name", "synthetic_age", "risk_score"],
        "shareable_test_profile",
    )
    _write_table(
        spark,
        [("c1", "p1", "claim-opened", 1000.0, "SP")],
        ["claim_id", "policy_id", "event_type", "amount", "region"],
        "shareable_test_claims",
    )

    run_shareable_view(
        consent_silver_table="shareable_test_consent_silver",
        profile_table="shareable_test_profile",
        claims_table="shareable_test_claims",
        consent_view_table="shareable_test_consent_view",
        shareable_view_table="shareable_test_shareable_view",
    )

    rows = {
        row["policy_id"]: row for row in spark.read.table("shareable_test_shareable_view").collect()
    }

    assert set(rows.keys()) == {"p1", "p2"}
    assert rows["p1"]["claim_id"] == "c1"
    assert rows["p2"]["claim_id"] is None


def test_shareable_view_skips_when_base_tables_missing(spark):
    run_shareable_view(
        consent_silver_table="shareable_test_missing_silver",
        profile_table="shareable_test_missing_profile",
        claims_table="shareable_test_missing_claims",
        consent_view_table="shareable_test_missing_consent_view",
        shareable_view_table="shareable_test_missing_shareable_view",
    )
    assert not spark.catalog.tableExists("shareable_test_missing_shareable_view")
