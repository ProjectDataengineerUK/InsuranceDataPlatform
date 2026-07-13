from datetime import datetime

from src.open_insurance.consent_scd import run_consent_scd


def test_run_consent_scd_populates_silver_and_results(spark):
    bronze_df = spark.createDataFrame(
        [("p1", "GRANTED", "insurer_a", "claims,profile", datetime(2026, 1, 1, 10, 0, 0))],
        ["policy_id", "consent_status", "target_institution", "scope", "event_timestamp"],
    )
    bronze_df.write.format("delta").mode("overwrite").saveAsTable("consent_scd_test_bronze")

    run_consent_scd(
        bronze_table="consent_scd_test_bronze",
        silver_table="consent_scd_test_silver",
        results_table="consent_scd_test_results",
    )

    silver_rows = bronze_df.sparkSession.read.table("consent_scd_test_silver").collect()
    assert len(silver_rows) == 1
    assert silver_rows[0]["policy_id"] == "p1"
    assert silver_rows[0]["is_current"] is True

    results_rows = bronze_df.sparkSession.read.table("consent_scd_test_results").collect()
    assert len(results_rows) >= 1


def test_run_consent_scd_is_noop_on_empty_bronze(spark):
    empty_df = spark.createDataFrame(
        [],
        "policy_id STRING, consent_status STRING, target_institution STRING, "
        "scope STRING, event_timestamp TIMESTAMP",
    )
    empty_df.write.format("delta").mode("overwrite").saveAsTable("consent_scd_test_empty_bronze")

    run_consent_scd(
        bronze_table="consent_scd_test_empty_bronze",
        silver_table="consent_scd_test_empty_silver",
        results_table="consent_scd_test_empty_results",
    )

    assert not spark.catalog.tableExists("consent_scd_test_empty_silver")
