from datetime import datetime
from decimal import Decimal

from src.regulatory.gold_susep_export import build_gold_susep_claims

SILVER_COLUMNS = [
    "external_reference_id",
    "source_system",
    "policy_id",
    "event_timestamp",
    "amount",
    "region",
    "cause_code",
]

RECONCILIATION_COLUMNS = ["external_reference_id", "discrepancy_type"]


def _empty_reconciliation_df(spark):
    return spark.createDataFrame([], schema=", ".join(f"{c} string" for c in RECONCILIATION_COLUMNS))


def test_output_columns_match_real_susep_layout_plus_compliance(spark):
    silver_df = spark.createDataFrame(
        [("ref1", "insurer_a", "123", datetime(2020, 5, 4), Decimal("1000.00"), "SP", 6)],
        SILVER_COLUMNS,
    )

    result_df = build_gold_susep_claims(silver_df, _empty_reconciliation_df(spark))

    assert result_df.columns == [
        "COD_APO",
        "D_OCORR",
        "INDENIZ",
        "REGIAO",
        "CAUSA",
        "susep_compliant",
        "compliance_issues",
    ]


def test_resolved_amount_uses_median_across_sources(spark):
    silver_df = spark.createDataFrame(
        [
            ("ref1", "insurer_a", "123", datetime(2020, 5, 4), Decimal("1000.00"), "SP", 6),
            ("ref1", "insurer_b", "123", datetime(2020, 5, 4), Decimal("1200.00"), "SP", 6),
            ("ref1", "insurer_c", "123", datetime(2020, 5, 4), Decimal("1100.00"), "SP", 6),
        ],
        SILVER_COLUMNS,
    )

    result = build_gold_susep_claims(silver_df, _empty_reconciliation_df(spark)).collect()[0]

    assert float(result["INDENIZ"]) == 1100.0
    assert result["COD_APO"] == "123"
    assert result["D_OCORR"] == "20200504"


def test_fully_valid_claim_is_compliant(spark):
    silver_df = spark.createDataFrame(
        [("ref1", "insurer_a", "123", datetime(2020, 5, 4), Decimal("1000.00"), "SP", 6)],
        SILVER_COLUMNS,
    )

    result = build_gold_susep_claims(silver_df, _empty_reconciliation_df(spark)).collect()[0]

    assert result["susep_compliant"] is True
    assert result["compliance_issues"] == ""


def test_invalid_cause_code_is_flagged_non_compliant(spark):
    silver_df = spark.createDataFrame(
        [("ref1", "insurer_a", "123", datetime(2020, 5, 4), Decimal("1000.00"), "SP", 99)],
        SILVER_COLUMNS,
    )

    result = build_gold_susep_claims(silver_df, _empty_reconciliation_df(spark)).collect()[0]

    assert result["susep_compliant"] is False
    assert "causa_invalida" in result["compliance_issues"]


def test_missing_required_field_is_flagged_non_compliant(spark):
    silver_df = spark.createDataFrame(
        [("ref1", "insurer_a", None, datetime(2020, 5, 4), Decimal("1000.00"), "SP", 6)],
        SILVER_COLUMNS,
    )

    result = build_gold_susep_claims(silver_df, _empty_reconciliation_df(spark)).collect()[0]

    assert result["susep_compliant"] is False
    assert "policy_id_ausente" in result["compliance_issues"]


def test_reconciliation_discrepancy_is_flagged_non_compliant(spark):
    silver_df = spark.createDataFrame(
        [("ref1", "insurer_a", "123", datetime(2020, 5, 4), Decimal("1000.00"), "SP", 6)],
        SILVER_COLUMNS,
    )
    reconciliation_df = spark.createDataFrame(
        [("ref1", "amount_mismatch")], RECONCILIATION_COLUMNS
    )

    result = build_gold_susep_claims(silver_df, reconciliation_df).collect()[0]

    assert result["susep_compliant"] is False
    assert "reconciliacao:amount_mismatch" in result["compliance_issues"]
