from datetime import datetime
from decimal import Decimal

from src.regulatory.gold_susep_export import build_gold_susep_claims


def test_output_columns_match_real_susep_layout(spark):
    silver_df = spark.createDataFrame(
        [
            (
                "ref1",
                "insurer_a",
                "123",
                datetime(2020, 5, 4),
                Decimal("1000.00"),
                "SP",
                6,
            )
        ],
        ["external_reference_id", "source_system", "policy_id", "event_timestamp", "amount", "region", "cause_code"],
    )

    result_df = build_gold_susep_claims(silver_df)

    assert result_df.columns == ["COD_APO", "D_OCORR", "INDENIZ", "REGIAO", "CAUSA"]


def test_resolved_amount_uses_median_across_sources(spark):
    silver_df = spark.createDataFrame(
        [
            ("ref1", "insurer_a", "123", datetime(2020, 5, 4), Decimal("1000.00"), "SP", 6),
            ("ref1", "insurer_b", "123", datetime(2020, 5, 4), Decimal("1200.00"), "SP", 6),
            ("ref1", "insurer_c", "123", datetime(2020, 5, 4), Decimal("1100.00"), "SP", 6),
        ],
        ["external_reference_id", "source_system", "policy_id", "event_timestamp", "amount", "region", "cause_code"],
    )

    result = build_gold_susep_claims(silver_df).collect()[0]

    assert float(result["INDENIZ"]) == 1100.0
    assert result["COD_APO"] == "123"
    assert result["D_OCORR"] == "20200504"
