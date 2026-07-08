import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, lit, when
from pyspark.sql.functions import sum as spark_sum

from src.common.spark_session import get_spark
from src.fraud.streaming_score import DEFAULT_SCORE_THRESHOLD, score_claims

DEFAULT_AUTO_APPROVAL_AMOUNT_THRESHOLD = 5000.0


def apply_auto_approval(
    df: DataFrame,
    amount_threshold: float = DEFAULT_AUTO_APPROVAL_AMOUNT_THRESHOLD,
    fraud_threshold: float = DEFAULT_SCORE_THRESHOLD,
) -> DataFrame:
    is_low_value = col("amount") <= lit(amount_threshold)
    is_low_risk = col("fraud_score") < lit(fraud_threshold)
    return df.withColumn("auto_approved", is_low_value & is_low_risk)


def build_claims_gold(silver_claims_df: DataFrame) -> DataFrame:
    scored = score_claims(silver_claims_df)
    return apply_auto_approval(scored).select(
        "claim_id",
        "policy_id",
        "customer_id",
        "event_timestamp",
        "amount",
        "region",
        "vehicle_type",
        "fraud_score",
        "fraud_flag",
        "auto_approved",
        "event_date",
    )


def build_region_aggregates(claims_gold_df: DataFrame) -> DataFrame:
    return claims_gold_df.groupBy("region", "event_date").agg(
        count("claim_id").alias("claim_count"),
        avg("amount").alias("avg_claim_amount"),
        spark_sum(when(col("fraud_flag"), 1).otherwise(0)).alias("flagged_fraud_count"),
        spark_sum(when(col("auto_approved"), 1).otherwise(0)).alias("auto_approved_count"),
    )


def _write_gold_partition(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    run_date: str,
) -> None:
    writer = df.write.format("delta").partitionBy("event_date")

    if spark.catalog.tableExists(table_name):
        writer.mode("overwrite").option("replaceWhere", f"event_date = '{run_date}'").saveAsTable(
            table_name
        )
    else:
        writer.mode("overwrite").saveAsTable(table_name)


def run_gold_job(
    silver_table: str,
    gold_claims_table: str,
    gold_agg_table: str,
    run_date: str,
) -> None:
    spark = get_spark("gold-aggregate")

    silver_df = spark.read.table(silver_table).filter(col("event_date") == lit(run_date))
    claims_gold_df = build_claims_gold(silver_df)
    _write_gold_partition(spark, claims_gold_df, gold_claims_table, run_date)

    region_agg_df = build_region_aggregates(claims_gold_df)
    _write_gold_partition(spark, region_agg_df, gold_agg_table, run_date)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--gold-claims-table", required=True)
    parser.add_argument("--gold-agg-table", required=True)
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()

    run_gold_job(args.silver_table, args.gold_claims_table, args.gold_agg_table, args.run_date)


if __name__ == "__main__":
    main()
