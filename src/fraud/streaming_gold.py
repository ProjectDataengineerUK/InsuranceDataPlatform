import argparse
import sys
from pathlib import Path

# Job roda como spark_python_task via workspace files (sem empacotamento em
# wheel) — Databricks só põe o diretório do próprio script no sys.path, não a
# raiz do bundle. Sem isso, "from src..." abaixo falha com ModuleNotFoundError.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.streaming import StreamingQuery

from src.common.delta_merge import merge_into_delta
from src.common.spark_session import get_spark
from src.fraud.streaming_score import DEFAULT_SCORE_THRESHOLD, score_claims
from src.quality.checks import check_not_null, check_unique, persist_results

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
    approved = apply_auto_approval(scored).withColumn("_scored_at", current_timestamp())
    return approved.select(
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
        # carregados do Bronze via Silver — usados por
        # scripts/measure_pipeline_latency.py para medir AT-001/latência de
        # score de fraude contra o SLA real, não apenas revisão de código.
        "_ingested_at",
        "_scored_at",
    )


def process_batch(
    batch_df: DataFrame,
    batch_id: int,
    gold_claims_table: str,
    results_table: str,
) -> None:
    if batch_df.rdd.isEmpty():
        return

    claims_gold_df = build_claims_gold(batch_df)

    results = check_not_null(
        claims_gold_df, ["claim_id", "policy_id", "customer_id"], gold_claims_table
    )
    results.append(check_unique(claims_gold_df, ["claim_id"], gold_claims_table))
    persist_results(batch_df.sparkSession, results, results_table)

    merge_into_delta(claims_gold_df, gold_claims_table, key_column="claim_id")


def run_fraud_scoring_stream(
    silver_table: str,
    gold_claims_table: str,
    checkpoint_path: str,
    results_table: str,
) -> StreamingQuery:
    spark: SparkSession = get_spark(f"fraud-scoring-stream-{gold_claims_table}")
    silver_stream = spark.readStream.format("delta").table(silver_table)

    return (
        silver_stream.writeStream.foreachBatch(
            lambda batch_df, batch_id: process_batch(
                batch_df, batch_id, gold_claims_table, results_table
            )
        )
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="30 seconds")
        .start()
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--gold-claims-table", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    parser.add_argument("--results-table", required=True)
    args = parser.parse_args()

    query = run_fraud_scoring_stream(
        args.silver_table, args.gold_claims_table, args.checkpoint_path, args.results_table
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
