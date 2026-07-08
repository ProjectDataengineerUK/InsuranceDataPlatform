import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.window import Window

from src.common.delta_merge import merge_into_delta
from src.common.spark_session import get_spark
from src.quality.checks import check_not_null, check_unique, persist_results


def _deduplicate(df: DataFrame, key_column: str, order_column: str) -> DataFrame:
    window = Window.partitionBy(key_column).orderBy(df[order_column].desc())
    return (
        df.withColumn("_row_number", row_number().over(window))
        .filter("_row_number = 1")
        .drop("_row_number")
    )


def process_batch(
    batch_df: DataFrame,
    batch_id: int,
    silver_table: str,
    key_column: str,
    results_table: str,
) -> None:
    if batch_df.rdd.isEmpty():
        return

    deduped = _deduplicate(batch_df, key_column, order_column="_ingested_at")
    deduped = deduped.withColumn("event_date", col("event_timestamp").cast("date"))

    results = check_not_null(deduped, [key_column, "event_type"], silver_table)
    results.append(check_unique(deduped, [key_column], silver_table))
    persist_results(batch_df.sparkSession, results, results_table)

    merge_into_delta(deduped, silver_table, key_column)


def run_silver_transform(
    bronze_table: str,
    silver_table: str,
    checkpoint_path: str,
    key_column: str,
    results_table: str,
) -> StreamingQuery:
    spark: SparkSession = get_spark(f"silver-transform-{silver_table}")
    bronze_stream = spark.readStream.format("delta").table(bronze_table)

    return (
        bronze_stream.writeStream.foreachBatch(
            lambda batch_df, batch_id: process_batch(
                batch_df, batch_id, silver_table, key_column, results_table
            )
        )
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="1 minute")
        .start()
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    parser.add_argument("--key-column", required=True)
    parser.add_argument("--results-table", required=True)
    args = parser.parse_args()

    query = run_silver_transform(
        args.bronze_table,
        args.silver_table,
        args.checkpoint_path,
        args.key_column,
        args.results_table,
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
