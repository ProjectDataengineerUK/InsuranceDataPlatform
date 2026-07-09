import argparse
import sys
from pathlib import Path

# Job roda como spark_python_task via workspace files (sem empacotamento em
# wheel) — Databricks só põe o diretório do próprio script no sys.path, não a
# raiz do bundle. Sem isso, "from src..." abaixo falha com ModuleNotFoundError.
# Databricks executa o job via exec(compile(source, filename, 'exec')), que
# nao injeta __file__ nos globals — cai pro co_filename do frame atual.
_this_file = globals().get("__file__") or sys._getframe().f_code.co_filename
sys.path.insert(0, str(Path(_this_file).resolve().parents[2]))

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, from_json, lit
from pyspark.sql.streaming import StreamingQuery

from src.common.kafka_config import get_kafka_options
from src.common.schemas import SCHEMA_REGISTRY
from src.common.spark_session import get_spark

REQUIRED_FIELDS = ["event_type", "event_timestamp"]


def _parse_events(raw_stream: DataFrame, schema) -> DataFrame:
    return raw_stream.select(
        col("key").cast("string").alias("event_key"),
        col("value").cast("string").alias("raw_value"),
        from_json(col("value").cast("string"), schema).alias("payload"),
        col("timestamp").alias("kafka_timestamp"),
    ).withColumn("_ingested_at", current_timestamp())


def _is_malformed(parsed_df: DataFrame) -> DataFrame:
    condition = col("payload").isNull()
    for field in REQUIRED_FIELDS:
        condition = condition | col(f"payload.{field}").isNull()
    return condition


def _write_batch(batch_df: DataFrame, batch_id: int, bronze_table: str) -> None:
    malformed = _is_malformed(batch_df)

    valid_df = (
        batch_df.filter(~malformed)
        .select("event_key", "payload.*", "kafka_timestamp", "_ingested_at")
        .withColumn("_ingested_date", col("_ingested_at").cast("date"))
    )
    if not valid_df.rdd.isEmpty():
        (
            valid_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("_ingested_date")
            .saveAsTable(bronze_table)
        )

    quarantine_df = (
        batch_df.filter(malformed)
        .select("event_key", "raw_value", "kafka_timestamp", "_ingested_at")
        .withColumn("_reason", lit("schema_validation_failed"))
        .withColumn("_ingested_date", col("_ingested_at").cast("date"))
    )
    if not quarantine_df.rdd.isEmpty():
        (
            quarantine_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("_ingested_date")
            .saveAsTable(f"{bronze_table}_quarantine")
        )


def run_bronze_ingest(topic: str, bronze_table: str, checkpoint_path: str) -> StreamingQuery:
    if topic not in SCHEMA_REGISTRY:
        raise ValueError(f"no schema registered for topic '{topic}'")

    spark = get_spark(f"bronze-ingest-{topic}")
    schema = SCHEMA_REGISTRY[topic]

    raw_stream = spark.readStream.format("kafka").options(**get_kafka_options(topic)).load()
    parsed_stream = _parse_events(raw_stream, schema)

    return (
        parsed_stream.writeStream.foreachBatch(
            lambda batch_df, batch_id: _write_batch(batch_df, batch_id, bronze_table)
        )
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="30 seconds")
        .start()
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True)
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    args = parser.parse_args()

    query = run_bronze_ingest(args.topic, args.bronze_table, args.checkpoint_path)
    query.awaitTermination()


if __name__ == "__main__":
    main()
