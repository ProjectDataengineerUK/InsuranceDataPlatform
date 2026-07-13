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
from pyspark.sql.functions import max as spark_max
from pyspark.sql.streaming import StreamingQuery

from src.common.kafka_config import get_kafka_options
from src.common.schemas import SCHEMA_REGISTRY
from src.common.spark_session import get_spark
from src.streaming.kafka_lag_reporter import report_offsets

DEFAULT_REQUIRED_FIELDS = ["event_type", "event_timestamp"]


def _parse_events(raw_stream: DataFrame, schema) -> DataFrame:
    return raw_stream.select(
        col("key").cast("string").alias("event_key"),
        col("value").cast("string").alias("raw_value"),
        from_json(col("value").cast("string"), schema).alias("payload"),
        col("timestamp").alias("kafka_timestamp"),
        # partition/offset nunca são persistidos no Bronze (os .select(...)
        # abaixo em _write_batch listam colunas explícitas) — servem só pra
        # calcular o offset processado por partição e reportar pro
        # kafka_lag_reporter, pra consumer lag aparecer na Metrics API.
        col("partition"),
        col("offset"),
    ).withColumn("_ingested_at", current_timestamp())


def _is_malformed(parsed_df: DataFrame, required_fields: list[str]) -> DataFrame:
    condition = col("payload").isNull()
    for field in required_fields:
        condition = condition | col(f"payload.{field}").isNull()
    return condition


def _write_batch(
    batch_df: DataFrame, batch_id: int, bronze_table: str, required_fields: list[str], topic: str
) -> None:
    malformed = _is_malformed(batch_df, required_fields)

    valid_df = (
        batch_df.filter(~malformed)
        .select("event_key", "payload.*", "kafka_timestamp", "_ingested_at")
        .withColumn("_ingested_date", col("_ingested_at").cast("date"))
    )
    if not valid_df.isEmpty():
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
    if not quarantine_df.isEmpty():
        (
            quarantine_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("_ingested_date")
            .saveAsTable(f"{bronze_table}_quarantine")
        )

    offset_rows = batch_df.groupBy("partition").agg(spark_max("offset").alias("max_offset")).collect()
    partition_offsets = {row["partition"]: row["max_offset"] for row in offset_rows}
    report_offsets(topic, group_id=f"databricks-bronze-{topic}", partition_offsets=partition_offsets)


def run_bronze_ingest(
    topic: str,
    bronze_table: str,
    checkpoint_path: str,
    required_fields: list[str] | None = None,
) -> StreamingQuery:
    if topic not in SCHEMA_REGISTRY:
        raise ValueError(f"no schema registered for topic '{topic}'")

    spark = get_spark(f"bronze-ingest-{topic}")
    schema = SCHEMA_REGISTRY[topic]
    effective_required_fields = (
        DEFAULT_REQUIRED_FIELDS if required_fields is None else required_fields
    )

    raw_stream = spark.readStream.format("kafka").options(**get_kafka_options(topic)).load()
    parsed_stream = _parse_events(raw_stream, schema)

    return (
        parsed_stream.writeStream.foreachBatch(
            lambda batch_df, batch_id: _write_batch(
                batch_df, batch_id, bronze_table, effective_required_fields, topic
            )
        )
        .option("checkpointLocation", checkpoint_path)
        # Compute serverless (obrigatório neste workspace) não suporta trigger
        # ProcessingTime "infinito" — só AvailableNow/Once. O job já roda como
        # "continuous job" no Databricks (ver resources/jobs.bronze.yml), que
        # reinicia o run assim que este termina, então availableNow entrega o
        # mesmo efeito prático de streaming quase-contínuo.
        .trigger(availableNow=True)
        .start()
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True)
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    parser.add_argument(
        "--required-fields",
        default=None,
        help="Campos obrigatórios (separados por vírgula) pra considerar um registro "
        "válido no Bronze — default: event_type,event_timestamp (eventos operacionais). "
        "Tópicos com layout raw/heterogêneo (ex.: regulatory-claim-report) devem passar "
        "só a chave de junção compartilhada (ex.: external_reference_id), já que a "
        "validação de negócio de verdade acontece na Silver, não aqui.",
    )
    args = parser.parse_args()

    required_fields = (
        [field.strip() for field in args.required_fields.split(",")]
        if args.required_fields
        else None
    )
    query = run_bronze_ingest(
        args.topic, args.bronze_table, args.checkpoint_path, required_fields
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
