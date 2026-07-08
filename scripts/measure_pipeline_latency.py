"""Mede AT-001 (latência Kafka -> Bronze), a latência de visibilidade do score
de fraude (Bronze -> Gold, alvo < 1 min do DEFINE) e AT-003 (volume sustentado
sem perda) contra um pipeline rodando de verdade em um workspace Databricks.

Só produz números reais depois que:
  1. o producer (src/ingestion/producer) estiver publicando eventos de verdade
     no Kafka (Confluent Cloud);
  2. os jobs `bronze_ingest`, `silver_transform` e `fraud_score_stream`
     estiverem rodando (deploy em dev já funciona via CI/CD).

Rodar como um job avulso no Databricks (ex.: `databricks bundle run` com um
job ad-hoc apontando para este script) ou via notebook, sempre com
`--catalog` do ambiente onde os jobs estão rodando. Não requer nenhuma nova
infraestrutura: lê apenas as tabelas Delta que os próprios jobs já escrevem.
"""

import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, date_trunc, expr
from pyspark.sql.functions import max as spark_max

from src.common.spark_session import get_spark

KAFKA_TO_BRONZE_SLA_SECONDS = 120  # AT-001: Kafka -> Bronze < 2 min
FRAUD_SCORE_SLA_SECONDS = 60  # DEFINE: score de fraude visível em < 1 min
EXPECTED_EVENTS_PER_MINUTE = 100  # replay.events_per_minute em config.yaml


def _recent(df, timestamp_column: str, window_minutes: int):
    return df.filter(
        col(timestamp_column) >= expr(f"current_timestamp() - INTERVAL {window_minutes} MINUTES")
    )


def measure_kafka_to_bronze_latency(
    spark: SparkSession, bronze_table: str, window_minutes: int = 15
) -> dict:
    """AT-001: latência entre o evento chegar no Kafka (kafka_timestamp) e ser
    persistido no Bronze (_ingested_at)."""
    df = _recent(spark.read.table(bronze_table), "_ingested_at", window_minutes)
    if df.rdd.isEmpty():
        return {"sample_size": 0, "note": "sem eventos recentes no Bronze"}

    latency_df = df.withColumn(
        "latency_seconds",
        expr("unix_timestamp(_ingested_at) - unix_timestamp(kafka_timestamp)"),
    )
    row = latency_df.agg(
        avg("latency_seconds").alias("avg"),
        spark_max("latency_seconds").alias("max"),
        count("*").alias("sample_size"),
    ).first()

    max_seconds = row["max"] or 0
    return {
        "sample_size": row["sample_size"],
        "avg_seconds": row["avg"],
        "max_seconds": max_seconds,
        "sla_seconds": KAFKA_TO_BRONZE_SLA_SECONDS,
        "within_sla": max_seconds <= KAFKA_TO_BRONZE_SLA_SECONDS,
    }


def measure_fraud_score_latency(
    spark: SparkSession, gold_claims_table: str, window_minutes: int = 15
) -> dict:
    """Alvo do DEFINE: score de fraude visível em < 1 min. Mede o tempo entre
    a chegada do claim no Bronze (_ingested_at, carregado através do Silver)
    e o momento em que o job fraud_score_stream escreve o score em Gold
    (_scored_at)."""
    df = _recent(spark.read.table(gold_claims_table), "_scored_at", window_minutes)
    if df.rdd.isEmpty():
        return {"sample_size": 0, "note": "sem claims recentes em gold.claims"}

    latency_df = df.withColumn(
        "latency_seconds",
        expr("unix_timestamp(_scored_at) - unix_timestamp(_ingested_at)"),
    )
    row = latency_df.agg(
        avg("latency_seconds").alias("avg"),
        spark_max("latency_seconds").alias("max"),
        count("*").alias("sample_size"),
    ).first()

    max_seconds = row["max"] or 0
    return {
        "sample_size": row["sample_size"],
        "avg_seconds": row["avg"],
        "max_seconds": max_seconds,
        "sla_seconds": FRAUD_SCORE_SLA_SECONDS,
        "within_sla": max_seconds <= FRAUD_SCORE_SLA_SECONDS,
    }


def measure_throughput(spark: SparkSession, bronze_table: str, window_minutes: int = 15) -> dict:
    """AT-003: pico de volume não pode causar perda de dados. Aproxima isso
    comparando o volume ingerido por minuto contra o volume configurado no
    producer (replay.events_per_minute) e observando o tamanho da
    quarentena — quedas abaixo do esperado ou quarentena crescendo são sinal
    de perda/backpressure, não prova definitiva sem métricas de streaming."""
    valid_df = _recent(spark.read.table(bronze_table), "_ingested_at", window_minutes)

    quarantine_table = f"{bronze_table}_quarantine"
    quarantine_count = 0
    if spark.catalog.tableExists(quarantine_table):
        quarantine_count = _recent(
            spark.read.table(quarantine_table), "_ingested_at", window_minutes
        ).count()

    per_minute = (
        valid_df.groupBy(date_trunc("minute", col("_ingested_at")).alias("minute"))
        .agg(count("*").alias("events"))
        .orderBy("minute")
        .collect()
    )
    events_per_minute = [row["events"] for row in per_minute]

    return {
        "quarantine_count": quarantine_count,
        "minutes_observed": len(events_per_minute),
        "events_per_minute": events_per_minute,
        "expected_events_per_minute": EXPECTED_EVENTS_PER_MINUTE,
        "minutes_below_80pct_expected": sum(
            1 for events in events_per_minute if events < 0.8 * EXPECTED_EVENTS_PER_MINUTE
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="ex.: insurance_dev")
    parser.add_argument("--bronze-table", default=None, help="default: {catalog}.bronze.claims")
    parser.add_argument("--gold-claims-table", default=None, help="default: {catalog}.gold.claims")
    parser.add_argument("--window-minutes", type=int, default=15)
    args = parser.parse_args()

    bronze_table = args.bronze_table or f"{args.catalog}.bronze.claims"
    gold_claims_table = args.gold_claims_table or f"{args.catalog}.gold.claims"

    spark = get_spark("measure-pipeline-latency")

    report = {
        "at_001_kafka_to_bronze_latency": measure_kafka_to_bronze_latency(
            spark, bronze_table, args.window_minutes
        ),
        "fraud_score_visibility_latency": measure_fraud_score_latency(
            spark, gold_claims_table, args.window_minutes
        ),
        "at_003_throughput": measure_throughput(spark, bronze_table, args.window_minutes),
    }
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
