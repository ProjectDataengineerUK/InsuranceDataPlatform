"""Mede AT-001 (latência Kafka -> Bronze), a latência de visibilidade do score
de fraude (Bronze -> Gold, alvo < 1 min do DEFINE) e AT-003 (volume sustentado
sem perda) contra um pipeline rodando de verdade em um workspace Databricks.

Roda como job agendado (`resources/jobs.pipeline_monitoring.yml`, a cada
15 min) — persiste cada checagem em `monitoring._pipeline_latency_results` e
alerta (mesmo secret `sla-webhook-url` usado por sla_alerts.py/model_drift.py)
quando alguma latência sai do SLA. Não requer nenhuma infraestrutura nova:
lê apenas as tabelas Delta que os próprios jobs (bronze_ingest,
fraud_score_stream) já escrevem.
"""

import argparse
import json
import logging
import sys
from pathlib import Path

# Job roda como spark_python_task via workspace files (sem empacotamento em
# wheel) — Databricks só põe o diretório do próprio script no sys.path, não a
# raiz do bundle. Sem isso, "from src..." abaixo falha com ModuleNotFoundError.
# Databricks executa o job via exec(compile(source, filename, 'exec')), que
# nao injeta __file__ nos globals — cai pro co_filename do frame atual.
_this_file = globals().get("__file__") or sys._getframe().f_code.co_filename
sys.path.insert(0, str(Path(_this_file).resolve().parents[1]))

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, current_timestamp, date_trunc, expr
from pyspark.sql.functions import max as spark_max
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from src.common.delta_write import append_or_create
from src.common.secrets import get_secret
from src.common.spark_session import get_spark

logger = logging.getLogger(__name__)

# Schema explícito pro createDataFrame de persist_report — quando o pipeline
# não tem eventos recentes, nenhum dos 3 relatórios tem chave "within_sla",
# então a coluna inteira fica None. Spark Connect (ao contrário do Spark
# clássico) não infere tipo de uma coluna inteiramente nula e falha com
# CANNOT_DETERMINE_TYPE — schema explícito evita a inferência.
_REPORT_ROW_SCHEMA = StructType(
    [
        StructField("metric_name", StringType(), nullable=False),
        StructField("details_json", StringType(), nullable=False),
        StructField("within_sla", BooleanType(), nullable=True),
    ]
)

KAFKA_TO_BRONZE_SLA_SECONDS = 120  # AT-001: Kafka -> Bronze < 2 min
# Alvo original do DEFINE era < 1 min (60s), mas fraud_score_stream virou
# schedule */5min (não mais continuous) por cota de serverless do workspace
# trial (ver docs/ARCHITECTURE.md, "fraud_score_stream virou schedule") — com
# 60s este check reprovava sempre, estruturalmente, mesmo com o pipeline
# saudável. 420s (7 min) cobre o pior caso realista: um claim que chega logo
# após uma execução começar espera quase um ciclo inteiro (5 min) + tempo de
# execução do job antes de ser pontuado.
FRAUD_SCORE_SLA_SECONDS = 420
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
    if df.isEmpty():
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
    if df.isEmpty():
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
    minutes_observed = len(events_per_minute)
    minutes_below_80pct_expected = sum(
        1 for events in events_per_minute if events < 0.8 * EXPECTED_EVENTS_PER_MINUTE
    )

    return {
        "quarantine_count": quarantine_count,
        "minutes_observed": minutes_observed,
        "events_per_minute": events_per_minute,
        "expected_events_per_minute": EXPECTED_EVENTS_PER_MINUTE,
        "minutes_below_80pct_expected": minutes_below_80pct_expected,
        # None (não False) quando não há nenhum minuto observado na janela —
        # "sem dado" e "abaixo do esperado" são coisas diferentes; o bug
        # original deixava all=None e o app (pipeline_monitoring.py) tratava
        # isso como falso, mostrando "fora do SLA" mesmo sem nenhum dado.
        "within_sla": None if minutes_observed == 0 else minutes_below_80pct_expected == 0,
    }


def send_sla_breach_alert(breaches: list[str], webhook_url: str | None) -> None:
    if not breaches:
        return

    message = f"Pipeline fora do SLA em: {', '.join(breaches)}."
    if not webhook_url:
        logger.warning("sla-webhook-url not set, logging alert instead: %s", message)
        return

    response = requests.post(webhook_url, json={"text": message}, timeout=10)
    response.raise_for_status()


def persist_report(spark: SparkSession, report: dict, results_table: str) -> None:
    rows = [
        (metric_name, json.dumps(details, default=str), details.get("within_sla"))
        for metric_name, details in report.items()
    ]
    report_df = spark.createDataFrame(rows, schema=_REPORT_ROW_SCHEMA).withColumn(
        "_checked_at", current_timestamp()
    )
    append_or_create(report_df, results_table)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="ex.: insurance_dev")
    parser.add_argument("--bronze-table", default=None, help="default: {catalog}.bronze.claims")
    parser.add_argument("--gold-claims-table", default=None, help="default: {catalog}.gold.claims")
    parser.add_argument("--results-table", default=None, help="default: {catalog}.monitoring._pipeline_latency_results")
    parser.add_argument("--window-minutes", type=int, default=15)
    args = parser.parse_args()

    bronze_table = args.bronze_table or f"{args.catalog}.bronze.claims"
    gold_claims_table = args.gold_claims_table or f"{args.catalog}.gold.claims"
    results_table = args.results_table or f"{args.catalog}.monitoring._pipeline_latency_results"

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

    persist_report(spark, report, results_table)

    breaches = [name for name, details in report.items() if details.get("within_sla") is False]
    webhook_url = get_secret(
        "insurance-platform", "sla-webhook-url", "SLA_WEBHOOK_URL", required=False
    )
    send_sla_breach_alert(breaches, webhook_url)


if __name__ == "__main__":
    main()
