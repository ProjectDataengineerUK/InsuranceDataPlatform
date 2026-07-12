import argparse
import logging
import sys
from pathlib import Path

# Job roda como spark_python_task via workspace files (sem empacotamento em
# wheel) — Databricks só põe o diretório do próprio script no sys.path, não a
# raiz do bundle. Sem isso, "from src..." abaixo falha com ModuleNotFoundError.
# Databricks executa o job via exec(compile(source, filename, 'exec')), que
# nao injeta __file__ nos globals — cai pro co_filename do frame atual.
_this_file = globals().get("__file__") or sys._getframe().f_code.co_filename
sys.path.insert(0, str(Path(_this_file).resolve().parents[2]))

import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    collect_set,
    concat,
    concat_ws,
    current_timestamp,
    date_format,
    first,
    lit,
    when,
)
from pyspark.sql.functions import expr as spark_expr

from src.common.delta_write import append_or_create
from src.common.secrets import get_secret
from src.common.spark_session import get_spark
from src.ingestion.producer.datasets.susep_loader import CAUSE_LABELS

logger = logging.getLogger(__name__)

DQ_FAILURE_RATE_ALERT_THRESHOLD = 0.05
DISCREPANCY_RATE_ALERT_THRESHOLD = 0.10

# Confirmado no layout oficial (mesma Tabela V já usada em susep_loader.py) —
# reaproveitado aqui, não duplicado, como fonte de verdade de causas válidas.
VALID_CAUSE_CODES = list(CAUSE_LABELS.keys())


def build_gold_susep_claims(silver_df: DataFrame, reconciliation_df: DataFrame) -> DataFrame:
    # Layout literal já confirmado em src/ingestion/producer/datasets/susep_loader.py
    # (Estrutura dos Arquivos R_AUTO e S_AUTO) — nenhum campo novo é inventado
    # além dos 5 já confirmados. INDENIZ usa a mediana entre as fontes que
    # reportaram o sinistro (mesma regra de resolução de src/regulatory/reconcile.py).
    base = (
        silver_df.groupBy("external_reference_id")
        .agg(
            first("policy_id", ignorenulls=True).alias("COD_APO"),
            first("event_timestamp", ignorenulls=True).alias("_event_timestamp"),
            spark_expr("percentile_approx(amount, 0.5)").alias("INDENIZ"),
            first("region", ignorenulls=True).alias("REGIAO"),
            first("cause_code", ignorenulls=True).alias("CAUSA"),
        )
        .withColumn("D_OCORR", date_format(col("_event_timestamp"), "yyyyMMdd"))
    )

    # Um external_reference_id pode ter mais de um discrepancy_type (ex.:
    # amount_mismatch E missing_in_source) — collect_set + concat_ws agrega
    # os dois num único valor por linha antes do join.
    discrepancies_by_ref = reconciliation_df.groupBy("external_reference_id").agg(
        concat_ws(",", collect_set("discrepancy_type")).alias("_discrepancy_types")
    )

    joined = base.join(discrepancies_by_ref, on="external_reference_id", how="left")

    # susep_compliant/compliance_issues: status de conformidade POR CONTRATO,
    # não só o resumo agregado de build_dq_summary. Deliberadamente NÃO inclui
    # a checagem de região aqui — REGIAO usa um allow-list data-driven em
    # standardize.py (proxy por consenso entre fontes, sem tabela oficial
    # confirmada), então marcar um contrato como "fora das regras" só por
    # região reprovaria falsos positivos. concat_ws ignora argumentos NULL —
    # uma linha 100% conforme produz string vazia "", não NULL.
    compliance_issues = concat_ws(
        ",",
        when(col("COD_APO").isNull(), lit("policy_id_ausente")),
        when(col("D_OCORR").isNull(), lit("data_ausente")),
        when(col("INDENIZ").isNull(), lit("valor_ausente")),
        when(~col("CAUSA").isin(VALID_CAUSE_CODES), lit("causa_invalida")),
        when(
            col("_discrepancy_types").isNotNull(),
            concat(lit("reconciliacao:"), col("_discrepancy_types")),
        ),
    )

    return (
        joined.withColumn("compliance_issues", compliance_issues)
        .withColumn("susep_compliant", col("compliance_issues") == "")
        .select(
            "COD_APO",
            "D_OCORR",
            "INDENIZ",
            "REGIAO",
            "CAUSA",
            "susep_compliant",
            "compliance_issues",
        )
    )


def build_dq_summary(dq_results_df: DataFrame, reconciliation_df: DataFrame) -> DataFrame:
    dq_total = dq_results_df.count()
    dq_failed = dq_results_df.filter(~col("passed")).count()
    amount_mismatch_count = reconciliation_df.filter(
        col("discrepancy_type") == "amount_mismatch"
    ).count()
    missing_in_source_count = reconciliation_df.filter(
        col("discrepancy_type") == "missing_in_source"
    ).count()

    spark = dq_results_df.sparkSession
    return spark.createDataFrame(
        [(dq_total, dq_failed, amount_mismatch_count, missing_in_source_count)],
        ["dq_checks_total", "dq_checks_failed", "amount_mismatch_count", "missing_in_source_count"],
    ).withColumn("_generated_at", current_timestamp())


def send_alert_if_needed(dq_total: int, dq_failed: int, discrepancy_count: int, claim_count: int) -> None:
    dq_failure_rate = (dq_failed / dq_total) if dq_total else 0.0
    discrepancy_rate = (discrepancy_count / claim_count) if claim_count else 0.0

    if dq_failure_rate <= DQ_FAILURE_RATE_ALERT_THRESHOLD and (
        discrepancy_rate <= DISCREPANCY_RATE_ALERT_THRESHOLD
    ):
        return

    message = (
        f"Regulatory pipeline: DQ failure rate {dq_failure_rate:.1%} "
        f"(limiar {DQ_FAILURE_RATE_ALERT_THRESHOLD:.0%}), discrepancy rate "
        f"{discrepancy_rate:.1%} (limiar {DISCREPANCY_RATE_ALERT_THRESHOLD:.0%})."
    )
    webhook_url = get_secret("insurance-platform", "sla-webhook-url", "SLA_WEBHOOK_URL", required=False)
    if not webhook_url:
        logger.warning("sla-webhook-url not set, logging alert instead: %s", message)
        return

    response = requests.post(webhook_url, json={"text": message}, timeout=10)
    response.raise_for_status()


def run_gold_export_job(
    silver_table: str,
    dq_results_table: str,
    reconciliation_results_table: str,
    gold_claims_table: str,
    gold_summary_table: str,
) -> None:
    spark = get_spark("regulatory-gold-export")
    silver_df = spark.read.table(silver_table)
    reconciliation_df = spark.read.table(reconciliation_results_table)

    gold_claims_df = build_gold_susep_claims(silver_df, reconciliation_df)
    append_or_create(gold_claims_df, gold_claims_table)

    # Acumulado desde sempre (persist_results só faz append, sem run_id) —
    # simplificação de MVP documentada; "por execução" de verdade é fast-follow.
    dq_results_df = spark.read.table(dq_results_table).filter(col("table_name") == silver_table)

    summary_df = build_dq_summary(dq_results_df, reconciliation_df)
    append_or_create(summary_df, gold_summary_table)

    summary_row = summary_df.first()
    send_alert_if_needed(
        dq_total=summary_row["dq_checks_total"],
        dq_failed=summary_row["dq_checks_failed"],
        discrepancy_count=(
            summary_row["amount_mismatch_count"] + summary_row["missing_in_source_count"]
        ),
        claim_count=gold_claims_df.count(),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--dq-results-table", required=True)
    parser.add_argument("--reconciliation-results-table", required=True)
    parser.add_argument("--gold-claims-table", required=True)
    parser.add_argument("--gold-summary-table", required=True)
    args = parser.parse_args()

    run_gold_export_job(
        args.silver_table,
        args.dq_results_table,
        args.reconciliation_results_table,
        args.gold_claims_table,
        args.gold_summary_table,
    )


if __name__ == "__main__":
    main()
