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
from pyspark.sql.functions import col, from_unixtime, regexp_replace, to_timestamp
from pyspark.sql.types import DecimalType

from src.common.delta_write import append_or_create
from src.common.spark_session import get_spark
from src.ingestion.producer.datasets.susep_loader import CAUSE_LABELS
from src.quality.checks import check_allowed_values, check_not_null, persist_results

CANONICAL_COLUMNS = [
    "external_reference_id",
    "source_system",
    "policy_id",
    "event_timestamp",
    "amount",
    "region",
    "cause_code",
]

CAUSE_CODE_ALLOWED_VALUES = set(CAUSE_LABELS.keys())

# Consenso mínimo de fontes reportando o mesmo valor de região pro mesmo
# external_reference_id pra esse valor entrar no allow-list de região —
# usado só porque nenhuma tabela oficial de códigos de região SUSEP foi
# confirmada neste repo ainda (ver docs/ARCHITECTURE.md); é um proxy
# data-driven, não hardcoda nenhum valor específico (nem o sentinel "ZZ"
# usado por src/ingestion/producer/datasets/regulatory_feeds.py pra simular
# código inválido).
REGION_CONSENSUS_MIN_SOURCES = 2


def _standardize_insurer_a(raw_df: DataFrame) -> DataFrame:
    subset = raw_df.filter(col("source_system") == "insurer_a")
    # "R$ 1.234,56" -> remove prefixo/milhar -> "1234,56" -> vírgula decimal -> "1234.56"
    amount_clean = regexp_replace(col("valor_indenizacao"), r"R\$\s*", "")
    amount_clean = regexp_replace(amount_clean, r"\.", "")
    amount_clean = regexp_replace(amount_clean, ",", ".")
    return subset.select(
        col("external_reference_id"),
        col("source_system"),
        col("numero_apolice").alias("policy_id"),
        to_timestamp(col("data_ocorrencia"), "dd/MM/yyyy").alias("event_timestamp"),
        amount_clean.cast(DecimalType(18, 2)).alias("amount"),
        col("regiao_sinistro").alias("region"),
        col("codigo_causa").cast("int").alias("cause_code"),
    )


def _standardize_insurer_b(raw_df: DataFrame) -> DataFrame:
    subset = raw_df.filter(col("source_system") == "insurer_b")
    return subset.select(
        col("external_reference_id"),
        col("source_system"),
        col("POLICY_NUM").alias("policy_id"),
        to_timestamp(col("EVENT_DATE"), "yyyy-MM-dd").alias("event_timestamp"),
        col("CLAIM_AMOUNT").cast(DecimalType(18, 2)).alias("amount"),
        col("REGION_CODE").alias("region"),
        col("CAUSE_CD").cast("int").alias("cause_code"),
    )


def _standardize_insurer_c(raw_df: DataFrame) -> DataFrame:
    subset = raw_df.filter(col("source_system") == "insurer_c")
    # occurrenceDate/amountCents chegam como StringType (REGULATORY_CLAIM_RAW_SCHEMA)
    # a partir de valores originalmente int no producer (epoch millis, cents) —
    # from_json serializa inteiros grandes em notação científica (ex.:
    # "1.5895008E12"), que cast("long") direto rejeita como malformado
    # (confirmado em produção: CAST_INVALID_INPUT). cast("double") primeiro
    # aceita notação científica; convertendo pra long depois é exato nessas
    # faixas de valor (bem abaixo do limite de precisão de um double).
    return subset.select(
        col("external_reference_id"),
        col("source_system"),
        col("policyId").alias("policy_id"),
        from_unixtime(col("occurrenceDate").cast("double").cast("long") / 1000)
        .cast("timestamp")
        .alias("event_timestamp"),
        (col("amountCents").cast("double").cast("long") / 100.0)
        .cast(DecimalType(18, 2))
        .alias("amount"),
        col("regionCode").alias("region"),
        col("causeCode").cast("int").alias("cause_code"),
    )


def standardize_regulatory_claims(raw_df: DataFrame) -> DataFrame:
    return (
        _standardize_insurer_a(raw_df)
        .unionByName(_standardize_insurer_b(raw_df))
        .unionByName(_standardize_insurer_c(raw_df))
    )


def _region_allowlist(standardized_df: DataFrame) -> list[str]:
    consensus = (
        standardized_df.groupBy("external_reference_id", "region")
        .count()
        .filter(col("count") >= REGION_CONSENSUS_MIN_SOURCES)
    )
    return [row["region"] for row in consensus.select("region").distinct().collect()]


def run_standardize_job(bronze_table: str, silver_table: str, results_table: str) -> bool:
    spark = get_spark("regulatory-standardize")
    raw_df = spark.read.table(bronze_table)
    standardized_df = standardize_regulatory_claims(raw_df)

    results = check_not_null(
        standardized_df,
        ["external_reference_id", "policy_id", "event_timestamp", "amount"],
        silver_table,
    )
    results.append(
        check_allowed_values(standardized_df, "cause_code", CAUSE_CODE_ALLOWED_VALUES, silver_table)
    )
    region_allowed_values = set(_region_allowlist(standardized_df))
    if region_allowed_values:
        results.append(
            check_allowed_values(standardized_df, "region", region_allowed_values, silver_table)
        )

    passed = persist_results(spark, results, results_table)

    append_or_create(standardized_df, silver_table)
    return passed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--results-table", required=True)
    args = parser.parse_args()

    passed = run_standardize_job(args.bronze_table, args.silver_table, args.results_table)
    print(f"regulatory standardize DQ passed: {passed}")


if __name__ == "__main__":
    main()
