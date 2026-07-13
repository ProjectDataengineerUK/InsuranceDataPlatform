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

from src.common.scd2_merge import merge_scd2_into_delta
from src.common.spark_session import get_spark
from src.quality.checks import check_not_null, check_unique, persist_results

# policy_id, não customer_id: gold.claims.customer_id é sempre NULL neste
# projeto (dados reais anonimizados, ver src/common/schemas.py). policy_id é
# a chave estável de fato disponível.
KEY_COLUMN = "policy_id"
EFFECTIVE_COLUMN = "event_timestamp"


def run_consent_scd(
    bronze_table: str,
    silver_table: str,
    results_table: str,
) -> None:
    spark = get_spark("open-insurance-consent-scd")
    bronze_df: DataFrame = spark.read.table(bronze_table)

    if bronze_df.isEmpty():
        return

    results = check_not_null(bronze_df, [KEY_COLUMN, "consent_status"], bronze_table)
    results.append(check_unique(bronze_df, [KEY_COLUMN, EFFECTIVE_COLUMN], bronze_table))
    persist_results(spark, results, results_table)

    merge_scd2_into_delta(
        bronze_df,
        silver_table,
        key_column=KEY_COLUMN,
        effective_column=EFFECTIVE_COLUMN,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--results-table", required=True)
    args = parser.parse_args()

    run_consent_scd(args.bronze_table, args.silver_table, args.results_table)


if __name__ == "__main__":
    main()
