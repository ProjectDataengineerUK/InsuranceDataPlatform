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
        # Compute serverless (obrigatório neste workspace) não suporta trigger
        # ProcessingTime "infinito" — só AvailableNow/Once. O job já roda como
        # "continuous job" no Databricks (ver resources/jobs.silver.yml), que
        # reinicia o run assim que este termina, então availableNow entrega o
        # mesmo efeito prático de streaming quase-contínuo.
        .trigger(availableNow=True)
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
