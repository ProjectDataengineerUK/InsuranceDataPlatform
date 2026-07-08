import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, lit, when
from pyspark.sql.functions import sum as spark_sum

from src.common.spark_session import get_spark
from src.quality.checks import check_not_null, persist_results

# Idempotentes: seguro reaplicar a cada execução do job. Substitui o passo
# manual `databricks sql query --file sql/governance_setup.sql` documentado
# no README — mantido em sql/ apenas como referência/fallback.
GOVERNANCE_MASK_FUNCTION_SQL = """
CREATE OR REPLACE FUNCTION {catalog}.gold.mask_customer_id(customer_id STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('insurance-data-team') THEN customer_id
    ELSE sha2(customer_id, 256)
  END
"""

GOVERNANCE_APPLY_MASK_SQL = """
ALTER TABLE {catalog}.gold.claims
  ALTER COLUMN customer_id
  SET MASK {catalog}.gold.mask_customer_id
"""


def build_region_aggregates(claims_gold_df: DataFrame) -> DataFrame:
    return claims_gold_df.groupBy("region", "event_date").agg(
        count("claim_id").alias("claim_count"),
        avg("amount").alias("avg_claim_amount"),
        spark_sum(when(col("fraud_flag"), 1).otherwise(0)).alias("flagged_fraud_count"),
        spark_sum(when(col("auto_approved"), 1).otherwise(0)).alias("auto_approved_count"),
    )


def _write_gold_partition(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    run_date: str,
) -> None:
    writer = df.write.format("delta").partitionBy("event_date")

    if spark.catalog.tableExists(table_name):
        writer.mode("overwrite").option("replaceWhere", f"event_date = '{run_date}'").saveAsTable(
            table_name
        )
    else:
        writer.mode("overwrite").saveAsTable(table_name)


def apply_governance(spark: SparkSession, catalog: str, gold_claims_table: str) -> None:
    """Reaplica o masking de customer_id a cada execução — não depende de um
    passo manual pós-deploy nem de uma segunda ferramenta de IaC disputando a
    tabela criada pelos jobs Spark (ver Issue #7 do BUILD_REPORT)."""
    if not spark.catalog.tableExists(gold_claims_table):
        return

    spark.sql(GOVERNANCE_MASK_FUNCTION_SQL.format(catalog=catalog))
    spark.sql(GOVERNANCE_APPLY_MASK_SQL.format(catalog=catalog))


def run_gold_job(
    catalog: str,
    gold_claims_table: str,
    gold_agg_table: str,
    results_table: str,
    run_date: str,
) -> None:
    spark = get_spark("gold-aggregate")

    claims_gold_df = spark.read.table(gold_claims_table).filter(col("event_date") == lit(run_date))
    region_agg_df = build_region_aggregates(claims_gold_df)

    results = check_not_null(region_agg_df, ["region", "event_date"], gold_agg_table)
    persist_results(spark, results, results_table)

    _write_gold_partition(spark, region_agg_df, gold_agg_table, run_date)
    apply_governance(spark, catalog, gold_claims_table)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--gold-claims-table", required=True)
    parser.add_argument("--gold-agg-table", required=True)
    parser.add_argument("--results-table", required=True)
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()

    run_gold_job(
        args.catalog,
        args.gold_claims_table,
        args.gold_agg_table,
        args.results_table,
        args.run_date,
    )


if __name__ == "__main__":
    main()
