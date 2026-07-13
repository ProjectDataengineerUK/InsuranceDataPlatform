from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window


def merge_scd2_into_delta(
    batch_df: DataFrame,
    table_name: str,
    key_column: str,
    effective_column: str,
) -> None:
    spark = batch_df.sparkSession

    # Múltiplos eventos da mesma chave no mesmo batch: só o mais recente vira
    # a nova versão vigente candidata (volume baixo esperado para este caso
    # de uso — eventos intermediários do mesmo batch não entram no histórico
    # nesta versão, limitação documentada em DESIGN_OPEN_INSURANCE.md).
    window = Window.partitionBy(key_column).orderBy(col(effective_column).desc())
    latest_per_key = (
        batch_df.withColumn("_rn", row_number().over(window))
        .filter("_rn = 1")
        .drop("_rn")
        .withColumn("is_current", lit(True))
        .withColumn("valid_from", col(effective_column))
        .withColumn("valid_to", lit(None).cast("timestamp"))
    )

    if not spark.catalog.tableExists(table_name):
        latest_per_key.write.format("delta").mode("overwrite").saveAsTable(table_name)
        return

    delta_table = DeltaTable.forName(spark, table_name)

    # Passo 1: fecha a versão vigente quando o evento novo é de fato mais
    # recente que ela. Eventos fora de ordem (mais antigos que a versão já
    # vigente) não casam essa condição e a versão vigente permanece intacta.
    (
        delta_table.alias("target")
        .merge(
            latest_per_key.alias("source"),
            f"target.{key_column} = source.{key_column} AND target.is_current = true",
        )
        .whenMatchedUpdate(
            condition=f"source.{effective_column} > target.{effective_column}",
            set={"is_current": "false", "valid_to": f"source.{effective_column}"},
        )
        .execute()
    )

    # Passo 2: só insere como nova versão vigente as chaves cuja versão
    # vigente, após o passo 1, não seja mais recente ou igual ao evento do
    # source — isso exclui exatamente as chaves fora de ordem que não
    # dispararam o fechamento acima (evita duplicar is_current=true).
    still_current_and_newer_or_equal = (
        spark.read.table(table_name)
        .filter("is_current = true")
        .alias("t")
        .join(
            latest_per_key.alias("s"),
            (col(f"t.{key_column}") == col(f"s.{key_column}"))
            & (col(f"t.{effective_column}") >= col(f"s.{effective_column}")),
            "inner",
        )
        .select(col(f"s.{key_column}").alias(key_column))
        .distinct()
    )

    to_insert = latest_per_key.join(still_current_and_newer_or_equal, key_column, "left_anti")
    if not to_insert.isEmpty():
        to_insert.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
            table_name
        )
