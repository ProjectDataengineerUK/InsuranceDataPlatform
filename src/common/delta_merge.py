from delta.tables import DeltaTable
from pyspark.sql import DataFrame


def merge_into_delta(
    batch_df: DataFrame,
    table_name: str,
    key_column: str,
    partition_column: str = "event_date",
) -> None:
    if not batch_df.sparkSession.catalog.tableExists(table_name):
        batch_df.write.format("delta").mode("overwrite").partitionBy(partition_column).saveAsTable(
            table_name
        )
        return

    delta_table = DeltaTable.forName(batch_df.sparkSession, table_name)
    (
        delta_table.alias("target")
        .merge(batch_df.alias("source"), f"target.{key_column} = source.{key_column}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
