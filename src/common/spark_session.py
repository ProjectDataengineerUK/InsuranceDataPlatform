from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    # Databricks Runtime já vem com Delta embutido no classpath, então este
    # get_spark() ali só reaproveita a sessão ativa (getOrCreate). Fora do
    # Databricks (CI, testes locais), o pacote delta-spark instalado via pip
    # não traz o JAR sozinho — configure_spark_with_delta_pip resolve isso via
    # Maven, senão o catálogo Delta falha com ClassNotFoundException.
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
