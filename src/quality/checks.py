from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import max as spark_max


@dataclass
class DQResult:
    check_name: str
    table_name: str
    passed: bool
    failed_rows: int


def check_not_null(df: DataFrame, columns: list[str], table_name: str) -> list[DQResult]:
    results = []
    for column in columns:
        failed = df.filter(col(column).isNull()).count()
        results.append(DQResult(f"not_null:{column}", table_name, failed == 0, failed))
    return results


def check_unique(df: DataFrame, key_columns: list[str], table_name: str) -> DQResult:
    total = df.count()
    distinct = df.select(*key_columns).distinct().count()
    failed_rows = total - distinct
    return DQResult(f"unique:{','.join(key_columns)}", table_name, failed_rows == 0, failed_rows)


def check_range(
    df: DataFrame,
    column: str,
    min_value: float,
    max_value: float,
    table_name: str,
) -> DQResult:
    failed = df.filter((col(column) < min_value) | (col(column) > max_value)).count()
    return DQResult(f"range:{column}", table_name, failed == 0, failed)


def check_freshness(
    df: DataFrame,
    timestamp_column: str,
    max_lag_minutes: int,
    table_name: str,
) -> DQResult:
    latest = df.select(spark_max(col(timestamp_column)).alias("latest_ts")).first()
    if latest is None or latest["latest_ts"] is None:
        return DQResult(f"freshness:{timestamp_column}", table_name, False, 1)

    lag_minutes_row = df.sparkSession.sql(
        "SELECT (unix_timestamp(current_timestamp()) - "
        f"unix_timestamp(timestamp'{latest['latest_ts']}')) / 60 AS lag_minutes"
    ).first()
    lag_minutes = lag_minutes_row["lag_minutes"] if lag_minutes_row else None
    passed = lag_minutes is not None and lag_minutes <= max_lag_minutes
    return DQResult(f"freshness:{timestamp_column}", table_name, passed, 0 if passed else 1)


def persist_results(spark: SparkSession, results: list[DQResult], results_table: str) -> bool:
    rows = [(r.check_name, r.table_name, r.passed, r.failed_rows) for r in results]
    results_df = spark.createDataFrame(
        rows, ["check_name", "table_name", "passed", "failed_rows"]
    ).withColumn("_checked_at", current_timestamp())
    results_df.write.format("delta").mode("append").saveAsTable(results_table)
    return all(r.passed for r in results)
