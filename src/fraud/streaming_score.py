from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    greatest,
    hour,
    least,
    lit,
    stddev,
    when,
)
from pyspark.sql.window import Window

NIGHT_START_HOUR = 22
NIGHT_END_HOUR = 6
FREQUENCY_WINDOW_DAYS = 1
DEFAULT_SCORE_THRESHOLD = 0.7

FEATURE_WEIGHTS = {
    "night_claim": 0.25,
    "high_frequency": 0.35,
    "amount_outlier": 0.40,
}


def add_night_claim_feature(df: DataFrame) -> DataFrame:
    claim_hour = hour(col("event_timestamp"))
    is_night = (claim_hour >= NIGHT_START_HOUR) | (claim_hour < NIGHT_END_HOUR)
    return df.withColumn("feature_night_claim", when(is_night, lit(1.0)).otherwise(lit(0.0)))


def add_frequency_feature(df: DataFrame, max_expected_claims: int = 3) -> DataFrame:
    window = (
        Window.partitionBy("customer_id")
        .orderBy(col("event_timestamp").cast("long"))
        .rangeBetween(-FREQUENCY_WINDOW_DAYS * 86400, 0)
    )
    with_count = df.withColumn("claims_last_24h", count("claim_id").over(window))
    normalized = least(
        col("claims_last_24h") / lit(float(max_expected_claims)), lit(1.0)
    )
    return with_count.withColumn("feature_high_frequency", normalized)


def add_amount_outlier_feature(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("region")
    with_stats = df.withColumn("_region_avg_amount", avg("amount").over(window)).withColumn(
        "_region_stddev_amount", stddev("amount").over(window)
    )
    z_score = (col("amount") - col("_region_avg_amount")) / greatest(
        col("_region_stddev_amount"), lit(1.0)
    )
    normalized = least(greatest(z_score / lit(3.0), lit(0.0)), lit(1.0))
    return with_stats.withColumn("feature_amount_outlier", normalized).drop(
        "_region_avg_amount", "_region_stddev_amount"
    )


def compute_fraud_score(df: DataFrame) -> DataFrame:
    with_features = add_amount_outlier_feature(
        add_frequency_feature(add_night_claim_feature(df))
    )

    score_expr = (
        col("feature_night_claim") * lit(FEATURE_WEIGHTS["night_claim"])
        + col("feature_high_frequency") * lit(FEATURE_WEIGHTS["high_frequency"])
        + col("feature_amount_outlier") * lit(FEATURE_WEIGHTS["amount_outlier"])
    )

    return with_features.withColumn("fraud_score", score_expr)


def apply_fraud_flag(df: DataFrame, threshold: float = DEFAULT_SCORE_THRESHOLD) -> DataFrame:
    return df.withColumn("fraud_flag", col("fraud_score") >= lit(threshold))


def score_claims(df: DataFrame, threshold: float = DEFAULT_SCORE_THRESHOLD) -> DataFrame:
    scored = compute_fraud_score(df)
    return apply_fraud_flag(scored, threshold)
