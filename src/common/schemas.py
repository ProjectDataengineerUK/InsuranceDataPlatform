from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CLAIM_EVENT_SCHEMA = StructType(
    [
        StructField("claim_id", StringType(), nullable=False),
        StructField("policy_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_timestamp", TimestampType(), nullable=False),
        StructField("amount", DecimalType(18, 2), nullable=True),
        StructField("region", StringType(), nullable=True),
        StructField("vehicle_type", StringType(), nullable=True),
        StructField("source", StringType(), nullable=True),
    ]
)

POLICY_EVENT_SCHEMA = StructType(
    [
        StructField("policy_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_timestamp", TimestampType(), nullable=False),
        StructField("premium_amount", DecimalType(18, 2), nullable=True),
        StructField("coverage_type", StringType(), nullable=True),
        StructField("region", StringType(), nullable=True),
        StructField("source", StringType(), nullable=True),
    ]
)

CUSTOMER_EVENT_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_timestamp", TimestampType(), nullable=False),
        StructField("region", StringType(), nullable=True),
        StructField("source", StringType(), nullable=True),
    ]
)

SCHEMA_REGISTRY = {
    "claim-opened": CLAIM_EVENT_SCHEMA,
    "claim-updated": CLAIM_EVENT_SCHEMA,
    "policy-created": POLICY_EVENT_SCHEMA,
    "customer-updated": CUSTOMER_EVENT_SCHEMA,
}
