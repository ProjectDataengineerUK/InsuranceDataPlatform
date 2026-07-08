import os


def _get_secret(scope: str, key: str, env_fallback: str) -> str:
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]
        from pyspark.sql import SparkSession

        dbutils = DBUtils(SparkSession.getActiveSession())
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception as exc:
        value = os.environ.get(env_fallback)
        if not value:
            raise RuntimeError(
                f"Missing secret '{key}' in scope '{scope}' and no fallback "
                f"env var '{env_fallback}' set"
            ) from exc
        return value


def get_kafka_options(topic: str, secret_scope: str = "insurance-platform") -> dict:
    bootstrap_servers = _get_secret(
        secret_scope, "confluent-bootstrap-servers", "CONFLUENT_BOOTSTRAP_SERVERS"
    )
    api_key = _get_secret(secret_scope, "confluent-api-key", "CONFLUENT_API_KEY")
    api_secret = _get_secret(secret_scope, "confluent-api-secret", "CONFLUENT_API_SECRET")

    jaas_config = (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{api_key}" password="{api_secret}";'
    )

    return {
        "kafka.bootstrap.servers": bootstrap_servers,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas_config,
        "subscribe": topic,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
    }
