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

    # O conector Kafka do Databricks Runtime usa o Kafka client sombreado sob
    # kafkashaded.org.apache.kafka.* — referenciar a classe não-sombreada
    # (org.apache.kafka...) faz a JVM não achar o LoginModule em runtime.
    jaas_config = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
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


def get_admin_client_config(secret_scope: str = "insurance-platform") -> dict:
    # Config nativa do confluent_kafka (librdkafka), não do conector Kafka do
    # Spark (get_kafka_options acima) — usada por kafka_lag_reporter.py pra
    # fazer commit administrativo de offsets num consumer group real, fora do
    # Spark Structured Streaming.
    bootstrap_servers = _get_secret(
        secret_scope, "confluent-bootstrap-servers", "CONFLUENT_BOOTSTRAP_SERVERS"
    )
    api_key = _get_secret(secret_scope, "confluent-api-key", "CONFLUENT_API_KEY")
    api_secret = _get_secret(secret_scope, "confluent-api-secret", "CONFLUENT_API_SECRET")

    return {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": api_key,
        "sasl.password": api_secret,
    }
