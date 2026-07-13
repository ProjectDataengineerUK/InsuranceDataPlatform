import os
from datetime import UTC, datetime, timedelta
from typing import Any

import requests

METRICS_API_URL = "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"

# https://api.telemetry.confluent.cloud/docs/descriptors — nomes confirmados
# via documentação oficial do Confluent Cloud Metrics API.
CONSUMER_LAG_METRIC = "io.confluent.kafka.server/consumer_lag_offsets"
RECEIVED_BYTES_METRIC = "io.confluent.kafka.server/received_bytes"

DEFAULT_LOOKBACK_MINUTES = 15


def is_configured() -> bool:
    return bool(
        os.environ.get("CONFLUENT_METRICS_API_KEY")
        and os.environ.get("CONFLUENT_METRICS_API_SECRET")
        and os.environ.get("CONFLUENT_CLUSTER_ID")
    )


def _query_metric(
    metric: str, group_by: list[str], lookback_minutes: int = DEFAULT_LOOKBACK_MINUTES
) -> list[dict[str, Any]]:
    # Cloud API Key de conta (Confluent Cloud > perfil > API Keys > Cloud
    # resource management) — diferente da API Key de cluster já usada pelo
    # producer (CONFLUENT_API_KEY/SECRET) pra produzir/consumir eventos, que
    # não tem escopo pra consultar a Metrics API.
    api_key = os.environ["CONFLUENT_METRICS_API_KEY"]
    api_secret = os.environ["CONFLUENT_METRICS_API_SECRET"]
    cluster_id = os.environ["CONFLUENT_CLUSTER_ID"]

    now = datetime.now(UTC).replace(microsecond=0)
    start = now - timedelta(minutes=lookback_minutes)
    body = {
        "aggregations": [{"metric": metric, "agg": "MAX"}],
        "filter": {
            "op": "AND",
            "filters": [{"field": "resource.kafka.id", "op": "EQ", "value": cluster_id}],
        },
        "group_by": group_by,
        "granularity": "PT1M",
        "intervals": [f"{start.isoformat()}/{now.isoformat()}"],
    }
    response = requests.post(
        METRICS_API_URL,
        json=body,
        auth=(api_key, api_secret),
        timeout=15,
    )
    response.raise_for_status()
    return response.json().get("data", [])


def fetch_consumer_lag(lookback_minutes: int = DEFAULT_LOOKBACK_MINUTES) -> list[dict[str, Any]]:
    # Consumer groups sem consumer ativo no momento não aparecem no
    # resultado — comportamento documentado da própria API, não ausência de
    # dado nosso.
    return _query_metric(
        CONSUMER_LAG_METRIC,
        group_by=["metric.consumer_group_id", "metric.topic", "metric.partition"],
        lookback_minutes=lookback_minutes,
    )


def fetch_partition_throughput(
    lookback_minutes: int = DEFAULT_LOOKBACK_MINUTES,
) -> list[dict[str, Any]]:
    # received_bytes por partição na mesma janela — usado como proxy de
    # "partition skew" (partições recebendo volume desigual entre si), já
    # que a API não expõe um metric_name dedicado de skew diretamente.
    return _query_metric(
        RECEIVED_BYTES_METRIC,
        group_by=["metric.topic", "metric.partition"],
        lookback_minutes=lookback_minutes,
    )
