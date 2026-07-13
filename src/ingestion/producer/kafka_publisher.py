import json
import logging
import math
import random
import time
from collections.abc import Iterator
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)

DEFAULT_TOPIC_PARTITIONS = 6
DEFAULT_TOPIC_REPLICATION_FACTOR = 3


def _sanitize_for_json(event: dict[str, Any]) -> dict[str, Any]:
    # pd.DataFrame(rows) força colunas numéricas mistas com None pro dtype
    # float64, convertendo None em NaN silenciosamente (ex.:
    # regulatory_feeds.py usa None pra simular campo ausente em
    # CLAIM_AMOUNT/amountCents) — json.dumps serializa float('nan') como o
    # token JSON inválido "NaN" (não "null"), que o from_json/cast do lado
    # Spark não trata como ausente, e sim como a string literal "NaN"
    # (CAST_INVALID_INPUT confirmado num deploy real). Convertendo de volta
    # pra None aqui garante que "campo ausente" sempre vire JSON null.
    return {
        key: (None if isinstance(value, float) and math.isnan(value) else value)
        for key, value in event.items()
    }


def build_producer(bootstrap_servers: str, api_key: str, api_secret: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": api_key,
            "sasl.password": api_secret,
            # Client telemetry (KIP-714) hangs against some Confluent Cloud
            # clusters — the GetTelemetrySubscriptions request times out after
            # ~6min and tears down the whole connection. Disable it.
            "enable.metrics.push": False,
        }
    )


def ensure_topic_exists(
    bootstrap_servers: str,
    api_key: str,
    api_secret: str,
    topic: str,
    num_partitions: int = DEFAULT_TOPIC_PARTITIONS,
    replication_factor: int = DEFAULT_TOPIC_REPLICATION_FACTOR,
    request_timeout_seconds: int = 30,
) -> None:
    # Este cluster Confluent Cloud não cria tópicos automaticamente na
    # primeira produção (confirmado em produção: publish_events falhou com
    # KafkaError{code=_UNKNOWN_TOPIC} pro tópico novo regulatory-claim-report,
    # enquanto os 3 tópicos operacionais já existentes seguiram funcionando
    # normalmente). Chamado uma vez por fonte/thread em main.py antes de
    # publicar — múltiplas threads podem chamar isso para o MESMO tópico
    # concorrentemente (regulatory-claim-report é compartilhado por
    # insurer_a/b/c); create_topics é idempotente aqui porque o erro
    # "already exists" da segunda chamada em diante é esperado e ignorado,
    # mesmo padrão de append_or_create (src/common/delta_write.py) pra
    # tabelas Delta criadas concorrentemente.
    admin_client = AdminClient(
        {
            "bootstrap.servers": bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": api_key,
            "sasl.password": api_secret,
        }
    )
    new_topic = NewTopic(
        topic, num_partitions=num_partitions, replication_factor=replication_factor
    )
    futures = admin_client.create_topics([new_topic], request_timeout=request_timeout_seconds)

    for created_topic, future in futures.items():
        try:
            future.result()
            logger.info("created topic %s", created_topic)
        except Exception as exc:
            if "already exists" not in str(exc).lower():
                raise
            logger.info("topic %s already exists, skipping creation", created_topic)


def _delivery_callback(err, msg) -> None:
    if err is not None:
        logger.error("failed to deliver message to %s: %s", msg.topic(), err)


def publish_events(
    producer: Producer,
    topic: str,
    events: list[dict[str, Any]],
    events_per_minute: int,
    shuffle: bool = True,
    loop: bool = True,
    max_duration_seconds: float | None = None,
) -> Iterator[dict[str, Any]]:
    if not events:
        raise ValueError(f"no events to publish for topic '{topic}'")

    delay_seconds = 60.0 / max(events_per_minute, 1)
    ordered_events = list(events)
    start_time = time.monotonic()

    def _duration_exceeded() -> bool:
        return (
            max_duration_seconds is not None
            and (time.monotonic() - start_time) >= max_duration_seconds
        )

    while True:
        batch = ordered_events[:]
        if shuffle:
            random.shuffle(batch)

        for event in batch:
            if _duration_exceeded():
                producer.flush()
                return

            key = str(
                event.get("claim_id")
                or event.get("policy_id")
                or event.get("customer_id")
                or event.get("external_reference_id")
            )
            payload = json.dumps(_sanitize_for_json(event), default=str).encode("utf-8")
            producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=payload,
                callback=_delivery_callback,
            )
            producer.poll(0)
            yield event
            time.sleep(delay_seconds)

        producer.flush()
        if not loop or _duration_exceeded():
            break
