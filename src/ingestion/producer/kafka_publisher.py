import json
import logging
import random
import time
from collections.abc import Iterator
from typing import Any

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


def build_producer(bootstrap_servers: str, api_key: str, api_secret: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": api_key,
            "sasl.password": api_secret,
        }
    )


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

            key = str(event.get("claim_id") or event.get("policy_id") or event.get("customer_id"))
            payload = json.dumps(event, default=str).encode("utf-8")
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
