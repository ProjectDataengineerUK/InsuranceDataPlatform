import logging

from confluent_kafka import TopicPartition
from confluent_kafka.admin import AdminClient, ConsumerGroupTopicPartitions

from src.common.kafka_config import get_admin_client_config

logger = logging.getLogger(__name__)


def report_offsets(topic: str, group_id: str, partition_offsets: dict[int, int]) -> None:
    # Spark Structured Streaming não usa consumer groups do Kafka pra sua
    # própria tolerância a falhas (isso é 100% controlado pelo checkpoint,
    # ver bronze_ingest.py) — sem isto, nenhum consumer group real jamais
    # existiria pro Confluent Cloud reportar em consumer_lag_offsets. Este
    # commit administrativo (via Admin API, KIP-496) existe só pra dar essa
    # visibilidade externa; nunca deve derrubar a ingestão real, por isso o
    # try/except amplo — perder uma atualização de lag é aceitável, perder um
    # batch de dados não é.
    if not partition_offsets:
        return

    try:
        admin = AdminClient(get_admin_client_config())
        offsets = [
            TopicPartition(topic, partition, offset + 1)
            for partition, offset in partition_offsets.items()
        ]
        futures = admin.alter_consumer_group_offsets(
            [ConsumerGroupTopicPartitions(group_id, offsets)]
        )
        for future in futures.values():
            future.result()
    except Exception:
        logger.warning(
            "failed to report Kafka offsets for lag monitoring (topic=%s, group=%s)",
            topic,
            group_id,
            exc_info=True,
        )
