import logging
import os
import threading
from pathlib import Path

import yaml

from src.ingestion.producer.datasets.ans_loader import load_policy_events
from src.ingestion.producer.datasets.base_loader import download_csv
from src.ingestion.producer.datasets.susep_loader import load_claim_events
from src.ingestion.producer.kafka_publisher import build_producer, publish_events

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent / "config.yaml"

SOURCE_LOADERS = {
    "susep": load_claim_events,
    "ans": load_policy_events,
}


def load_config() -> dict:
    with open(CONFIG_PATH) as config_file:
        return yaml.safe_load(config_file)


def run_source(source_name: str, source_config: dict, config: dict) -> None:
    dataset_url = source_config.get("dataset_url")
    dataset_path = source_config["dataset_path"]

    if dataset_url:
        download_csv(dataset_url, dataset_path)

    loader = SOURCE_LOADERS[source_name]
    events_df = loader(dataset_path, source_config)
    events = events_df.to_dict(orient="records")

    kafka_cfg = config["kafka"]
    producer = build_producer(
        bootstrap_servers=os.environ[kafka_cfg["bootstrap_servers_env"]],
        api_key=os.environ[kafka_cfg["api_key_env"]],
        api_secret=os.environ[kafka_cfg["api_secret_env"]],
    )

    topic_key = source_config["event_topic"]
    topic = config["topics"][topic_key]
    replay_cfg = config["replay"]

    logger.info(
        "starting producer for source=%s topic=%s events_per_minute=%s",
        source_name,
        topic,
        replay_cfg["events_per_minute"],
    )

    for _ in publish_events(
        producer=producer,
        topic=topic,
        events=events,
        events_per_minute=replay_cfg["events_per_minute"],
        shuffle=replay_cfg["shuffle"],
        loop=replay_cfg["loop"],
    ):
        pass


def main() -> None:
    config = load_config()
    threads = []

    for source_name, source_config in config["sources"].items():
        if not source_config.get("enabled"):
            logger.info("source %s disabled, skipping", source_name)
            continue
        if source_name not in SOURCE_LOADERS:
            logger.warning("no loader implemented for source %s, skipping", source_name)
            continue

        thread = threading.Thread(
            target=run_source,
            args=(source_name, source_config, config),
            daemon=True,
            name=f"producer-{source_name}",
        )
        thread.start()
        threads.append(thread)

    if not threads:
        raise RuntimeError("no enabled sources with an implemented loader — nothing to publish")

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
