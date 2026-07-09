import argparse
import logging
import os
import sys
from pathlib import Path

# Job roda como spark_python_task via workspace files (sem empacotamento em
# wheel) — Databricks só põe o diretório do próprio script no sys.path, não a
# raiz do bundle. Sem isso, "from src..." abaixo falha com ModuleNotFoundError.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, unix_timestamp

from src.common.spark_session import get_spark

logger = logging.getLogger(__name__)

DEFAULT_SLA_HOURS = 24


def find_sla_breaches(gold_claims_df: DataFrame, sla_hours: int = DEFAULT_SLA_HOURS) -> DataFrame:
    age_hours = (
        unix_timestamp(current_timestamp()) - unix_timestamp(col("event_timestamp"))
    ) / 3600
    return gold_claims_df.filter(
        (~col("auto_approved")) & (age_hours > lit(sla_hours))
    )


def send_alert(breach_count: int, webhook_url: str | None) -> None:
    if breach_count == 0:
        return

    message = f"{breach_count} sinistro(s) ultrapassaram o SLA e aguardam revisão manual."
    if not webhook_url:
        logger.warning("SLA_WEBHOOK_URL not set, logging alert instead: %s", message)
        return

    response = requests.post(webhook_url, json={"text": message}, timeout=10)
    response.raise_for_status()


def run_sla_monitor(gold_claims_table: str, sla_hours: int = DEFAULT_SLA_HOURS) -> int:
    spark = get_spark("sla-monitor")
    gold_claims_df = spark.read.table(gold_claims_table)

    breaches_df = find_sla_breaches(gold_claims_df, sla_hours)
    breach_count = breaches_df.count()

    send_alert(breach_count, os.environ.get("SLA_WEBHOOK_URL"))
    return breach_count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gold-claims-table", required=True)
    parser.add_argument("--sla-hours", type=int, default=DEFAULT_SLA_HOURS)
    args = parser.parse_args()

    breach_count = run_sla_monitor(args.gold_claims_table, args.sla_hours)
    print(f"SLA breaches found: {breach_count}")


if __name__ == "__main__":
    main()
