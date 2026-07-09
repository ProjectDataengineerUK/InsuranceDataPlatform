import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

# Job roda como spark_python_task via workspace files (sem empacotamento em
# wheel) — Databricks só põe o diretório do próprio script no sys.path, não a
# raiz do bundle. Sem isso, "from src..." abaixo falha com ModuleNotFoundError.
# Databricks executa o job via exec(compile(source, filename, 'exec')), que
# nao injeta __file__ nos globals — cai pro co_filename do frame atual.
_this_file = globals().get("__file__") or sys._getframe().f_code.co_filename
sys.path.insert(0, str(Path(_this_file).resolve().parents[2]))

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, lit, unix_timestamp

from src.common.delta_write import append_or_create
from src.common.secrets import get_secret
from src.common.spark_session import get_spark
from src.fraud.streaming_score import FEATURE_COLUMNS, compute_fraud_score

logger = logging.getLogger(__name__)

DEFAULT_LOOKBACK_HOURS = 24
DEFAULT_DRIFT_THRESHOLD = 2.0
MIN_STDDEV_FLOOR = 0.05


@dataclass
class DriftResult:
    feature_name: str
    baseline_mean: float
    baseline_stddev: float
    current_mean: float
    drift_ratio: float
    drift_detected: bool


def compute_drift(
    baseline: dict[str, tuple[float, float]],
    current: dict[str, float],
    threshold: float = DEFAULT_DRIFT_THRESHOLD,
    min_stddev: float = MIN_STDDEV_FLOOR,
) -> list[DriftResult]:
    results = []
    for feature_name, (baseline_mean, baseline_stddev) in baseline.items():
        current_mean = current.get(feature_name)
        if current_mean is None:
            continue
        effective_stddev = max(baseline_stddev, min_stddev)
        drift_ratio = abs(current_mean - baseline_mean) / effective_stddev
        results.append(
            DriftResult(
                feature_name=feature_name,
                baseline_mean=baseline_mean,
                baseline_stddev=baseline_stddev,
                current_mean=current_mean,
                drift_ratio=drift_ratio,
                drift_detected=drift_ratio > threshold,
            )
        )
    return results


def load_baseline(spark: SparkSession, baseline_table: str) -> dict[str, tuple[float, float]]:
    baseline_df = spark.read.table(baseline_table)
    return {
        row["feature_name"]: (row["mean"], row["stddev"])
        for row in baseline_df.select("feature_name", "mean", "stddev").collect()
    }


def compute_current_feature_means(
    spark: SparkSession, gold_claims_table: str, lookback_hours: int
) -> dict[str, float]:
    claims_df = spark.read.table(gold_claims_table)
    recent_df = claims_df.filter(
        (unix_timestamp(current_timestamp()) - unix_timestamp(claims_df["event_timestamp"]))
        <= lit(lookback_hours * 3600)
    )
    # feature_high_frequency é uma contagem por cliente numa janela — recalculá-la
    # só sobre a janela recente do monitor (em vez do histórico completo, como no
    # scoring ao vivo em streaming_gold.py) muda um pouco sua escala absoluta.
    # Aceitável para detecção de drift *relativo*; não corrigimos isso aqui.
    featured_df = compute_fraud_score(recent_df)
    agg_exprs = [avg(col_name).alias(col_name) for col_name in FEATURE_COLUMNS]
    row = featured_df.agg(*agg_exprs).first()
    if row is None:
        return {}
    return {col_name: row[col_name] for col_name in FEATURE_COLUMNS}


def send_alert(drift_results: list[DriftResult], webhook_url: str | None) -> None:
    detected = [r for r in drift_results if r.drift_detected]
    if not detected:
        return

    feature_list = ", ".join(r.feature_name for r in detected)
    message = f"Drift detectado em {len(detected)} feature(s) do modelo de fraude: {feature_list}."
    if not webhook_url:
        logger.warning(
            "sla-webhook-url não configurado, logando alerta em vez de enviar: %s", message
        )
        return

    response = requests.post(webhook_url, json={"text": message}, timeout=10)
    response.raise_for_status()


def run_drift_monitor(
    gold_claims_table: str,
    baseline_table: str,
    results_table: str,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    threshold: float = DEFAULT_DRIFT_THRESHOLD,
) -> list[DriftResult]:
    spark = get_spark("model-drift-monitor")

    if not spark.catalog.tableExists(baseline_table):
        # Nenhum modelo foi promovido ainda (train_model.py só escreve a
        # baseline na primeira promoção) — nada para comparar.
        logger.warning(
            "%s ainda não existe — nenhum modelo promovido, pulando checagem de drift",
            baseline_table,
        )
        return []

    baseline = load_baseline(spark, baseline_table)
    current = compute_current_feature_means(spark, gold_claims_table, lookback_hours)
    drift_results = compute_drift(baseline, current, threshold)

    if drift_results:
        rows = [
            (
                r.feature_name,
                r.baseline_mean,
                r.baseline_stddev,
                r.current_mean,
                r.drift_ratio,
                r.drift_detected,
                lookback_hours,
            )
            for r in drift_results
        ]
        results_df = spark.createDataFrame(
            rows,
            [
                "feature_name",
                "baseline_mean",
                "baseline_stddev",
                "current_mean",
                "drift_ratio",
                "drift_detected",
                "window_hours",
            ],
        ).withColumn("_checked_at", current_timestamp())
        append_or_create(results_df, results_table)

    webhook_url = get_secret(
        "insurance-platform", "sla-webhook-url", "SLA_WEBHOOK_URL", required=False
    )
    send_alert(drift_results, webhook_url)
    return drift_results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gold-claims-table", required=True)
    parser.add_argument("--baseline-table", required=True)
    parser.add_argument("--results-table", required=True)
    parser.add_argument("--lookback-hours", type=int, default=DEFAULT_LOOKBACK_HOURS)
    parser.add_argument("--drift-threshold", type=float, default=DEFAULT_DRIFT_THRESHOLD)
    args = parser.parse_args()

    results = run_drift_monitor(
        args.gold_claims_table,
        args.baseline_table,
        args.results_table,
        args.lookback_hours,
        args.drift_threshold,
    )
    detected = sum(1 for r in results if r.drift_detected)
    print(f"drift check: {len(results)} feature(s) checked, {detected} flagged")


if __name__ == "__main__":
    main()
