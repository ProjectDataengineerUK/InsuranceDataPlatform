import argparse
import logging
import sys
from collections.abc import Callable
from pathlib import Path

# Job roda como spark_python_task via workspace files (sem empacotamento em
# wheel) — Databricks só põe o diretório do próprio script no sys.path, não a
# raiz do bundle. Sem isso, "from src..." abaixo falha com ModuleNotFoundError.
# Databricks executa o job via exec(compile(source, filename, 'exec')), que
# nao injeta __file__ nos globals — cai pro co_filename do frame atual.
_this_file = globals().get("__file__") or sys._getframe().f_code.co_filename
sys.path.insert(0, str(Path(_this_file).resolve().parents[2]))

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.streaming import StreamingQuery

from src.common.delta_merge import merge_into_delta
from src.common.spark_session import get_spark
from src.fraud.streaming_score import DEFAULT_SCORE_THRESHOLD, FEATURE_COLUMNS, score_claims
from src.quality.checks import check_not_null, check_unique, persist_results

logger = logging.getLogger(__name__)

DEFAULT_AUTO_APPROVAL_AMOUNT_THRESHOLD = 5000.0


def load_champion_model_udf(experiment_name: str) -> Callable[..., Column] | None:
    """Shadow scoring: carrega o modelo mlflow marcado como champion (tag
    model_stage=champion na run — não é alias de Model Registry, ver
    ARCHITECTURE.md sobre o bloqueio de UC Model Registry) como Spark UDF.

    Best-effort e nunca bloqueante: se não existir champion ainda
    (bootstrap), ou se mlflow.pyfunc.spark_udf não funcionar neste compute
    serverless/Spark Connect (API nunca exercitada nesta sessão, ao
    contrário de tracking/dbutils.secrets já provados), retorna None e
    fraud_score_stream segue rodando só com a heurística, que continua
    sendo a única fonte real de auto_approved/fraud_flag.
    """
    try:
        import mlflow
        from mlflow import MlflowClient

        mlflow.set_tracking_uri("databricks")
        mlflow.set_registry_uri("databricks")
        client = MlflowClient()
        experiment = client.get_experiment_by_name(experiment_name)
        if experiment is None:
            return None

        champion_runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            filter_string="tags.model_stage = 'champion'",
            max_results=1,
        )
        if not champion_runs:
            logger.info("nenhum modelo champion ainda — shadow scoring desligado")
            return None

        run_id = champion_runs[0].info.run_id
        return mlflow.pyfunc.spark_udf(
            get_spark("fraud-scoring-model-load"),
            model_uri=f"runs:/{run_id}/model",
            result_type="double",
        )
    except Exception:
        logger.warning(
            "falha ao carregar modelo champion para shadow scoring — seguindo só com "
            "a heurística",
            exc_info=True,
        )
        return None


def apply_auto_approval(
    df: DataFrame,
    amount_threshold: float = DEFAULT_AUTO_APPROVAL_AMOUNT_THRESHOLD,
    fraud_threshold: float = DEFAULT_SCORE_THRESHOLD,
) -> DataFrame:
    is_low_value = col("amount") <= lit(amount_threshold)
    is_low_risk = col("fraud_score") < lit(fraud_threshold)
    return df.withColumn("auto_approved", is_low_value & is_low_risk)


def build_claims_gold(
    silver_claims_df: DataFrame, model_udf: Callable[..., Column] | None = None
) -> DataFrame:
    scored = score_claims(silver_claims_df)
    # Shadow scoring: model_fraud_score é só observabilidade — nunca entra em
    # apply_auto_approval, que continua decidindo 100% pela heurística.
    if model_udf is not None:
        scored = scored.withColumn("model_fraud_score", model_udf(*FEATURE_COLUMNS))
    else:
        scored = scored.withColumn("model_fraud_score", lit(None).cast("double"))
    approved = apply_auto_approval(scored).withColumn("_scored_at", current_timestamp())
    return approved.select(
        "claim_id",
        "policy_id",
        "customer_id",
        "event_timestamp",
        "amount",
        "region",
        "vehicle_type",
        "fraud_score",
        "fraud_flag",
        "model_fraud_score",
        "auto_approved",
        "event_date",
        # carregados do Bronze via Silver — usados por
        # scripts/measure_pipeline_latency.py para medir AT-001/latência de
        # score de fraude contra o SLA real, não apenas revisão de código.
        "_ingested_at",
        "_scored_at",
    )


def process_batch(
    batch_df: DataFrame,
    batch_id: int,
    gold_claims_table: str,
    results_table: str,
    model_udf: Callable[..., Column] | None = None,
) -> None:
    if batch_df.isEmpty():
        return

    claims_gold_df = build_claims_gold(batch_df, model_udf)

    results = check_not_null(
        claims_gold_df, ["claim_id", "policy_id", "customer_id"], gold_claims_table
    )
    results.append(check_unique(claims_gold_df, ["claim_id"], gold_claims_table))
    persist_results(batch_df.sparkSession, results, results_table)

    merge_into_delta(claims_gold_df, gold_claims_table, key_column="claim_id")


def run_fraud_scoring_stream(
    silver_table: str,
    gold_claims_table: str,
    checkpoint_path: str,
    results_table: str,
    experiment_name: str | None = None,
) -> StreamingQuery:
    spark: SparkSession = get_spark(f"fraud-scoring-stream-{gold_claims_table}")
    silver_stream = spark.readStream.format("delta").table(silver_table)

    # Carregado uma vez por run do job (não por micro-batch) — evita recarregar
    # o modelo a cada foreachBatch; um champion novo só é pego no próximo
    # restart do job contínuo (ex.: próximo deploy, ou o próprio Databricks
    # reiniciando após o availableNow terminar).
    model_udf = load_champion_model_udf(experiment_name) if experiment_name else None

    return (
        silver_stream.writeStream.foreachBatch(
            lambda batch_df, batch_id: process_batch(
                batch_df, batch_id, gold_claims_table, results_table, model_udf
            )
        )
        .option("checkpointLocation", checkpoint_path)
        # Compute serverless (obrigatório neste workspace) não suporta trigger
        # ProcessingTime "infinito" — só AvailableNow/Once. O job já roda como
        # "continuous job" no Databricks (ver resources/jobs.fraud_score.yml),
        # que reinicia o run assim que este termina, então availableNow
        # entrega o mesmo efeito prático de streaming quase-contínuo.
        .trigger(availableNow=True)
        .start()
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--gold-claims-table", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    parser.add_argument("--results-table", required=True)
    parser.add_argument(
        "--experiment-name",
        default=None,
        help="Nome do experimento mlflow do train_model.py — usado só para localizar o "
        "run champion (shadow scoring); sem isso, o job roda só com a heurística.",
    )
    args = parser.parse_args()

    query = run_fraud_scoring_stream(
        args.silver_table,
        args.gold_claims_table,
        args.checkpoint_path,
        args.results_table,
        args.experiment_name,
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
