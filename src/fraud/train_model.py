import argparse
import sys
from pathlib import Path

# Job roda como spark_python_task via workspace files (sem empacotamento em
# wheel) — Databricks só põe o diretório do próprio script no sys.path, não a
# raiz do bundle. Sem isso, "from src..." abaixo falha com ModuleNotFoundError.
# Databricks executa o job via exec(compile(source, filename, 'exec')), que
# nao injeta __file__ nos globals — cai pro co_filename do frame atual.
_this_file = globals().get("__file__") or sys._getframe().f_code.co_filename
sys.path.insert(0, str(Path(_this_file).resolve().parents[2]))

import mlflow
import pandas as pd
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException
from mlflow.models import infer_signature
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split

from src.common.spark_session import get_spark
from src.fraud.streaming_score import FEATURE_COLUMNS, compute_fraud_score

LABEL_COLUMN = "fraud_flag"
WEAK_LABEL_THRESHOLD = 0.7


def extract_training_features(gold_claims_table: str) -> pd.DataFrame:
    spark = get_spark("fraud-train-features")
    silver_df = spark.read.table(gold_claims_table)
    scored_df = compute_fraud_score(silver_df)
    labeled_df = scored_df.withColumn(
        LABEL_COLUMN, (scored_df["fraud_score"] >= WEAK_LABEL_THRESHOLD)
    )
    return labeled_df.select(*FEATURE_COLUMNS, LABEL_COLUMN).toPandas()


def should_retrain(
    force: bool, bootstrap: bool, latest_drift_detected: bool | None
) -> tuple[bool, str]:
    if force:
        return True, "forced"
    if bootstrap:
        return True, "bootstrap: baseline ou drift-results table ainda não existe"
    if latest_drift_detected:
        return True, "drift detectado na última checagem"
    return False, "nenhum drift detectado, pulando (use --force para forçar)"


def get_latest_drift_signal(spark: SparkSession, drift_results_table: str) -> bool | None:
    if not spark.catalog.tableExists(drift_results_table):
        return None
    results_df = spark.read.table(drift_results_table)
    if results_df.isEmpty():
        return None
    latest_checked_at = results_df.selectExpr("max(_checked_at) as latest").first()["latest"]
    latest_rows = results_df.filter(results_df["_checked_at"] == latest_checked_at)
    return latest_rows.filter(latest_rows["drift_detected"]).limit(1).count() > 0


def should_promote(new_f1: float, champion_f1: float | None) -> bool:
    # > estrito (não >=) evita trocar o alias champion por ruído/empate.
    return champion_f1 is None or new_f1 > champion_f1


def get_champion_f1(client: MlflowClient, registered_model_name: str) -> float | None:
    try:
        champion_mv = client.get_model_version_by_alias(registered_model_name, "champion")
    except MlflowException as exc:
        if exc.error_code != "RESOURCE_DOES_NOT_EXIST":
            raise
        return None
    return client.get_run(champion_mv.run_id).data.metrics.get("f1")


def write_feature_baseline(
    x_train: pd.DataFrame,
    registered_model_name: str,
    model_version: str,
    baseline_table: str,
) -> None:
    stats = x_train.agg(["mean", "std"])
    rows = [
        (
            registered_model_name,
            model_version,
            feature,
            float(stats.loc["mean", feature]),
            float(stats.loc["std", feature]),
        )
        for feature in FEATURE_COLUMNS
    ]
    spark = get_spark("fraud-train-baseline")
    baseline_df = spark.createDataFrame(
        rows, ["model_name", "model_version", "feature_name", "mean", "stddev"]
    ).withColumn("_computed_at", current_timestamp())
    # Baseline é um snapshot pequeno (uma linha por feature) do champion atual
    # — substitui a tabela inteira a cada promoção, sem manter histórico.
    baseline_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(baseline_table)


def train_fraud_model(
    features_df: pd.DataFrame,
    experiment_name: str,
    registered_model_name: str,
    baseline_table: str,
    random_state: int = 42,
) -> dict:
    # mlflow tenta descobrir a registry URI lendo spark.conf.get(...) via Spark
    # Connect (compute serverless), que rejeita esse config específico com
    # CONFIG_NOT_AVAILABLE. Setar tracking/registry URI explicitamente evita
    # essa resolução automática via sessão Spark.
    mlflow.set_tracking_uri("databricks")
    # O legacy Workspace Model Registry está desabilitado neste workspace —
    # "databricks-uc" registra no Unity Catalog, que exige um nome de modelo
    # de 3 níveis (catalog.schema.model), não um nome solto.
    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(experiment_name)

    x = features_df[FEATURE_COLUMNS]
    y = features_df[LABEL_COLUMN]
    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.2, random_state=random_state, stratify=y
    )

    client = MlflowClient()

    with mlflow.start_run() as run:
        model = RandomForestClassifier(n_estimators=100, max_depth=6, random_state=random_state)
        model.fit(x_train, y_train)

        predictions = model.predict(x_test)
        metrics = {
            "precision": precision_score(y_test, predictions, zero_division=0),
            "recall": recall_score(y_test, predictions, zero_division=0),
            "f1": f1_score(y_test, predictions, zero_division=0),
        }

        mlflow.log_params({"n_estimators": 100, "max_depth": 6})
        mlflow.log_metrics(metrics)
        # Unity Catalog exige assinatura de input/output no modelo (o legacy
        # Workspace Model Registry aceitava sem, só com um aviso).
        signature = infer_signature(x_test, predictions)
        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            registered_model_name=registered_model_name,
            signature=signature,
            input_example=x_test.iloc[:5],
        )

        # search_model_versions em vez de ModelInfo.registered_model_version —
        # esse atributo nem sempre existe entre versões do mlflow; a busca
        # filtrada por run_id é determinística e sempre retorna exatamente a
        # versão recém-criada por este run.
        versions = client.search_model_versions(
            f"name='{registered_model_name}' and run_id='{run.info.run_id}'"
        )
        new_version = versions[0].version

        champion_f1 = get_champion_f1(client, registered_model_name)
        promoted = should_promote(metrics["f1"], champion_f1)

        client.set_model_version_tag(
            registered_model_name, new_version, "eval_f1", str(metrics["f1"])
        )
        if promoted:
            client.set_registered_model_alias(registered_model_name, "champion", new_version)
            client.set_model_version_tag(
                registered_model_name, new_version, "training_outcome", "promoted"
            )
            write_feature_baseline(x_train, registered_model_name, new_version, baseline_table)
        else:
            client.set_model_version_tag(
                registered_model_name, new_version, "training_outcome", "rejected"
            )

        return {
            "run_id": run.info.run_id,
            "version": new_version,
            "f1": metrics["f1"],
            "champion_f1": champion_f1,
            "promoted": promoted,
        }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gold-claims-table", required=True)
    parser.add_argument("--experiment-name", default="insurance-fraud-detection")
    parser.add_argument("--drift-results-table")
    parser.add_argument("--feature-baseline-table")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    catalog = args.gold_claims_table.split(".")[0]
    registered_model_name = f"{catalog}.models.insurance_fraud_classifier"
    baseline_table = args.feature_baseline_table or f"{catalog}.monitoring._feature_baseline"
    drift_results_table = args.drift_results_table or f"{catalog}.monitoring._model_drift_results"

    spark = get_spark("fraud-train-gate")
    bootstrap = not (
        spark.catalog.tableExists(baseline_table)
        and spark.catalog.tableExists(drift_results_table)
    )
    latest_drift_detected = get_latest_drift_signal(spark, drift_results_table)
    retrain, reason = should_retrain(args.force, bootstrap, latest_drift_detected)
    print(f"should_retrain={retrain} ({reason})")
    if not retrain:
        return

    features_df = extract_training_features(args.gold_claims_table)
    result = train_fraud_model(
        features_df, args.experiment_name, registered_model_name, baseline_table
    )
    print(
        f"trained fraud model v{result['version']} run_id={result['run_id']} "
        f"f1={result['f1']:.4f} champion_f1={result['champion_f1']} promoted={result['promoted']}"
    )


if __name__ == "__main__":
    main()
