import argparse

import mlflow
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split

from src.common.spark_session import get_spark
from src.fraud.streaming_score import compute_fraud_score

FEATURE_COLUMNS = ["feature_night_claim", "feature_high_frequency", "feature_amount_outlier"]
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


def train_fraud_model(
    features_df: pd.DataFrame,
    experiment_name: str = "insurance-fraud-detection",
    random_state: int = 42,
) -> str:
    mlflow.set_experiment(experiment_name)

    x = features_df[FEATURE_COLUMNS]
    y = features_df[LABEL_COLUMN]
    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.2, random_state=random_state, stratify=y
    )

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
        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            registered_model_name="insurance_fraud_classifier",
        )

        return run.info.run_id


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gold-claims-table", required=True)
    parser.add_argument("--experiment-name", default="insurance-fraud-detection")
    args = parser.parse_args()

    features_df = extract_training_features(args.gold_claims_table)
    run_id = train_fraud_model(features_df, args.experiment_name)
    print(f"trained fraud model, mlflow run_id={run_id}")


if __name__ == "__main__":
    main()
