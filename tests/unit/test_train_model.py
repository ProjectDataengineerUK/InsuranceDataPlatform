from datetime import datetime

from src.fraud.train_model import get_latest_drift_signal, should_promote, should_retrain


def test_should_promote_when_no_champion_exists():
    assert should_promote(new_f1=0.5, champion_f1=None) is True


def test_should_promote_when_strictly_better():
    assert should_promote(new_f1=0.8, champion_f1=0.7) is True


def test_should_promote_rejects_tie_or_worse():
    assert should_promote(new_f1=0.7, champion_f1=0.7) is False
    assert should_promote(new_f1=0.6, champion_f1=0.7) is False


def test_should_retrain_forced_overrides_everything():
    retrain, _ = should_retrain(force=True, bootstrap=False, latest_drift_detected=False)
    assert retrain is True


def test_should_retrain_bootstrap_when_tables_missing():
    retrain, reason = should_retrain(force=False, bootstrap=True, latest_drift_detected=None)
    assert retrain is True
    assert "bootstrap" in reason


def test_should_retrain_on_drift_signal():
    retrain, reason = should_retrain(force=False, bootstrap=False, latest_drift_detected=True)
    assert retrain is True
    assert "drift" in reason


def test_should_retrain_skips_when_no_drift():
    retrain, reason = should_retrain(force=False, bootstrap=False, latest_drift_detected=False)
    assert retrain is False
    assert "pulando" in reason


def test_get_latest_drift_signal_none_when_table_missing(spark):
    assert get_latest_drift_signal(spark, "nonexistent_drift_results_table") is None


def test_get_latest_drift_signal_reads_latest_batch(spark):
    older = spark.createDataFrame(
        [("feature_a", True, datetime(2026, 1, 1, 0, 0, 0))],
        ["feature_name", "drift_detected", "_checked_at"],
    )
    older.write.format("delta").saveAsTable("drift_signal_test")

    newer = spark.createDataFrame(
        [("feature_a", False, datetime(2026, 1, 2, 0, 0, 0))],
        ["feature_name", "drift_detected", "_checked_at"],
    )
    newer.write.format("delta").mode("append").saveAsTable("drift_signal_test")

    assert get_latest_drift_signal(spark, "drift_signal_test") is False
