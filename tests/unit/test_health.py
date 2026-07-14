import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "apps" / "insurance_platform_app"))

from health import (
    GREEN,
    RED,
    UNKNOWN,
    YELLOW,
    build_architecture_dot,
    compliance_status,
    count_threshold_status,
    drift_status,
    freshness_status,
    sla_status,
    table_exists_status,
)


def test_freshness_status_unknown_when_no_timestamp():
    assert freshness_status(None) == UNKNOWN


def test_freshness_status_green_when_recent():
    assert freshness_status(datetime.now(UTC) - timedelta(minutes=1)) == GREEN


def test_freshness_status_red_when_stale():
    assert freshness_status(datetime.now(UTC) - timedelta(hours=2)) == RED


def test_compliance_status_unknown_when_no_data():
    assert compliance_status(0, 0) == UNKNOWN


def test_compliance_status_green_when_high_rate():
    assert compliance_status(96, 4) == GREEN


def test_compliance_status_yellow_when_moderate_rate():
    assert compliance_status(85, 15) == YELLOW


def test_compliance_status_red_when_low_rate():
    assert compliance_status(50, 50) == RED


def test_count_threshold_status_green_below_yellow():
    assert count_threshold_status(0, yellow_at=1, red_at=10) == GREEN


def test_count_threshold_status_yellow_between_thresholds():
    assert count_threshold_status(5, yellow_at=1, red_at=10) == YELLOW


def test_count_threshold_status_red_above_threshold():
    assert count_threshold_status(10, yellow_at=1, red_at=10) == RED


def test_drift_status_unknown_when_no_rows():
    assert drift_status([]) == UNKNOWN


def test_drift_status_green_when_no_drift_detected():
    assert drift_status([{"drift_detected": False}, {"drift_detected": False}]) == GREEN


def test_drift_status_yellow_never_red_when_drift_detected():
    assert drift_status([{"drift_detected": True}]) == YELLOW


def test_sla_status_maps_none_true_false():
    assert sla_status(None) == UNKNOWN
    assert sla_status(True) == GREEN
    assert sla_status(False) == RED


def test_table_exists_status():
    assert table_exists_status(False) == UNKNOWN
    assert table_exists_status(True) == GREEN


def test_build_architecture_dot_includes_status_colors():
    dot = build_architecture_dot({"bronze_operational": GREEN, "gold_fraud": RED})

    assert "digraph pipeline" in dot
    assert "#2ecc71" in dot
    assert "#e74c3c" in dot
    # Nó sem status explícito cai no cinza (unknown), não quebra a geração.
    assert "#bdc3c7" in dot
