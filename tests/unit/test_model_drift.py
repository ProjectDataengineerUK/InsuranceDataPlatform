from src.monitoring.model_drift import compute_drift


def test_compute_drift_flags_feature_beyond_threshold():
    baseline = {"feature_a": (0.5, 0.1)}
    current = {"feature_a": 0.9}

    results = compute_drift(baseline, current, threshold=2.0)

    assert len(results) == 1
    assert results[0].drift_detected is True
    assert results[0].drift_ratio > 2.0


def test_compute_drift_does_not_flag_small_shift():
    baseline = {"feature_a": (0.5, 0.2)}
    current = {"feature_a": 0.55}

    results = compute_drift(baseline, current, threshold=2.0)

    assert results[0].drift_detected is False


def test_compute_drift_uses_min_stddev_floor_to_avoid_division_by_zero():
    baseline = {"feature_a": (0.5, 0.0)}
    current = {"feature_a": 0.5}

    results = compute_drift(baseline, current, threshold=2.0, min_stddev=0.05)

    assert results[0].drift_ratio == 0.0
    assert results[0].drift_detected is False


def test_compute_drift_skips_features_missing_from_current():
    baseline = {"feature_a": (0.5, 0.1), "feature_b": (1.0, 0.2)}
    current = {"feature_a": 0.5}

    results = compute_drift(baseline, current)

    assert len(results) == 1
    assert results[0].feature_name == "feature_a"
