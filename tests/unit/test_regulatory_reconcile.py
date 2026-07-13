from src.regulatory.reconcile import reconcile_claims

ACTIVE_SOURCES = {"insurer_a", "insurer_b", "insurer_c"}


def test_amount_within_tolerance_is_not_flagged():
    rows = [
        {"external_reference_id": "ref1", "source_system": "insurer_a", "amount": 1000.0},
        {"external_reference_id": "ref1", "source_system": "insurer_b", "amount": 1005.0},
        {"external_reference_id": "ref1", "source_system": "insurer_c", "amount": 1002.0},
    ]

    results = reconcile_claims(rows, ACTIVE_SOURCES)

    assert not any(r.discrepancy_type == "amount_mismatch" for r in results)


def test_amount_beyond_tolerance_is_flagged_with_median_resolved_amount():
    rows = [
        {"external_reference_id": "ref1", "source_system": "insurer_a", "amount": 1000.0},
        {"external_reference_id": "ref1", "source_system": "insurer_b", "amount": 1200.0},
        {"external_reference_id": "ref1", "source_system": "insurer_c", "amount": 1100.0},
    ]

    results = reconcile_claims(rows, ACTIVE_SOURCES)

    mismatches = [r for r in results if r.discrepancy_type == "amount_mismatch"]
    assert len(mismatches) == 1
    assert mismatches[0].resolved_amount == 1100.0


def test_reference_id_missing_from_one_active_source_is_flagged():
    rows = [
        {"external_reference_id": "ref1", "source_system": "insurer_a", "amount": 1000.0},
        {"external_reference_id": "ref1", "source_system": "insurer_b", "amount": 1000.0},
    ]

    results = reconcile_claims(rows, ACTIVE_SOURCES)

    missing = [r for r in results if r.discrepancy_type == "missing_in_source"]
    assert len(missing) == 1
    assert missing[0].external_reference_id == "ref1"
    assert set(missing[0].sources_reporting) == {"insurer_a", "insurer_b"}


def test_reference_id_reported_by_all_active_sources_is_not_falsely_flagged():
    rows = [
        {"external_reference_id": "ref1", "source_system": "insurer_a", "amount": 1000.0},
        {"external_reference_id": "ref1", "source_system": "insurer_b", "amount": 1000.0},
        {"external_reference_id": "ref1", "source_system": "insurer_c", "amount": 1000.0},
    ]

    results = reconcile_claims(rows, ACTIVE_SOURCES)

    assert results == []


def test_null_amount_does_not_crash_and_is_excluded_from_comparison():
    # amount pode ser None (try_cast em standardize.py vira NULL pra entrada
    # malformada, ex.: "NaN" já gravado no Bronze antes do fix do producer)
    # — não deve derrubar a reconciliação (TypeError: float(None) confirmado
    # em produção antes deste fix).
    rows = [
        {"external_reference_id": "ref1", "source_system": "insurer_a", "amount": 1000.0},
        {"external_reference_id": "ref1", "source_system": "insurer_b", "amount": None},
        {"external_reference_id": "ref1", "source_system": "insurer_c", "amount": 1000.0},
    ]

    results = reconcile_claims(rows, ACTIVE_SOURCES)

    assert not any(r.discrepancy_type == "amount_mismatch" for r in results)


def test_all_amounts_null_for_a_claim_only_flags_missing_in_source():
    rows = [
        {"external_reference_id": "ref1", "source_system": "insurer_a", "amount": None},
    ]

    results = reconcile_claims(rows, ACTIVE_SOURCES)

    assert len(results) == 1
    assert results[0].discrepancy_type == "missing_in_source"
    assert set(results[0].sources_reporting) == {"insurer_a"}
