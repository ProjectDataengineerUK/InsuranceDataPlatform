import pandas as pd

from src.ingestion.producer.datasets.susep_loader import normalize_susep_claims


def _row(**overrides):
    base = {
        "COD_APO": "287756959",
        "D_OCORR": "20200504",
        "INDENIZ": "520.0",
        "REGIAO": "21",
        "CAUSA": "9",
    }
    base.update(overrides)
    return base


def test_normalizes_real_susep_row_shape():
    df = pd.DataFrame([_row()])

    result = normalize_susep_claims(df)

    assert len(result) == 1
    row = result.iloc[0]
    assert row["policy_id"] == "287756959"
    assert row["amount"] == 520.0
    assert row["region"] == "21"
    assert row["event_timestamp"] == pd.Timestamp("2020-05-04")
    assert row["vehicle_type"] == "outros"
    assert row["source"] == "susep"
    assert row["claim_id"]


def test_decodes_known_cause_codes():
    df = pd.DataFrame([_row(CAUSA="6"), _row(CAUSA="4")])

    result = normalize_susep_claims(df)

    assert result.iloc[0]["vehicle_type"] == "incendio"
    assert result.iloc[1]["vehicle_type"] == "colisao_parcial"


def test_malformed_date_becomes_nat():
    df = pd.DataFrame([_row(D_OCORR="00000000")])

    result = normalize_susep_claims(df)

    assert pd.isna(result.iloc[0]["event_timestamp"])


def test_each_row_gets_a_unique_claim_id():
    df = pd.DataFrame([_row(), _row()])

    result = normalize_susep_claims(df)

    assert result.iloc[0]["claim_id"] != result.iloc[1]["claim_id"]
