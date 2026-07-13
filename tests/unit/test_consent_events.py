import pandas as pd

from src.ingestion.producer.datasets.consent_events import (
    CONSENT_INSTITUTIONS,
    load_consent_events,
)

SOURCE_CONFIG = {"sample_rows": None, "sample_size": 2}


def _write_base_csv(tmp_path):
    csv_path = tmp_path / "susep_sinistros.csv"
    df = pd.DataFrame(
        [
            {
                "COD_APO": "287756959",
                "D_OCORR": "20200504",
                "INDENIZ": "520.0",
                "REGIAO": "21",
                "CAUSA": "9",
            },
            {
                "COD_APO": "111222333",
                "D_OCORR": "20200601",
                "INDENIZ": "1500.0",
                "REGIAO": "35",
                "CAUSA": "6",
            },
        ]
    )
    df.to_csv(csv_path, sep=";", encoding="latin-1", index=False)
    return str(csv_path)


def test_load_consent_events_uses_policy_ids_from_claims(tmp_path):
    csv_path = _write_base_csv(tmp_path)

    events = load_consent_events(csv_path, SOURCE_CONFIG)

    assert set(events["policy_id"]) == {"287756959", "111222333"}
    assert events["consent_status"].isin(["GRANTED", "REVOKED"]).all()
    assert events["target_institution"].isin(CONSENT_INSTITUTIONS).all()


def test_load_consent_events_every_policy_has_a_granted_event(tmp_path):
    csv_path = _write_base_csv(tmp_path)

    events = load_consent_events(csv_path, SOURCE_CONFIG)
    granted = events[events["consent_status"] == "GRANTED"]

    assert set(granted["policy_id"]) == {"287756959", "111222333"}


def test_load_consent_events_is_deterministic(tmp_path):
    # event_timestamp é relativo ao "agora" da chamada, então não entra na
    # comparação — o que precisa ser determinístico é a seleção/atribuição de
    # policy_id/consent_status/target_institution (seed fixo).
    csv_path = _write_base_csv(tmp_path)

    columns = ["policy_id", "consent_status", "target_institution", "scope"]
    first = load_consent_events(csv_path, SOURCE_CONFIG)[columns]
    second = load_consent_events(csv_path, SOURCE_CONFIG)[columns]

    assert first.to_dict(orient="records") == second.to_dict(orient="records")
