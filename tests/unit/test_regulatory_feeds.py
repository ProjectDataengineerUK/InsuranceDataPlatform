import pandas as pd

from src.ingestion.producer.datasets.regulatory_feeds import (
    load_insurer_a_events,
    load_insurer_b_events,
    load_insurer_c_events,
)

SOURCE_CONFIG = {"sample_rows": None}


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


def test_shared_external_reference_id_matches_across_sources(tmp_path):
    csv_path = _write_base_csv(tmp_path)

    a = load_insurer_a_events(csv_path, SOURCE_CONFIG)
    b = load_insurer_b_events(csv_path, SOURCE_CONFIG)
    c = load_insurer_c_events(csv_path, SOURCE_CONFIG)

    ids_a = set(a["external_reference_id"])
    ids_b = set(b["external_reference_id"])
    ids_c = set(c["external_reference_id"])

    # As 3 fontes derivam do mesmo sample determinístico -> mesmo conjunto de
    # ids na ausência de linhas dropadas por MISSING_IN_SOURCE_RATE.
    assert ids_a & ids_b
    assert ids_a & ids_c


def test_each_source_uses_its_own_layout(tmp_path):
    csv_path = _write_base_csv(tmp_path)

    a = load_insurer_a_events(csv_path, SOURCE_CONFIG)
    b = load_insurer_b_events(csv_path, SOURCE_CONFIG)
    c = load_insurer_c_events(csv_path, SOURCE_CONFIG)

    assert {"numero_apolice", "data_ocorrencia", "valor_indenizacao"}.issubset(a.columns)
    assert {"POLICY_NUM", "EVENT_DATE", "CLAIM_AMOUNT"}.issubset(b.columns)
    assert {"policyId", "occurrenceDate", "amountCents"}.issubset(c.columns)

    # insurer_a: moeda em formato "R$ 1.234,56" quando o campo não foi dropado.
    valid_amounts = a["valor_indenizacao"].dropna()
    if len(valid_amounts) > 0:
        assert valid_amounts.iloc[0].startswith("R$")


def test_external_reference_id_never_null(tmp_path):
    csv_path = _write_base_csv(tmp_path)

    for loader in (load_insurer_a_events, load_insurer_b_events, load_insurer_c_events):
        events = loader(csv_path, SOURCE_CONFIG)
        assert events["external_reference_id"].notna().all()
        assert events["source_system"].notna().all()
