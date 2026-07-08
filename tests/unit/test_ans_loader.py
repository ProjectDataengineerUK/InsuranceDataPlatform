import pandas as pd

from src.ingestion.producer.datasets.ans_loader import normalize_ans_policies


def test_inclusion_code_maps_to_policy_created():
    df = pd.DataFrame(
        [{"CD_OPERADORA": "123", "CD_PLANO_RPS": "P1", "DT_INCLUSAO": "2024-01-01", "ID_MOTIVO_MOVIMENTO": 11}]
    )

    result = normalize_ans_policies(df)

    assert result.iloc[0]["event_type"] == "policy-created"
    assert result.iloc[0]["policy_id"] == "P1"


def test_cancellation_code_maps_to_policy_cancelled():
    df = pd.DataFrame(
        [{"CD_OPERADORA": "123", "CD_PLANO_RPS": "P1", "DT_INCLUSAO": "2024-01-01", "ID_MOTIVO_MOVIMENTO": 44}]
    )

    result = normalize_ans_policies(df)

    assert result.iloc[0]["event_type"] == "policy-cancelled"


def test_unrecognized_code_maps_to_policy_updated():
    df = pd.DataFrame(
        [{"CD_OPERADORA": "123", "CD_PLANO_RPS": "P1", "DT_INCLUSAO": "2024-01-01", "ID_MOTIVO_MOVIMENTO": 21}]
    )

    result = normalize_ans_policies(df)

    assert result.iloc[0]["event_type"] == "policy-updated"


def test_missing_policy_id_generates_fallback_uuid():
    df = pd.DataFrame(
        [{"CD_OPERADORA": "123", "DT_INCLUSAO": "2024-01-01", "ID_MOTIVO_MOVIMENTO": 11}]
    )

    result = normalize_ans_policies(df)

    assert result.iloc[0]["policy_id"]
    assert result.iloc[0]["customer_id"] is None
