import uuid

import pandas as pd

from src.ingestion.producer.datasets.base_loader import apply_column_mapping, read_csv_safely

ANS_OPEN_DATA_PORTAL = "https://dadosabertos.ans.gov.br/FTP/PDA/dados_de_beneficiarios_por_operadora/"

# Confirmado no dicionário oficial da ANS (dicionario_de_dados_sib.ods, pasta
# dados_de_beneficiarios_por_operadora): cada linha é um evento real de
# movimentação cadastral de beneficiário (inclusão, cancelamento, reativação,
# retificação), não um agregado — ao contrário da SUSEP. `ID_MOTIVO_MOVIMENTO`
# identifica o tipo de movimento; usamos isso para inferir o event_type.
COLUMN_MAPPING = {
    "CD_OPERADORA": "operator_code",
    "CD_PLANO_RPS": "policy_id",
    "CD_PLANO_SCPA": "policy_id_legacy",
    "DT_INCLUSAO": "event_timestamp",
    "ID_MOTIVO_MOVIMENTO": "movement_reason_code",
    "TP_SEXO": "sex",
}

# Códigos confirmados no dicionário oficial (ID_MOTIVO_MOVIMENTO).
INCLUSION_CODES = {11, 12, 13, 14, 16, 17, 71}
CANCELLATION_CODES = {41, 42, 43, 44, 45, 46, 47, 48, 72, 73, 74}


def _infer_event_type(reason_code: float) -> str:
    if pd.isna(reason_code):
        return "policy-updated"
    code = int(reason_code)
    if code in INCLUSION_CODES:
        return "policy-created"
    if code in CANCELLATION_CODES:
        return "policy-cancelled"
    return "policy-updated"


def normalize_ans_policies(df: pd.DataFrame) -> pd.DataFrame:
    normalized = apply_column_mapping(df, COLUMN_MAPPING)

    if "policy_id" not in normalized.columns:
        normalized["policy_id"] = normalized.get("policy_id_legacy")
    normalized["policy_id"] = normalized["policy_id"].apply(
        lambda value: str(value) if pd.notna(value) else str(uuid.uuid4())
    )

    if "movement_reason_code" in normalized.columns:
        normalized["event_type"] = normalized["movement_reason_code"].apply(_infer_event_type)
    else:
        normalized["event_type"] = "policy-updated"

    # A ANS não expõe um identificador de beneficiário reutilizável (anonimização
    # LGPD) — customer_id, premium_amount, coverage_type e region não estão
    # disponíveis neste dataset e ficam null até uma fonte complementar ser
    # identificada.
    normalized["customer_id"] = None
    normalized["premium_amount"] = None
    normalized["coverage_type"] = None
    normalized["region"] = None
    normalized["source"] = "ans"

    return normalized.drop(columns=["policy_id_legacy", "movement_reason_code"], errors="ignore")


def load_policy_events(csv_path: str, source_config: dict) -> pd.DataFrame:
    del source_config  # não utilizado — mantido para uma interface uniforme com load_claim_events
    raw_df = read_csv_safely(csv_path)
    return normalize_ans_policies(raw_df)
