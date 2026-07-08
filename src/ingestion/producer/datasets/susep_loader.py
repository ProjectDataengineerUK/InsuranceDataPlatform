import uuid

import pandas as pd

from src.ingestion.producer.datasets.base_loader import apply_column_mapping, read_csv_safely

SUSEP_OPEN_DATA_PORTAL = "https://www.gov.br/susep/pt-br/central-de-conteudos/dados-estatisticos/bases-anonimizadas"

# Confirmado no layout oficial "Estrutura dos Arquivos R_AUTO e S_AUTO" (versão
# 12/2023, baixado de dadosabertos.susep.gov.br/dadosabertos/auto_apos_2006/
# Dados_AUTO.pdf): S_AUTO é MICRODADO real — um registro por sinistro avisado,
# já anonimizado (COD_APO/COD_ENDOSSO substituem apólice/endosso reais). Não é
# a base agregada do AUTOSEG; nenhuma geração sintética é necessária aqui.
COLUMN_MAPPING = {
    "COD_APO": "policy_id",
    "D_OCORR": "event_timestamp",
    "INDENIZ": "amount",
    "REGIAO": "region",
    "CAUSA": "cause_code",
}

# Tabela V do layout oficial (Códigos de causas de sinistros).
CAUSE_LABELS = {
    1: "roubo_furto",
    2: "roubo",
    3: "furto",
    4: "colisao_parcial",
    5: "colisao_indenizacao_integral",
    6: "incendio",
    7: "assistencia_24h",
    9: "outros",
}


def _decode_cause(cause_code: float) -> str:
    if pd.isna(cause_code):
        return "outros"
    return CAUSE_LABELS.get(int(cause_code), "outros")


def normalize_susep_claims(df: pd.DataFrame) -> pd.DataFrame:
    normalized = apply_column_mapping(df, COLUMN_MAPPING)

    normalized["claim_id"] = [str(uuid.uuid4()) for _ in range(len(normalized))]

    # D_OCORR vem no formato AAAAMMDD (ex: "20200504"); linhas com data
    # ausente/corrompida viram NaT e são isoladas como malformadas no Bronze.
    normalized["event_timestamp"] = pd.to_datetime(
        normalized["event_timestamp"].astype(str), format="%Y%m%d", errors="coerce"
    )

    normalized["amount"] = pd.to_numeric(normalized["amount"], errors="coerce")
    normalized["region"] = normalized["region"].astype(str)
    normalized["vehicle_type"] = normalized["cause_code"].apply(_decode_cause)

    normalized["policy_id"] = normalized["policy_id"].apply(
        lambda value: str(value) if pd.notna(value) else str(uuid.uuid4())
    )
    normalized["customer_id"] = None
    normalized["event_type"] = "claim-opened"
    normalized["source"] = "susep"

    return normalized.drop(columns=["cause_code"], errors="ignore")


def load_claim_events(csv_path: str, source_config: dict) -> pd.DataFrame:
    sample_rows = source_config.get("sample_rows")
    raw_df = read_csv_safely(csv_path, encoding="latin-1", sep=";", sample_rows=sample_rows)
    return normalize_susep_claims(raw_df)
