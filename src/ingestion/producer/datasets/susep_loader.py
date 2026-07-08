import uuid
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from src.ingestion.producer.datasets.base_loader import read_csv_safely

SUSEP_OPEN_DATA_PORTAL = "https://www.gov.br/susep/pt-br/acesso-a-informacao/dados-abertos"

# O dataset público da SUSEP (sistema AUTOSEG) é AGREGADO, não um registro por
# sinistro: cada linha representa contagens/somas (EXPOSICAO, PREMIO, FREQ_SIN1..9,
# INDENIZ1..9) para um grupamento de categoria tarifária/região/modelo/ano/sexo/
# faixa etária — confirmado no PDF oficial "DEFINICOES_AUTOSEG.pdf" da SUSEP.
# Por isso geramos sinistros sintéticos por linha, calibrados nessas distribuições
# reais (frequência e indenização média por grupo), em vez de fazer replay direto.
# Resolva a URL do CSV mais recente em: SUSEP_OPEN_DATA_PORTAL /
# src/ingestion/producer/config.yaml (sources.susep.dataset_url).

COVERAGE_LABELS = {
    "1": "roubo_furto",
    "2": "colisao_parcial",
    "3": "colisao_perda_total",
    "4": "incendio",
    "9": "outras_coberturas",
}

# Nomes de coluna candidatos para as chaves de agrupamento — não confirmados no
# dicionário oficial consultado (que documentava só as colunas de medida). Ajustar
# assim que o CSV real for inspecionado.
REGION_COLUMN_CANDIDATES = ["REGIAO", "regiao", "REGIAO_CIRCULACAO", "UF", "uf"]

AMOUNT_LOG_STDDEV = 0.4  # dispersão em torno da indenização média do grupo


def _find_region(row: pd.Series) -> str:
    for candidate in REGION_COLUMN_CANDIDATES:
        if candidate in row and pd.notna(row[candidate]):
            return str(row[candidate])
    return "UNKNOWN"


def _random_timestamp(start: datetime, end: datetime, rng: np.random.Generator) -> datetime:
    span_seconds = max(int((end - start).total_seconds()), 1)
    offset = int(rng.integers(0, span_seconds))
    return start + timedelta(seconds=offset)


def generate_synthetic_claims(
    aggregates_df: pd.DataFrame,
    reference_period_start: datetime,
    reference_period_end: datetime,
    random_seed: int = 42,
) -> pd.DataFrame:
    rng = np.random.default_rng(random_seed)
    synthetic_rows = []

    for _, row in aggregates_df.iterrows():
        region = _find_region(row)

        for suffix, label in COVERAGE_LABELS.items():
            freq_col = f"FREQ_SIN{suffix}"
            indeniz_col = f"INDENIZ{suffix}"
            if freq_col not in row or indeniz_col not in row:
                continue

            frequency = int(row[freq_col]) if pd.notna(row[freq_col]) else 0
            total_indeniz = float(row[indeniz_col]) if pd.notna(row[indeniz_col]) else 0.0
            if frequency <= 0 or total_indeniz <= 0:
                continue

            avg_amount = total_indeniz / frequency
            sampled_amounts = rng.lognormal(
                mean=np.log(max(avg_amount, 1.0)), sigma=AMOUNT_LOG_STDDEV, size=frequency
            )

            for amount in sampled_amounts:
                synthetic_rows.append(
                    {
                        "claim_id": str(uuid.uuid4()),
                        "policy_id": None,
                        "customer_id": None,
                        "event_type": "claim-opened",
                        "event_timestamp": _random_timestamp(
                            reference_period_start, reference_period_end, rng
                        ),
                        "amount": round(float(amount), 2),
                        "region": region,
                        "vehicle_type": label,
                        "source": "susep_synthetic",
                    }
                )

    return pd.DataFrame(synthetic_rows)


def load_claim_events(csv_path: str, source_config: dict) -> pd.DataFrame:
    reference_period_start = datetime.fromisoformat(source_config["reference_period_start"])
    reference_period_end = datetime.fromisoformat(source_config["reference_period_end"])

    aggregates_df = read_csv_safely(csv_path)
    return generate_synthetic_claims(aggregates_df, reference_period_start, reference_period_end)
