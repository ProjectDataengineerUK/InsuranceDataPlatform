import hashlib
import random

import pandas as pd

from src.ingestion.producer.datasets.base_loader import read_csv_safely
from src.ingestion.producer.datasets.susep_loader import CAUSE_LABELS

# Taxas de injeção de defeito/discrepância (nomeadas, não magic numbers) —
# cada fonte usa sua própria seed (ver _build_source_feed) pra que os padrões
# de defeito das 3 fontes não coincidam, dando trabalho de verdade pra
# reconciliação (src/regulatory/reconcile.py).
MISSING_REQUIRED_FIELD_RATE = 0.05
INVALID_CODE_RATE = 0.03
AMOUNT_DISCREPANCY_RATE = 0.05
AMOUNT_DISCREPANCY_MAX_PCT = 0.15
MISSING_IN_SOURCE_RATE = 0.02

# Garantidamente fora do allow-list de src/regulatory/standardize.py (que usa
# CAUSE_LABELS.keys() como allowed_values em check_allowed_values).
_INVALID_CAUSE_CODE = max(CAUSE_LABELS.keys()) + 90
_INVALID_REGION_CODE = "ZZ"


def _external_reference_id(row: pd.Series) -> str:
    # Calculado a partir da linha pristina (antes de qualquer injeção de
    # defeito) — garante que as 3 fontes derivem o mesmo id pro "mesmo"
    # sinistro, e que esse id nunca seja corrompido pelas etapas abaixo.
    natural_key = f"{row['COD_APO']}|{row['D_OCORR']}|{row['INDENIZ']}|{row['REGIAO']}|{row['CAUSA']}"
    return hashlib.sha256(natural_key.encode("utf-8")).hexdigest()[:24]


def _load_base_sample(csv_path: str, source_config: dict) -> pd.DataFrame:
    sample_rows = source_config.get("sample_rows")
    # Mesmo csv_path/sample_rows/random_seed (default=42) do source susep ->
    # df.sample() em base_loader.read_csv_safely é determinístico, então as
    # 3 fontes fictícias pegam exatamente o mesmo subconjunto de sinistros
    # reais, sem nenhuma coordenação extra entre threads.
    df = read_csv_safely(csv_path, encoding="latin-1", sep=";", sample_rows=sample_rows)
    df = df.dropna(subset=["COD_APO", "D_OCORR", "INDENIZ", "REGIAO", "CAUSA"]).reset_index(
        drop=True
    )
    df["external_reference_id"] = df.apply(_external_reference_id, axis=1)
    return df


def _format_currency_ptbr(amount: float) -> str:
    return f"R$ {amount:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")


def _build_source_feed(
    base_df: pd.DataFrame,
    source_system: str,
    layout: str,
    rng_seed: int,
) -> pd.DataFrame:
    rng = random.Random(rng_seed)
    rows = []

    for _, row in base_df.iterrows():
        if rng.random() < MISSING_IN_SOURCE_RATE:
            continue

        amount = float(row["INDENIZ"])
        if rng.random() < AMOUNT_DISCREPANCY_RATE:
            perturbation = rng.uniform(-AMOUNT_DISCREPANCY_MAX_PCT, AMOUNT_DISCREPANCY_MAX_PCT)
            amount = round(amount * (1 + perturbation), 2)

        cause_code = int(row["CAUSA"]) if pd.notna(row["CAUSA"]) else _INVALID_CAUSE_CODE
        if rng.random() < INVALID_CODE_RATE:
            cause_code = _INVALID_CAUSE_CODE

        region = str(row["REGIAO"])
        if rng.random() < INVALID_CODE_RATE:
            region = _INVALID_REGION_CODE

        policy_id = str(row["COD_APO"])
        event_date_raw = str(row["D_OCORR"])
        drop_field = rng.random() < MISSING_REQUIRED_FIELD_RATE
        dropped_field = rng.choice(["policy_id", "event_date", "amount"]) if drop_field else None

        record = {
            "external_reference_id": row["external_reference_id"],
            "source_system": source_system,
        }

        if layout == "insurer_a":
            event_date = pd.to_datetime(event_date_raw, format="%Y%m%d", errors="coerce")
            record.update(
                {
                    "numero_apolice": None if dropped_field == "policy_id" else policy_id,
                    "data_ocorrencia": (
                        None
                        if dropped_field == "event_date" or pd.isna(event_date)
                        else event_date.strftime("%d/%m/%Y")
                    ),
                    "valor_indenizacao": (
                        None if dropped_field == "amount" else _format_currency_ptbr(amount)
                    ),
                    "regiao_sinistro": region,
                    "codigo_causa": str(cause_code),
                }
            )
        elif layout == "insurer_b":
            event_date = pd.to_datetime(event_date_raw, format="%Y%m%d", errors="coerce")
            record.update(
                {
                    "POLICY_NUM": None if dropped_field == "policy_id" else policy_id,
                    "EVENT_DATE": (
                        None
                        if dropped_field == "event_date" or pd.isna(event_date)
                        else event_date.strftime("%Y-%m-%d")
                    ),
                    "CLAIM_AMOUNT": None if dropped_field == "amount" else amount,
                    "REGION_CODE": region,
                    "CAUSE_CD": str(cause_code),
                }
            )
        else:  # insurer_c
            event_date = pd.to_datetime(event_date_raw, format="%Y%m%d", errors="coerce")
            epoch_millis = None if pd.isna(event_date) else int(event_date.timestamp() * 1000)
            record.update(
                {
                    "policyId": None if dropped_field == "policy_id" else policy_id,
                    "occurrenceDate": (
                        None if dropped_field == "event_date" else epoch_millis
                    ),
                    "amountCents": (
                        None if dropped_field == "amount" else int(round(amount * 100))
                    ),
                    "regionCode": region,
                    "causeCode": str(cause_code),
                }
            )

        rows.append(record)

    return pd.DataFrame(rows)


def load_insurer_a_events(csv_path: str, source_config: dict) -> pd.DataFrame:
    base_df = _load_base_sample(csv_path, source_config)
    return _build_source_feed(base_df, "insurer_a", "insurer_a", rng_seed=101)


def load_insurer_b_events(csv_path: str, source_config: dict) -> pd.DataFrame:
    base_df = _load_base_sample(csv_path, source_config)
    return _build_source_feed(base_df, "insurer_b", "insurer_b", rng_seed=202)


def load_insurer_c_events(csv_path: str, source_config: dict) -> pd.DataFrame:
    base_df = _load_base_sample(csv_path, source_config)
    return _build_source_feed(base_df, "insurer_c", "insurer_c", rng_seed=303)
