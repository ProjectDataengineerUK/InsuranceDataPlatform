import random
from datetime import UTC, datetime, timedelta

import pandas as pd

from src.ingestion.producer.datasets.base_loader import read_csv_safely
from src.ingestion.producer.datasets.susep_loader import normalize_susep_claims

CONSENT_INSTITUTIONS = ["insurer_a", "insurer_b", "insurer_c"]

# Fração dos policy_id sorteados que também recebem uma revogação (além da
# concessão) — dá variedade de histórico pro SCD2 sem exigir coordenação
# externa (sample_seed=42 fixo, mesmo espírito de regulatory_feeds.py).
REVOKED_RATE = 0.2
SAMPLE_SEED = 42


def load_consent_events(csv_path: str, source_config: dict) -> pd.DataFrame:
    # Reaproveita o mesmo CSV real da SUSEP (mesmo dataset_url/dataset_path
    # do source susep) só pra ter um pool de policy_id que também aparece em
    # gold.claims — sem isso, o consentimento nunca casaria com nenhum
    # sinistro real na view final (ver DESIGN_OPEN_INSURANCE.md, Decision 6).
    sample_rows = source_config.get("sample_rows")
    raw_df = read_csv_safely(csv_path, encoding="latin-1", sep=";", sample_rows=sample_rows)
    claims_df = normalize_susep_claims(raw_df)

    policy_ids = claims_df["policy_id"].dropna().unique().tolist()
    sample_size = min(source_config.get("sample_size", 200), len(policy_ids))

    rng = random.Random(SAMPLE_SEED)
    sampled_policy_ids = rng.sample(policy_ids, sample_size)

    now = datetime.now(UTC)
    rows = []
    for policy_id in sampled_policy_ids:
        institution = rng.choice(CONSENT_INSTITUTIONS)
        granted_at = now - timedelta(minutes=rng.randint(1, 10_000))
        rows.append(
            {
                "policy_id": policy_id,
                "consent_status": "GRANTED",
                "target_institution": institution,
                "scope": "claims,profile",
                "event_timestamp": granted_at,
            }
        )
        if rng.random() < REVOKED_RATE:
            rows.append(
                {
                    "policy_id": policy_id,
                    "consent_status": "REVOKED",
                    "target_institution": institution,
                    "scope": "claims,profile",
                    "event_timestamp": granted_at + timedelta(minutes=rng.randint(1, 1000)),
                }
            )

    return pd.DataFrame(rows)
