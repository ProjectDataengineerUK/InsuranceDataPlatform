import argparse
import hashlib
import random
import sys
from pathlib import Path

# Job roda como spark_python_task via workspace files (sem empacotamento em
# wheel) — Databricks só põe o diretório do próprio script no sys.path, não a
# raiz do bundle. Sem isso, "from src..." abaixo falha com ModuleNotFoundError.
# Databricks executa o job via exec(compile(source, filename, 'exec')), que
# nao injeta __file__ nos globals — cai pro co_filename do frame atual.
_this_file = globals().get("__file__") or sys._getframe().f_code.co_filename
sys.path.insert(0, str(Path(_this_file).resolve().parents[2]))

from src.common.spark_session import get_spark

FIRST_NAMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fábio", "Gabriela", "Heitor"]
LAST_NAMES = ["Silva", "Souza", "Costa", "Oliveira", "Pereira", "Santos", "Almeida"]

MIN_SYNTHETIC_AGE = 18
MAX_SYNTHETIC_AGE = 80


def _seed_for(policy_id: str) -> int:
    return int(hashlib.sha256(policy_id.encode("utf-8")).hexdigest(), 16) % (2**32)


def generate_profile(policy_id: str) -> dict:
    rng = random.Random(_seed_for(policy_id))
    return {
        "policy_id": policy_id,
        "synthetic_name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
        "synthetic_age": rng.randint(MIN_SYNTHETIC_AGE, MAX_SYNTHETIC_AGE),
        "risk_score": round(rng.uniform(0.0, 1.0), 4),
    }


def run_profile_generator(claims_table: str, profile_table: str) -> None:
    spark = get_spark("open-insurance-profile-generator")
    claims_df = spark.read.table(claims_table)

    # policy_id, não customer_id: gold.claims.customer_id é sempre NULL neste
    # projeto (dados reais anonimizados, ver src/common/schemas.py).
    policy_ids = [
        row["policy_id"]
        for row in claims_df.select("policy_id").distinct().collect()
        if row["policy_id"] is not None
    ]

    profiles = [generate_profile(policy_id) for policy_id in policy_ids]
    if not profiles:
        return

    profile_df = spark.createDataFrame(profiles)
    profile_df.write.format("delta").mode("overwrite").saveAsTable(profile_table)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--claims-table", required=True)
    parser.add_argument("--profile-table", required=True)
    args = parser.parse_args()

    run_profile_generator(args.claims_table, args.profile_table)


if __name__ == "__main__":
    main()
