import argparse
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

CONSENT_VIEW_SQL = """
CREATE OR REPLACE VIEW {consent_view_table} AS
SELECT policy_id, consent_status, target_institution, scope, valid_from
FROM {consent_silver_table}
WHERE is_current = true
"""

# LEFT JOIN com claims: cliente com consentimento ativo mas sem nenhum
# sinistro ainda aparece (colunas de claims nulas) — um histórico limpo é
# dado tão relevante quanto um com sinistros (ver DESIGN_OPEN_INSURANCE.md,
# Decision 6).
SHAREABLE_VIEW_SQL = """
CREATE OR REPLACE VIEW {shareable_view_table} AS
SELECT
    consent.policy_id,
    consent.target_institution,
    consent.scope,
    profile.synthetic_name,
    profile.synthetic_age,
    profile.risk_score,
    claims.claim_id,
    claims.event_type,
    claims.amount,
    claims.region
FROM {consent_view_table} consent
JOIN {profile_table} profile
    ON consent.policy_id = profile.policy_id
LEFT JOIN {claims_table} claims
    ON consent.policy_id = claims.policy_id
WHERE consent.consent_status = 'GRANTED'
"""


def run_shareable_view(
    consent_silver_table: str,
    profile_table: str,
    claims_table: str,
    consent_view_table: str,
    shareable_view_table: str,
) -> None:
    spark = get_spark("open-insurance-shareable-view")

    # Nas primeiras execuções (antes do primeiro evento de consentimento ou
    # do primeiro run do profile_generator), as tabelas base ainda podem não
    # existir — CREATE VIEW sobre uma tabela ausente falha, então adia
    # silenciosamente até a próxima execução agendada, mesmo espírito de
    # degradação graciosa já usado em outros pontos da plataforma.
    if not spark.catalog.tableExists(consent_silver_table):
        return

    spark.sql(
        CONSENT_VIEW_SQL.format(
            consent_view_table=consent_view_table,
            consent_silver_table=consent_silver_table,
        )
    )

    if not spark.catalog.tableExists(profile_table) or not spark.catalog.tableExists(
        claims_table
    ):
        return

    spark.sql(
        SHAREABLE_VIEW_SQL.format(
            shareable_view_table=shareable_view_table,
            consent_view_table=consent_view_table,
            profile_table=profile_table,
            claims_table=claims_table,
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--consent-silver-table", required=True)
    parser.add_argument("--profile-table", required=True)
    parser.add_argument("--claims-table", required=True)
    parser.add_argument("--consent-view-table", required=True)
    parser.add_argument("--shareable-view-table", required=True)
    args = parser.parse_args()

    run_shareable_view(
        args.consent_silver_table,
        args.profile_table,
        args.claims_table,
        args.consent_view_table,
        args.shareable_view_table,
    )


if __name__ == "__main__":
    main()
