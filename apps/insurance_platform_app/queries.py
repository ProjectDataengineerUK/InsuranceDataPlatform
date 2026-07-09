import os
from dataclasses import dataclass


@dataclass
class SqlQuery:
    sql: str
    params: dict


def build_claim_lookup_query(claim_id: str) -> SqlQuery:
    return SqlQuery(
        sql="SELECT * FROM gold.claims WHERE claim_id = :claim_id",
        params={"claim_id": claim_id},
    )


def build_policy_lookup_query(policy_id: str) -> SqlQuery:
    return SqlQuery(
        sql="SELECT * FROM gold.claims WHERE policy_id = :policy_id ORDER BY event_timestamp DESC",
        params={"policy_id": policy_id},
    )


def build_customer_lookup_query(customer_id: str) -> SqlQuery:
    # customer_id é sempre nulo em gold.claims neste repo — nem SUSEP nem ANS
    # expõem um identificador de cliente reutilizável (ver docs/ARCHITECTURE.md,
    # "Natureza real dos datasets públicos"). Esta query existe pra manter a
    # tela funcional caso uma fonte complementar seja adicionada no futuro; a
    # página Streamlit correspondente avisa essa limitação em vez de fingir
    # que a busca funciona.
    return SqlQuery(
        sql="SELECT * FROM gold.claims WHERE customer_id = :customer_id",
        params={"customer_id": customer_id},
    )


def build_source_system_search_query(source_system: str) -> SqlQuery:
    # Bancos/seguradoras ficam em silver.regulatory_claims (via source_system)
    # — gold.regulatory_susep_claims já é agregado por external_reference_id
    # e não retém essa coluna (ver src/regulatory/gold_susep_export.py).
    return SqlQuery(
        sql=(
            "SELECT * FROM silver.regulatory_claims "
            "WHERE source_system = :source_system ORDER BY event_timestamp DESC"
        ),
        params={"source_system": source_system},
    )


def build_dq_summary_query(row_limit: int = 50) -> SqlQuery:
    return SqlQuery(
        sql="SELECT * FROM gold.regulatory_dq_summary ORDER BY _generated_at DESC LIMIT :row_limit",
        params={"row_limit": row_limit},
    )


def build_reconciliation_query(row_limit: int = 50) -> SqlQuery:
    return SqlQuery(
        sql=(
            "SELECT * FROM monitoring._regulatory_reconciliation_results "
            "ORDER BY detected_at DESC LIMIT :row_limit"
        ),
        params={"row_limit": row_limit},
    )


def build_pipeline_health_query(row_limit: int = 50) -> SqlQuery:
    return SqlQuery(
        sql="SELECT * FROM monitoring._pipeline_latency_results ORDER BY _checked_at DESC LIMIT :row_limit",
        params={"row_limit": row_limit},
    )


def build_table_freshness_query(table: str, timestamp_column: str) -> SqlQuery:
    # table/timestamp_column vêm sempre de constantes internas (ver
    # pages/status.py), nunca de input do usuário — não passar essa função
    # com valores vindos de formulário sem validar antes, já que nomes de
    # tabela/coluna não podem ser bind params.
    return SqlQuery(sql=f"SELECT MAX({timestamp_column}) AS latest_ts FROM {table}", params={})


def get_connection():
    # Único ponto que toca o Databricks SQL Connector de verdade — não coberto
    # por unit test (mesma limitação já documentada neste repo: warehouse real
    # não existe neste sandbox). Padrão oficial de auth pra Databricks Apps:
    # Config() lê DATABRICKS_CLIENT_ID/SECRET injetados pelo runtime do app,
    # sem código diferente entre rodar localmente (OAuth U2M) e em produção
    # (service principal do app).
    from databricks import sql as databricks_sql
    from databricks.sdk.core import Config

    cfg = Config()
    return databricks_sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.environ['WAREHOUSE_ID']}",
        credentials_provider=lambda: cfg.authenticate,
    )


def run_query(connection, query: SqlQuery) -> list[dict]:
    with connection.cursor() as cursor:
        cursor.execute(query.sql, query.params)
        columns = [description[0] for description in cursor.description]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
