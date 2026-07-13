import os
from dataclasses import dataclass


@dataclass
class SqlQuery:
    sql: str
    params: dict


def build_susep_claims_query(compliant: bool, row_limit: int = 200) -> SqlQuery:
    # susep_compliant/compliance_issues são calculados por contrato em
    # src/regulatory/gold_susep_export.py::build_gold_susep_claims — não é
    # uma checagem refeita aqui, só uma leitura do resultado já persistido.
    return SqlQuery(
        sql=(
            "SELECT COD_APO, D_OCORR, INDENIZ, REGIAO, CAUSA, compliance_issues "
            "FROM gold.regulatory_susep_claims WHERE susep_compliant = :compliant "
            "ORDER BY D_OCORR DESC LIMIT :row_limit"
        ),
        params={"compliant": compliant, "row_limit": row_limit},
    )


def build_susep_compliance_summary_query() -> SqlQuery:
    return SqlQuery(
        sql=(
            "SELECT susep_compliant, COUNT(*) AS total "
            "FROM gold.regulatory_susep_claims GROUP BY susep_compliant"
        ),
        params={},
    )


def build_source_system_volume_query() -> SqlQuery:
    # Volume de sinistros reportado por cada banco/seguradora fictício (ver
    # src/ingestion/producer/datasets/regulatory_feeds.py) — proxy simples de
    # "bancos x seguradoras"; discrepância por banco individual exigiria
    # explodir sources_reporting em monitoring._regulatory_reconciliation_results,
    # deixado como melhoria futura.
    return SqlQuery(
        sql=(
            "SELECT source_system, COUNT(*) AS total_sinistros "
            "FROM silver.regulatory_claims GROUP BY source_system ORDER BY total_sinistros DESC"
        ),
        params={},
    )


def build_fraud_probability_query(row_limit: int = 100) -> SqlQuery:
    # fraud_score é a heurística real que decide fraud_flag/auto_approved
    # (streaming_score.py); model_fraud_score é o shadow score do modelo
    # mlflow, só observabilidade — nunca decide nada sozinho (ver
    # docs/ARCHITECTURE.md, "Shadow scoring do modelo campeão").
    return SqlQuery(
        sql=(
            "SELECT claim_id, policy_id, region, amount, fraud_score, fraud_flag, "
            "model_fraud_score, auto_approved, event_timestamp "
            "FROM gold.claims ORDER BY fraud_score DESC LIMIT :row_limit"
        ),
        params={"row_limit": row_limit},
    )


def build_sla_breach_query(sla_hours: int = 24, row_limit: int = 100) -> SqlQuery:
    # Mesma regra de src/monitoring/sla_alerts.py::find_sla_breaches — sinistros
    # não auto-aprovados aguardando revisão manual há mais que sla_hours.
    return SqlQuery(
        sql=(
            "SELECT claim_id, policy_id, region, amount, event_timestamp, "
            "(unix_timestamp(current_timestamp()) - unix_timestamp(event_timestamp)) / 3600 "
            "AS age_hours FROM gold.claims WHERE NOT auto_approved "
            "AND (unix_timestamp(current_timestamp()) - unix_timestamp(event_timestamp)) / 3600 "
            "> :sla_hours ORDER BY age_hours DESC LIMIT :row_limit"
        ),
        params={"sla_hours": sla_hours, "row_limit": row_limit},
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


def build_dq_results_query(row_limit: int = 100) -> SqlQuery:
    return SqlQuery(
        sql=(
            "SELECT check_name, table_name, passed, failed_rows, _checked_at "
            "FROM monitoring._dq_results ORDER BY _checked_at DESC LIMIT :row_limit"
        ),
        params={"row_limit": row_limit},
    )


def build_model_drift_query(row_limit: int = 50) -> SqlQuery:
    return SqlQuery(
        sql=(
            "SELECT feature_name, baseline_mean, current_mean, drift_ratio, "
            "drift_detected, window_hours, _checked_at FROM monitoring._model_drift_results "
            "ORDER BY _checked_at DESC LIMIT :row_limit"
        ),
        params={"row_limit": row_limit},
    )


def build_pipeline_latency_query(row_limit: int = 50) -> SqlQuery:
    return SqlQuery(
        sql=(
            "SELECT metric_name, details_json, within_sla, _checked_at "
            "FROM monitoring._pipeline_latency_results ORDER BY _checked_at DESC LIMIT :row_limit"
        ),
        params={"row_limit": row_limit},
    )


def build_cost_usage_by_product_query(lookback_days: int = 30) -> SqlQuery:
    # system.billing.usage é uma system table nativa do Databricks (Unity
    # Catalog) — precisa estar habilitada pelo account admin do workspace
    # (Settings -> Feature enablement -> System tables); pode não existir
    # neste workspace trial. A página que chama isso trata falha como
    # "não habilitado", não como erro de código (ver pages/custos.py).
    return SqlQuery(
        sql=(
            "SELECT billing_origin_product, usage_date, SUM(usage_quantity) AS usage_quantity "
            "FROM system.billing.usage "
            "WHERE usage_date >= date_sub(current_date(), :lookback_days) "
            "GROUP BY billing_origin_product, usage_date "
            "ORDER BY usage_date DESC, usage_quantity DESC"
        ),
        params={"lookback_days": lookback_days},
    )


def build_cost_usage_by_job_query(lookback_days: int = 30, row_limit: int = 20) -> SqlQuery:
    return SqlQuery(
        sql=(
            "SELECT usage_metadata.job_id AS job_id, SUM(usage_quantity) AS usage_quantity "
            "FROM system.billing.usage "
            "WHERE usage_date >= date_sub(current_date(), :lookback_days) "
            "AND usage_metadata.job_id IS NOT NULL "
            "GROUP BY usage_metadata.job_id ORDER BY usage_quantity DESC LIMIT :row_limit"
        ),
        params={"lookback_days": lookback_days, "row_limit": row_limit},
    )


def build_table_freshness_query(table: str, timestamp_column: str) -> SqlQuery:
    # table/timestamp_column vêm sempre de constantes internas (ver
    # pages/status.py), nunca de input do usuário — não passar essa função
    # com valores vindos de formulário sem validar antes, já que nomes de
    # tabela/coluna não podem ser bind params.
    return SqlQuery(sql=f"SELECT MAX({timestamp_column}) AS latest_ts FROM {table}", params={})


def build_quarantine_rate_query(bronze_table: str) -> SqlQuery:
    # bronze_table vem sempre de constante interna (ver pages/performance.py),
    # nunca de input do usuário — mesma ressalva de build_table_freshness_query.
    return SqlQuery(
        sql=(
            f"SELECT (SELECT COUNT(*) FROM {bronze_table}) AS valid_count, "
            f"(SELECT COUNT(*) FROM {bronze_table}_quarantine) AS quarantine_count"
        ),
        params={},
    )


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
        catalog=os.environ["CATALOG"],
    )


def run_query(connection, query: SqlQuery) -> list[dict]:
    with connection.cursor() as cursor:
        cursor.execute(query.sql, query.params)
        columns = [description[0] for description in cursor.description]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
