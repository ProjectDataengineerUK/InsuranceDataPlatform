import streamlit as st
from health import (
    ARCHITECTURE_NODES,
    STATUS_LABELS,
    UNKNOWN,
    build_architecture_dot,
    compliance_status,
    count_threshold_status,
    drift_status,
    freshness_status,
    table_exists_status,
)
from queries import (
    build_dq_results_query,
    build_model_drift_query,
    build_reconciliation_query,
    build_sla_breach_query,
    build_susep_compliance_summary_query,
    build_table_count_query,
    build_table_freshness_query,
    get_connection,
    run_query,
)

st.title("Insurance Regulatory Platform")
st.caption(
    "Arquitetura do projeto ponta a ponta — pipeline operacional, pipeline "
    "regulatório SUSEP, Open Insurance e MLOps de fraude — com status real "
    "calculado a partir das mesmas tabelas que as demais páginas consultam. "
    "🟢 saudável · 🟡 atenção (não bloqueia nada sozinho) · 🔴 fora do esperado · "
    "⚪ sem dado suficiente pra avaliar ainda."
)

try:
    connection = get_connection()
except Exception as exc:  # noqa: BLE001 — painel de status nunca deve derrubar o app
    st.error(f"Não foi possível conectar ao SQL Warehouse: {exc}")
    st.stop()

statuses: dict[str, str] = {}
details: dict[str, str] = {}


def _freshness_check(node_id: str, table: str, timestamp_column: str) -> None:
    try:
        rows = run_query(connection, build_table_freshness_query(table, timestamp_column))
        latest_ts = rows[0]["latest_ts"] if rows else None
        statuses[node_id] = freshness_status(latest_ts)
        details[node_id] = f"último registro: {latest_ts}" if latest_ts else "sem dados ainda"
    except Exception as exc:  # noqa: BLE001
        statuses[node_id] = UNKNOWN
        details[node_id] = "sem dados ainda" if "TABLE_OR_VIEW_NOT_FOUND" in str(exc) else str(exc)


_freshness_check("bronze_operational", "bronze.claims", "_ingested_at")
_freshness_check("silver_operational", "silver.claims", "_ingested_at")
_freshness_check("gold_fraud", "gold.claims", "_scored_at")
_freshness_check("bronze_regulatory", "bronze.regulatory_claims_raw", "_ingested_at")

# Kafka Producer não tem sinal SQL direto (roda fora do Databricks, via
# GitHub Actions) — Bronze fresco já implica producer+ingestão funcionando;
# tratado como "sem dado direto" em vez de simular uma checagem que não existe.
statuses["kafka_producer"] = UNKNOWN
details["kafka_producer"] = "sem checagem direta (roda fora do Databricks) — inferir via Bronze"

try:
    drift_rows = run_query(connection, build_model_drift_query(row_limit=20))
    statuses["fraud_model"] = UNKNOWN if not drift_rows else "green"
    details["fraud_model"] = "champion ativo" if drift_rows else "nenhum champion promovido ainda"

    statuses["model_drift"] = drift_status(drift_rows)
    detected = [row for row in drift_rows if row["drift_detected"]]
    details["model_drift"] = (
        f"{len(detected)} feature(s) com drift nas últimas {len(drift_rows)} checagens"
        if drift_rows
        else "aguardando o primeiro champion promovido"
    )
except Exception as exc:  # noqa: BLE001
    statuses["fraud_model"] = UNKNOWN
    statuses["model_drift"] = UNKNOWN
    # monitoring._model_drift_results só é criada quando model_drift_monitor
    # roda com uma baseline já escrita (train_model.py só escreve baseline
    # quando promove o 1º champion) — TABLE_OR_VIEW_NOT_FOUND aqui é "sem
    # champion ainda", não erro (mesmo tratamento de dataops_mlops_sentinel.py
    # e pipeline_monitoring.py, que faltava neste ponto específico).
    if "TABLE_OR_VIEW_NOT_FOUND" in str(exc):
        details["fraud_model"] = details["model_drift"] = "aguardando o primeiro champion promovido"
    else:
        details["fraud_model"] = details["model_drift"] = str(exc)

try:
    breach_rows = run_query(connection, build_sla_breach_query(sla_hours=24, row_limit=200))
    statuses["sla_breach"] = count_threshold_status(len(breach_rows), yellow_at=1, red_at=20)
    details["sla_breach"] = f"{len(breach_rows)} sinistro(s) aguardando revisão há mais de 24h"
except Exception as exc:  # noqa: BLE001
    statuses["sla_breach"] = UNKNOWN
    details["sla_breach"] = (
        "aguardando o primeiro sinistro pontuado (gold.claims ainda não existe)"
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc)
        else str(exc)
    )

try:
    reconciliation_rows = run_query(connection, build_reconciliation_query(row_limit=200))
    statuses["reconciliation"] = count_threshold_status(len(reconciliation_rows), yellow_at=1, red_at=10)
    details["reconciliation"] = f"{len(reconciliation_rows)} discrepância(s) recente(s) entre fontes"
except Exception as exc:  # noqa: BLE001
    statuses["reconciliation"] = UNKNOWN
    details["reconciliation"] = (
        "aguardando a primeira execução do pipeline regulatório"
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc)
        else str(exc)
    )

try:
    summary_rows = run_query(connection, build_susep_compliance_summary_query())
    compliant = sum(r["total"] for r in summary_rows if r["susep_compliant"])
    non_compliant = sum(r["total"] for r in summary_rows if not r["susep_compliant"])
    statuses["susep_compliance"] = compliance_status(compliant, non_compliant)
    total = compliant + non_compliant
    rate = f"{(compliant / total * 100):.1f}%" if total else "N/A"
    details["susep_compliance"] = f"{compliant}/{total} contratos aderentes ({rate})"
except Exception as exc:  # noqa: BLE001
    statuses["susep_compliance"] = UNKNOWN
    details["susep_compliance"] = (
        "aguardando a primeira execução do export SUSEP (gold.regulatory_susep_claims ainda não existe)"
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc)
        else str(exc)
    )

try:
    dq_rows = run_query(connection, build_dq_results_query(row_limit=100))
    failed = [row for row in dq_rows if not row["passed"]]
    statuses["dq_checks"] = count_threshold_status(len(failed), yellow_at=1, red_at=10)
    details["dq_checks"] = (
        f"{len(failed)} falha(s) nas últimas {len(dq_rows)} checagens" if dq_rows else "sem checagens ainda"
    )
    if not dq_rows:
        statuses["dq_checks"] = UNKNOWN
except Exception as exc:  # noqa: BLE001
    statuses["dq_checks"] = UNKNOWN
    details["dq_checks"] = "sem checagens ainda" if "TABLE_OR_VIEW_NOT_FOUND" in str(exc) else str(exc)

try:
    latency_rows = run_query(connection, build_table_count_query("monitoring._pipeline_latency_results"))
    row_count = latency_rows[0]["total"] if latency_rows else 0
    statuses["pipeline_latency"] = UNKNOWN if row_count == 0 else "green"
    details["pipeline_latency"] = (
        "ver página 'Monitoramento & Latência do Pipeline' pro detalhe por métrica"
        if row_count
        else "sem checagens ainda"
    )
except Exception as exc:  # noqa: BLE001
    statuses["pipeline_latency"] = UNKNOWN
    details["pipeline_latency"] = "sem checagens ainda" if "TABLE_OR_VIEW_NOT_FOUND" in str(exc) else str(exc)

try:
    shareable_rows = run_query(connection, build_table_count_query("gold.open_insurance_shareable"))
    shareable_count = shareable_rows[0]["total"] or 0
    statuses["open_insurance"] = table_exists_status(shareable_count > 0)
    details["open_insurance"] = (
        f"{shareable_count} registro(s) compartilháveis" if shareable_count else "aguardando primeira execução"
    )
except Exception as exc:  # noqa: BLE001
    statuses["open_insurance"] = UNKNOWN
    details["open_insurance"] = "aguardando primeira execução" if "TABLE_OR_VIEW_NOT_FOUND" in str(exc) else str(exc)

st.graphviz_chart(build_architecture_dot(statuses), use_container_width=True)

legend_cols = st.columns(4)
for col, status in zip(legend_cols, ["green", "yellow", "red", UNKNOWN], strict=False):
    col.caption(STATUS_LABELS[status])

st.divider()
st.subheader("Detalhe por componente")
st.dataframe(
    [
        {
            "componente": ARCHITECTURE_NODES[node_id][0].replace("\n", " "),
            "status": STATUS_LABELS[status],
            "detalhe": details.get(node_id, ""),
        }
        for node_id, status in statuses.items()
    ],
    use_container_width=True,
)
