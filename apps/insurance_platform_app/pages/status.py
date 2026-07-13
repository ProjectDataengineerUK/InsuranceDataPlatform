from datetime import UTC, datetime, timedelta

import streamlit as st
from queries import (
    build_dq_summary_query,
    build_reconciliation_query,
    build_susep_compliance_summary_query,
    build_table_freshness_query,
    get_connection,
    run_query,
)

# Generoso o bastante pro producer (agendado a cada 10 min via GitHub
# Actions) e os jobs contínuos (bronze/silver/gold) ficarem dentro do
# esperado sem gerar falso alarme — ver docs/ARCHITECTURE.md pro SLA real
# medido por scripts/measure_pipeline_latency.py (mais rigoroso, 2 min/1 min).
FRESHNESS_THRESHOLD_MINUTES = 30

STATUS_CHECKS = [
    ("Kafka → Bronze (operacional)", "bronze.claims", "_ingested_at"),
    ("Silver (operacional)", "silver.claims", "_ingested_at"),
    ("Gold (fraude)", "gold.claims", "_scored_at"),
    ("Bronze (regulatório)", "bronze.regulatory_claims_raw", "_ingested_at"),
    # gold.regulatory_susep_claims não tem coluna de timestamp de
    # processamento (só D_OCORR, data do sinistro — string "yyyyMMdd", não
    # datetime, quebrava .replace(tzinfo=...) com TypeError, confirmado em
    # produção). regulatory_dq_summary é gerado pelo mesmo job
    # (gold_susep_export.py::run_gold_export_job) e já tem _generated_at.
    ("Gold (SUSEP export)", "gold.regulatory_dq_summary", "_generated_at"),
]

st.title("Insurance Regulatory Platform")
st.caption(
    "Visão consolidada: conformidade SUSEP, fraude, reconciliação entre fontes "
    "e saúde do pipeline (operacional + regulatório)."
)

try:
    connection = get_connection()
except Exception as exc:  # noqa: BLE001 — painel de status nunca deve derrubar o app
    st.error(f"Não foi possível conectar ao SQL Warehouse: {exc}")
    st.stop()

cols = st.columns(len(STATUS_CHECKS) + 1)

for col, (label, table, timestamp_column) in zip(cols, STATUS_CHECKS, strict=False):
    try:
        rows = run_query(connection, build_table_freshness_query(table, timestamp_column))
        latest_ts = rows[0]["latest_ts"] if rows else None
        is_fresh = latest_ts is not None and (
            datetime.now(UTC) - latest_ts.replace(tzinfo=UTC)
            < timedelta(minutes=FRESHNESS_THRESHOLD_MINUTES)
        )
        col.metric(label, "✔ OK" if is_fresh else "✖ Sem dados recentes")
    except Exception as exc:  # noqa: BLE001 — cada card falha isolado, não o painel inteiro
        col.metric(label, "✖ Erro")
        col.caption(str(exc))

alert_col = cols[-1]
try:
    reconciliation_rows = run_query(connection, build_reconciliation_query(row_limit=10))
    alert_col.metric("Discrepâncias recentes", str(len(reconciliation_rows)))
except Exception as exc:  # noqa: BLE001
    alert_col.metric("Discrepâncias recentes", "✖ Erro")
    alert_col.caption(str(exc))

st.divider()
st.subheader("Conformidade SUSEP (resumo)")
try:
    summary_rows = run_query(connection, build_susep_compliance_summary_query())
    compliant_total = sum(r["total"] for r in summary_rows if r["susep_compliant"])
    non_compliant_total = sum(r["total"] for r in summary_rows if not r["susep_compliant"])
    total = compliant_total + non_compliant_total
    summary_cols = st.columns(3)
    summary_cols[0].metric("Contratos aderentes", compliant_total)
    summary_cols[1].metric("Contratos fora das regras", non_compliant_total)
    summary_cols[2].metric(
        "Taxa de conformidade", f"{(compliant_total / total * 100):.1f}%" if total else "N/A"
    )
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar conformidade SUSEP: {exc}")

try:
    dq_rows = run_query(connection, build_dq_summary_query(row_limit=1))
    if dq_rows:
        st.caption(
            f"Última checagem de DQ regulatório: {dq_rows[0]['dq_checks_failed']} de "
            f"{dq_rows[0]['dq_checks_total']} verificações falharam "
            f"({dq_rows[0]['_generated_at']})."
        )
except Exception:  # noqa: BLE001
    pass
