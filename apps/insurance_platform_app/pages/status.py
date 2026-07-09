from datetime import UTC, datetime, timedelta

import streamlit as st
from queries import (
    build_dq_summary_query,
    build_reconciliation_query,
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
    ("Streaming Online / Kafka Connected", "bronze.claims", "_ingested_at"),
    ("Bronze", "bronze.claims", "_ingested_at"),
    ("Silver", "silver.claims", "_ingested_at"),
    ("Gold", "gold.claims", "_scored_at"),
]

st.title("Insurance Regulatory Platform")
st.caption("Status da plataforma — visão consolidada dos pipelines operacional e regulatório.")

try:
    connection = get_connection()
except Exception as exc:  # noqa: BLE001 — painel de status nunca deve derrubar o app
    st.error(f"Não foi possível conectar ao SQL Warehouse: {exc}")
    st.stop()

cols = st.columns(len(STATUS_CHECKS) + 2)

for col, (label, table, timestamp_column) in zip(cols, STATUS_CHECKS, strict=False):
    try:
        rows = run_query(connection, build_table_freshness_query(table, timestamp_column))
        latest_ts = rows[0]["latest_ts"] if rows else None
        is_fresh = latest_ts is not None and (
            datetime.now(UTC) - latest_ts.replace(tzinfo=UTC) < timedelta(minutes=FRESHNESS_THRESHOLD_MINUTES)
        )
        col.metric(label, "✔ OK" if is_fresh else "✖ Sem dados recentes")
    except Exception as exc:  # noqa: BLE001 — cada card falha isolado, não o painel inteiro
        col.metric(label, "✖ Erro")
        col.caption(str(exc))

sla_col, alert_col = cols[-2], cols[-1]

try:
    dq_rows = run_query(connection, build_dq_summary_query(row_limit=1))
    sla_col.metric("SLA / Qualidade", "✔ OK" if dq_rows and dq_rows[0]["dq_checks_failed"] == 0 else "⚠ Ver DQ")
except Exception as exc:  # noqa: BLE001
    sla_col.metric("SLA / Qualidade", "✖ Erro")
    sla_col.caption(str(exc))

try:
    reconciliation_rows = run_query(connection, build_reconciliation_query(row_limit=10))
    alert_col.metric("Alertas", f"{len(reconciliation_rows)} discrepância(s) recente(s)")
except Exception as exc:  # noqa: BLE001
    alert_col.metric("Alertas", "✖ Erro")
    alert_col.caption(str(exc))

st.divider()
st.info(
    "Custos: nenhuma tabela de FinOps existe neste repo ainda "
    "(fora de escopo do MVP — ver DEFINE_INSURANCE_VISUALIZATION_LAYER.md, Out of Scope)."
)
