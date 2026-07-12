import json

import streamlit as st
from queries import build_pipeline_latency_query, get_connection, run_query

st.title("Monitoramento & Latência do Pipeline")
st.caption(
    "Dados de scripts/measure_pipeline_latency.py (job pipeline_latency_monitor, a "
    "cada 15 min): latência Kafka → Bronze (alvo < 2 min), latência de visibilidade "
    "do score de fraude Bronze → Gold (alvo < 1 min) e throughput vs. volume esperado."
)

try:
    connection = get_connection()
except Exception as exc:  # noqa: BLE001
    st.error(f"Não foi possível conectar ao SQL Warehouse: {exc}")
    st.stop()

try:
    rows = run_query(connection, build_pipeline_latency_query(row_limit=200))
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar latência do pipeline: {exc}")
    st.stop()

if not rows:
    st.info("Nenhuma checagem de latência registrada ainda.")
    st.stop()

latest_by_metric: dict[str, dict] = {}
for row in rows:
    if row["metric_name"] not in latest_by_metric:
        latest_by_metric[row["metric_name"]] = row

cols = st.columns(len(latest_by_metric))
for col, (metric_name, row) in zip(cols, latest_by_metric.items(), strict=False):
    details = json.loads(row["details_json"])
    status = "✔ dentro do SLA" if row["within_sla"] else "✖ fora do SLA"
    sample_size = details.get("sample_size")
    col.metric(metric_name, status, help=f"sample_size={sample_size}" if sample_size else None)

st.divider()
st.subheader("Detalhe da checagem mais recente por métrica")
for metric_name, row in latest_by_metric.items():
    with st.expander(f"{metric_name} — {row['_checked_at']}"):
        st.json(json.loads(row["details_json"]))

st.divider()
st.subheader("Histórico (200 checagens mais recentes)")
st.dataframe(
    [{k: v for k, v in row.items() if k != "details_json"} for row in rows],
    use_container_width=True,
)
