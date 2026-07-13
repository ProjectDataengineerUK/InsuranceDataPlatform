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

METRIC_LABELS = {
    "at_001_kafka_to_bronze_latency": "Kafka → Bronze (latência)",
    "fraud_score_visibility_latency": "Score de fraude (Bronze → Gold)",
    "at_003_throughput": "Throughput vs. esperado",
}


def _status_label(within_sla: bool | None) -> str:
    if within_sla is None:
        return "◯ sem dados suficientes na janela"
    return "✔ dentro do SLA" if within_sla else "✖ fora do SLA"


latest_by_metric: dict[str, dict] = {}
for row in rows:
    if row["metric_name"] not in latest_by_metric:
        latest_by_metric[row["metric_name"]] = row

cols = st.columns(len(latest_by_metric))
for col, (metric_name, row) in zip(cols, latest_by_metric.items(), strict=False):
    details = json.loads(row["details_json"])
    label = METRIC_LABELS.get(metric_name, metric_name)

    if metric_name == "at_003_throughput":
        # Sem latência única de resumo — o número mais informativo é quantos
        # minutos, dos observados, ficaram abaixo de 80% do volume esperado.
        below = details.get("minutes_below_80pct_expected")
        observed = details.get("minutes_observed")
        value = f"{below}/{observed} min abaixo do esperado" if observed else "sem dados"
    elif details.get("sample_size", 0) == 0:
        value = "sem dados"
    else:
        avg_seconds = details.get("avg_seconds")
        max_seconds = details.get("max_seconds")
        sla_seconds = details.get("sla_seconds")
        value = f"méd {avg_seconds:.0f}s / máx {max_seconds:.0f}s (SLA {sla_seconds}s)"

    col.metric(label, value, delta=_status_label(row["within_sla"]), delta_color="off")

st.divider()
st.subheader("Tendência (200 checagens mais recentes)")
st.caption(
    "Latência máxima por checagem, ao longo do tempo — útil pra ver se uma "
    "degradação é um pico isolado ou uma tendência sustentada."
)
trend_cols = st.columns(2)
for col, metric_name in zip(
    trend_cols, ["at_001_kafka_to_bronze_latency", "fraud_score_visibility_latency"], strict=False
):
    metric_rows = [row for row in rows if row["metric_name"] == metric_name]
    series = {
        row["_checked_at"]: json.loads(row["details_json"]).get("max_seconds")
        for row in reversed(metric_rows)
        if json.loads(row["details_json"]).get("sample_size", 0) > 0
    }
    col.caption(METRIC_LABELS.get(metric_name, metric_name))
    if series:
        col.line_chart(series)
    else:
        col.info("Sem amostras suficientes na janela pra plotar tendência.")

st.divider()
st.subheader("Detalhe da checagem mais recente por métrica")
for metric_name, row in latest_by_metric.items():
    with st.expander(f"{METRIC_LABELS.get(metric_name, metric_name)} — {row['_checked_at']}"):
        st.json(json.loads(row["details_json"]))

st.divider()
st.subheader("Histórico (200 checagens mais recentes)")
st.dataframe(
    [{k: v for k, v in row.items() if k != "details_json"} for row in rows],
    use_container_width=True,
)
