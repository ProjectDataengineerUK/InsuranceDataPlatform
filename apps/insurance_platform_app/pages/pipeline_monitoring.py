import json

import streamlit as st
from queries import build_pipeline_latency_query, get_connection, run_query

st.title("Monitoramento & Latência do Pipeline")
st.caption(
    "Dados de scripts/measure_pipeline_latency.py (job pipeline_latency_monitor, a "
    "cada 15 min): latência Kafka → Bronze (alvo < 2 min), latência de visibilidade "
    "do score de fraude Bronze → Gold (alvo < 1 min) e throughput vs. volume esperado."
)

with st.expander("🩺 Latência ou throughput fora do SLA — o que fazer", expanded=True):
    st.markdown(
        "- **Kafka → Bronze fora do SLA (>2min) de forma sustentada** → confirme em "
        "'Visão Geral' se `bronze_ingest` está avançando; um job de ingestão parado ou "
        "competindo por cota de serverless (`RESOURCE_EXHAUSTED`, ver "
        "docs/ARCHITECTURE.md) produz exatamente esse sintoma, não necessariamente "
        "Kafka lento.\n"
        "- **Score de fraude fora do SLA (>7min)** → depende do Kafka→Bronze já estar "
        "ok primeiro (é a etapa seguinte); se ambos estiverem fora, resolva o "
        "Kafka→Bronze antes — o score não tem como ficar rápido com Bronze atrasado.\n"
        "- **'sem dados suficientes na janela' persistente, não só uma checagem** → o "
        "job de ingestão pode estar parado (não é 'sem tráfego pontual'); cheque o "
        "campo 'X/200 checagens com amostras' de cada métrica abaixo — cobertura baixa "
        "sustentada é sinal de job parado, não de janela de 15min sem sorte."
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


def _has_sample(metric_name: str, details: dict) -> bool:
    if metric_name == "at_003_throughput":
        return bool(details.get("minutes_observed"))
    return bool(details.get("sample_size"))


st.caption(f"Última checagem do job: {rows[0]['_checked_at']}")

# Cada checagem roda sobre uma janela de só 15 min (--window-minutes) — sem
# tráfego passando bem naquele instante, a checagem mais recente fica vazia
# mesmo que o pipeline esteja saudável. Por isso separamos "checagem mais
# recente" (pra status/SLA) de "última checagem com amostras de verdade" (pra
# mostrar um número em vez de só "sem dados").
latest_by_metric: dict[str, dict] = {}
last_data_by_metric: dict[str, dict] = {}
samples_seen: dict[str, int] = {name: 0 for name in METRIC_LABELS}
checks_seen: dict[str, int] = {name: 0 for name in METRIC_LABELS}
breaches_seen: dict[str, int] = {name: 0 for name in METRIC_LABELS}

for row in rows:
    metric_name = row["metric_name"]
    if metric_name not in METRIC_LABELS:
        continue
    details = json.loads(row["details_json"])
    checks_seen[metric_name] += 1
    if metric_name not in latest_by_metric:
        latest_by_metric[metric_name] = row
    if _has_sample(metric_name, details):
        samples_seen[metric_name] += 1
        if metric_name not in last_data_by_metric:
            last_data_by_metric[metric_name] = row
        if row["within_sla"] is False:
            breaches_seen[metric_name] += 1

cols = st.columns(len(latest_by_metric))
for col, (metric_name, row) in zip(cols, latest_by_metric.items(), strict=False):
    label = METRIC_LABELS.get(metric_name, metric_name)
    data_row = last_data_by_metric.get(metric_name)
    data_details = json.loads(data_row["details_json"]) if data_row else None

    if data_details is None:
        value = "sem dados"
    elif metric_name == "at_003_throughput":
        # Sem latência única de resumo — o número mais informativo é quantos
        # minutos, dos observados, ficaram abaixo de 80% do volume esperado.
        below = data_details.get("minutes_below_80pct_expected")
        observed = data_details.get("minutes_observed")
        value = f"{below}/{observed} min abaixo do esperado"
    else:
        avg_seconds = data_details.get("avg_seconds")
        max_seconds = data_details.get("max_seconds")
        sla_seconds = data_details.get("sla_seconds")
        value = f"méd {avg_seconds:.0f}s / máx {max_seconds:.0f}s (SLA {sla_seconds}s)"

    col.metric(label, value, delta=_status_label(row["within_sla"]), delta_color="off")
    col.caption(f"{samples_seen[metric_name]}/{checks_seen[metric_name]} checagens (200 mais recentes) com amostras")
    if data_row is not None and data_row is not row:
        col.caption(f"última janela com dados: {data_row['_checked_at']}")
    elif data_row is None:
        col.caption("nenhuma amostra nas últimas 200 checagens")

    breach_ratio = breaches_seen[metric_name] / samples_seen[metric_name] if samples_seen[metric_name] else 0
    if breach_ratio > 0.2:
        col.warning(
            f"{breaches_seen[metric_name]}/{samples_seen[metric_name]} checagens com "
            "amostra ficaram fora do SLA — não é pico isolado, ver checklist acima."
        )

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
