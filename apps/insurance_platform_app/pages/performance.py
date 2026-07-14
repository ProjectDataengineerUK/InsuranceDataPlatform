import json

import streamlit as st
from confluent_metrics import fetch_consumer_lag, fetch_partition_throughput, is_configured
from queries import (
    build_pipeline_latency_query,
    build_table_count_query,
    get_connection,
    run_query,
)

st.title("Performance Kafka & Databricks")
st.caption(
    "Taxa de quarentena por tópico (registros rejeitados na ingestão Bronze) e "
    "throughput vs. volume esperado (replay.events_per_minute em config.yaml)."
)

# Tópicos operacionais + regulatório — mesma lista de resources/jobs.bronze.yml
# e resources/jobs.regulatory_bronze.yml. bronze_table vem de constante interna,
# nunca de input do usuário (ver ressalva em build_quarantine_rate_query).
BRONZE_TABLES = [
    ("claim-opened", "bronze.claims"),
    ("policy-created", "bronze.policies"),
    ("customer-updated", "bronze.customers"),
    ("regulatory-claim-report", "bronze.regulatory_claims_raw"),
]

try:
    connection = get_connection()
except Exception as exc:  # noqa: BLE001
    st.error(f"Não foi possível conectar ao SQL Warehouse: {exc}")
    st.stop()

st.subheader("Taxa de quarentena por tópico")
quarantine_rows = []
for topic, bronze_table in BRONZE_TABLES:
    row: dict = {"topic": topic, "valid_count": None, "quarantine_count": None, "quarantine_rate_pct": None}

    try:
        row["valid_count"] = run_query(connection, build_table_count_query(bronze_table))[0]["total"] or 0
    except Exception as exc:  # noqa: BLE001
        # bronze.<table> só existe depois do primeiro evento válido do tópico
        # (bronze_ingest.py só escreve — e cria a tabela — quando há pelo
        # menos 1 linha), então TABLE_OR_VIEW_NOT_FOUND aqui é "sem dados
        # ainda", não erro.
        row["error"] = (
            "Sem dados válidos ainda (tabela criada só após o primeiro evento do tópico)"
            if "TABLE_OR_VIEW_NOT_FOUND" in str(exc)
            else str(exc)
        )

    try:
        row["quarantine_count"] = (
            run_query(connection, build_table_count_query(f"{bronze_table}_quarantine"))[0]["total"] or 0
        )
    except Exception as exc:  # noqa: BLE001
        # bronze.<table>_quarantine só é criada no primeiro evento MALFORMADO
        # — um tópico saudável, sem nenhum registro quarentenado, nunca cria
        # essa tabela. Tratado à parte de valid_count (consultas separadas em
        # build_table_count_query) pra essa ausência não mascarar contagem
        # válida real como "sem dados".
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc):
            row["quarantine_count"] = 0
        else:
            row["error"] = str(exc)

    if row["valid_count"] is not None and row["quarantine_count"] is not None:
        total = row["valid_count"] + row["quarantine_count"]
        row["quarantine_rate_pct"] = round(row["quarantine_count"] / total * 100, 2) if total else 0.0

    quarantine_rows.append(row)

valid_totals = [row["valid_count"] for row in quarantine_rows if row["valid_count"] is not None]
quarantine_totals = [row["quarantine_count"] for row in quarantine_rows if row["quarantine_count"] is not None]
total_valid = sum(valid_totals)
total_quarantine = sum(quarantine_totals)

summary_cols = st.columns(3)
summary_cols[0].metric("Total válidos (todos os tópicos)", total_valid)
summary_cols[1].metric("Total em quarentena", total_quarantine)
summary_cols[2].metric(
    "Taxa geral de quarentena",
    f"{total_quarantine / (total_valid + total_quarantine) * 100:.2f}%" if (total_valid + total_quarantine) else "sem dados",
)

st.dataframe(quarantine_rows, use_container_width=True)

rate_by_topic = {
    row["topic"]: row["quarantine_rate_pct"] for row in quarantine_rows if row["quarantine_rate_pct"] is not None
}
if rate_by_topic:
    st.caption("Taxa de quarentena por tópico (%)")
    st.bar_chart(rate_by_topic)

st.divider()
st.subheader("Throughput vs. volume esperado")
try:
    latency_rows = run_query(connection, build_pipeline_latency_query(row_limit=200))
    throughput_rows_history = [row for row in latency_rows if row["metric_name"] == "at_003_throughput"]
    # A checagem mais recente roda sobre uma janela de só 15 min — sem
    # tráfego passando bem naquele instante, "minutes_observed" fica 0 mesmo
    # com o pipeline saudável. Por isso caímos pra última checagem que teve
    # amostra de verdade, em vez de só mostrar "0" sem contexto.
    throughput_row = next(
        (
            row
            for row in throughput_rows_history
            if json.loads(row["details_json"]).get("minutes_observed")
        ),
        None,
    )
    if throughput_row:
        details = json.loads(throughput_row["details_json"])
        if throughput_rows_history and throughput_row is not throughput_rows_history[0]:
            st.caption(f"Última janela com amostras: {throughput_row['_checked_at']}")
        cols = st.columns(3)
        cols[0].metric("Volume esperado (eventos/min)", details.get("expected_events_per_minute"))
        cols[1].metric("Minutos observados", details.get("minutes_observed"))
        cols[2].metric(
            "Minutos abaixo de 80% do esperado", details.get("minutes_below_80pct_expected")
        )
        events_per_minute = details.get("events_per_minute") or []
        if events_per_minute:
            st.line_chart(events_per_minute)
    else:
        st.info("Nenhuma checagem de throughput com amostras nas últimas 200 janelas.")
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar throughput: {exc}")

st.divider()
st.subheader("Consumer lag e partition skew (Confluent Cloud, nativo)")
if not is_configured():
    st.info(
        "Não configurado — requer CONFLUENT_METRICS_API_KEY/SECRET (Cloud API Key de "
        "conta, diferente da API Key de cluster já usada pelo producer) e "
        "CONFLUENT_CLUSTER_ID. Ver docs/ARCHITECTURE.md."
    )
else:
    lookback_minutes = st.slider(
        "Janela de observação (minutos)", min_value=5, max_value=60, value=15, step=5
    )
    st.caption(
        "Um consumer group sem atividade no momento da consulta não aparece na "
        "API do Confluent Cloud — isso é comportamento documentado da própria "
        "API, não ausência de dado nosso. Se a janela de 15 min estiver vazia, "
        "tente alargar."
    )

    try:
        lag_rows = fetch_consumer_lag(lookback_minutes=lookback_minutes)
        if lag_rows:
            total_lag = sum(row.get("value") or 0 for row in lag_rows)
            distinct_groups = {row.get("metric.consumer_group_id") for row in lag_rows}
            worst_row = max(lag_rows, key=lambda row: row.get("value") or 0)

            lag_cols = st.columns(3)
            lag_cols[0].metric("Lag total (soma de offsets)", int(total_lag))
            lag_cols[1].metric("Consumer groups ativos", len(distinct_groups))
            lag_cols[2].metric(
                "Pior partição",
                f"{worst_row.get('metric.topic')}[{worst_row.get('metric.partition')}]",
                delta=f"lag {worst_row.get('value')}",
                delta_color="off",
            )

            st.caption(f"Consumer lag por grupo/tópico/partição (últimos {lookback_minutes} min, máximo)")
            st.dataframe(lag_rows, use_container_width=True)
        else:
            st.info("Nenhum consumer group ativo com lag registrado na janela.")
    except Exception as exc:  # noqa: BLE001
        st.error(f"Erro ao consultar consumer lag: {exc}")

    try:
        throughput_rows = fetch_partition_throughput(lookback_minutes=lookback_minutes)
        if throughput_rows:
            bytes_by_partition: dict[str, float] = {}
            for row in throughput_rows:
                key = f"{row.get('metric.topic')}[{row.get('metric.partition')}]"
                bytes_by_partition[key] = bytes_by_partition.get(key, 0) + (row.get("value") or 0)

            total_bytes = sum(bytes_by_partition.values())
            skew_cols = st.columns(2)
            skew_cols[0].metric("Bytes recebidos (total, todas as partições)", int(total_bytes))
            skew_cols[1].metric("Partições com tráfego", len(bytes_by_partition))

            st.caption(f"Bytes recebidos por partição (proxy de partition skew, últimos {lookback_minutes} min)")
            st.bar_chart(bytes_by_partition)
            st.dataframe(throughput_rows, use_container_width=True)
        else:
            st.info("Nenhum dado de throughput por partição na janela.")
    except Exception as exc:  # noqa: BLE001
        st.error(f"Erro ao consultar throughput por partição: {exc}")
