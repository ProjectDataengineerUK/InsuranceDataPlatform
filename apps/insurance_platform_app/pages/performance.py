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

st.dataframe(quarantine_rows, use_container_width=True)

st.divider()
st.subheader("Throughput vs. volume esperado")
try:
    latency_rows = run_query(connection, build_pipeline_latency_query(row_limit=50))
    throughput_row = next(
        (row for row in latency_rows if row["metric_name"] == "at_003_throughput"), None
    )
    if throughput_row:
        details = json.loads(throughput_row["details_json"])
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
        st.info("Nenhuma checagem de throughput registrada ainda.")
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
    try:
        lag_rows = fetch_consumer_lag()
        if lag_rows:
            st.caption("Consumer lag por grupo/tópico/partição (últimos 15 min, máximo)")
            st.dataframe(lag_rows, use_container_width=True)
        else:
            st.info("Nenhum consumer group ativo com lag registrado na janela.")
    except Exception as exc:  # noqa: BLE001
        st.error(f"Erro ao consultar consumer lag: {exc}")

    try:
        throughput_rows = fetch_partition_throughput()
        if throughput_rows:
            st.caption("Bytes recebidos por partição (proxy de partition skew, últimos 15 min)")
            st.dataframe(throughput_rows, use_container_width=True)
        else:
            st.info("Nenhum dado de throughput por partição na janela.")
    except Exception as exc:  # noqa: BLE001
        st.error(f"Erro ao consultar throughput por partição: {exc}")
