import json

import streamlit as st
from queries import (
    build_pipeline_latency_query,
    build_quarantine_rate_query,
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
    try:
        result = run_query(connection, build_quarantine_rate_query(bronze_table))[0]
        valid_count = result["valid_count"] or 0
        quarantine_count = result["quarantine_count"] or 0
        total = valid_count + quarantine_count
        rate = (quarantine_count / total * 100) if total else 0.0
        quarantine_rows.append(
            {
                "topic": topic,
                "valid_count": valid_count,
                "quarantine_count": quarantine_count,
                "quarantine_rate_pct": round(rate, 2),
            }
        )
    except Exception as exc:  # noqa: BLE001
        # bronze.<table>/bronze.<table>_quarantine só existem depois do
        # primeiro evento válido/malformado daquele tópico (bronze_ingest.py
        # só escreve — e cria a tabela — quando há pelo menos 1 linha),
        # então TABLE_OR_VIEW_NOT_FOUND aqui é "sem dados ainda", não erro.
        message = (
            "Sem dados ainda (tabela criada só após o primeiro evento do tópico)"
            if "TABLE_OR_VIEW_NOT_FOUND" in str(exc)
            else str(exc)
        )
        quarantine_rows.append(
            {
                "topic": topic,
                "valid_count": None,
                "quarantine_count": None,
                "quarantine_rate_pct": None,
                "error": message,
            }
        )

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
st.info(
    "Fora de escopo ainda: métricas nativas do Confluent Cloud (consumer lag, "
    "partition skew) — este painel só reflete o que os próprios jobs Spark "
    "medem no Bronze, não a API de métricas do Kafka. Fica como melhoria futura."
)
