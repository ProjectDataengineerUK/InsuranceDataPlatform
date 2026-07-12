import streamlit as st
from queries import build_fraud_probability_query, build_sla_breach_query, get_connection, run_query

st.title("Probabilidade de Fraude")
st.caption(
    "fraud_score é a heurística real que decide fraud_flag/auto_approved "
    "(src/fraud/streaming_score.py). model_fraud_score é o score do modelo mlflow "
    "campeão, em shadow mode — só observabilidade, nunca decide sozinho (ver "
    "docs/ARCHITECTURE.md, 'Shadow scoring do modelo campeão')."
)

try:
    connection = get_connection()
except Exception as exc:  # noqa: BLE001
    st.error(f"Não foi possível conectar ao SQL Warehouse: {exc}")
    st.stop()

row_limit = st.slider("Quantidade de sinistros", min_value=10, max_value=500, value=100, step=10)

try:
    rows = run_query(connection, build_fraud_probability_query(row_limit=row_limit))
    st.dataframe(rows, use_container_width=True) if rows else st.info(
        "Nenhum sinistro pontuado ainda."
    )
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar probabilidade de fraude: {exc}")

st.divider()
st.subheader("Sinistros fora do SLA (aguardando revisão manual)")
sla_hours = st.number_input("SLA (horas)", min_value=1, max_value=168, value=24)

try:
    breach_rows = run_query(connection, build_sla_breach_query(sla_hours=int(sla_hours)))
    st.dataframe(breach_rows, use_container_width=True) if breach_rows else st.success(
        "Nenhum sinistro fora do SLA."
    )
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar SLA: {exc}")
