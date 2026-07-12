import streamlit as st
from queries import build_dq_results_query, build_model_drift_query, get_connection, run_query

st.title("DataOps / MLOps Sentinela")
st.caption(
    "Qualidade de dados (src/quality/checks.py, todas as camadas) e drift de "
    "features do modelo de fraude (src/monitoring/model_drift.py, a cada 6h). "
    "Nenhum dos dois falha o pipeline sozinho — ficam registrados aqui pra "
    "auditoria e decisão de retrain."
)

try:
    connection = get_connection()
except Exception as exc:  # noqa: BLE001
    st.error(f"Não foi possível conectar ao SQL Warehouse: {exc}")
    st.stop()

st.subheader("Qualidade de dados — checagens recentes")
only_failed = st.checkbox("Mostrar só falhas", value=True)

try:
    dq_rows = run_query(connection, build_dq_results_query(row_limit=200))
    if only_failed:
        dq_rows = [row for row in dq_rows if not row["passed"]]
    st.dataframe(dq_rows, use_container_width=True) if dq_rows else st.success(
        "Nenhuma falha de qualidade de dados nas últimas checagens."
    )
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar qualidade de dados: {exc}")

st.divider()
st.subheader("Drift de features do modelo de fraude")

try:
    drift_rows = run_query(connection, build_model_drift_query(row_limit=100))
    if drift_rows:
        detected = [row for row in drift_rows if row["drift_detected"]]
        st.metric("Features com drift detectado (últimas 100 checagens)", len(detected))
        st.dataframe(drift_rows, use_container_width=True)
    else:
        st.info("Nenhuma checagem de drift registrada ainda.")
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar drift do modelo: {exc}")
