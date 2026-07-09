import streamlit as st
from queries import build_dq_summary_query, build_reconciliation_query, get_connection, run_query

st.title("Auditoria")

try:
    connection = get_connection()
except Exception as exc:  # noqa: BLE001
    st.error(f"Não foi possível conectar ao SQL Warehouse: {exc}")
    st.stop()

st.subheader("Qualidade de dados (regulatório)")
try:
    dq_rows = run_query(connection, build_dq_summary_query(row_limit=20))
    st.dataframe(dq_rows, use_container_width=True) if dq_rows else st.info("Sem execuções ainda.")
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar DQ summary: {exc}")

st.subheader("Reconciliação entre fontes")
try:
    reconciliation_rows = run_query(connection, build_reconciliation_query(row_limit=50))
    (
        st.dataframe(reconciliation_rows, use_container_width=True)
        if reconciliation_rows
        else st.info("Nenhuma discrepância registrada ainda.")
    )
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar reconciliação: {exc}")
