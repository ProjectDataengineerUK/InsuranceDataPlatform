import streamlit as st
from queries import (
    build_source_system_volume_query,
    build_susep_claims_query,
    build_susep_compliance_summary_query,
    get_connection,
    run_query,
)

st.title("Conformidade SUSEP")
st.caption(
    "Status por contrato calculado em src/regulatory/gold_susep_export.py: campos "
    "obrigatórios ausentes, código de causa fora da Tabela V oficial, ou discrepância "
    "de reconciliação entre fontes tornam um contrato 'fora das regras'. Região não "
    "entra nessa checagem — o allow-list de região é um proxy data-driven sem tabela "
    "oficial confirmada (ver docs/ARCHITECTURE.md), não um critério de conformidade."
)

try:
    connection = get_connection()
except Exception as exc:  # noqa: BLE001
    st.error(f"Não foi possível conectar ao SQL Warehouse: {exc}")
    st.stop()

try:
    summary_rows = run_query(connection, build_susep_compliance_summary_query())
    compliant_total = sum(r["total"] for r in summary_rows if r["susep_compliant"])
    non_compliant_total = sum(r["total"] for r in summary_rows if not r["susep_compliant"])
    cols = st.columns(2)
    cols[0].metric("Contratos aderentes", compliant_total)
    cols[1].metric("Contratos fora das regras", non_compliant_total)
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar resumo de conformidade: {exc}")

st.divider()

view = st.radio(
    "Visualizar",
    ["Contratos aderentes", "Contratos fora das regras"],
    horizontal=True,
)

try:
    rows = run_query(
        connection, build_susep_claims_query(compliant=(view == "Contratos aderentes"))
    )
    if rows:
        st.dataframe(rows, use_container_width=True)
    else:
        st.info("Nenhum contrato encontrado nesta categoria.")
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar contratos: {exc}")

st.divider()
st.subheader("Volume por banco/seguradora")
try:
    volume_rows = run_query(connection, build_source_system_volume_query())
    if volume_rows:
        st.dataframe(volume_rows, use_container_width=True)
    else:
        st.info("Sem dados de bancos/seguradoras ainda.")
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar volume por fonte: {exc}")
