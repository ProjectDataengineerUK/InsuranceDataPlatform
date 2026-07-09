import streamlit as st
from queries import build_claim_lookup_query, get_connection, run_query

st.title("Consultar Sinistro")

claim_id = st.text_input("Claim ID")

if st.button("Buscar", disabled=not claim_id):
    try:
        connection = get_connection()
        rows = run_query(connection, build_claim_lookup_query(claim_id))
    except Exception as exc:  # noqa: BLE001
        st.error(f"Erro ao consultar: {exc}")
    else:
        if rows:
            st.dataframe(rows, use_container_width=True)
        else:
            st.warning("Nenhum sinistro encontrado para este claim_id.")
