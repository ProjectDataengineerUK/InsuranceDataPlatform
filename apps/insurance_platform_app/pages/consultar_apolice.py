import streamlit as st
from queries import build_policy_lookup_query, get_connection, run_query

st.title("Consultar Apólice")

policy_id = st.text_input("Policy ID")

if st.button("Buscar", disabled=not policy_id):
    try:
        connection = get_connection()
        rows = run_query(connection, build_policy_lookup_query(policy_id))
    except Exception as exc:  # noqa: BLE001
        st.error(f"Erro ao consultar: {exc}")
    else:
        if rows:
            st.dataframe(rows, use_container_width=True)
        else:
            st.warning("Nenhum sinistro encontrado para este policy_id.")
