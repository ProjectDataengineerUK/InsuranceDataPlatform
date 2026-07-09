import streamlit as st
from queries import build_source_system_search_query, get_connection, run_query

st.title("Pesquisar Banco")

# Único source_system rotulado como banco nas 3 fontes fictícias do módulo
# regulatório (ver src/ingestion/producer/datasets/regulatory_feeds.py).
BANKS = {"Banco Beta Seguros": "insurer_b"}

bank_label = st.selectbox("Banco", list(BANKS))

if st.button("Buscar"):
    try:
        connection = get_connection()
        rows = run_query(connection, build_source_system_search_query(BANKS[bank_label]))
    except Exception as exc:  # noqa: BLE001
        st.error(f"Erro ao consultar: {exc}")
    else:
        if rows:
            st.dataframe(rows, use_container_width=True)
        else:
            st.info("Nenhum sinistro reportado por este banco ainda.")
