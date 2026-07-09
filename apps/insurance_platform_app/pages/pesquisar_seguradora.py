import streamlit as st
from queries import build_source_system_search_query, get_connection, run_query

st.title("Pesquisar Seguradora")

# As 2 fontes fictícias rotuladas como seguradora (ver
# src/ingestion/producer/datasets/regulatory_feeds.py) + a fonte real SUSEP
# (sinistros operacionais, sem source_system — tratada à parte).
INSURERS = {"Seguradora Alfa": "insurer_a", "Seguradora Gama": "insurer_c"}

insurer_label = st.selectbox("Seguradora", list(INSURERS))

if st.button("Buscar"):
    try:
        connection = get_connection()
        rows = run_query(connection, build_source_system_search_query(INSURERS[insurer_label]))
    except Exception as exc:  # noqa: BLE001
        st.error(f"Erro ao consultar: {exc}")
    else:
        if rows:
            st.dataframe(rows, use_container_width=True)
        else:
            st.info("Nenhum sinistro reportado por esta seguradora ainda.")
