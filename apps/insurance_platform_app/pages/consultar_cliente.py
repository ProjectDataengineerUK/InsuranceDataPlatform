import streamlit as st
from queries import build_customer_lookup_query, get_connection, run_query

st.title("Consultar Cliente")

st.warning(
    "Limitação conhecida: nem a fonte SUSEP nem a ANS expõem um identificador de "
    "cliente reutilizável (ver docs/ARCHITECTURE.md, 'Natureza real dos datasets "
    "públicos') — customer_id é sempre nulo em gold.claims hoje. Esta tela fica "
    "pronta para quando uma fonte complementar com customer_id for integrada."
)

customer_id = st.text_input("Customer ID")

if st.button("Buscar", disabled=not customer_id):
    try:
        connection = get_connection()
        rows = run_query(connection, build_customer_lookup_query(customer_id))
    except Exception as exc:  # noqa: BLE001
        st.error(f"Erro ao consultar: {exc}")
    else:
        if rows:
            st.dataframe(rows, use_container_width=True)
        else:
            st.info("Nenhum resultado — esperado, dado a limitação acima.")
