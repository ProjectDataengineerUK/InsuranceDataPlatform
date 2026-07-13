import streamlit as st
from queries import (
    build_cost_usage_by_job_query,
    build_cost_usage_by_product_query,
    get_connection,
    run_query,
)

st.title("Custos")
st.caption(
    "Consumo real de DBUs via system.billing.usage (system table nativa do Unity "
    "Catalog). Requer habilitação prévia pelo account admin do workspace "
    "(Settings → Feature enablement → System tables) — se não estiver habilitada "
    "neste workspace, as consultas abaixo falham e mostram essa mensagem em vez "
    "de travar a página."
)

try:
    connection = get_connection()
except Exception as exc:  # noqa: BLE001
    st.error(f"Não foi possível conectar ao SQL Warehouse: {exc}")
    st.stop()

lookback_days = st.slider("Janela (dias)", min_value=1, max_value=90, value=30)

st.subheader("Consumo por produto (DBUs)")
try:
    product_rows = run_query(connection, build_cost_usage_by_product_query(lookback_days))
    if product_rows:
        st.dataframe(product_rows, use_container_width=True)
    else:
        st.info("Sem dados de consumo no período selecionado.")
except Exception as exc:  # noqa: BLE001
    st.warning(
        "Não foi possível consultar system.billing.usage — provavelmente as system "
        f"tables não estão habilitadas neste workspace. Detalhe: {exc}"
    )

st.divider()
st.subheader("Top jobs por consumo (DBUs)")
try:
    job_rows = run_query(connection, build_cost_usage_by_job_query(lookback_days, row_limit=20))
    if job_rows:
        st.dataframe(job_rows, use_container_width=True)
    else:
        st.info("Sem dados de consumo por job no período selecionado.")
except Exception as exc:  # noqa: BLE001
    st.warning(f"Não foi possível consultar consumo por job. Detalhe: {exc}")
