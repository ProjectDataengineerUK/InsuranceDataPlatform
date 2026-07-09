import streamlit as st
from databricks.sdk.core import Config

st.title("Lineage")

TABLES = [
    "gold.claims",
    "gold.regulatory_susep_claims",
    "gold.regulatory_dq_summary",
    "silver.regulatory_claims",
]

table = st.selectbox("Tabela", TABLES)

# Formato de URL da tela de lineage do Unity Catalog no Databricks
# (Catalog Explorer, aba "Lineage") — confirmar contra o workspace real no
# primeiro deploy, já que este caminho não foi testado neste sandbox.
schema, table_name = table.split(".", 1)
try:
    host = Config().host
    lineage_url = f"https://{host}/explore/data/{{catalog}}/{schema}/{table_name}?o=0&tab=lineage"
    st.link_button("Abrir no Catalog Explorer", lineage_url)
    st.caption(
        "Substitua {catalog} pelo catálogo do ambiente (ex.: insurance_prod) — "
        "o app não injeta esse valor automaticamente ainda."
    )
except Exception as exc:  # noqa: BLE001
    st.error(f"Não foi possível montar o link de lineage: {exc}")
