import streamlit as st

st.title("Dashboard")

# O AI/BI Dashboard (Lakeview) não é escrito à mão neste repo — é prototipado
# na UI do workspace prod e capturado via `databricks bundle generate
# dashboard --existing-id <id>` (ver DESIGN_INSURANCE_VISUALIZATION_LAYER.md,
# Decision 4). Até esse passo rodar, esta página só documenta como chegar lá;
# depois de gerado, troque DASHBOARD_URL pela URL real do dashboard publicado
# (formato usual: https://<workspace-host>/dashboardsv3/<dashboard-id>/published).
DASHBOARD_URL = None

if DASHBOARD_URL:
    st.link_button("Abrir dashboard", DASHBOARD_URL)
else:
    st.info(
        "Dashboard ainda não publicado. Ver README.md 'Ativando a camada de "
        "visualização' para o passo a passo (prototipar na UI + "
        "`databricks bundle generate dashboard`)."
    )
