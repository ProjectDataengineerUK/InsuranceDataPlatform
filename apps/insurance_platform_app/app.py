import streamlit as st

st.set_page_config(page_title="Insurance Regulatory Platform", layout="wide")

pages = [
    st.Page("pages/status.py", title="Status da Plataforma", default=True),
    st.Page("pages/consultar_apolice.py", title="Consultar Apólice"),
    st.Page("pages/consultar_cliente.py", title="Consultar Cliente"),
    st.Page("pages/consultar_sinistro.py", title="Consultar Sinistro"),
    st.Page("pages/pesquisar_banco.py", title="Pesquisar Banco"),
    st.Page("pages/pesquisar_seguradora.py", title="Pesquisar Seguradora"),
    st.Page("pages/dashboard_link.py", title="Dashboard"),
    st.Page("pages/lineage.py", title="Lineage"),
    st.Page("pages/auditoria.py", title="Auditoria"),
]

navigation = st.navigation(pages)
navigation.run()
