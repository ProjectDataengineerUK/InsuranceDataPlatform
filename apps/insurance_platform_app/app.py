import streamlit as st

st.set_page_config(page_title="Insurance Regulatory Platform", layout="wide")

pages = [
    st.Page("pages/status.py", title="Visão Geral", default=True),
    st.Page("pages/chat.py", title="Chat com os dados"),
    st.Page("pages/susep_compliance.py", title="Conformidade SUSEP"),
    st.Page("pages/open_insurance.py", title="Open Insurance"),
    st.Page("pages/fraud_probability.py", title="Probabilidade de Fraude"),
    st.Page("pages/pipeline_monitoring.py", title="Monitoramento & Latência"),
    st.Page("pages/dataops_mlops_sentinel.py", title="DataOps/MLOps Sentinela"),
    st.Page("pages/performance.py", title="Performance Kafka & Databricks"),
    st.Page("pages/custos.py", title="Custos"),
    st.Page("pages/lineage.py", title="Lineage"),
]

navigation = st.navigation(pages)
navigation.run()
