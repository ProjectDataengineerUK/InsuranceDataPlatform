from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

st.title("FAQ — Preparação de Entrevista")
st.caption(
    "Kafka, Databricks e Spark Structured Streaming do básico ao staff, com FinOps e "
    "casos reais deste projeto — mesmo conteúdo publicado como Artifact, embutido aqui "
    "pra ficar dentro do app. Renderizado num iframe isolado (design/CSS próprios, "
    "independentes do tema do Streamlit)."
)

html_path = Path(__file__).resolve().parent.parent / "assets" / "faq_interview.html"
html_content = html_path.read_text(encoding="utf-8")

components.html(html_content, height=2400, scrolling=True)
