import streamlit as st
from queries import (
    build_fraud_model_divergence_query,
    build_fraud_probability_query,
    build_fraud_probability_summary_query,
    build_sla_breach_query,
    get_connection,
    run_query,
)

st.title("Probabilidade de Fraude")
st.caption(
    "fraud_score é a heurística real que decide fraud_flag/auto_approved "
    "(src/fraud/streaming_score.py). model_fraud_score é o score do modelo mlflow "
    "campeão, em shadow mode — só observabilidade, nunca decide sozinho (ver "
    "docs/ARCHITECTURE.md, 'Shadow scoring do modelo campeão')."
)

try:
    connection = get_connection()
except Exception as exc:  # noqa: BLE001
    st.error(f"Não foi possível conectar ao SQL Warehouse: {exc}")
    st.stop()

try:
    summary_rows = run_query(connection, build_fraud_probability_summary_query())
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar resumo de fraude: {exc}")
    st.stop()

summary = summary_rows[0] if summary_rows else {}
total_claims = summary.get("total_claims") or 0

if not total_claims:
    st.info("Nenhum sinistro pontuado ainda.")
    st.stop()

flagged = summary.get("flagged_fraud") or 0
auto_approved = summary.get("auto_approved") or 0
model_scored = summary.get("model_scored") or 0

cols = st.columns(4)
cols[0].metric("Sinistros pontuados", total_claims)
cols[1].metric(
    "Sinalizados como fraude (heurística)", flagged, delta=f"{flagged / total_claims:.1%}", delta_color="off"
)
cols[2].metric(
    "Auto-aprovados", auto_approved, delta=f"{auto_approved / total_claims:.1%}", delta_color="off"
)
cols[3].metric(
    "Cobertura do modelo shadow",
    f"{model_scored}/{total_claims}",
    delta=f"{model_scored / total_claims:.1%}",
    delta_color="off",
)

avg_fraud_score = summary.get("avg_fraud_score")
if avg_fraud_score is not None:
    avg_model_fraud_score = summary.get("avg_model_fraud_score")
    avg_cols = st.columns(2)
    avg_cols[0].metric("Score médio — heurística", f"{avg_fraud_score:.3f}")
    avg_cols[1].metric(
        "Score médio — modelo (shadow)",
        f"{avg_model_fraud_score:.3f}" if avg_model_fraud_score is not None else "sem dados",
    )

st.divider()
st.subheader("Sinistros mais suspeitos (heurística)")
row_limit = st.slider("Quantidade de sinistros", min_value=10, max_value=500, value=100, step=10)

try:
    rows = run_query(connection, build_fraud_probability_query(row_limit=row_limit))
    if rows:
        st.dataframe(rows, use_container_width=True)
    else:
        st.info("Nenhum sinistro pontuado ainda.")
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar probabilidade de fraude: {exc}")

if model_scored:
    st.divider()
    st.subheader("Maiores divergências heurística × modelo (shadow)")
    st.caption(
        "fraud_score decide, model_fraud_score só observa — os sinistros onde os "
        "dois mais discordam são o sinal mais útil pra avaliar o modelo antes de "
        "uma eventual troca de decisão."
    )
    try:
        divergence_rows = run_query(connection, build_fraud_model_divergence_query(row_limit=50))
        if divergence_rows:
            st.dataframe(divergence_rows, use_container_width=True)
        else:
            st.info("Nenhuma divergência calculável ainda.")
    except Exception as exc:  # noqa: BLE001
        st.error(f"Erro ao consultar divergência heurística × modelo: {exc}")

st.divider()
st.subheader("Sinistros fora do SLA (aguardando revisão manual)")
sla_hours = st.number_input("SLA (horas)", min_value=1, max_value=168, value=24)

try:
    breach_rows = run_query(connection, build_sla_breach_query(sla_hours=int(sla_hours)))
    if breach_rows:
        st.dataframe(breach_rows, use_container_width=True)
    else:
        st.success("Nenhum sinistro fora do SLA.")
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar SLA: {exc}")
