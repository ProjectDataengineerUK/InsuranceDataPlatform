import streamlit as st
from queries import (
    build_open_insurance_consent_history_query,
    build_open_insurance_consent_summary_query,
    build_open_insurance_shareable_query,
    get_connection,
    run_query,
)

st.title("Open Insurance")
st.caption(
    "Fluxo de consentimento pra compartilhamento de dados: bronze.consent_events "
    "→ silver.consent_current (SCD2) → gold.customer_consent (estado vigente) → "
    "gold.open_insurance_shareable (payload compartilhável). Chave é policy_id, "
    "não customer_id — gold.claims.customer_id é sempre NULL nas duas fontes "
    "originais (SUSEP/ANS), ver docs/ARCHITECTURE.md. gold.open_insurance_shareable "
    "só inclui apólices com consent_status = 'GRANTED'."
)

try:
    connection = get_connection()
except Exception as exc:  # noqa: BLE001
    st.error(f"Não foi possível conectar ao SQL Warehouse: {exc}")
    st.stop()

try:
    summary_rows = run_query(connection, build_open_insurance_consent_summary_query())
except Exception as exc:  # noqa: BLE001
    if "TABLE_OR_VIEW_NOT_FOUND" in str(exc):
        st.info(
            "Nenhum consentimento processado ainda (aguardando a primeira execução "
            "de open_insurance_pipeline)."
        )
        st.stop()
    st.error(f"Erro ao consultar resumo de consentimento: {exc}")
    st.stop()

if not summary_rows:
    st.info("Nenhum consentimento vigente ainda.")
    st.stop()

granted = sum(row["total"] for row in summary_rows if row["consent_status"] == "GRANTED")
revoked = sum(row["total"] for row in summary_rows if row["consent_status"] != "GRANTED")
total_consents = granted + revoked

cols = st.columns(3)
cols[0].metric("Apólices com consentimento vigente", total_consents)
cols[1].metric("Concedido (GRANTED)", granted)
cols[2].metric("Revogado/outro status", revoked)

by_institution: dict[str, int] = {}
for row in summary_rows:
    institution = row["target_institution"] or "desconhecida"
    by_institution[institution] = by_institution.get(institution, 0) + row["total"]

st.caption("Consentimento vigente por instituição de destino")
st.bar_chart(by_institution)

st.divider()
st.subheader("Payload compartilhável (consentimento ativo)")
st.caption(
    "gold.open_insurance_shareable — o que seria de fato enviado à instituição de "
    "destino hoje, sob consentimento GRANTED. LEFT JOIN com sinistros: uma apólice "
    "sem nenhum sinistro ainda aparece (histórico limpo é um dado tão relevante "
    "quanto um com sinistros, ver DESIGN_OPEN_INSURANCE.md)."
)
row_limit = st.slider("Quantidade de registros", min_value=10, max_value=500, value=100, step=10)
try:
    shareable_rows = run_query(connection, build_open_insurance_shareable_query(row_limit=row_limit))
    if shareable_rows:
        st.dataframe(shareable_rows, use_container_width=True)
    else:
        st.info("Nenhum registro compartilhável ainda.")
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar payload compartilhável: {exc}")

st.divider()
st.subheader("Histórico de consentimento (SCD2)")
st.caption(
    "silver.consent_current — inclui versões passadas (is_current = false), não só "
    "o estado vigente. Útil pra auditoria: quando um consentimento foi concedido, "
    "revogado ou trocado de instituição."
)
only_current = st.checkbox("Mostrar só o estado vigente", value=False)
try:
    history_rows = run_query(connection, build_open_insurance_consent_history_query(row_limit=200))
    if only_current:
        history_rows = [row for row in history_rows if row["is_current"]]
    if history_rows:
        st.dataframe(history_rows, use_container_width=True)
    else:
        st.info("Nenhum evento de consentimento processado ainda.")
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar histórico de consentimento: {exc}")
