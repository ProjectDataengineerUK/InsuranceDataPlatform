import streamlit as st
from queries import (
    build_dq_summary_query,
    build_reconciliation_query,
    build_source_system_volume_query,
    build_susep_claims_query,
    build_susep_compliance_summary_query,
    get_connection,
    run_query,
)

st.title("Conformidade SUSEP")
st.caption(
    "Status por contrato calculado em src/regulatory/gold_susep_export.py: campos "
    "obrigatórios ausentes, código de causa fora da Tabela V oficial, ou discrepância "
    "de reconciliação entre fontes tornam um contrato 'fora das regras'. Região não "
    "entra nessa checagem — o allow-list de região é um proxy data-driven sem tabela "
    "oficial confirmada (ver docs/ARCHITECTURE.md), não um critério de conformidade."
)

try:
    connection = get_connection()
except Exception as exc:  # noqa: BLE001
    st.error(f"Não foi possível conectar ao SQL Warehouse: {exc}")
    st.stop()

try:
    summary_rows = run_query(connection, build_susep_compliance_summary_query())
    compliant_total = sum(r["total"] for r in summary_rows if r["susep_compliant"])
    non_compliant_total = sum(r["total"] for r in summary_rows if not r["susep_compliant"])
    cols = st.columns(2)
    cols[0].metric("Contratos aderentes", compliant_total)
    cols[1].metric("Contratos fora das regras", non_compliant_total)
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar resumo de conformidade: {exc}")

st.divider()

view = st.radio(
    "Visualizar",
    ["Contratos aderentes", "Contratos fora das regras"],
    horizontal=True,
)

try:
    rows = run_query(
        connection, build_susep_claims_query(compliant=(view == "Contratos aderentes"))
    )
    if rows:
        st.dataframe(rows, use_container_width=True)
    else:
        st.info("Nenhum contrato encontrado nesta categoria.")
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar contratos: {exc}")

st.divider()
st.subheader("Volume por banco/seguradora")
try:
    volume_rows = run_query(connection, build_source_system_volume_query())
    if volume_rows:
        st.dataframe(volume_rows, use_container_width=True)
    else:
        st.info("Sem dados de bancos/seguradoras ainda.")
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar volume por fonte: {exc}")

st.divider()
st.subheader("Discrepâncias de reconciliação entre fontes")
st.caption(
    "src/regulatory/reconcile.py — por external_reference_id, entre as fontes "
    "insurer_a/b/c. amount_mismatch: spread entre valores reportados acima da "
    "tolerância (2% do menor valor ou R$ 5,00, o que for maior). missing_in_source: "
    "uma fonte ativa no batch não reportou esse sinistro. resolved_amount é a "
    "mediana entre as fontes que reportaram — decisão de MVP, não o que um "
    "processo regulatório real faria."
)
try:
    reconciliation_rows = run_query(connection, build_reconciliation_query(row_limit=200))
    if reconciliation_rows:
        by_type: dict[str, int] = {}
        for row in reconciliation_rows:
            by_type[row["discrepancy_type"]] = by_type.get(row["discrepancy_type"], 0) + 1

        recon_cols = st.columns(len(by_type) + 1)
        recon_cols[0].metric("Discrepâncias recentes (últimas 200)", len(reconciliation_rows))
        for col, (discrepancy_type, count) in zip(recon_cols[1:], by_type.items(), strict=False):
            col.metric(discrepancy_type, count)

        st.dataframe(reconciliation_rows, use_container_width=True)
    else:
        st.success("Nenhuma discrepância de reconciliação recente.")
except Exception as exc:  # noqa: BLE001
    st.error(
        "aguardando a primeira execução do pipeline regulatório"
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc)
        else f"Erro ao consultar reconciliação: {exc}"
    )

st.divider()
st.subheader("Qualidade de dados regulatório (cumulativo)")
st.caption(
    "gold.regulatory_dq_summary — soma tudo desde a primeira execução, sem "
    "conceito de run_id por enquanto (ver docs/ARCHITECTURE.md); útil pra "
    "tendência geral, não pra isolar uma execução específica."
)
try:
    dq_summary_rows = run_query(connection, build_dq_summary_query(row_limit=50))
    if dq_summary_rows:
        latest = dq_summary_rows[0]
        dq_total = latest.get("dq_checks_total") or 0
        dq_failed = latest.get("dq_checks_failed") or 0
        dq_cols = st.columns(4)
        dq_cols[0].metric("Checagens de DQ (cumulativo)", dq_total)
        dq_cols[1].metric(
            "Taxa de falha", f"{dq_failed / dq_total * 100:.1f}%" if dq_total else "sem dados"
        )
        dq_cols[2].metric("Amount mismatch (cumulativo)", latest.get("amount_mismatch_count") or 0)
        dq_cols[3].metric("Missing in source (cumulativo)", latest.get("missing_in_source_count") or 0)
        st.dataframe(dq_summary_rows, use_container_width=True)
    else:
        st.info("Nenhum resumo de qualidade de dados regulatório ainda.")
except Exception as exc:  # noqa: BLE001
    st.error(
        "aguardando a primeira execução do export SUSEP"
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc)
        else f"Erro ao consultar resumo de DQ regulatório: {exc}"
    )
