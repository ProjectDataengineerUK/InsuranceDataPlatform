import streamlit as st
from queries import build_dq_results_query, build_model_drift_query, get_connection, run_query

st.title("DataOps / MLOps Sentinela")
st.caption(
    "Qualidade de dados (src/quality/checks.py, todas as camadas) e drift de "
    "features do modelo de fraude (src/monitoring/model_drift.py, a cada 6h). "
    "Nenhum dos dois falha o pipeline sozinho — ficam registrados aqui pra "
    "auditoria e decisão de retrain."
)

try:
    connection = get_connection()
except Exception as exc:  # noqa: BLE001
    st.error(f"Não foi possível conectar ao SQL Warehouse: {exc}")
    st.stop()

try:
    dq_rows = run_query(connection, build_dq_results_query(row_limit=200))
except Exception as exc:  # noqa: BLE001
    st.error(f"Erro ao consultar qualidade de dados: {exc}")
    dq_rows = []

# monitoring._model_drift_results só é criada quando model_drift_monitor roda
# com uma baseline já escrita (train_model.py só escreve baseline quando
# promove um champion) — antes do primeiro champion promovido,
# TABLE_OR_VIEW_NOT_FOUND aqui é "sem dados ainda", não erro. drift_rows fica
# None nesse caso (distinto de [], que é "rodou mas não achou nada").
drift_rows: list[dict] | None
try:
    drift_rows = run_query(connection, build_model_drift_query(row_limit=100))
except Exception as exc:  # noqa: BLE001
    if "TABLE_OR_VIEW_NOT_FOUND" in str(exc):
        drift_rows = None
    else:
        st.error(f"Erro ao consultar drift do modelo: {exc}")
        drift_rows = []

st.subheader("Maturidade de monitoramento — resumo")
st.caption(
    "Cobertura e saúde dos dois sentinelas: quantas checagens rodaram e que "
    "fração passou/está estável, não só a lista de falhas."
)
maturity_cols = st.columns(4)

dq_total = len(dq_rows)
dq_passed = sum(1 for row in dq_rows if row["passed"])
maturity_cols[0].metric("Checagens de DQ (últimas 200)", dq_total)
maturity_cols[1].metric(
    "Taxa de aprovação DQ", f"{dq_passed / dq_total * 100:.1f}%" if dq_total else "sem dados"
)

if drift_rows is None:
    maturity_cols[2].metric("Checagens de drift", "aguardando 1º champion")
    maturity_cols[3].metric("Taxa de features em drift", "—")
else:
    drift_total = len(drift_rows)
    drift_detected_count = sum(1 for row in drift_rows if row["drift_detected"])
    maturity_cols[2].metric("Checagens de drift (últimas 100)", drift_total)
    maturity_cols[3].metric(
        "Taxa de features em drift",
        f"{drift_detected_count / drift_total * 100:.1f}%" if drift_total else "sem dados",
    )

st.divider()
st.subheader("Qualidade de dados — por camada")
if dq_rows:
    layer_stats: dict[str, dict[str, int]] = {}
    for row in dq_rows:
        layer = row["table_name"].split(".")[0] if row["table_name"] else "desconhecida"
        stats = layer_stats.setdefault(layer, {"total": 0, "passed": 0})
        stats["total"] += 1
        stats["passed"] += 1 if row["passed"] else 0

    layer_rows = [
        {
            "camada": layer,
            "checagens": stats["total"],
            "aprovadas": stats["passed"],
            "taxa_aprovacao_pct": round(stats["passed"] / stats["total"] * 100, 1) if stats["total"] else 0.0,
        }
        for layer, stats in sorted(layer_stats.items())
    ]
    st.dataframe(layer_rows, use_container_width=True)
    st.bar_chart({row["camada"]: row["taxa_aprovacao_pct"] for row in layer_rows})
else:
    st.info("Nenhuma checagem de qualidade de dados registrada ainda.")

st.divider()
st.subheader("Qualidade de dados — checagens recentes")
only_failed = st.checkbox("Mostrar só falhas", value=True)

display_rows = [row for row in dq_rows if not row["passed"]] if only_failed else dq_rows
if display_rows:
    st.dataframe(display_rows, use_container_width=True)
elif dq_rows:
    st.success("Nenhuma falha de qualidade de dados nas últimas checagens.")
else:
    st.info("Nenhuma checagem de qualidade de dados registrada ainda.")

st.divider()
st.subheader("Drift de features do modelo de fraude")

if drift_rows is None:
    st.info("Nenhuma checagem de drift registrada ainda (aguardando o primeiro modelo campeão promovido).")
elif not drift_rows:
    st.info("Nenhuma checagem de drift registrada ainda.")
else:
    detected = [row for row in drift_rows if row["drift_detected"]]
    st.metric("Features com drift detectado (últimas 100 checagens)", len(detected))

    # drift_rows já vem ordenado DESC por _checked_at (build_model_drift_query)
    # — o primeiro valor visto por feature é o mais recente.
    latest_ratio_by_feature: dict[str, float] = {}
    for row in drift_rows:
        latest_ratio_by_feature.setdefault(row["feature_name"], row["drift_ratio"])
    st.caption("Drift ratio mais recente por feature (1.0 = igual à baseline)")
    st.bar_chart(latest_ratio_by_feature)

    st.dataframe(drift_rows, use_container_width=True)
