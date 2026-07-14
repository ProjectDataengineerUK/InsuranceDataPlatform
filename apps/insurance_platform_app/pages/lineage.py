import streamlit as st
from databricks.sdk.core import Config
from health import GREEN, STATUS_COLORS, STATUS_LABELS, UNKNOWN, freshness_status
from queries import build_table_count_query, build_table_freshness_query, get_connection, run_query

st.title("Lineage")
st.caption(
    "Fluxo real de dados de ponta a ponta pra tabela selecionada — contagem de "
    "linhas e freshness ao vivo em cada etapa (Kafka → Bronze → Silver → Gold → "
    "consumo), calculado a partir das mesmas tabelas que as demais páginas "
    "consultam, não só um link pro Catalog Explorer."
)

# stage: id, label, tabela física consultável (None = Kafka/consumo, sem SQL
# direto neste app) e coluna de timestamp pra freshness (None = tabela sem
# coluna de ingestão/geração, só dá pra contar linhas). Hardcoded pelo mesmo
# motivo de custos.py/performance.py: o app roda com source_code_path restrito
# a apps/insurance_platform_app (ver resources/visualization.yml), sem acesso
# em runtime ao resto do repo.
LINEAGE_FLOWS: dict[str, list[dict]] = {
    "gold.claims": [
        {"id": "kafka", "label": "Kafka\nclaim-opened", "table": None, "freshness_column": None},
        {"id": "bronze", "label": "Bronze\nbronze.claims", "table": "bronze.claims", "freshness_column": "_ingested_at"},
        {
            "id": "silver",
            "label": "Silver\nsilver.claims\n(dedup + DQ)",
            "table": "silver.claims",
            "freshness_column": "_ingested_at",
        },
        {
            "id": "gold",
            "label": "Gold\ngold.claims\n(fraud_score + model_fraud_score)",
            "table": "gold.claims",
            "freshness_column": "_scored_at",
        },
        {
            "id": "downstream",
            "label": "Consumo\nProbabilidade de Fraude, SLA,\ndrift/retrain",
            "table": None,
            "freshness_column": None,
        },
    ],
    "silver.regulatory_claims": [
        {
            "id": "kafka",
            "label": "Kafka\nregulatory-claim-report\n(insurer_a/b/c)",
            "table": None,
            "freshness_column": None,
        },
        {
            "id": "bronze",
            "label": "Bronze\nbronze.regulatory_claims_raw",
            "table": "bronze.regulatory_claims_raw",
            "freshness_column": "_ingested_at",
        },
        {
            "id": "silver",
            "label": "Silver\nsilver.regulatory_claims\n(standardize.py)",
            "table": "silver.regulatory_claims",
            "freshness_column": "_ingested_at",
        },
        {
            "id": "downstream",
            "label": "Consumo\nreconcile.py → gold.regulatory_*",
            "table": None,
            "freshness_column": None,
        },
    ],
    "gold.regulatory_susep_claims": [
        {
            "id": "kafka",
            "label": "Kafka\nregulatory-claim-report\n(insurer_a/b/c)",
            "table": None,
            "freshness_column": None,
        },
        {
            "id": "bronze",
            "label": "Bronze\nbronze.regulatory_claims_raw",
            "table": "bronze.regulatory_claims_raw",
            "freshness_column": "_ingested_at",
        },
        {
            "id": "silver",
            "label": "Silver\nsilver.regulatory_claims",
            "table": "silver.regulatory_claims",
            "freshness_column": "_ingested_at",
        },
        {
            "id": "reconcile",
            "label": "Reconciliação\n(mediana entre fontes)",
            "table": "monitoring._regulatory_reconciliation_results",
            "freshness_column": "detected_at",
        },
        {
            "id": "gold",
            "label": "Gold\ngold.regulatory_susep_claims",
            "table": "gold.regulatory_susep_claims",
            # Sem coluna de ingestão/geração nesta tabela (só campos de negócio:
            # COD_APO/D_OCORR/INDENIZ/REGIAO/CAUSA — ver
            # gold_susep_export.py::build_gold_susep_claims) — só contagem.
            "freshness_column": None,
        },
        {"id": "downstream", "label": "Consumo\nConformidade SUSEP", "table": None, "freshness_column": None},
    ],
    "gold.regulatory_dq_summary": [
        {
            "id": "kafka",
            "label": "Kafka\nregulatory-claim-report\n(insurer_a/b/c)",
            "table": None,
            "freshness_column": None,
        },
        {
            "id": "bronze",
            "label": "Bronze\nbronze.regulatory_claims_raw",
            "table": "bronze.regulatory_claims_raw",
            "freshness_column": "_ingested_at",
        },
        {
            "id": "silver",
            "label": "Silver\nsilver.regulatory_claims",
            "table": "silver.regulatory_claims",
            "freshness_column": "_ingested_at",
        },
        {
            "id": "gold",
            "label": "Gold\ngold.regulatory_dq_summary\n(cumulativo)",
            "table": "gold.regulatory_dq_summary",
            "freshness_column": "_generated_at",
        },
        {
            "id": "downstream",
            "label": "Consumo\nDataOps/MLOps Sentinela",
            "table": None,
            "freshness_column": None,
        },
    ],
}

TABLES = list(LINEAGE_FLOWS.keys())

table = st.selectbox("Tabela", TABLES)

try:
    connection = get_connection()
except Exception as exc:  # noqa: BLE001
    st.error(f"Não foi possível conectar ao SQL Warehouse: {exc}")
    st.stop()

stages = LINEAGE_FLOWS[table]
statuses: dict[str, str] = {}
node_labels: dict[str, str] = {}
detail_rows: list[dict] = []

for stage in stages:
    stage_id = stage["id"]
    physical_table = stage["table"]
    label = stage["label"]

    if physical_table is None:
        statuses[stage_id] = UNKNOWN
        node_labels[stage_id] = label
        detail_rows.append(
            {
                "etapa": label.replace("\n", " "),
                "tabela": "—",
                "linhas": "—",
                "última atualização": "sem checagem direta (roda fora do Databricks ou é consumo downstream)",
            }
        )
        continue

    row_count = None
    count_error = None
    try:
        count_rows = run_query(connection, build_table_count_query(physical_table))
        row_count = count_rows[0]["total"] or 0
    except Exception as exc:  # noqa: BLE001
        count_error = str(exc)

    latest_ts = None
    if row_count is not None and stage["freshness_column"]:
        try:
            fresh_rows = run_query(connection, build_table_freshness_query(physical_table, stage["freshness_column"]))
            latest_ts = fresh_rows[0]["latest_ts"] if fresh_rows else None
        except Exception:  # noqa: BLE001
            latest_ts = None

    if row_count is None:
        statuses[stage_id] = UNKNOWN
        detail_note = (
            "tabela ainda não existe (sem eventos processados ainda)"
            if "TABLE_OR_VIEW_NOT_FOUND" in (count_error or "")
            else f"erro: {count_error}"
        )
    elif stage["freshness_column"]:
        statuses[stage_id] = freshness_status(latest_ts)
        detail_note = f"último registro: {latest_ts}" if latest_ts else "sem dados ainda"
    else:
        statuses[stage_id] = GREEN if row_count else UNKNOWN
        detail_note = "sem coluna de timestamp nesta tabela — só contagem"

    node_labels[stage_id] = f"{label}\n{row_count if row_count is not None else '?'} linha(s)"
    detail_rows.append(
        {
            "etapa": label.replace("\n", " "),
            "tabela": physical_table,
            "linhas": row_count if row_count is not None else "erro",
            "última atualização": detail_note,
        }
    )


def _build_lineage_dot(stages: list[dict], statuses: dict[str, str], node_labels: dict[str, str]) -> str:
    lines = ["digraph lineage {", "  rankdir=LR;", '  node [shape=box, style="filled,rounded", fontname="Helvetica"];']
    for stage in stages:
        stage_id = stage["id"]
        color = STATUS_COLORS[statuses.get(stage_id, UNKNOWN)]
        label = node_labels.get(stage_id, stage["label"])
        lines.append(f'  {stage_id} [label="{label}", fillcolor="{color}"];')
    for previous, current in zip(stages, stages[1:], strict=False):
        lines.append(f"  {previous['id']} -> {current['id']};")
    lines.append("}")
    return "\n".join(lines)


st.graphviz_chart(_build_lineage_dot(stages, statuses, node_labels), use_container_width=True)

legend_cols = st.columns(4)
for col, status in zip(legend_cols, ["green", "yellow", "red", UNKNOWN], strict=False):
    col.caption(STATUS_LABELS[status])

st.divider()
st.subheader("Detalhe por etapa")
st.dataframe(detail_rows, use_container_width=True)

st.divider()
schema, table_name = table.split(".", 1)
try:
    host = Config().host
    lineage_url = f"https://{host}/explore/data/{{catalog}}/{schema}/{table_name}?o=0&tab=lineage"
    st.link_button("Abrir lineage completo no Catalog Explorer", lineage_url)
    st.caption(
        "Substitua {catalog} pelo catálogo do ambiente (ex.: insurance_prod) — "
        "o app não injeta esse valor automaticamente ainda."
    )
except Exception as exc:  # noqa: BLE001
    st.error(f"Não foi possível montar o link de lineage: {exc}")
