import streamlit as st
from queries import (
    build_cost_usage_by_job_query,
    build_cost_usage_by_product_query,
    get_connection,
    run_query,
)

st.title("Custos")
st.caption(
    "Consumo real de DBUs via system.billing.usage (system table nativa do Unity "
    "Catalog). Requer habilitação prévia pelo account admin do workspace "
    "(Settings → Feature enablement → System tables) — se não estiver habilitada "
    "neste workspace, as consultas abaixo falham e mostram essa mensagem em vez "
    "de travar a página."
)

# Perfil de compute dos jobs do bundle (resources/jobs.*.yml) — hardcoded
# porque o app roda com source_code_path restrito a
# apps/insurance_platform_app (ver resources/visualization.yml), sem acesso
# em runtime ao resto do repo pra ler os YAMLs. Reflete o estado real do
# bundle no momento em que esta página foi escrita; atualizar se os jobs
# mudarem de modo.
JOB_COMPUTE_PROFILE = [
    {"job": "bronze_ingest", "modo": "continuous (24/7)", "topicos/cadência": "claim-opened, policy-created, customer-updated"},
    {"job": "silver_transform", "modo": "continuous (24/7)", "topicos/cadência": "—"},
    {"job": "regulatory_bronze_ingest", "modo": "continuous (24/7)", "topicos/cadência": "regulatory-claim-report"},
    {"job": "fraud_score_stream", "modo": "schedule */5min", "topicos/cadência": "convertido de continuous em 2026-07-13"},
    {"job": "gold_aggregate", "modo": "schedule */10min", "topicos/cadência": "—"},
    {"job": "pipeline_latency_monitor", "modo": "schedule */15min", "topicos/cadência": "—"},
    {"job": "open_insurance_bronze_ingest", "modo": "schedule */15min", "topicos/cadência": "—"},
    {"job": "open_insurance_pipeline", "modo": "schedule */30min", "topicos/cadência": "—"},
    {"job": "regulatory_pipeline", "modo": "schedule */30min", "topicos/cadência": "—"},
    {"job": "model_drift_monitor", "modo": "schedule a cada 6h", "topicos/cadência": "—"},
    {"job": "fraud_model_training", "modo": "schedule a cada 6h (+30min)", "topicos/cadência": "só retreina se 1ª execução, drift detectado, ou --force"},
]

COST_RECOMMENDATIONS = [
    "Converter bronze_ingest, silver_transform e regulatory_bronze_ingest de "
    "continuous pra schedule — mesmo padrão já aplicado em fraud_score_stream "
    "em 2026-07-13 por contenção de cota serverless (ver docs/ARCHITECTURE.md). "
    "bronze_ingest já roda internamente via trigger(availableNow=True); a troca "
    "é só configuração do bundle (continuous: → schedule:), sem mudança de "
    "código — já provado ao reaproveitar o mesmo script pra "
    "open_insurance_bronze_ingest.",
    "Ambiente dev hoje paga um custo de oportunidade, não de DBU: bronze_ingest "
    "e silver_transform ficam PAUSED em dev (databricks.yml) só pra caber na "
    "cota de serverless compartilhada com prod. Convertê-los pra schedule "
    "destravaria testes em dev sem reintroduzir o consumo 24/7 que causou o "
    "PAUSED em primeiro lugar.",
    "Cadências próximas (gold_aggregate */10min, pipeline_latency_monitor "
    "*/15min) merecem atenção se algum dia competirem por cota como "
    "fraud_score_stream competiu — espaçar os cron primeiro é mais barato que "
    "aumentar compute.",
]

COST_ALREADY_OPTIMIZED = [
    "fraud_model_training só retreina de fato na 1ª execução, quando a última "
    "checagem de drift detectou algo, ou com --force explícito — evita pagar "
    "um treino a cada 6h sem necessidade (src/ml/train_model.py).",
    "O SQL Warehouse da camada de visualização reaproveita o 'Serverless "
    "Starter Warehouse' pré-provisionado do workspace em vez de criar um "
    "endpoint serverless novo faturável (terraform/warehouse.tf) — contas "
    "trial nem conseguem provisionar um segundo endpoint.",
    "open_insurance e regulatory_pipeline já nasceram como schedule, não "
    "continuous — lição do RESOURCE_EXHAUSTED anterior aplicada de saída, sem "
    "precisar de um segundo incidente pra ensinar isso.",
]

st.subheader("Janela de oportunidade — otimização de custo")
st.caption(
    "Baseado na configuração real dos jobs do bundle (resources/jobs.*.yml), "
    "não em system.billing.usage — por isso funciona mesmo se as system "
    "tables não estiverem habilitadas neste workspace (ver aviso abaixo)."
)

continuous_jobs = [row for row in JOB_COMPUTE_PROFILE if row["modo"].startswith("continuous")]
profile_cols = st.columns(3)
profile_cols[0].metric("Jobs no bundle", len(JOB_COMPUTE_PROFILE))
profile_cols[1].metric("Continuous (compute 24/7)", len(continuous_jobs))
profile_cols[2].metric("Schedule (compute só na janela)", len(JOB_COMPUTE_PROFILE) - len(continuous_jobs))

st.dataframe(JOB_COMPUTE_PROFILE, use_container_width=True)

st.markdown("**Oportunidades de redução de custo:**")
for recommendation in COST_RECOMMENDATIONS:
    st.markdown(f"- {recommendation}")

st.markdown("**Já otimizado (evitar retrabalho aqui):**")
for item in COST_ALREADY_OPTIMIZED:
    st.markdown(f"- {item}")

st.divider()
st.subheader("Consumo real (DBUs) — validação contra system.billing.usage")
st.caption(
    "Os números abaixo, quando disponíveis, confirmam ou refutam a análise "
    "estrutural acima — jobs continuous devem concentrar a maior parte do "
    "consumo."
)

try:
    connection = get_connection()
except Exception as exc:  # noqa: BLE001
    st.error(f"Não foi possível conectar ao SQL Warehouse: {exc}")
    st.stop()

lookback_days = st.slider("Janela (dias)", min_value=1, max_value=90, value=30)

st.subheader("Consumo por produto (DBUs)")
try:
    product_rows = run_query(connection, build_cost_usage_by_product_query(lookback_days))
    if product_rows:
        st.dataframe(product_rows, use_container_width=True)
    else:
        st.info("Sem dados de consumo no período selecionado.")
except Exception as exc:  # noqa: BLE001
    st.warning(
        "Não foi possível consultar system.billing.usage — provavelmente as system "
        f"tables não estão habilitadas neste workspace. Detalhe: {exc}"
    )

st.divider()
st.subheader("Top jobs por consumo (DBUs)")
try:
    job_rows = run_query(connection, build_cost_usage_by_job_query(lookback_days, row_limit=20))
    if job_rows:
        st.dataframe(job_rows, use_container_width=True)
    else:
        st.info("Sem dados de consumo por job no período selecionado.")
except Exception as exc:  # noqa: BLE001
    st.warning(f"Não foi possível consultar consumo por job. Detalhe: {exc}")
