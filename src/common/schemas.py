from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CLAIM_EVENT_SCHEMA = StructType(
    [
        StructField("claim_id", StringType(), nullable=False),
        StructField("policy_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_timestamp", TimestampType(), nullable=False),
        StructField("amount", DecimalType(18, 2), nullable=True),
        StructField("region", StringType(), nullable=True),
        StructField("vehicle_type", StringType(), nullable=True),
        StructField("source", StringType(), nullable=True),
    ]
)

POLICY_EVENT_SCHEMA = StructType(
    [
        StructField("policy_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_timestamp", TimestampType(), nullable=False),
        StructField("premium_amount", DecimalType(18, 2), nullable=True),
        StructField("coverage_type", StringType(), nullable=True),
        StructField("region", StringType(), nullable=True),
        StructField("source", StringType(), nullable=True),
    ]
)

CUSTOMER_EVENT_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_timestamp", TimestampType(), nullable=False),
        StructField("region", StringType(), nullable=True),
        StructField("source", StringType(), nullable=True),
    ]
)

# consent_status: "GRANTED" ou "REVOKED". target_institution reaproveita as
# 3 instituições fictícias do módulo regulatório (insurer_a/b/c) — ver
# src/open_insurance/consent_scd.py e docs/ARCHITECTURE.md.
#
# Chave é policy_id, não customer_id: susep_loader.py e ans_loader.py zeram
# customer_id de propósito (dados reais anonimizados, sem identificador de
# cliente) — customer_id é sempre NULL em gold.claims. policy_id é o único
# identificador estável e sempre populado (real ou fallback uuid4).
CONSENT_EVENT_SCHEMA = StructType(
    [
        StructField("policy_id", StringType(), nullable=False),
        StructField("consent_status", StringType(), nullable=False),
        StructField("target_institution", StringType(), nullable=False),
        StructField("scope", StringType(), nullable=True),
        StructField("event_timestamp", TimestampType(), nullable=False),
    ]
)

# Superset de campos crus (não tipados — datas/moeda ficam StringType de
# propósito) das 3 fontes fictícias de src/ingestion/producer/datasets/
# regulatory_feeds.py. from_json tolera chaves ausentes por registro (viram
# null), então um único schema acomoda os 3 layouts heterogêneos no mesmo
# tópico/tabela — padronização real acontece em src/regulatory/standardize.py,
# não aqui.
REGULATORY_CLAIM_RAW_SCHEMA = StructType(
    [
        StructField("external_reference_id", StringType(), nullable=False),
        StructField("source_system", StringType(), nullable=False),
        # Seguradora Alfa (insurer_a): snake_case em português.
        StructField("numero_apolice", StringType(), nullable=True),
        StructField("data_ocorrencia", StringType(), nullable=True),
        StructField("valor_indenizacao", StringType(), nullable=True),
        StructField("regiao_sinistro", StringType(), nullable=True),
        StructField("codigo_causa", StringType(), nullable=True),
        # Banco Beta Seguros (insurer_b): UPPER_SNAKE.
        StructField("POLICY_NUM", StringType(), nullable=True),
        StructField("EVENT_DATE", StringType(), nullable=True),
        StructField("CLAIM_AMOUNT", StringType(), nullable=True),
        StructField("REGION_CODE", StringType(), nullable=True),
        StructField("CAUSE_CD", StringType(), nullable=True),
        # Seguradora Gama (insurer_c): camelCase.
        StructField("policyId", StringType(), nullable=True),
        StructField("occurrenceDate", StringType(), nullable=True),
        StructField("amountCents", StringType(), nullable=True),
        StructField("regionCode", StringType(), nullable=True),
        StructField("causeCode", StringType(), nullable=True),
    ]
)

SCHEMA_REGISTRY = {
    "claim-opened": CLAIM_EVENT_SCHEMA,
    "claim-updated": CLAIM_EVENT_SCHEMA,
    "policy-created": POLICY_EVENT_SCHEMA,
    "customer-updated": CUSTOMER_EVENT_SCHEMA,
    "regulatory-claim-report": REGULATORY_CLAIM_RAW_SCHEMA,
    "consent-updated": CONSENT_EVENT_SCHEMA,
}
