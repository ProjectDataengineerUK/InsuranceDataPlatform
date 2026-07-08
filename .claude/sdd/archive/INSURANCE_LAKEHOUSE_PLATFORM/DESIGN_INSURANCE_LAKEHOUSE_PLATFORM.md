# DESIGN: Insurance Lakehouse Platform

> Design técnico para uma plataforma de dados de seguros 100% Databricks (Kafka + Spark Structured Streaming, sem DLT/Lakeflow), com arquitetura medallion, governança via Unity Catalog e CI/CD via GitHub Actions + Databricks Asset Bundles.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | INSURANCE_LAKEHOUSE_PLATFORM |
| **Date** | 2026-07-08 |
| **Author** | design-agent |
| **DEFINE** | [DEFINE_INSURANCE_LAKEHOUSE_PLATFORM.md](./DEFINE_INSURANCE_LAKEHOUSE_PLATFORM.md) |
| **Status** | ✅ Shipped |

---

## Architecture Overview

```text
┌───────────────────────────────────────────────────────────────────────────────────┐
│                         INSURANCE LAKEHOUSE PLATFORM                                │
├───────────────────────────────────────────────────────────────────────────────────┤
│                                                                                       │
│  [SUSEP CSV]   [ANS CSV]   [Open Insurance API — fast-follow]                       │
│        │            │                    │                                          │
│        └────────────┴────────────────────┘                                          │
│                      │                                                               │
│                      ▼                                                               │
│           ┌─────────────────────┐                                                    │
│           │  Kafka Producer      │  Docker · Python · confluent-kafka                │
│           │  (replay controlado) │  lê CSVs, publica eventos em ritmo configurável   │
│           └──────────┬───────────┘                                                   │
│                      │                                                               │
│                      ▼                                                               │
│           ┌─────────────────────┐                                                    │
│           │  Confluent Cloud     │  Tópicos: claim-opened, claim-updated,            │
│           │  (Kafka, free tier)  │  policy-created, customer-updated                 │
│           └──────────┬───────────┘                                                   │
│                      │                                                               │
│                      ▼                                                               │
│  ══════════════════════ DATABRICKS (workspace único) ══════════════════════════      │
│                      │                                                               │
│           ┌─────────────────────┐                                                    │
│           │ Bronze Job           │  Spark Structured Streaming (Databricks Job)      │
│           │ (Kafka → Delta)      │  raw, append-only, schema-on-read                 │
│           └──────────┬───────────┘                                                   │
│                      ▼                                                               │
│              [Bronze Delta Tables]                                                   │
│                      │                                                               │
│           ┌─────────────────────┐                                                    │
│           │ Silver Job           │  Spark (streaming/micro-batch), dedup, parsing,   │
│           │ (dedup/quality)      │  normalização, quality checks (custom PySpark)    │
│           └──────────┬───────────┘                                                   │
│                      ▼                                                               │
│              [Silver Delta Tables]                                                   │
│                      │                                                               │
│           ┌─────────────────────┐                                                    │
│           │ Gold Job             │  agregações + Fraud Scoring (streaming) +         │
│           │ (analytics + fraude) │  Auto-Approval rule engine                        │
│           └──────────┬───────────┘                                                   │
│                      ▼                                                               │
│              [Gold Delta Tables]                                                     │
│                      │                                                               │
│   ┌──────────────────┼──────────────────┬────────────────────┐                      │
│   ▼                  ▼                  ▼                    ▼                      │
│ [SQL Warehouse    [MLflow —        [Databricks         [API externa                  │
│  + Dashboards]     modelos de       Workflows           — fast-follow]               │
│                    fraude/churn]    (alertas SLA)]                                   │
│                                                                                        │
│  Governança: Unity Catalog (catálogos dev/staging/prod, RLS, masking, lineage, CDF)  │
│  IaC: Terraform (UC, secrets, policies) + Databricks Asset Bundles (jobs/workflows)  │
│  CI/CD: GitHub Actions (test → validate bundle → deploy dev → deploy prod)           │
└───────────────────────────────────────────────────────────────────────────────────┘
```

---

## Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| Dataset Loader | Baixa e normaliza os CSVs públicos (SUSEP, ANS) para uso local do producer | Python, `requests`/`httpx`, pandas |
| Kafka Producer | Republica linhas dos datasets como eventos, em ritmo configurável, simulando tempo real | Python, `confluent-kafka`, Docker |
| Kafka Broker | Transporte de eventos entre producer e Databricks | Confluent Cloud (free tier) |
| Bronze Ingestion Job | Consome os tópicos Kafka e grava raw em Delta, sem transformação | PySpark, Structured Streaming, Databricks Jobs |
| Silver Transformation Job | Dedup, parsing, normalização, schema evolution, quality checks | PySpark (streaming/micro-batch) |
| Data Quality Module | Funções reutilizáveis de validação (not_null, unique, range, freshness) e registro de resultados | PySpark custom (sem DLT expectations) |
| Gold Aggregation Job | Agregações de negócio (sinistralidade, ticket médio, etc.) | PySpark (batch incremental via Workflows) |
| Fraud Scoring Module | Score de fraude em streaming (distância, horário, histórico, frequência) | PySpark + MLflow model serving/UDF |
| ML Training Pipeline | Treina modelos (fraude, cancelamento, inadimplência) e registra no Model Registry | PySpark ML / scikit-learn + MLflow |
| Unity Catalog | Governança: catálogos por ambiente, RLS, column masking, lineage, audit logs | Unity Catalog |
| Databricks Workflows | Orquestração de todos os jobs (Bronze/Silver/Gold/ML/alertas) | Databricks Workflows |
| Databricks Asset Bundles | Definição declarativa de jobs/workflows/permissions por ambiente | DAB (`databricks.yml`) |
| Terraform | Provisiona objetos que o DAB não cobre: catálogos UC, secrets scopes, cluster policies | Terraform + Databricks provider |
| CI/CD Pipeline | Testes automatizados, validação de bundle, deploy dev/prod | GitHub Actions |
| BI Layer | Consumo analítico | Databricks SQL Warehouse + Databricks AI/BI Dashboards |

---

## Key Decisions

### Decision 1: Spark Structured Streaming puro em vez de DLT/Lakeflow

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-08 |

**Context:** Era preciso escolher o motor de orquestração das camadas Silver/Gold — DLT/Lakeflow Declarative Pipelines (sugestão original do context.md) vs. jobs Spark explícitos.

**Choice:** Todas as camadas (Bronze/Silver/Gold) são implementadas como jobs PySpark explícitos, orquestrados via Databricks Workflows.

**Rationale:** Decisão explícita do usuário no BRAINSTORM — o objetivo é demonstrar domínio de Structured Streaming puro, não de pipelines declarativos.

**Alternatives Rejected:**
1. DLT/Lakeflow Declarative Pipelines — rejeitado por decisão explícita do usuário.

**Consequences:**
- Mais código boilerplate para checkpointing, controle de schema evolution e orquestração (que o DLT resolveria automaticamente)
- Mais controle fino sobre a lógica de streaming, melhor para demonstrar competência técnica em portfólio

---

### Decision 2: Framework de qualidade de dados custom em PySpark (sem DLT expectations)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-08 |

**Context:** Sem DLT, não há `@dlt.expect_or_drop` nativo para regras de qualidade declarativas.

**Choice:** Módulo `src/quality/checks.py` com funções reutilizáveis (`check_not_null`, `check_unique`, `check_range`, `check_freshness`) aplicadas após cada escrita em Silver/Gold. Resultados gravados em uma tabela `_dq_results` no catálogo de monitoramento; o Workflow falha ou alerta conforme o threshold configurado.

**Rationale:** Mantém a decisão "100% Spark" e ainda garante testabilidade, rastreabilidade e um gate de qualidade real (satisfaz AT-002 e o Success Criteria de testes automatizados).

**Alternatives Rejected:**
1. Great Expectations — dependência externa mais pesada, desnecessária para o volume de portfólio.
2. Sem checks formais — rejeitado, pois viola o Success Criteria de qualidade automatizada do DEFINE.

**Consequences:**
- Mais código para manter, mas zero dependências externas
- Métricas de qualidade viram um artefato de observabilidade (tabela `_dq_results`) reutilizável em dashboards

---

### Decision 3: Kafka hospedado no Confluent Cloud (free tier), producer fora do Databricks

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-08 |

**Context:** Databricks não hospeda Kafka nativamente; era preciso decidir onde rodar o broker e o producer que simula eventos em tempo real a partir dos datasets públicos estáticos.

**Choice:** Kafka broker no Confluent Cloud (free tier). Producer Python containerizado (Docker), separado da plataforma Databricks, com taxa de eventos configurável.

**Rationale:** Confluent Cloud evita administrar servidor Kafka; manter o producer fora do Databricks reforça a narrativa de "fonte de eventos externa real" (mais fiel a um cenário de produção, onde o Kafka nunca seria alimentado por um notebook Databricks).

**Alternatives Rejected:**
1. Kafka em Docker local/VM — mais barato, mas rejeitado no BRAINSTORM por ser menos realista para evolução a produção.
2. Producer rodando como notebook/job Databricks — rejeitado por misturar a responsabilidade de "sistema fonte" com a plataforma de processamento.

**Consequences:**
- Precisa de um lugar para o producer rodar continuamente (Docker local, VM barata, ou execução agendada via GitHub Actions/cron)
- Limites do free tier do Confluent Cloud precisam ser monitorados (ver Assumption A-002 do DEFINE)

---

### Decision 4: Unity Catalog com catálogos por ambiente (não workspaces separados)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-08 |

**Context:** Era preciso decidir a topologia de ambientes (dev/staging/prod).

**Choice:** Um único workspace Databricks; três catálogos Unity Catalog (`insurance_dev`, `insurance_staging`, `insurance_prod`), com Databricks Asset Bundles selecionando o catálogo via `target`.

**Rationale:** Aprovado no BRAINSTORM (Approach A) — melhor equilíbrio entre custo/simplicidade de portfólio e capacidade real de evoluir para produção.

**Alternatives Rejected:**
1. Workspaces físicos separados por ambiente — mais caro e mais lento de provisionar/iterar em fase de portfólio.
2. Ambiente único sem separação — conflita com o requisito de evolução para produção.

**Consequences:**
- Isolamento é lógico (catálogo), não físico — aceitável para o estágio atual, documentado como trade-off explícito
- Migração futura para workspaces separados é possível sem reescrever a lógica dos jobs (só muda o target do DAB)

---

### Decision 5: Fronteira entre Databricks Asset Bundles e Terraform

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-08 |

**Context:** Tanto DAB quanto Terraform podem gerenciar recursos Databricks; era preciso definir a fronteira para evitar duplicação/conflito de estado.

**Choice:** DAB (`databricks.yml` + `resources/*.yml`) gerencia tudo que muda com frequência: Jobs, Workflows, permissions de jobs. Terraform gerencia o que muda raramente: catálogos/schemas do Unity Catalog, secret scopes, cluster policies, external locations.

**Rationale:** Separação de responsabilidades comum em times Databricks maduros — evita que dois sistemas de IaC gerenciem o mesmo recurso.

**Alternatives Rejected:**
1. Terraform gerenciando tudo, incluindo jobs — mais verboso e redundante com o que o DAB já resolve nativamente.

**Consequences:**
- Dois pipelines de deploy coexistindo (Terraform via GitHub Actions + `databricks bundle deploy`), documentados claramente no README para evitar confusão

---

### Decision 6: Fraude como caso de uso avançado prioritário (resolve Open Question #3 do DEFINE)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-08 |

**Context:** O DEFINE deixou em aberto se "fraude" ou "aprovação automática" deveria ser o caso de uso SHOULD prioritário da primeira entrega.

**Choice:** Detecção de fraude (score em streaming) é implementada primeiro; aprovação automática reutiliza o score de fraude como um dos insumos de decisão logo em seguida.

**Rationale:** Mais vistoso para portfólio, combina Spark Streaming + MLflow (dois pilares do projeto), e cria a base de dados/score que o caso de uso de aprovação automática também precisa.

**Alternatives Rejected:**
1. Aprovação automática primeiro — mais simples, mas menos demonstrativo de streaming + ML avançado.

**Consequences:**
- Aprovação automática (COULD→SHOULD) tem uma dependência direta do módulo de fraude, o que deve ser refletido no File Manifest e na ordem de build

---

### Decision 7: SUSEP + ANS como fontes obrigatórias do MVP; Open Insurance Brasil como fast-follow

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-08 |

**Context:** Pesquisa confirmou que o Open Insurance Brasil (Fase 1 — Dados Abertos) exige registro como participante no diretório sandbox da Área do Desenvolvedor antes de consumir as APIs padronizadas, mesmo para dados não sensíveis. Isso adiciona fricção e uma dependência externa fora do controle do time.

**Choice:** O MVP usa SUSEP (obrigatório, CSV público sem autenticação) + ANS (CSV público) como as 2 fontes exigidas pelo Success Criteria do DEFINE. A integração com Open Insurance Brasil é desenhada (ver Integration Points) mas implementada como fast-follow, após o credenciamento ser resolvido.

**Rationale:** Evita bloquear o cronograma do MVP por um processo de credenciamento externo (registro no diretório, homologação de conformance) que não depende só do time do projeto.

**Alternatives Rejected:**
1. Aguardar o credenciamento do Open Insurance antes de iniciar o MVP — atrasaria todo o projeto por uma dependência externa.

**Consequences:**
- Success Criteria "pelo menos 2 fontes públicas" é satisfeito com SUSEP + ANS
- Open Insurance Brasil vira um item de roadmap rastreado, não um bloqueador

---

## File Manifest

| # | File | Action | Purpose | Agent | Dependencies |
|---|------|--------|---------|-------|--------------|
| 1 | `src/common/spark_session.py` | Create | Factory de SparkSession com configs padrão (AQE, Photon-friendly) | @spark-engineer | None |
| 2 | `src/common/schemas.py` | Create | Schemas Spark (StructType) para claim, policy, customer, vehicle | @medallion-architect | None |
| 3 | `src/common/kafka_config.py` | Create | Configuração de conexão Kafka (Confluent Cloud) via Databricks secrets | @spark-streaming-architect | None |
| 4 | `src/ingestion/producer/datasets/susep_loader.py` | Create | Baixa e normaliza CSVs da SUSEP | @data-contracts-engineer | None |
| 5 | `src/ingestion/producer/datasets/ans_loader.py` | Create | Baixa e normaliza CSVs da ANS | @data-contracts-engineer | None |
| 6 | `src/ingestion/producer/kafka_publisher.py` | Create | Publica eventos no Kafka em ritmo configurável | @spark-streaming-architect | 4, 5 |
| 7 | `src/ingestion/producer/main.py` | Create | Entry point do producer (loop de replay) | @spark-streaming-architect | 6 |
| 8 | `src/ingestion/producer/config.yaml` | Create | Configuração de tópicos, taxa de eventos, fontes ativas | @spark-streaming-architect | None |
| 9 | `src/ingestion/producer/Dockerfile` | Create | Empacota o producer para execução containerizada | @ci-cd-specialist | 7, 8 |
| 10 | `src/streaming/bronze_ingest.py` | Create | Job Structured Streaming: Kafka → Bronze Delta | @spark-streaming-architect | 1, 2, 3 |
| 11 | `src/quality/checks.py` | Create | Funções reutilizáveis de qualidade de dados (not_null, unique, range, freshness) | @data-quality-analyst | 1 |
| 12 | `src/streaming/silver_transform.py` | Create | Dedup, parsing, normalização, aplica quality checks | @medallion-architect | 1, 2, 11 |
| 13 | `src/fraud/streaming_score.py` | Create | Calcula score de fraude em streaming (distância, horário, histórico, frequência) | @spark-engineer | 1, 12 |
| 14 | `src/streaming/gold_aggregate.py` | Create | Agregações de negócio + integra score de fraude + regra de aprovação automática | @medallion-architect | 12, 13 |
| 15 | `src/fraud/train_model.py` | Create | Pipeline de treino do modelo de fraude (MLflow) | @spark-engineer | 12 |
| 16 | `src/monitoring/sla_alerts.py` | Create | Job de monitoramento de SLA com alertas | @spark-engineer | 12 |
| 17 | `terraform/main.tf` | Create | Provider Databricks + backend do state | @ci-cd-specialist | None |
| 18 | `terraform/unity_catalog.tf` | Create | Catálogos/schemas dev/staging/prod, RLS, column masking | @lakehouse-architect | 17 |
| 19 | `terraform/secrets.tf` | Create | Secret scope para credenciais Confluent Cloud | @lakehouse-architect | 17 |
| 20 | `terraform/variables.tf` | Create | Variáveis parametrizáveis por ambiente | @ci-cd-specialist | None |
| 21 | `databricks.yml` | Create | Root do Databricks Asset Bundle (targets dev/staging/prod) | @ci-cd-specialist | None |
| 22 | `resources/jobs.bronze.yml` | Create | Definição do Job Bronze via DAB | @ci-cd-specialist | 10, 21 |
| 23 | `resources/jobs.silver.yml` | Create | Definição do Job Silver via DAB | @ci-cd-specialist | 12, 21 |
| 24 | `resources/jobs.gold.yml` | Create | Definição do Job Gold via DAB | @ci-cd-specialist | 14, 21 |
| 25 | `resources/jobs.ml_training.yml` | Create | Definição do Job de treino de ML via DAB | @ci-cd-specialist | 15, 21 |
| 26 | `.github/workflows/ci.yml` | Create | Roda testes (pytest) em cada PR | @ci-cd-specialist | None |
| 27 | `.github/workflows/deploy.yml` | Create | Valida bundle e faz deploy dev (PR)/prod (merge main) | @ci-cd-specialist | 21-25, 26 |
| 28 | `tests/unit/test_silver_transform.py` | Create | Testes unitários de dedup/normalização | @spark-engineer | 12 |
| 29 | `tests/unit/test_quality_checks.py` | Create | Testes unitários do módulo de qualidade | @data-quality-analyst | 11 |
| 30 | `tests/unit/test_fraud_score.py` | Create | Testes unitários do score de fraude | @spark-engineer | 13 |
| 31 | `tests/integration/test_bronze_to_gold.py` | Create | Teste de integração ponta a ponta (schema fixo → Bronze→Silver→Gold) | @spark-engineer | 10, 12, 14 |
| 32 | `docs/ARCHITECTURE.md` | Create | Documentação da arquitetura para o repositório público | (general) | None |
| 33 | `README.md` | Create | Instruções de setup, execução local e deploy | (general) | 21, 27 |

**Total Files:** 33

---

## Agent Assignment Rationale

| Agent | Files Assigned | Why This Agent |
|-------|----------------|-----------------|
| @spark-streaming-architect | 3, 6, 7, 8, 10 | Especialista em Kafka + Spark Structured Streaming — ingestão e producer |
| @medallion-architect | 2, 12, 14 | Especialista em arquitetura Bronze/Silver/Gold e progressão de qualidade |
| @spark-engineer | 1, 13, 15, 16, 28, 30, 31 | PySpark geral — transformações, scoring, testes |
| @data-quality-analyst | 11, 29 | Especialista em checks de qualidade de dados e observabilidade |
| @data-contracts-engineer | 4, 5 | Especialista em contratos de dados — formaliza schema/origem das fontes públicas |
| @lakehouse-architect | 18, 19 | Especialista em Unity Catalog, governança e catálogos |
| @ci-cd-specialist | 9, 17, 20, 21, 22, 23, 24, 25, 26, 27 | Especialista em Terraform, Databricks Asset Bundles e GitHub Actions |
| (general) | 32, 33 | Documentação — não requer especialista de domínio |

**Agent Discovery:**
- Agentes considerados a partir do roster disponível (`agentspec:*`) com foco em Databricks/Spark/Kafka/Terraform/CI-CD/qualidade de dados
- Matched por: tipo de arquivo, palavras-chave de propósito (streaming, quality, terraform, ci/cd), domínios de KB relevantes

---

## Code Patterns

### Pattern 1: Job Bronze — Kafka → Delta via Structured Streaming

```python
# src/streaming/bronze_ingest.py
# Uso: job Databricks (Workflow) que consome um tópico Kafka e grava raw em Delta,
# sem nenhuma transformação além de parse do envelope Kafka.

from pyspark.sql.functions import col, from_json, current_timestamp
from src.common.spark_session import get_spark
from src.common.kafka_config import get_kafka_options
from src.common.schemas import CLAIM_EVENT_SCHEMA

def run_bronze_ingest(topic: str, bronze_table: str, checkpoint_path: str) -> None:
    spark = get_spark("bronze-ingest")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .options(**get_kafka_options(topic))
        .load()
    )

    bronze_df = (
        raw_stream
        .select(
            col("key").cast("string").alias("event_key"),
            from_json(col("value").cast("string"), CLAIM_EVENT_SCHEMA).alias("payload"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select("event_key", "payload.*", "kafka_timestamp")
        .withColumn("_ingested_at", current_timestamp())
    )

    (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(processingTime="30 seconds")
        .toTable(bronze_table)
    )
```

### Pattern 2: Módulo de qualidade de dados (sem DLT expectations)

```python
# src/quality/checks.py
# Uso: aplicado após cada escrita em Silver/Gold. Registra resultado em
# uma tabela `_dq_results` e retorna um booleano para o Workflow decidir
# se falha ou apenas alerta.

from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, lit, current_timestamp

@dataclass
class DQResult:
    check_name: str
    table_name: str
    passed: bool
    failed_rows: int

def check_not_null(df: DataFrame, columns: list[str], table_name: str) -> list[DQResult]:
    results = []
    for column in columns:
        failed = df.filter(col(column).isNull()).count()
        results.append(DQResult(f"not_null:{column}", table_name, failed == 0, failed))
    return results

def check_unique(df: DataFrame, key_columns: list[str], table_name: str) -> DQResult:
    total = df.count()
    distinct = df.select(*key_columns).distinct().count()
    failed_rows = total - distinct
    return DQResult(f"unique:{','.join(key_columns)}", table_name, failed_rows == 0, failed_rows)

def persist_results(spark, results: list[DQResult], results_table: str) -> bool:
    rows = [(r.check_name, r.table_name, r.passed, r.failed_rows) for r in results]
    df = spark.createDataFrame(rows, ["check_name", "table_name", "passed", "failed_rows"])
    df = df.withColumn("_checked_at", current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(results_table)
    return all(r.passed for r in results)
```

### Pattern 3: Configuração do producer (config.yaml)

```yaml
# src/ingestion/producer/config.yaml
kafka:
  bootstrap_servers: "${CONFLUENT_BOOTSTRAP_SERVERS}"
  security_protocol: "SASL_SSL"
  sasl_mechanism: "PLAIN"
  # credenciais via variáveis de ambiente / Databricks secret scope, nunca hardcoded

topics:
  claim_opened: "claim-opened"
  policy_created: "policy-created"
  customer_updated: "customer-updated"

sources:
  susep:
    enabled: true
    dataset_path: "data/susep_sinistros.csv"
    event_topic: "claim_opened"
  ans:
    enabled: true
    dataset_path: "data/ans_dados.csv"
    event_topic: "policy_created"
  open_insurance:
    enabled: false  # fast-follow — depende de credenciamento no sandbox

replay:
  events_per_minute: 100
  shuffle: true
  loop: true
```

---

## Data Flow

```text
1. Dataset Loader baixa CSVs públicos (SUSEP obrigatório, ANS obrigatório) uma vez
   e os disponibiliza localmente para o producer
   │
   ▼
2. Kafka Producer (Docker) lê as linhas e publica eventos nos tópicos Kafka
   configurados, em ritmo controlado (events_per_minute), simulando tempo real
   │
   ▼
3. Confluent Cloud recebe e retém os eventos nos tópicos (claim-opened,
   policy-created, customer-updated)
   │
   ▼
4. Bronze Job (Spark Structured Streaming) consome os tópicos continuamente
   e grava raw em Delta Bronze, sem transformação (append-only)
   │
   ▼
5. Silver Job lê a Bronze incrementalmente, aplica dedup, parsing, normalização
   e os quality checks (src/quality/checks.py); resultado grava em Delta Silver
   e em `_dq_results`
   │
   ▼
6. Gold Job lê a Silver, calcula agregações de negócio, aciona o módulo de
   Fraud Scoring (streaming) e a regra de Auto-Approval, grava em Delta Gold
   │
   ▼
7. Consumidores finais leem a Gold: Databricks SQL Warehouse (dashboards),
   MLflow (modelos treinados sobre a Silver/Gold), Databricks Workflows
   (alertas de SLA), e futuramente API externa (fast-follow)
```

---

## Integration Points

| External System | Integration Type | Authentication |
|------------------|-------------------|------------------|
| SUSEP (dados.gov.br / gov.br/susep) | Download CSV (HTTP) | Nenhuma (dados públicos) |
| ANS (dados abertos) | Download CSV (HTTP) | Nenhuma (dados públicos) |
| Open Insurance Brasil (fast-follow) | REST API (Fase 1 — dados abertos) | Registro como participante no diretório sandbox da Área do Desenvolvedor |
| Confluent Cloud | Kafka protocol (SASL_SSL) | API Key/Secret via Databricks secret scope |
| GitHub | CI/CD trigger (push/PR) | GitHub Actions token + Databricks service principal (OIDC ou PAT em secret) |
| Databricks REST/DAB | Deploy de jobs/workflows | Databricks service principal por ambiente |

---

## Testing Strategy

| Test Type | Scope | Files | Tools | Coverage Goal |
|-----------|-------|-------|-------|-----------------|
| Unit | Transformações Spark (dedup, normalização, fraud score, quality checks) | `tests/unit/*.py` | pytest + chispa/pytest-spark (local Spark session) | 80% |
| Integration | Fluxo Bronze→Silver→Gold com dados fixos (fixtures) | `tests/integration/test_bronze_to_gold.py` | pytest + Spark local + Delta local | Caminhos principais (AT-001, AT-002, AT-003) |
| CI Gate | Bundle válido (`databricks bundle validate`) antes do deploy | `.github/workflows/deploy.yml` | Databricks CLI | 100% dos deploys |
| E2E (manual, pós-deploy) | Producer real publicando no Confluent Cloud dev, ponta a ponta até Gold | Manual (checklist no README) | Databricks Workspace (dev) | Happy path (AT-001) |

---

## Error Handling

| Error Type | Handling Strategy | Retry? |
|------------|---------------------|--------|
| Evento Kafka malformado (schema inválido) | Isolado em tabela de quarentena (`*_quarantine`) via `from_json` com `mode="PERMISSIVE"` + filtro de payload nulo; não derruba o stream | Não (fica para reprocessamento manual) |
| Falha de conexão com Kafka/Confluent Cloud | Databricks Workflow com política de retry (ex: 3 tentativas, backoff exponencial) | Sim |
| Schema drift na origem (novo campo) | `mergeSchema=true` na Bronze; Silver valida contra schema esperado e loga divergência sem quebrar | Não (alerta + segue) |
| Falha de job downstream (Silver/Gold) | Databricks Workflow com alerta (email/webhook) e job marcado como falho; não avança para a próxima camada | Sim (conforme política do Workflow) |
| Rajada de volume acima do esperado (AT-003) | Trigger `processingTime` + autoscaling do cluster/serverless absorve o pico; backpressure nativo do Structured Streaming | N/A (comportamento nativo) |

---

## Configuration

| Config Key | Type | Default | Description |
|------------|------|---------|--------------|
| `replay.events_per_minute` | int | `100` | Taxa de eventos publicados pelo producer (satisfaz Success Criteria de volume) |
| `kafka.topics.*` | string | ver `config.yaml` | Nomes dos tópicos Kafka por tipo de evento |
| `bronze.checkpoint_path` | string | `/Volumes/{catalog}/bronze/_checkpoints/{topic}` | Local do checkpoint do Structured Streaming |
| `dq.failure_threshold_pct` | float | `0.0` | % máxima de falhas de qualidade toleradas antes de o Workflow falhar |
| `catalog.name` | string | `insurance_{env}` | Nome do catálogo Unity Catalog por ambiente |
| `fraud.score_threshold` | float | `0.7` | Score acima do qual um sinistro é sinalizado para revisão manual |

---

## Security Considerations

- Credenciais do Confluent Cloud (API Key/Secret) armazenadas em Databricks secret scope, nunca hardcoded em código ou `config.yaml`
- Unity Catalog com Row Level Security e Column Masking em colunas potencialmente sensíveis (mesmo com dados já anonimizados na origem, aplica defesa em profundidade)
- Service principals distintos por ambiente (dev/staging/prod) para o deploy via CI/CD, com permissões mínimas necessárias (least privilege)
- Nenhuma tentativa de re-identificação ou enriquecimento cruzado dos dados públicos anonimizados (alinhado à LGPD e ao Constraint do DEFINE)
- Audit logs do Unity Catalog habilitados desde o início para rastrear acesso aos dados

---

## Observability

| Aspect | Implementation |
|--------|------------------|
| Logging | Logging estruturado (JSON) via `logging` padrão do Python nos jobs Spark e no producer; correlacionável por `event_key`/`run_id` |
| Metrics | Tabela `_dq_results` (qualidade) + Databricks System Tables (custo, execução de jobs) + Lakehouse Monitoring nas tabelas Gold |
| Tracing | Não crítico para o MVP (jobs batch/streaming); rastreamento fica a cargo dos Job Run IDs do Databricks Workflows |
| Alerting | Databricks Workflows com notificação (email/webhook) em falha de job ou violação de SLA (`src/monitoring/sla_alerts.py`) |

---

## Pipeline Architecture

### DAG Diagram

```text
[SUSEP CSV] ──extract──┐
                        ├──▶ [Kafka Producer] ──▶ [Kafka Topics] ──▶ [Bronze Job] ──▶ [Bronze Delta]
[ANS CSV]   ──extract──┘                                                                   │
                                                                                             ▼
                                                                                     [Silver Job + DQ]
                                                                                             │
                                                                                             ▼
                                                                                     [Silver Delta]
                                                                                             │
                                                                          ┌──────────────────┼──────────────────┐
                                                                          ▼                  ▼                  ▼
                                                                  [Gold Job]          [Fraud Score]     [Auto-Approval]
                                                                          │
                                                                          ▼
                                                                    [Gold Delta] ──▶ [SQL Warehouse / Dashboards]
```

### Partition Strategy

| Table | Partition Key | Granularity | Rationale |
|-------|----------------|-------------|-----------|
| bronze.claims | `_ingested_date` (derivado de `_ingested_at`) | Diária | Volume de portfólio é baixo; partição diária evita small files excessivos |
| silver.claims | `event_date` | Diária | Alinhado ao padrão de consulta por período (dashboards, auditoria) |
| gold.claims_agg | `region`, `event_date` | Diária | Consultas de BI tipicamente filtram por região e período |

### Incremental Strategy

| Model | Strategy | Key Column | Lookback |
|-------|----------|------------|-----------|
| bronze.claims | Structured Streaming (append-only) | N/A (append) | N/A |
| silver.claims | Streaming/micro-batch com `MERGE` por chave (upsert) | `claim_id` | Últimas 24h (watermark) |
| gold.claims_agg | Batch incremental via Workflow (recalcula partições do dia) | `region + event_date` | Últimas 24h |

### Schema Evolution Plan

| Change Type | Handling | Rollback |
|-------------|----------|-----------|
| Nova coluna | `mergeSchema=true` na Bronze; Silver adiciona com valor default e backfill assíncrono | Ignorar coluna nova (não propagar para Silver) |
| Mudança de tipo | Período de dual-write (coluna nova + antiga) até validar, depois migra | Reverter para o tipo anterior via Delta `RESTORE` |
| Remoção de coluna | Deprecar no contrato de dados (Data Contract do DEFINE), remover após N dias documentados | Re-adicionar coluna via schema evolution |

### Data Quality Gates

| Gate | Tool | Threshold | Action on Failure |
|------|------|------------|----------------------|
| Not-null em chaves primárias (`claim_id`, `policy_id`) | `src/quality/checks.py::check_not_null` | 0 nulos | Bloqueia o Workflow (job falha) |
| Unicidade de chave primária em Silver/Gold | `src/quality/checks.py::check_unique` | 0 duplicados | Bloqueia o Workflow |
| Freshness da Bronze | `check_freshness` (comparação de timestamp) | < 2 minutos (assunção A-001 do DEFINE) | Alerta (não bloqueia, dado o free tier do Kafka) |
| % de eventos em quarentena | Contagem sobre total do batch | < 1% | Alerta; > 5% bloqueia o Workflow |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-07-08 | design-agent | Versão inicial, extraída de DEFINE_INSURANCE_LAKEHOUSE_PLATFORM.md |
| 1.1 | 2026-07-08 | ship-agent | Shipped e arquivado |

---

## Next Step

**Ready for:** `/build .claude/sdd/features/DESIGN_INSURANCE_LAKEHOUSE_PLATFORM.md`
