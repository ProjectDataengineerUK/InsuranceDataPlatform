# DESIGN: Open Insurance

> Technical design for implementing Open Insurance (módulo de consentimento do cliente para compartilhamento de dados, preparado para Delta Sharing)

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | OPEN_INSURANCE |
| **Date** | 2026-07-13 |
| **Author** | design-agent |
| **DEFINE** | [DEFINE_OPEN_INSURANCE.md](./DEFINE_OPEN_INSURANCE.md) |
| **Status** | Ready for Build |

---

## Architecture Overview

```text
┌───────────────────────────────────────────────────────────────────────────┐
│                         OPEN INSURANCE — DATA FLOW                        │
├───────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  [producer: consent_events.py] --consent-updated--> [Kafka/Confluent]     │
│                                                          │                │
│                            (scheduled, cron */15, availableNow=True)      │
│                                                          ▼                │
│                                          bronze.consent_events            │
│                                     (reaproveita bronze_ingest.py, sem    │
│                                      mudança de código)                  │
│                                                          │                │
│                       (scheduled, cron */30 — job open_insurance_pipeline)│
│                                                          ▼                │
│                                    silver.consent_current (SCD2)          │
│                              is_current / valid_from / valid_to          │
│                                                          │                │
│                                                          ▼                │
│                                    gold.customer_consent (VIEW)           │
│                                    WHERE is_current = true                │
│                                                          │                │
│  [profile_generator.py] ──────────► gold.open_insurance_profile          │
│  (determinístico, seed=customer_id, roda no mesmo job)                   │
│                                                          │                │
│  gold.claims (já existente) ─────────────────────────────┤                │
│                                                          ▼                │
│                              gold.open_insurance_shareable (VIEW)         │
│                        consent GRANTED + perfil + claims (LEFT JOIN)      │
│                                                          │                │
│                                              [futuro, fora do MVP]        │
│                                                          ▼                │
│                                    Delta Share "open-insurance-share"     │
│                                    recipients: insurer_a/b/c              │
│                                                                            │
└───────────────────────────────────────────────────────────────────────────┘
```

---

## Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| `consent_events.py` (producer dataset) | Gera eventos fictícios de consentimento (GRANTED/REVOKED) para uma amostra de `customer_id` já usados em claims/policies | Python, publica no mesmo `kafka_publisher.py` já existente |
| `bronze_ingest.py` (reaproveitado, sem mudança) | Ingestão do tópico `consent-updated` para `bronze.consent_events` | PySpark Structured Streaming, `trigger(availableNow=True)` |
| `scd2_merge.py` (novo) | Helper genérico de MERGE Delta com semântica SCD2 (fecha versão vigente + insere nova) | PySpark + Delta Lake `MERGE` |
| `consent_scd.py` (novo) | Aplica SCD2 sobre os eventos de bronze, produz `silver.consent_current` | PySpark batch |
| `profile_generator.py` (novo) | Gera perfil sintético determinístico por `customer_id` | PySpark batch, `random.Random(seed)` |
| `shareable_view.py` (novo) | Cria/atualiza as views `gold.customer_consent` e `gold.open_insurance_shareable` | Spark SQL (`CREATE OR REPLACE VIEW`) |

---

## Key Decisions

### Decision 1: Reaproveitar `bronze_ingest.py` sem nenhuma mudança de código, só de configuração de job

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-13 |

**Context:** A DEFINE exige que este módulo rode como job **agendado**, não contínuo, dado o histórico recente de `RESOURCE_EXHAUSTED` na cota de serverless deste workspace. Ao mesmo tempo, `bronze_ingest.py` já é genérico (schema vem de `SCHEMA_REGISTRY` por tópico) e já usa `trigger(availableNow=True)` internamente — o efeito "contínuo" dos outros jobs vem inteiramente do trigger type `continuous:` no `resources/*.yml`, não do código Python.

**Choice:** Adicionar `consent-updated` ao `SCHEMA_REGISTRY` (`src/common/schemas.py`) e reaproveitar `bronze_ingest.py` literalmente sem alterações, configurando o job novo (`open_insurance_bronze_ingest`) com `schedule:` (cron) em vez de `continuous:`.

**Rationale:** Zero código novo para a etapa de ingestão; a diferença entre "quase-contínuo" (jobs atuais) e "agendado de baixa frequência" (este módulo) é inteiramente uma escolha de configuração do bundle, não do pipeline em si.

**Alternatives Rejected:**
1. Escrever um ingestor batch dedicado para consentimento — rejeitado por duplicar exatamente o que `bronze_ingest.py` já faz (parse de schema, quarentena de malformados, escrita particionada).
2. Job contínuo dedicado — rejeitado explicitamente pela constraint de cota de serverless da DEFINE.

**Consequences:**
- Ganho: nenhum código de ingestão novo para revisar/testar.
- Trade-off: a frequência de sincronização do consentimento fica limitada à cadência do cron (ver Decision 4), não é near-real-time.

---

### Decision 2: SCD2 via novo helper `src/common/scd2_merge.py`, em vez de estender `delta_merge.py`

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-13 |

**Context:** `src/common/delta_merge.py` (`merge_into_delta`) implementa upsert simples (`whenMatchedUpdateAll`/`whenNotMatchedInsertAll`) — sobrescreve o registro existente, sem preservar histórico. A DEFINE exige explicitamente histórico completo de mudanças de consentimento (SCD2), que é confirmadamente um padrão novo neste projeto (nenhuma tabela usa `is_current`/`valid_from`/`valid_to` hoje).

**Choice:** Novo arquivo `src/common/scd2_merge.py` com `merge_scd2_into_delta(batch_df, table_name, key_column, effective_column)`, implementando o padrão de 2 passos padrão do Delta Lake: (1) `MERGE` fechando (`is_current=false`, `valid_to=<novo evento>`) a versão vigente quando chega um evento mais recente para a mesma chave; (2) `append` das novas versões vigentes (`is_current=true`, `valid_from=<evento>`, `valid_to=null`).

**Rationale:** Mantém `delta_merge.py` simples e focado em upsert "latest wins" (usado por `silver_transform.py`), sem sobrecarregá-lo com um caso de uso que nenhum outro consumidor precisa hoje. Um helper novo e nomeado deixa a intenção (histórico vs. sobrescrita) explícita em qualquer chamada.

**Alternatives Rejected:**
1. Adicionar um parâmetro `scd2: bool` a `merge_into_delta` — rejeitado por misturar duas semânticas bem diferentes numa função só, tornando os dois casos mais difíceis de entender isoladamente.
2. Usar `MERGE ... WHEN NOT MATCHED BY SOURCE` num único statement — rejeitado por ser mais difícil de explicar/testar que o padrão de 2 passos, sem ganho de performance relevante no volume baixo deste caso de uso.

**Consequences:**
- Ganho: padrão reaproveitável se outra feature futura precisar de SCD2.
- Trade-off: 2 operações Delta por batch (update + append) em vez de 1 MERGE só — aceitável dado o volume baixo de eventos de consentimento.

---

### Decision 3: Perfil sintético gerado por `random.Random(seed)`, seed derivado de hash do `customer_id` (mesmo espírito de `regulatory_feeds.py`)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-13 |

**Context:** A DEFINE pede um perfil determinístico (mesmo `customer_id` sempre gera o mesmo perfil), sem PII real. `regulatory_feeds.py` já resolve um problema parecido (defeitos determinísticos por fonte) usando `random.Random(rng_seed)` com seeds nomeadas.

**Choice:** `profile_generator.py` deriva um seed inteiro por `customer_id` via `int(hashlib.sha256(customer_id.encode()).hexdigest(), 16) % (2**32)`, usa esse seed num `random.Random` isolado por linha para sortear nome fictício (lista fixa de nomes/sobrenomes), idade (distribuição uniforme 18-80) e `risk_score` (uniforme 0-1). Roda como batch full-refresh (overwrite) sobre os `customer_id` distintos de `gold.claims` — não incremental, já que recalcular todo mundo é barato no volume deste projeto.

**Rationale:** Reaproveita a mesma técnica de determinismo já validada no projeto, sem introduzir uma biblioteca nova (ex.: Faker) só para nomes fictícios.

**Alternatives Rejected:**
1. Biblioteca `Faker` com seed global — rejeitado para não adicionar uma dependência nova (`requirements.txt`) para um gerador simples que uma lista fixa de nomes já resolve.
2. Perfil incremental (merge por `customer_id` novo) — rejeitado por complexidade desnecessária dado o volume baixo; overwrite completo é mais simples e igualmente correto (determinístico).

**Consequences:**
- Ganho: sem dependência nova.
- Trade-off: nomes fictícios vêm de uma lista fixa curta (menos variedade que Faker) — aceitável para uma demo.

---

### Decision 4: Tabelas novas ficam nos schemas `bronze`/`silver`/`gold` já existentes — sem novo schema, volume ou grant no Terraform

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-13 |

**Context:** A DEFINE deixou em aberto se as tabelas novas mereciam um schema `open_insurance` dedicado. Revisando o Terraform (`terraform/unity_catalog.tf`) e o precedente do módulo regulatório: os schemas do projeto são organizados **por camada** (bronze/silver/gold/monitoring/models), não por domínio — `regulatory_claims_raw`/`regulatory_susep_claims` já vivem dentro dos schemas de camada padrão, namespaced só pelo nome da tabela.

**Choice:** Seguir o mesmo padrão: `bronze.consent_events`, `silver.consent_current`, `gold.customer_consent`, `gold.open_insurance_profile`, `gold.open_insurance_shareable` — todos dentro dos schemas já existentes. O checkpoint de streaming reaproveita o Volume `bronze._checkpoints` já provisionado (subdiretório `consent-updated`, mesmo padrão do módulo regulatório).

**Rationale:** Os grants do Terraform (`databricks_grants.bronze_read_write`, `silver_read_write`, `gold_read_only_analysts`) já cobrem `CREATE_TABLE`/`SELECT`/`MODIFY` nesses schemas para `var.catalog_owner` — nenhuma tabela nova exige grant novo. Resolve a Open Question #2 da DEFINE sem exigir nenhuma mudança de Terraform.

**Alternatives Rejected:**
1. Schema `open_insurance` dedicado — rejeitado por quebrar a convenção camada-como-schema já estabelecida, e por exigir Terraform novo (schema + grants) sem benefício real de isolamento (o módulo regulatório já prova que domínios convivem bem dentro dos schemas de camada).

**Consequences:**
- Ganho: zero mudança em `terraform/*.tf` para este módulo.
- Trade-off: nenhum — é estritamente reaproveitamento do padrão já validado.

---

### Decision 5: Cadência dos jobs agendados — ingestão a cada 15 min, pipeline SCD2/perfil/view a cada 30 min

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-13 |

**Context:** DEFINE Open Question #3. Consentimento muda com bem menos frequência que sinistros, e a DEFINE explicitamente aceita que não há necessidade de tempo real (Assumption A-003).

**Choice:** `open_insurance_bronze_ingest` a cada 15 min (`0 */15 * * * ?`, mesma cadência do `pipeline_latency_monitor` já existente); `open_insurance_pipeline` (SCD2 + perfil + views) a cada 30 min (`0 */30 * * * ?`, mesma cadência do `regulatory_pipeline` já existente).

**Rationale:** Reaproveita cadências já usadas no projeto em vez de inventar uma nova, e é conservador o suficiente para minimizar sobreposição de clusters serverless simultâneos (lição aprendida no deploy real: cota apertada).

**Alternatives Rejected:**
1. Cadência mais curta (ex.: 5 min) — rejeitado por adicionar mais janelas de execução concorrentes com os demais jobs agendados (gold_aggregate a cada 10min, pipeline_monitoring a cada 15min), aumentando o risco de reencontrar `RESOURCE_EXHAUSTED`.

**Consequences:**
- Trade-off: até ~30-45 min de atraso entre um evento de consentimento e ele refletir na view — aceitável dado que não é um requisito real deste MVP.

---

### Decision 6: `gold.open_insurance_shareable` usa LEFT JOIN com `gold.claims` (resolve AT-005)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-13 |

**Context:** DEFINE Open Question #1 — cliente com consentimento ativo mas sem nenhum claim em `gold.claims`.

**Choice:** `LEFT JOIN` a partir de `gold.customer_consent` (filtrado `GRANTED`) + `gold.open_insurance_profile`, com `gold.claims` à direita — clientes sem sinistro aparecem com colunas de claims nulas, não ficam de fora.

**Rationale:** Um "histórico limpo" (sem sinistros) é, no mundo real de Open Insurance, um dado tão relevante quanto um histórico com sinistros — uma seguradora avaliando risco quer saber tanto "teve sinistro X" quanto "não teve nenhum sinistro registrado". Excluir esses clientes esconderia informação útil e criaria uma dependência implícita (só compartilha quem já é "cliente ativo" em claims) que a DEFINE não pediu.

**Alternatives Rejected:**
1. `INNER JOIN` (só clientes com pelo menos 1 claim) — rejeitado por esconder clientes de "risco baixo" (sem sinistro), que é informação valiosa, não ausência de informação.

**Consequences:**
- Trade-off: consumidores da view precisam tratar colunas de claims como nullable (`amount`, `event_type`, etc. podem ser `null`).

---

### Decision 7: Nome do tópico/domínio distinto do stub `open_insurance` já existente em `config.yaml`

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-13 |

**Context:** `src/ingestion/producer/config.yaml` já tem uma entrada `open_insurance: enabled: false` — um placeholder não relacionado, para uma integração *futura e real* com o sandbox Open Insurance Brasil (fonte de dados externa, diferente do que esta feature constrói).

**Choice:** Usar o tópico `consent-updated` e a chave de source `consent_simulator` no `config.yaml` — nomes claramente distintos do stub `open_insurance` já existente, para não confundir as duas coisas.

**Rationale:** Evita que um futuro leitor (ou uma futura integração real com o sandbox) confunda o simulador de consentimento desta feature com a integração externa de verdade que o stub já reserva.

**Consequences:**
- Nenhum trade-off — é só uma escolha de nomenclatura para evitar ambiguidade.

---

## File Manifest

| # | File | Action | Purpose | Agent | Dependencies |
|---|------|--------|---------|-------|--------------|
| 1 | `src/common/schemas.py` | Modify | Adiciona `CONSENT_EVENT_SCHEMA` + entrada em `SCHEMA_REGISTRY` | @databricks-spark-expert | None |
| 2 | `src/common/scd2_merge.py` | Create | Helper genérico de MERGE Delta com semântica SCD2 | @databricks-spark-expert | None |
| 3 | `src/open_insurance/__init__.py` | Create | Marca o pacote novo | @python-developer | None |
| 4 | `src/open_insurance/consent_scd.py` | Create | Lê `bronze.consent_events`, aplica SCD2, escreve `silver.consent_current` + DQ | @databricks-spark-expert | 2, 3 |
| 5 | `src/open_insurance/profile_generator.py` | Create | Gera `gold.open_insurance_profile` determinístico | @databricks-spark-expert | 3 |
| 6 | `src/open_insurance/shareable_view.py` | Create | Cria/atualiza `gold.customer_consent` e `gold.open_insurance_shareable` (Spark SQL) | @databricks-sql-expert | 3, 4, 5 |
| 7 | `src/ingestion/producer/datasets/consent_events.py` | Create | Gera eventos fictícios GRANTED/REVOKED para uma amostra de `customer_id` | @python-developer | None |
| 8 | `src/ingestion/producer/config.yaml` | Modify | Novo tópico `consent_updated` + source `consent_simulator` | @ci-cd-specialist | 7 |
| 9 | `src/ingestion/producer/main.py` | Modify | Registra `consent_simulator` em `SOURCE_LOADERS` | @python-developer | 7, 8 |
| 10 | `resources/jobs.open_insurance.yml` | Create | 2 jobs agendados: `open_insurance_bronze_ingest` e `open_insurance_pipeline` | @ci-cd-specialist | 1, 4, 5, 6 |
| 11 | `tests/unit/test_scd2_merge.py` | Create | Testa fechamento de versão + inserção de nova versão vigente | @test-generator | 2 |
| 12 | `tests/unit/test_consent_scd.py` | Create | Testa o job de SCD2 end-to-end (batch pequeno) | @test-generator | 4 |
| 13 | `tests/unit/test_profile_generator.py` | Create | Testa determinismo do perfil sintético (mesmo seed → mesmo resultado) | @test-generator | 5 |
| 14 | `tests/unit/test_shareable_view.py` | Create | Testa a lógica de filtro/join da view (via DataFrame, não SQL direto) | @test-generator | 6 |
| 15 | `docs/ARCHITECTURE.md` | Modify | Documenta o módulo novo (mesmo padrão de documentação dos demais módulos) | @doc-updater | 1-10 |

**Total Files:** 15 (10 novos, 5 modificados)

---

## Agent Assignment Rationale

| Agent | Files Assigned | Why This Agent |
|-------|-----------------|------------------|
| @databricks-spark-expert | 1, 2, 4, 5 | PySpark + Delta Lake MERGE (SCD2), Structured Streaming — especialista já usado nas demais transformações do projeto |
| @databricks-sql-expert | 6 | Criação de views Spark SQL sobre Unity Catalog |
| @python-developer | 3, 7, 9 | Código Python puro (geração de dados fictícios, registro de source) sem Spark |
| @ci-cd-specialist | 8, 10 | Configuração de bundle (YAML de jobs) e config do producer |
| @test-generator | 11, 12, 13, 14 | Geração de testes pytest para os módulos novos |
| @doc-updater | 15 | Atualização de documentação de arquitetura |

---

## Code Patterns

### Pattern 1: SCD2 merge (`src/common/scd2_merge.py`)

```python
import hashlib
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit


def merge_scd2_into_delta(
    batch_df: DataFrame,
    table_name: str,
    key_column: str,
    effective_column: str,
) -> None:
    spark = batch_df.sparkSession
    versioned_df = (
        batch_df
        .withColumn("is_current", lit(True))
        .withColumn("valid_from", col(effective_column))
        .withColumn("valid_to", lit(None).cast("timestamp"))
    )

    if not spark.catalog.tableExists(table_name):
        versioned_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        return

    delta_table = DeltaTable.forName(spark, table_name)

    # Passo 1: fecha a versão vigente quando um evento mais novo chega pra
    # mesma chave (ex.: mesmo customer_id concede e depois revoga).
    (
        delta_table.alias("target")
        .merge(
            versioned_df.alias("source"),
            f"target.{key_column} = source.{key_column} AND target.is_current = true",
        )
        .whenMatchedUpdate(
            condition=f"source.{effective_column} > target.valid_from",
            set={"is_current": "false", "valid_to": f"source.{effective_column}"},
        )
        .execute()
    )

    # Passo 2: insere as novas versões vigentes (append puro — não é upsert).
    versioned_df.write.format("delta").mode("append").saveAsTable(table_name)
```

### Pattern 2: Perfil sintético determinístico (`src/open_insurance/profile_generator.py`)

```python
import hashlib
import random

FIRST_NAMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fábio", "Gabriela", "Heitor"]
LAST_NAMES = ["Silva", "Souza", "Costa", "Oliveira", "Pereira", "Santos", "Almeida"]


def _seed_for(customer_id: str) -> int:
    return int(hashlib.sha256(customer_id.encode("utf-8")).hexdigest(), 16) % (2**32)


def generate_profile(customer_id: str) -> dict:
    rng = random.Random(_seed_for(customer_id))
    return {
        "customer_id": customer_id,
        "synthetic_name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
        "synthetic_age": rng.randint(18, 80),
        "risk_score": round(rng.uniform(0.0, 1.0), 4),
    }
```

### Pattern 3: View compartilhável (`src/open_insurance/shareable_view.py`)

```python
CREATE_SHAREABLE_VIEW_SQL = """
CREATE OR REPLACE VIEW {catalog}.gold.open_insurance_shareable AS
SELECT
    consent.customer_id,
    consent.target_institution,
    profile.synthetic_name,
    profile.synthetic_age,
    profile.risk_score,
    claims.claim_id,
    claims.event_type,
    claims.amount,
    claims.region
FROM {catalog}.gold.customer_consent consent
JOIN {catalog}.gold.open_insurance_profile profile
    ON consent.customer_id = profile.customer_id
LEFT JOIN {catalog}.gold.claims claims
    ON consent.customer_id = claims.customer_id
"""
```

### Pattern 4: Job agendado, reaproveitando `bronze_ingest.py` sem mudança

```yaml
# resources/jobs.open_insurance.yml (trecho — ingestão)
resources:
  jobs:
    open_insurance_bronze_ingest:
      name: insurance-open-insurance-bronze-ingest-${bundle.target}
      schedule:
        quartz_cron_expression: "0 */15 * * * ?"
        timezone_id: "America/Sao_Paulo"
        pause_status: UNPAUSED
      tasks:
        - task_key: bronze_consent_events
          environment_key: default
          spark_python_task:
            python_file: ../src/streaming/bronze_ingest.py
            parameters:
              - "--topic"
              - "consent-updated"
              - "--bronze-table"
              - "${var.catalog}.bronze.consent_events"
              - "--checkpoint-path"
              - "/Volumes/${var.catalog}/bronze/_checkpoints/consent-updated"
              - "--required-fields"
              - "customer_id,consent_status,target_institution"
      environments:
        - environment_key: default
          spec:
            environment_version: "2"
```

---

## Data Flow

```text
1. consent_events.py gera eventos fictícios GRANTED/REVOKED
   para uma amostra de customer_id (mesmo pool de gold.claims)
   │
   ▼
2. Producer publica em Kafka, tópico "consent-updated"
   (mesmo kafka_publisher.py, sem mudança)
   │
   ▼
3. Job agendado (15 min) roda bronze_ingest.py sem mudança de código
   → bronze.consent_events
   │
   ▼
4. Job agendado (30 min), task 1: consent_scd.py
   → aplica SCD2 via scd2_merge.py → silver.consent_current
   │
   ▼
5. Job agendado (30 min), task 2 (paralelo): profile_generator.py
   → gold.open_insurance_profile (overwrite determinístico)
   │
   ▼
6. Job agendado (30 min), task 3 (depende de 4 e 5): shareable_view.py
   → CREATE OR REPLACE VIEW gold.customer_consent (is_current=true)
   → CREATE OR REPLACE VIEW gold.open_insurance_shareable (consent+profile+claims)
```

---

## Integration Points

| External System | Integration Type | Authentication |
|------------------|-------------------|------------------|
| Confluent Cloud (Kafka) | Producer publica no tópico `consent-updated` | Mesmo secret scope `insurance-platform` já usado (`confluent-*`) |
| Unity Catalog | Tabelas/views novas em schemas já existentes | Sem credencial nova — mesmos grants já concedidos a `var.catalog_owner` |
| Delta Sharing (futuro, fora do MVP) | Share sobre `gold.open_insurance_shareable` | Documentado em `docs/ARCHITECTURE.md`, não provisionado |

---

## Testing Strategy

| Test Type | Scope | Files | Tools | Coverage Goal |
|-----------|-------|-------|-------|-----------------|
| Unit | `merge_scd2_into_delta` — fecha versão antiga + insere nova | `tests/unit/test_scd2_merge.py` | pytest + Delta local (mesmo padrão de `test_delta_merge.py`) | Casos: primeira inserção, GRANTED→REVOKED, evento fora de ordem (mais antigo que o vigente, deve ser ignorado) |
| Unit | `generate_profile` — determinismo | `tests/unit/test_profile_generator.py` | pytest | Mesmo `customer_id` chamado 2x produz o mesmo dict |
| Unit | `consent_scd.py` end-to-end (batch pequeno) | `tests/unit/test_consent_scd.py` | pytest + Spark local | Evento GRANTED aparece com `is_current=true`; revogação subsequente fecha a versão anterior |
| Unit | Lógica da view (join/filtro) | `tests/unit/test_shareable_view.py` | pytest + Spark local (monta os 3 DataFrames de entrada e valida o resultado do join, sem precisar de `CREATE VIEW` real) | AT-001, AT-002, AT-005 (LEFT JOIN — cliente sem claims aparece com claims nulos) |

---

## Error Handling

| Error Type | Handling Strategy | Retry? |
|------------|----------------------|--------|
| Evento de consentimento malformado (falta `customer_id`/`consent_status`) | Cai na quarentena já existente de `bronze_ingest.py` (`bronze.consent_events_quarantine`) — nenhum código novo necessário | Não (mesmo comportamento do resto do pipeline) |
| Evento fora de ordem (revogação chega antes da concessão, por timestamp) | `merge_scd2_into_delta` só fecha a versão vigente se `effective_column` do evento novo for maior que `valid_from` da vigente — eventos mais antigos são ignorados silenciosamente pelo merge (não geram erro, só não alteram o estado) | Não |
| `gold.claims` vazio no momento do join (cluster novo, sem dados ainda) | LEFT JOIN já trata isso naturalmente — view retorna clientes com claims nulos, não quebra | Não |

---

## Configuration

| Config Key | Type | Default | Description |
|------------|------|---------|--------------|
| `topics.consent_updated` (config.yaml do producer) | string | `"consent-updated"` | Nome do tópico Kafka |
| `sources.consent_simulator.enabled` | bool | `true` | Liga/desliga a geração de eventos de consentimento no producer |
| `sources.consent_simulator.sample_size` | int | `200` | Quantos `customer_id` distintos recebem eventos de consentimento simulados |

---

## Security Considerations

- Nenhum dado de PII real é introduzido — o perfil sintético é 100% gerado (nome/idade/score fictícios), e `customer_id` já é um identificador interno não-PII usado no resto da plataforma.
- `silver.consent_current` (SCD2) constitui a trilha de auditoria LGPD do módulo — preserva histórico completo de GRANTED/REVOKED, não sobrescreve.
- `gold.open_insurance_shareable` só expõe clientes com o consentimento **mais recente** igual a `GRANTED` — revogação remove o cliente automaticamente no próximo refresh (Decision 6 trata o caso de não haver claims, não o de consentimento negado).
- Delta Share real (fora do MVP) precisará, quando implementado, restringir o Recipient a enxergar só a view, nunca as tabelas base (`gold.claims`, `gold.open_insurance_profile` diretamente) — anotado em `docs/ARCHITECTURE.md` como requisito do próximo passo.

---

## Observability

| Aspect | Implementation |
|--------|------------------|
| Logging | Mesmo padrão dos demais módulos (sem logging customizado novo) |
| Data Quality | `consent_scd.py` roda `check_not_null`/`check_unique` (via `src/quality/checks.py`, já existente) sobre `customer_id`/`consent_status`, persistindo em `monitoring._dq_results` como os demais jobs |
| Métricas de negócio | Nenhuma nova — reaproveita o mesmo `monitoring._dq_results` já consultado pelo `pipeline_latency_monitor` e pelo Databricks App |

---

## Pipeline Architecture

### DAG Diagram

```text
[consent_simulator] ──produce──→ [Kafka: consent-updated]
                                        │
                          (job: open_insurance_bronze_ingest, */15min)
                                        ▼
                              bronze.consent_events
                                        │
                    (job: open_insurance_pipeline, */30min)
                    ┌───────────────────┼───────────────────┐
                    ▼                                       ▼
          silver.consent_current                  gold.open_insurance_profile
          (SCD2)                                   (determinístico, overwrite)
                    │                                       │
                    ▼                                       │
          gold.customer_consent (VIEW)                      │
                    │                                       │
                    └───────────────┬───────────────────────┘
                                    ▼
                    gold.open_insurance_shareable (VIEW)
                          + gold.claims (LEFT JOIN)
```

### Partition Strategy

| Table | Partition Key | Granularity | Rationale |
|-------|----------------|-------------|--------------|
| `bronze.consent_events` | `_ingested_date` | Diária | Mesmo padrão de todas as tabelas Bronze do projeto (via `bronze_ingest.py`, sem mudança) |
| `silver.consent_current` | Nenhuma | — | Volume baixo (1 linha vigente por cliente) não justifica partição |

### Incremental Strategy

| Model | Strategy | Key Column | Lookback |
|-------|----------|--------------|----------|
| `silver.consent_current` | SCD2 incremental via `merge_scd2_into_delta` | `customer_id` | N/A (processa todo novo evento desde o último checkpoint de bronze) |
| `gold.open_insurance_profile` | Full refresh (overwrite) | `customer_id` | N/A — recalculado a cada execução, determinístico |

### Schema Evolution Plan

| Change Type | Handling | Rollback |
|-------------|----------|----------|
| Novo campo em `consent-updated` (ex.: motivo da revogação) | Adicionar `nullable=True` em `CONSENT_EVENT_SCHEMA`, sem quebrar eventos antigos | Remover o campo do schema (registros antigos ficam sem o campo, sem erro) |
| Novo campo sintético no perfil | Adicionar em `generate_profile`, recalcula tudo no próximo run (overwrite) | Reverter o gerador, próximo run já sobrescreve |

### Data Quality Gates

| Gate | Tool | Threshold | Action on Failure |
|------|------|-----------|----------------------|
| `not_null` em `customer_id`/`consent_status` | `src/quality/checks.py` (`check_not_null`) | 0 nulos | Registra em `monitoring._dq_results` (mesmo padrão dos demais jobs — não bloqueia o pipeline, só alerta) |
| `unique` em `(customer_id)` vigente | `check_unique` sobre `silver.consent_current WHERE is_current` | 0 duplicatas | Registra em `monitoring._dq_results` |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|-----------|
| 1.0 | 2026-07-13 | design-agent | Versão inicial, a partir de `DEFINE_OPEN_INSURANCE.md` |

---

## Next Step

**Ready for:** `/build .claude/sdd/features/DESIGN_OPEN_INSURANCE.md`
