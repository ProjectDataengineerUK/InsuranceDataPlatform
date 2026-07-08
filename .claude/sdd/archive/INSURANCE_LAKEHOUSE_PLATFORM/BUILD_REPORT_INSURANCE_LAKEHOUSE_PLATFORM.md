# BUILD REPORT: Insurance Lakehouse Platform

> Implementação do MVP da plataforma de dados de seguros 100% Databricks (Kafka + Spark Structured Streaming), conforme DESIGN.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | INSURANCE_LAKEHOUSE_PLATFORM |
| **Date** | 2026-07-08 |
| **Author** | build-agent |
| **DEFINE** | [DEFINE_INSURANCE_LAKEHOUSE_PLATFORM.md](../features/DEFINE_INSURANCE_LAKEHOUSE_PLATFORM.md) |
| **DESIGN** | [DESIGN_INSURANCE_LAKEHOUSE_PLATFORM.md](../features/DESIGN_INSURANCE_LAKEHOUSE_PLATFORM.md) |
| **Status** | Complete (com ressalvas de verificação — ver Blockers) |

---

## Summary

| Metric | Value |
|--------|-------|
| **Tasks Completed** | 10/10 |
| **Files Created** | 56 (33 do manifesto do DESIGN + 23 arquivos de suporte/fix necessários para o projeto rodar — ver Deviations) |
| **Lines of Code** | ~1.962 |
| **Build Time** | 1 sessão |
| **Tests Passing** | 0/12 executados nesta sandbox (sem Java/JVM disponível) — 12/12 verificados por sintaxe (`py_compile`) e lint (`ruff`); execução real fica para o CI (GitHub Actions, que já provisiona Java 17) |
| **Agents Used** | 0 (build executado diretamente; nenhum subagente especialista foi invocado nesta sessão — ver nota em Agent Contributions) |

---

## Task Execution with Agent Attribution

| # | Task | Agent | Status | Notes |
|---|------|-------|--------|-------|
| 1 | Scaffold do projeto (requirements, módulos comuns, config) | (direct) | ✅ Complete | requirements.txt, pyproject.toml, .gitignore, src/common/* |
| 2 | Producer de ingestão (replay Kafka) | (direct) | ✅ Complete | loaders SUSEP/ANS, publisher, main, config, Dockerfile |
| 3 | Jobs Bronze/Silver + módulo de qualidade | (direct) | ✅ Complete | bronze_ingest.py, checks.py, silver_transform.py |
| 4 | Fraude, Gold, treino ML, monitoramento SLA | (direct) | ✅ Complete | streaming_score.py, gold_aggregate.py, train_model.py, sla_alerts.py |
| 5 | Terraform IaC (Unity Catalog, secrets) | (direct) | ✅ Complete | main.tf, unity_catalog.tf, secrets.tf, variables.tf |
| 6 | Databricks Asset Bundles (databricks.yml + resources) | (direct) | ✅ Complete | 4 jobs definidos (bronze, silver, gold, ml_training) |
| 7 | GitHub Actions CI/CD | (direct) | ✅ Complete | ci.yml, deploy.yml |
| 8 | Testes unitários e de integração | (direct) | ✅ Complete | 4 arquivos de teste + conftest.py |
| 9 | Documentação (README, ARCHITECTURE.md) | (direct) | ✅ Complete | + tfvars.example, data/.gitkeep |
| 10 | Verificação e Build Report | (direct) | ✅ Complete | Este documento |

**Legend:** ✅ Complete | 🔄 In Progress | ⏳ Pending | ❌ Blocked

**Nota sobre agentes:** O DESIGN atribuiu arquivos a agentes especialistas (`@spark-streaming-architect`, `@medallion-architect`, `@lakehouse-architect`, `@ci-cd-specialist`, `@data-quality-analyst`, `@data-contracts-engineer`, `@spark-engineer`). Nesta execução o código foi escrito diretamente, seguindo os padrões e a atribuição de responsabilidade definidos no DESIGN, sem delegação real via subagentes — a coluna "Agent" no manifesto original permanece como guia de especialização por área, não como registro de execução real.

---

## Files Created

| File | Lines | Verified | Notes |
| ---- | ----- | -------- | ----- |
| `requirements.txt` | 8 | ✅ | pyspark, delta-spark, confluent-kafka, mlflow, scikit-learn, pandas, requests, pyyaml |
| `requirements-dev.txt` | 5 | ✅ | pytest, pytest-cov, chispa, ruff |
| `pyproject.toml` | 11 | ✅ | config ruff + pytest |
| `.gitignore` | 18 | ✅ | inclui `terraform/environments/*.tfvars` |
| `src/common/spark_session.py` | 17 | ✅ py_compile + ruff | factory de SparkSession com AQE/Delta |
| `src/common/schemas.py` | 51 | ✅ py_compile + ruff | schemas claim/policy/customer + registry por tópico |
| `src/common/kafka_config.py` | 41 | ✅ py_compile + ruff | fallback de secrets via env var quando fora do Databricks |
| `src/ingestion/producer/datasets/base_loader.py` | 39 | ✅ py_compile + ruff | download + mapeamento de colunas genérico (não estava no manifesto original — extraído para evitar duplicação entre susep/ans loaders) |
| `src/ingestion/producer/datasets/susep_loader.py` | 38 | ✅ py_compile + ruff | URL exata do CSV não verificada (ver Blockers) |
| `src/ingestion/producer/datasets/ans_loader.py` | 37 | ✅ py_compile + ruff | URL exata do CSV não verificada (ver Blockers) |
| `src/ingestion/producer/kafka_publisher.py` | 64 | ✅ py_compile + ruff | publish_events com throttle configurável |
| `src/ingestion/producer/main.py` | 98 | ✅ py_compile + ruff | orquestra fontes em threads separadas |
| `src/ingestion/producer/config.yaml` | 30 | ✅ yaml.safe_load | dataset_url em branco, a preencher (ver Blockers) |
| `src/ingestion/producer/Dockerfile` | 13 | ✅ (revisão manual) | build não executado (sem Docker no sandbox) |
| `src/streaming/bronze_ingest.py` | 87 | ✅ py_compile + ruff | generalizado para os 3 tópicos via SCHEMA_REGISTRY; grava eventos malformados em `{bronze_table}_quarantine` (fix pós-revisão, ver Issues #9) |
| `src/quality/checks.py` | 67 | ✅ py_compile + ruff | not_null, unique, range, freshness |
| `src/streaming/silver_transform.py` | 105 | ✅ py_compile + ruff | dedup + MERGE upsert; corrigido para usar `spark.catalog.tableExists` (ver Issues) |
| `src/fraud/streaming_score.py` | 80 | ✅ py_compile + ruff | heurística de 3 features (noturno, frequência, outlier de valor) |
| `src/streaming/gold_aggregate.py` | 93 | ✅ py_compile + ruff | corrigido bug de `replaceWhere` em tabela inexistente (ver Issues) |
| `src/fraud/train_model.py` | 74 | ✅ py_compile + ruff | usa fraud_score heurístico como rótulo fraco (bootstrap) |
| `src/monitoring/sla_alerts.py` | 60 | ✅ py_compile + ruff | alerta via webhook, fallback para log |
| `terraform/main.tf` | 26 | ✅ terraform fmt | backend trocado de local para `cloud` (HCP Terraform) — ver Issues |
| `terraform/variables.tf` | 44 | ✅ terraform fmt | validação de `environment` restrita a dev/staging/prod |
| `terraform/unity_catalog.tf` | 64 | ✅ terraform fmt | masking de `customer_id` movido para `sql/governance_setup.sql` — ver Deviations |
| `terraform/secrets.tf` | 27 | ✅ terraform fmt | secret scope `insurance-platform`, alinhado a `kafka_config.py` |
| `terraform/environments/dev.tfvars.example` | 6 | ✅ (revisão manual) | não estava no manifesto original |
| `sql/governance_setup.sql` | 15 | ✅ (revisão manual) | não estava no manifesto original — ver Deviations |
| `databricks.yml` | 42 | ✅ yaml.safe_load | targets dev/staging/prod, sem empacotamento wheel (ver Deviations) |
| `resources/jobs.bronze.yml` | 52 | ✅ yaml.safe_load | job contínuo, 3 tasks (claims/policies/customers) |
| `resources/jobs.silver.yml` | 48 | ✅ yaml.safe_load | job contínuo, 2 tasks (claims/policies) |
| `resources/jobs.gold.yml` | 43 | ✅ yaml.safe_load | agendado a cada 10 min + task de SLA encadeada |
| `resources/jobs.ml_training.yml` | 27 | ✅ yaml.safe_load | agendado diariamente às 03:00 |
| `.github/workflows/ci.yml` | 50 | ✅ yaml.safe_load | lint + testes + validação de bundle |
| `.github/workflows/deploy.yml` | 71 | ✅ yaml.safe_load | terraform apply + bundle deploy, dev depois prod (gated) |
| `tests/conftest.py` | 19 | ✅ py_compile + ruff | fixture de SparkSession local |
| `tests/unit/test_bronze_ingest.py` | 30 | ✅ py_compile + ruff | cobre a lógica de quarentena (fix do Issue #9) — não estava no manifesto original |
| `tests/unit/test_quality_checks.py` | 46 | ✅ py_compile + ruff | 4 testes |
| `tests/unit/test_silver_transform.py` | 30 | ✅ py_compile + ruff | 2 testes |
| `tests/unit/test_fraud_score.py` | 58 | ✅ py_compile + ruff | 3 testes |
| `tests/integration/test_bronze_to_gold.py` | 48 | ✅ py_compile + ruff | 1 teste ponta a ponta (Bronze→Silver→Gold, in-memory) |
| `docs/ARCHITECTURE.md` | 69 | ✅ (revisão manual) | inclui limitações/roadmap |
| `README.md` | 99 | ✅ (revisão manual) | setup, deploy, governança pós-deploy |
| `data/.gitkeep` | 0 | ✅ | mantém a pasta `data/` versionada apesar do `.gitignore` |
| 13× `__init__.py` | 0 cada | ✅ | pacotes Python (`src`, `src/common`, `src/ingestion`, `src/ingestion/producer`, `src/ingestion/producer/datasets`, `src/streaming`, `src/quality`, `src/fraud`, `src/monitoring`, `tests`, `tests/unit`, `tests/integration`) |

**Total:** 56 arquivos, ~1.962 linhas.

---

## Verification Results

### Lint Check

```text
$ ruff check .
All checks passed!
```

**Status:** ✅ Pass

### Syntax Check (substitui Type Check — projeto Python sem type checker configurado)

```text
$ find src tests -name "*.py" | xargs python3 -m py_compile
OK: todos compilam
```

**Status:** ✅ Pass

### YAML / Terraform Syntax

```text
$ python3 -c "yaml.safe_load(...)" para databricks.yml, resources/*.yml,
  .github/workflows/*.yml, src/ingestion/producer/config.yaml
Todos OK

$ cd terraform && terraform fmt -check -diff
(sem output — formatação já correta)
```

**Status:** ✅ Pass

### Tests

```text
Não executados nesta sandbox: sem Java/JVM disponível (`java: comando não
encontrado`), pré-requisito do PySpark. Sem acesso a `apt`/`sudo` ou
`python3-venv` para contornar via ambiente isolado.

Os 12 testes (4 test_quality_checks + 2 test_silver_transform +
3 test_fraud_score + 2 test_bronze_ingest + 1 test_bronze_to_gold) foram
verificados por leitura manual da lógica e por py_compile/ruff.
O workflow `.github/workflows/ci.yml` já provisiona Java 17
(actions/setup-java@v4) e roda `pytest tests/unit` + `pytest tests/integration`
a cada PR — a primeira execução em CI é o ponto real de verificação de
runtime.
```

| Test | Result |
|------|--------|
| `test_check_not_null_detects_nulls` | ⏭️ Não executado (sem JVM) — revisado manualmente |
| `test_check_not_null_passes_when_no_nulls` | ⏭️ Não executado — revisado manualmente |
| `test_check_unique_detects_duplicates` | ⏭️ Não executado — revisado manualmente |
| `test_check_range_detects_out_of_range_values` | ⏭️ Não executado — revisado manualmente |
| `test_deduplicate_keeps_latest_by_order_column` | ⏭️ Não executado — revisado manualmente |
| `test_deduplicate_is_noop_when_no_duplicates` | ⏭️ Não executado — revisado manualmente |
| `test_add_night_claim_feature_flags_night_events` | ⏭️ Não executado — revisado manualmente |
| `test_add_frequency_feature_flags_high_frequency_customers` | ⏭️ Não executado — revisado manualmente |
| `test_compute_fraud_score_produces_bounded_scores` | ⏭️ Não executado — revisado manualmente |
| `test_malformed_json_is_flagged` | ⏭️ Não executado — revisado manualmente |
| `test_valid_event_is_not_flagged` | ⏭️ Não executado — revisado manualmente |
| `test_bronze_to_gold_pipeline_end_to_end` | ⏭️ Não executado — revisado manualmente (valores esperados recalculados à mão, ver corpo do teste) |

**Status:** ⏭️ 0/12 executados nesta sandbox (ambiente sem JVM) | 12/12 revisados por leitura + sintaxe/lint

---

## Issues Encountered

| # | Issue | Resolution |
|---|-------|------------|
| 1 | `raise RuntimeError(...)` dentro de `except Exception` sem `from err` (ruff B904) em `kafka_config.py` | Adicionado `from exc` |
| 2 | Geração de `claim_id`/`policy_id` fallback em `susep_loader.py`/`ans_loader.py` usando `Series.fillna` com índice desalinhado (bug real — produziria todos os valores como NaN quando a coluna de origem não existisse) | Reescrito para gerar UUID por linha diretamente, sem depender de alinhamento de índice |
| 3 | `gold_aggregate.py`: `write.mode("overwrite").option("replaceWhere", ...)` na primeira execução, quando a tabela Gold ainda não existe, é um padrão frágil do Delta (replaceWhere espera tabela existente) | Adicionado helper `_write_gold_partition` que checa `spark.catalog.tableExists` antes de decidir entre overwrite completo (primeira vez) e `replaceWhere` (execuções seguintes), mesmo padrão já usado em `silver_transform.py` |
| 4 | Uso de `DeltaTable.isDeltaTable(spark, table_name)` com um nome de tabela do catálogo (`catalog.schema.table`) — essa API espera um path de filesystem, não um identificador de catálogo, o que poderia falhar silenciosamente ou lançar erro | Trocado por `spark.catalog.tableExists(table_name)` em `silver_transform.py` e `gold_aggregate.py`, API documentada para identificadores de catálogo |
| 5 | Terraform `backend "local"` no DESIGN original perderia o state a cada execução do GitHub Actions (runners são efêmeros) | Trocado para `cloud` block (HCP Terraform/Terraform Cloud), com `TF_WORKSPACE` selecionando o ambiente (dev/prod) via tag, e `TF_TOKEN_app_terraform_io` como secret no `deploy.yml` |
| 6 | `databricks.yml` referenciava um artifact de wheel (`pip wheel . --no-deps`) sem que `pyproject.toml` tivesse seção `[build-system]`/`[project]` — build do bundle quebraria | Removido o artifact de wheel; jobs usam `spark_python_task` referenciando os `.py` sincronizados como Workspace Files pelo próprio DAB (mais simples para o estágio atual do projeto) |
| 7 | `unity_catalog.tf` originalmente tentava um `databricks_sql_table` gerenciando a tabela `claims` para aplicar masking — conflitaria com a tabela sendo criada via `saveAsTable` pelos jobs Spark (dois sistemas de IaC disputando o mesmo recurso) | Removido; masking movido para `sql/governance_setup.sql`, aplicado manualmente/via job após o primeiro deploy do Gold (documentado no README) |
| 8 | Sem Java/JVM no sandbox de build — `pytest` não pôde ser executado de fato; `python3 -m venv` também indisponível (`python3-venv` ausente, sem sudo) | Verificação por `py_compile` + `ruff` + revisão manual da lógica dos testes; execução real delegada ao CI (`ci.yml` já provisiona Java 17) |
| 9 | AT-002 do DEFINE exige isolamento de eventos malformados em quarentena; a primeira versão de `bronze_ingest.py` apenas deixava `from_json` produzir nulls silenciosamente, sem tabela de quarentena real (gap identificado na Acceptance Test Verification) | Reescrito para usar `foreachBatch`, separando o batch em `valid_df` (grava em `bronze_table`) e `quarantine_df` (grava em `{bronze_table}_quarantine`, com `raw_value` e `_reason`); adicionado `tests/unit/test_bronze_ingest.py` cobrindo a detecção |

---

## Deviations from Design

| Deviation | Reason | Impact |
|-----------|--------|--------|
| +22 arquivos além dos 33 do manifesto original (`requirements*.txt`, `pyproject.toml`, `.gitignore`, 13× `__init__.py`, `base_loader.py`, `sql/governance_setup.sql`, `terraform/environments/dev.tfvars.example`, `data/.gitkeep`, `tests/conftest.py`) | Necessários para o projeto Python ser importável, testável e instalável — o DESIGN listou os módulos de domínio mas não a infraestrutura mínima de projeto | Nenhum impacto negativo; são artefatos padrão de qualquer projeto Python real |
| `src/ingestion/producer/datasets/base_loader.py` criado (não estava no manifesto) | `susep_loader.py` e `ans_loader.py` compartilhavam lógica de download/mapeamento de colunas — extraído para evitar duplicação (DRY) | Reduz risco de divergência entre os dois loaders |
| Masking de `customer_id` implementado via `sql/governance_setup.sql` em vez de recurso Terraform dedicado | Evitar Terraform e Spark disputando ownership da mesma tabela (ver Issue #7) | Passo de governança agora é manual/pós-deploy, não 100% automatizado pelo `terraform apply` — documentado como próximo passo de automação no roadmap |
| `databricks.yml` não empacota `src/` como wheel | `pyproject.toml` não tinha metadata de build; wheel adicionaria complexidade desnecessária neste estágio | Jobs usam os `.py` diretamente via Workspace Files (funciona, mas empacotamento em wheel é recomendado ao amadurecer o projeto) |
| URLs exatas dos datasets SUSEP/ANS deixadas em branco em `config.yaml` | Os portais de dados abertos versionam recursos por período sem link fixo estável (confirmado durante o /design) — resolver exige navegação manual no portal | Bloqueia a execução real do producer até alguém preencher `dataset_url`; documentado no README como passo de setup |

---

## Blockers (if any)

| Blocker | Required Action | Owner |
|---------|-----------------|-------|
| URLs exatas dos CSVs da SUSEP/ANS não resolvidas | Navegar manualmente nos portais (links em `docs/ARCHITECTURE.md`) e preencher `sources.susep.dataset_url` / `sources.ans.dataset_url` em `src/ingestion/producer/config.yaml` | Usuário |
| Mapeamento de colunas em `susep_loader.py`/`ans_loader.py` (`COLUMN_MAPPING`) é uma melhor estimativa, não validada contra os CSVs reais | Baixar os arquivos reais e ajustar `COLUMN_MAPPING` conforme o schema real (Assumption A-003 do DEFINE) | Usuário / próxima sessão |
| Execução real dos testes (pytest com Spark) não verificada nesta sandbox (sem Java) | Rodar `pytest` localmente com Java 17 instalado, ou confiar na primeira execução do `ci.yml` | CI / usuário |
| Credenciais Confluent Cloud, Databricks e HCP Terraform ainda não provisionadas | Criar conta Confluent Cloud (free tier), workspace Databricks, organização HCP Terraform, e configurar os secrets listados em `deploy.yml` (`DATABRICKS_HOST_DEV/PROD`, `DATABRICKS_TOKEN_DEV/PROD`, `CONFLUENT_*`, `TF_CLOUD_TOKEN`) | Usuário |

---

## Acceptance Test Verification

| ID | Scenario | Status | Evidence |
|----|----------|--------|----------|
| AT-001 | Happy path — sinistro processado ponta a ponta | 🔶 Parcial | Lógica coberta por `tests/integration/test_bronze_to_gold.py` (revisão manual, não executado); execução real requer deploy em Databricks + Kafka reais |
| AT-002 | Evento malformado não quebra o pipeline | ✅ Verificado por revisão | `bronze_ingest.py` agora separa cada micro-batch em `valid_df`/`quarantine_df` via `_is_malformed` (payload nulo ou campos obrigatórios ausentes) e grava a quarentena em `{bronze_table}_quarantine`, preservando `raw_value`; coberto por `tests/unit/test_bronze_ingest.py` (revisado manualmente, não executado nesta sandbox) |
| AT-003 | Pico de volume não causa perda de dados | ⏭️ Não verificável nesta sandbox | Depende de comportamento nativo do Structured Streaming (trigger + backpressure) em cluster real; não há teste de carga incluído |
| AT-004 | CI/CD bloqueia merge com testes quebrados | ✅ Verificado por revisão | `ci.yml` roda `ruff check` + `pytest` como jobs obrigatórios antes de `validate-bundle`; falha de teste falha o workflow (comportamento padrão do GitHub Actions) |
| AT-005 | Governança aplica mascaramento/RLS | 🔶 Parcial | `sql/governance_setup.sql` implementa a função de masking; aplicação é manual pós-deploy (ver Deviations), não testada contra um workspace real |

**Nota:** AT-002 revelou um gap real entre o DESIGN (que menciona "área de quarentena") e a implementação — `bronze_ingest.py` atual não grava explicitamente os eventos que falham o parse em uma tabela `*_quarantine`. Recomenda-se tratar isso na próxima iteração (`/iterate DESIGN_INSURANCE_LAKEHOUSE_PLATFORM.md "adicionar tabela de quarentena explícita no Bronze"`).

---

## Final Status

### Overall: 🔄 IN PROGRESS (código completo, verificado estaticamente e com AT-002 corrigido; falta apenas execução real em ambiente com Java/Databricks)

**Completion Checklist:**

- [x] Todos os arquivos do manifesto do DESIGN criados (33/33) + suporte de projeto
- [x] Lint (`ruff`) e sintaxe (`py_compile`) passam em 100% dos arquivos
- [x] YAML e Terraform validados sintaticamente
- [ ] Testes executados de fato (bloqueado por falta de Java na sandbox — não bloqueia CI)
- [x] Quarentena explícita de eventos malformados no Bronze (AT-002 corrigido nesta sessão)
- [x] Nenhum blocker impede o /ship do MVP como está, desde que os itens acima virem itens de roadmap explícitos
- [x] Pronto para /ship com ressalvas documentadas

---

## Next Step

**Recomendado:** `/ship .claude/sdd/features/DEFINE_INSURANCE_LAKEHOUSE_PLATFORM.md`, registrando os itens pendentes (quarentena no Bronze, validação dos datasets reais, execução do CI) como lições aprendidas / próximos passos.

**Alternativa:** `/iterate DESIGN_INSURANCE_LAKEHOUSE_PLATFORM.md "adicionar tabela de quarentena explícita no Bronze"` antes do /ship, se o gap de AT-002 for considerado bloqueante.
