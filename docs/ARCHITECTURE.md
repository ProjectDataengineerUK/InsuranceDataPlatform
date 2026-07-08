# Arquitetura — Insurance Lakehouse Platform

> Ver histórico completo de decisões em [`.claude/sdd/features/`](../.claude/sdd/features/): `BRAINSTORM_INSURANCE_LAKEHOUSE_PLATFORM.md`, `DEFINE_INSURANCE_LAKEHOUSE_PLATFORM.md`, `DESIGN_INSURANCE_LAKEHOUSE_PLATFORM.md`.

## Visão geral

Plataforma de dados de seguros 100% Databricks: ingestão de dados públicos reais (SUSEP, ANS) via Kafka (Confluent Cloud), processados com Spark Structured Streaming puro (sem DLT/Lakeflow) em uma arquitetura medallion (Bronze → Silver → Gold), com detecção de fraude em streaming, aprovação automática, governança via Unity Catalog e CI/CD via GitHub Actions + Databricks Asset Bundles + Terraform.

```text
[SUSEP CSV]  [ANS CSV]
      │            │
      └─────┬──────┘
            ▼
    Kafka Producer (Docker)
            ▼
   Confluent Cloud (Kafka)
            ▼
      Bronze Job (Spark Structured Streaming)
            ▼
      Bronze Delta
            ▼
      Silver Job (dedup + quality checks)
            ▼
      Silver Delta
            ▼
      Fraud Score Job (streaming contínuo, <1min — fraude + auto-approval)
            ▼
      Gold Delta (claims)
            ▼
      Gold Aggregate Job (batch, 10 em 10 min — agregados por região + governança)
            ▼
      Gold Delta (claims_region_agg)
            ▼
  SQL Warehouse / Dashboards / MLflow / Alertas de SLA
```

## Por que essas escolhas

- **Spark Structured Streaming puro, sem DLT/Lakeflow**: decisão deliberada para demonstrar domínio de streaming nativo, não de pipelines declarativos.
- **Kafka no Confluent Cloud**: Databricks não hospeda Kafka nativamente; o producer roda fora da plataforma para simular fielmente uma fonte de eventos externa real.
- **Workspace único, catálogos por ambiente**: equilíbrio entre custo de portfólio e evolução real para produção (ver Decision 4 do DESIGN).
- **Framework de qualidade custom em PySpark**: substitui `@dlt.expect_or_drop`, mantendo a decisão "100% Spark".
- **SUSEP + ANS obrigatórios; Open Insurance Brasil como fast-follow**: a Fase 1 do Open Insurance exige credenciamento no diretório sandbox, o que adicionaria uma dependência externa fora do controle do time.

## Estrutura do repositório

```text
.
├── src/
│   ├── common/         # SparkSession, schemas, config Kafka
│   ├── ingestion/producer/   # download + replay dos datasets públicos para o Kafka
│   ├── streaming/       # jobs Bronze, Silver, Gold
│   ├── quality/         # framework de qualidade de dados (sem DLT)
│   ├── fraud/           # score de fraude (streaming) + treino do modelo (MLflow)
│   └── monitoring/      # alertas de SLA
├── terraform/           # Unity Catalog, secrets (IaC que muda raramente)
├── resources/            # Jobs/Workflows via Databricks Asset Bundles
├── databricks.yml        # root do bundle (targets dev/staging/prod)
├── sql/                  # scripts de governança (masking) — referência/fallback; aplicados automaticamente pelo job Gold Aggregate
├── tests/                # unit + integration (pytest + Spark local)
└── .github/workflows/    # CI (lint+test) e Deploy (Terraform + DAB)
```

## Fronteira Terraform x Databricks Asset Bundles

Terraform gerencia o que muda raramente: catálogos/schemas do Unity Catalog, secret scopes, grants. Databricks Asset Bundles gerencia o que muda com frequência: Jobs, Workflows, parâmetros. As tabelas Delta em si (Bronze/Silver/Gold) são criadas pelos próprios jobs Spark via `saveAsTable`/`MERGE` — nenhuma delas é gerenciada pelo Terraform, para evitar conflito de ownership entre os dois sistemas.

## Criação do catálogo Unity Catalog em contas trial com "Default Storage" (2026-07-08)

Em workspaces trial da Databricks com o recurso de conta "Default Storage" habilitado, `terraform apply` falha ao criar `databricks_catalog` do zero com `Error: cannot create catalog: Metastore storage root URL does not exist` — o Terraform não herda automaticamente o Default Storage do metastore, mesmo ele existindo e funcionando pela UI. Workaround adotado: o catálogo é criado uma vez manualmente pela UI do Databricks (que usa o Default Storage sem fricção) e o `deploy.yml` roda `terraform import databricks_catalog.insurance "insurance_${TF_VAR_environment}" || true` antes de cada `apply` — idempotente, só faz efeito na primeira vez que o catálogo existe fora do state. Para contas de produção com storage próprio (S3 + IAM), a alternativa correta é declarar `storage_root` explícito com uma External Location/Storage Credential gerenciados via Terraform.

## State do Terraform em CI (decisão pós-ship em 2026-07-08)

O DESIGN original previa HCP Terraform (Terraform Cloud) como backend remoto. Por preferência explícita do usuário (evitar depender de mais um serviço externo), trocamos para backend `local` com o `terraform.tfstate` restaurado/salvo via `actions/cache` a cada execução do `deploy.yml`, e um bloco `concurrency` no workflow garantindo que nunca haja dois `terraform apply` rodando ao mesmo tempo.

**Trade-offs aceitos conscientemente:**
- Sem lock distribuído real (mitigado, não eliminado, pela serialização do workflow via `concurrency`)
- O cache do GitHub Actions pode ser despejado (7 dias sem uso, ou limite de 10GB por repositório) — se isso acontecer, o próximo `terraform apply` não encontra o state anterior e tentaria recriar recursos já existentes (provavelmente falhando com "already exists" em vez de duplicar, já que os nomes são determinísticos, mas exigiria intervenção manual)
- Rodar `terraform apply` localmente (fora do CI) não tem acesso automático ao state salvo no cache do GitHub Actions — precisaria baixar manualmente via API ou reconciliar

**Quando reconsiderar:** se o projeto evoluir para múltiplos colaboradores rodando `terraform apply` com frequência, ou se um "already exists" real acontecer por cache perdido, vale migrar para um backend com lock de verdade (S3+DynamoDB, já que o Databricks trial atual está na AWS, ou HCP Terraform).

## Autenticação do Databricks CLI/DAB (2026-07-08)

`databricks.yml` não declara `workspace.host` nem `run_as.service_principal_name` — o Databricks CLI lê `DATABRICKS_HOST`/`DATABRICKS_TOKEN` diretamente do ambiente (já configurados como env vars no `deploy.yml`). A tentativa inicial de usar `${DATABRICKS_HOST}` no YAML quebrou o deploy real (`invalid character "{" in host name`) — essa sintaxe de shell não é suportada pelo DAB. Hoje os jobs rodam com o dono do PAT pessoal (`DATABRICKS_TOKEN_DEV`); migrar para service principals dedicados por ambiente (`run_as`) é um item de roadmap quando o projeto sair do trial.

## Natureza real dos datasets públicos (validado pós-ship em 2026-07-08)

- **SUSEP (AUTOSEG)**: confirmado via `DEFINICOES_AUTOSEG.pdf` (fonte oficial) que o dataset é **agregado** — cada linha é uma contagem/soma (`EXPOSICAO`, `PREMIO`, `FREQ_SIN1..9`, `INDENIZ1..9`) por grupamento de categoria tarifária/região/modelo/ano/sexo/faixa etária, não um sinistro individual. `susep_loader.py` gera sinistros sintéticos por linha, calibrados nas distribuições reais (frequência e indenização média por grupo) via amostragem log-normal — uma técnica estatística legítima, não dados inventados sem base real.
- **ANS (Dados de Beneficiários por Operadora)**: confirmado via `dicionario_de_dados_sib.ods` (fonte oficial) que é um dataset **real, um registro por movimentação de beneficiário** (inclusão, cancelamento, reativação, retificação), com `ID_MOTIVO_MOVIMENTO` identificando o tipo exato do evento. `ans_loader.py` usa os nomes de coluna reais (`CD_OPERADORA`, `CD_PLANO_RPS`, `DT_INCLUSAO`, `ID_MOTIVO_MOVIMENTO`) e infere `event_type` (`policy-created`/`policy-cancelled`/`policy-updated`) a partir dos códigos oficiais.
- **Limitação herdada**: nem SUSEP nem ANS expõem um identificador de cliente/segurado reutilizável (anonimização LGPD) — `customer_id` fica `None` em ambos os loaders. `premium_amount`, `coverage_type` e `region` também não estão disponíveis no dataset da ANS consultado.

## Limitações conhecidas / roadmap

- Open Insurance Brasil como fonte de dados: pendente de credenciamento no diretório sandbox.
- Chaves de agrupamento exatas do CSV real da SUSEP (região/categoria/modelo) não foram confirmadas no dicionário consultado — `susep_loader.py` usa nomes de coluna candidatos (`REGIAO`, `UF`, etc.) com fallback para `"UNKNOWN"`; ajustar assim que o CSV real for inspecionado.
- Volume real dos datasets SUSEP/ANS ainda não validado (ver Assumptions A-001 a A-004 do DEFINE) — recomenda-se um spike antes de dimensionar compute para produção real.
- Modelo de fraude usa rótulo fraco (heurística) para bootstrap inicial; evoluir para rótulos supervisionados reais é um próximo passo natural.
- `customer_id` não disponível em nenhuma das duas fontes — qualquer caso de uso que dependa de histórico por cliente (ex: frequência de sinistros do `streaming_score.py`) fica limitado até uma fonte complementar ser identificada.
