# Insurance Lakehouse Platform

Plataforma de dados de seguros 100% Databricks — ingestão de dados públicos reais (SUSEP, ANS) via Kafka, processamento em tempo real com Spark Structured Streaming puro (sem DLT/Lakeflow), arquitetura medallion (Bronze/Silver/Gold), detecção de fraude, aprovação automática, governança via Unity Catalog e CI/CD via GitHub Actions + Databricks Asset Bundles + Terraform.

Ver a arquitetura completa em [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) e o histórico de decisões em [`.claude/sdd/features/`](.claude/sdd/features/).

## Stack

- **Processamento:** Apache Spark (Structured Streaming), Delta Lake
- **Mensageria:** Apache Kafka via Confluent Cloud
- **Plataforma:** Databricks (Unity Catalog, Workflows, Asset Bundles, MLflow)
- **IaC:** Terraform + Databricks Asset Bundles
- **CI/CD:** GitHub Actions
- **ML:** scikit-learn + MLflow

## Setup local

### Pré-requisitos

- Python 3.11+
- Java 17 (necessário para o PySpark local)
- Conta no Confluent Cloud (free tier) com um cluster Kafka provisionado
- Databricks CLI (`pip install databricks-cli` ou via `curl` — ver [docs oficiais](https://docs.databricks.com/dev-tools/cli/))
- Terraform >= 1.5

### Instalação

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

### Rodando os testes

```bash
ruff check .
pytest tests/unit
pytest tests/integration
```

### Datasets públicos usados

1. **SUSEP** — microdados reais de sinistros de auto (um registro por sinistro avisado, já anonimizado), baixados automaticamente pelo producer a partir de `sources.susep.dataset_url` em `config.yaml` — ver layout oficial em [Bases de Dados Anonimizadas](https://www.gov.br/susep/pt-br/central-de-conteudos/dados-estatisticos/bases-anonimizadas).
2. **ANS** — movimentação real de beneficiários (inclusão/cancelamento/reativação), baixada a partir de `sources.ans.dataset_url` — schema confirmado em `ans_loader.py`, portal em [dados abertos ANS](https://dadosabertos.ans.gov.br/FTP/PDA/dados_de_beneficiarios_por_operadora/).

Os portais versionam os recursos por período — se o link em `config.yaml` expirar, é só localizar a versão atual no portal e atualizar `dataset_url`.

### Producer — como ele fica rodando

O producer (baixa o CSV, normaliza e publica no Kafka) **roda automaticamente via GitHub Actions** (`.github/workflows/producer.yml`): um `schedule` de 10 em 10 minutos dispara uma execução que publica eventos por até 8 minutos (`PRODUCER_MAX_DURATION_SECONDS`) e encerra sozinha (flush gracioso, sem depender de kill forçado), reusando os secrets `CONFLUENT_*` já configurados. Sem essa automação, os jobs contínuos no Databricks (`bronze_ingest`, `silver_transform`, `fraud_score_stream`) ficam de pé mas ociosos — nenhum evento chega no Kafka.

Pra rodar manualmente (debug local, ou uma rajada mais longa sob demanda):

```bash
export CONFLUENT_BOOTSTRAP_SERVERS="..."
export CONFLUENT_API_KEY="..."
export CONFLUENT_API_SECRET="..."

python -m src.ingestion.producer.main   # loop infinito por padrão (replay.loop: true)
```

Ou via Docker:

```bash
docker build -f src/ingestion/producer/Dockerfile -t insurance-producer .
docker run --env-file .env insurance-producer
```

Ou disparando o workflow manualmente pela aba Actions do GitHub (`workflow_dispatch`, com `duration_seconds` configurável).

## Deploy

### Infraestrutura (Terraform)

```bash
cd terraform
cp environments/dev.tfvars.example environments/dev.tfvars  # preencha os valores (arquivo já é ignorado pelo git)
terraform init
terraform apply -var-file=environments/dev.tfvars
```

### Jobs/Workflows (Databricks Asset Bundles)

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

O CI/CD (`.github/workflows/deploy.yml`) automatiza os dois passos acima para `dev` a cada push em `main`, seguido de deploy em `prod` (gated por aprovação manual via GitHub Environments).

**Sobre o state do Terraform no CI:** usamos backend `local`, com o arquivo `terraform.tfstate` restaurado/salvo via `actions/cache` entre execuções do workflow (sem depender de um serviço externo como HCP Terraform). O workflow usa `concurrency` para nunca rodar dois deploys em paralelo, mas não há lock distribuído de verdade — ver limitações em [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

### Governança

O masking de `customer_id` é aplicado automaticamente: o job `gold_aggregate` (a cada execução, 10 em 10 min) reaplica o `CREATE OR REPLACE FUNCTION` + `ALTER TABLE ... SET MASK` via `apply_governance()` em `src/streaming/gold_aggregate.py`, assim que a tabela `gold.claims` existir. Não há passo manual a rodar após o deploy. `sql/governance_setup.sql` fica só como referência/fallback caso precise ser aplicado manualmente fora do job (ex.: workspace novo, antes do primeiro run).

### Validação de latência e volume (AT-001 / AT-003)

Depois que o pipeline estiver rodando de verdade em `dev` (producer publicando no Kafka + jobs `bronze_ingest`/`fraud_score_stream` ativos), rode:

```bash
python scripts/measure_pipeline_latency.py --catalog insurance_dev
```

Mede latência Kafka→Bronze (alvo < 2 min), latência de visibilidade do score de fraude Bronze→Gold (alvo < 1 min) e volume ingerido por minuto vs. os `replay.events_per_minute` configurados no producer. Só produz números reais com o pipeline em execução — não substitui rodar o sistema de verdade.

## Estrutura do projeto

Ver [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#estrutura-do-repositório).

## Roadmap

Ver seção "Limitações conhecidas / roadmap" em [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#limitações-conhecidas--roadmap).
