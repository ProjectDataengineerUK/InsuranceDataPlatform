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

### Baixando os datasets públicos

1. **SUSEP** — acesse [Bases de Dados Anonimizadas](https://www.gov.br/susep/pt-br/central-de-conteudos/dados-estatisticos/bases-anonimizadas) e baixe o CSV mais recente do sistema AUTOSEG (seguros de automóvel). **Importante:** este dataset é agregado (contagens/somas por categoria tarifária, região, modelo, ano — ver `DEFINICOES_AUTOSEG.pdf` no mesmo site), não um registro por sinistro. `susep_loader.py` gera sinistros sintéticos calibrados nessas distribuições reais — ver `docs/ARCHITECTURE.md`.
2. **ANS** — acesse o [FTP de dados abertos](https://dadosabertos.ans.gov.br/FTP/PDA/dados_de_beneficiarios_por_operadora/) e baixe o CSV do período mais recente. Este dataset é real, um registro por movimentação de beneficiário (inclusão/cancelamento/reativação) — schema confirmado em `ans_loader.py`.
3. Configure as URLs em `src/ingestion/producer/config.yaml` (`sources.susep.dataset_url`, `sources.ans.dataset_url`) — os portais versionam os recursos por período, então não há um link fixo.

### Rodando o producer localmente

```bash
export CONFLUENT_BOOTSTRAP_SERVERS="..."
export CONFLUENT_API_KEY="..."
export CONFLUENT_API_SECRET="..."

python -m src.ingestion.producer.main
```

Ou via Docker:

```bash
docker build -f src/ingestion/producer/Dockerfile -t insurance-producer .
docker run --env-file .env insurance-producer
```

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

### Governança pós-deploy

Depois do primeiro deploy do job Gold (que cria a tabela `claims`), aplique o masking de `customer_id`:

```bash
databricks sql query --file sql/governance_setup.sql
```

## Estrutura do projeto

Ver [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#estrutura-do-repositório).

## Roadmap

Ver seção "Limitações conhecidas / roadmap" em [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#limitações-conhecidas--roadmap).
