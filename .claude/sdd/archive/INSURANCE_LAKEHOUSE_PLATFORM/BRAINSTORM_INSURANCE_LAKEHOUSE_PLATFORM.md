# BRAINSTORM: Insurance Lakehouse Platform

> Sessão exploratória para clarificar intenção e abordagem antes da captura formal de requisitos

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | INSURANCE_LAKEHOUSE_PLATFORM |
| **Date** | 2026-07-08 |
| **Author** | brainstorm-agent |
| **Status** | Ready for Define |

---

## Initial Idea

**Raw Input:** Projeto de plataforma de dados de seguros (`InsuranceDataPlatform`). O diretório do projeto estava vazio; o contexto real veio de um arquivo `context.md` (encontrado em `.claude/hooks/context.md`) contendo uma conversa de ideação prévia no ChatGPT sobre uma "Insurance Lakehouse Platform" 100% Databricks.

**Context Gathered:**
- Projeto começou como diretório vazio (sem código, sem manifests).
- O único artefato existente era `context.md`, um export de conversa do ChatGPT com a ideação inicial do projeto (arquitetura, fontes de dados, casos de uso, nome do projeto).
- Confirmado via pesquisa web que a SUSEP disponibiliza dados abertos reais de sinistros de seguro (auto, rural, compreensivo), anonimizados conforme LGPD, como parte do Plano de Dados Abertos (PDA) 2024-2026, em formato CSV.

**Technical Context Observed (for Define):**

| Aspect | Observation | Implication |
|--------|-------------|-------------|
| Likely Location | `src/{ingestion,streaming,bronze,silver,gold,analytics,ml,monitoring}/`, `bundles/`, `terraform/`, `workflows/` | Estrutura de repositório sugerida no context.md original, a validar no /design |
| Relevant KB Domains | Databricks/Spark streaming, Delta Lake, Unity Catalog, Kafka, Databricks Asset Bundles, Terraform, MLflow | Agentes recomendados: `@spark-streaming-architect`, `@lakeflow-expert` (referência, não uso — ver decisão sobre DLT), `@databricks-spark-expert`, `@ci-cd-specialist` |
| IaC Patterns | Nenhum ainda — Terraform + Databricks Asset Bundles planejados | Infra as code desde o início, mesmo em fase de portfólio |

---

## Discovery Questions & Answers

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | Qual o problema principal a resolver? | Pipeline de sinistros/underwriting em tempo real | Define o domínio como streaming, não batch tradicional |
| 2 | Qual evento dispara o processamento? | Ambos: novo sinistro E nova solicitação de apólice/underwriting | Precisa de múltiplos tópicos Kafka (claim-*, policy-*) |
| 3 | Quem consome a saída? | Sistemas de decisão internos, analistas humanos, dashboards BI, APIs externas/parceiros | Gold layer precisa servir múltiplos padrões de consumo (SQL Warehouse, API, dashboard) |
| 4 | Stack/infra já definida? | Sim — 100% Databricks | Elimina GCP/AWS/Fabric como opções |
| 5 | Qual ferramenta de streaming? | Kafka + **somente Spark** (Structured Streaming) — sem DLT/Lakeflow declarativo | Toda Silver/Gold é Spark job orquestrado via Databricks Workflows, não pipelines declarativos |
| 6 | Propósito do projeto? | Ambos — começa como portfólio/demonstração, deve evoluir para produção real | Arquitetura precisa ser demo-friendly (custo baixo, fácil de rodar) mas com padrões de produção reais (CI/CD, IaC, governança) |
| 7 | Fontes de dados? | Datasets públicos reais (SUSEP, ANS, Open Insurance Brasil) | Sem geração sintética pura — usar dados reais anonimizados como base, republicados via producer para simular tempo real |
| 8 | Onde roda o Kafka? | Confluent Cloud (free tier) | Databricks não hospeda Kafka nativamente; Confluent Cloud evita gerenciar servidor |
| 9 | Escopo do MVP (fase 1)? | Pipeline Bronze→Silver→Gold completo E TODOS os casos de uso (fraude, aprovação automática, ML, SLA, auditoria, governança, CI/CD) — nada cortado por YAGNI | Escopo grande desde a fase 1, por decisão explícita do usuário (ver seção YAGNI abaixo) |
| 10 | Ferramenta de CI/CD? | GitHub Actions | Mais natural para repositório público de portfólio |
| 11 | Amostras de dados disponíveis? | Nenhuma ainda — precisa identificar fontes | `/define` e `/design` devem incluir tarefa de levantamento e validação de schema dos datasets SUSEP/ANS/Open Insurance |

**Minimum Questions:** 11 (mínimo de 3 excedido)

---

## Sample Data Inventory

| Type | Location | Count | Notes |
|------|----------|-------|-------|
| Input files | N/A ainda | 0 | A identificar: SUSEP (PDA 2024-2026, dados de sinistros auto/rural/compreensivo, anonimizados LGPD, CSV) via `dados.gov.br` e `gov.br/susep/pt-br/acesso-a-informacao/dados-abertos`; ANS; Open Insurance Brasil |
| Output examples | N/A | 0 | A definir no /design junto com o schema Bronze/Silver/Gold |
| Ground truth | N/A | 0 | Datasets SUSEP servem como referência real de distribuição de sinistros para validar o producer |
| Related code | N/A | 0 | Projeto começa do zero |

**How samples will be used:**

- Base estática real (SUSEP/ANS/Open Insurance) para o producer Python republicar linhas como eventos Kafka, simulando fluxo em tempo real
- Referência de schema para desenho das tabelas Bronze (raw) e regras de qualidade da Silver
- Distribuições reais (valor de sinistro, região, tipo de veículo) para calibrar os casos de uso futuros de fraude/ML

---

## Approaches Explored

### Approach A: Workspace único, catálogos por ambiente via Unity Catalog + DAB ⭐ Recommended

**Description:** Um único workspace Databricks (trial/free), com Unity Catalog usando catálogos/schemas distintos por ambiente (dev/staging/prod). Databricks Asset Bundles (DAB) controla o deploy via `targets` no `databricks.yml`. Compute serverless sempre que possível (Jobs serverless, SQL Warehouse serverless) para minimizar custo e administração.

**Pros:**
- Baixo custo e fricção de setup — ideal para fase de portfólio
- Evolui naturalmente para produção (basta adicionar mais rigor de governança/RBAC nos catálogos, sem reestruturar workspace)
- CI/CD simples: um único ponto de deploy, múltiplos targets

**Cons:**
- Isolamento entre ambientes é lógico (catálogo), não físico (workspace) — menos realista que ambientes enterprise com workspaces segregados

**Why Recommended:** Equilibra o objetivo dual do projeto (portfólio barato agora, produção real depois) sem over-engineering prematuro.

---

### Approach B: Workspaces separados por ambiente

**Description:** Um workspace Databricks distinto para dev, staging e prod — padrão comum em grandes empresas.

**Pros:**
- Isolamento físico total entre ambientes
- Mais próximo do que uma seguradora real usaria em produção

**Cons:**
- Custo e complexidade de setup mais altos, ruim para portfólio (múltiplos trials/assinaturas)
- Mais lento para iterar durante a fase de demonstração

---

### Approach C: Ambiente único, sem separação dev/staging/prod

**Description:** Um único ambiente "the pipeline" rodando direto, sem distinção de ambientes.

**Pros:**
- Setup mais rápido para demo pura

**Cons:**
- Conflita diretamente com o objetivo confirmado de evoluir para produção real
- Não demonstra maturidade de CI/CD multi-ambiente, que é um dos diferenciais buscados para portfólio

**Rejeitada** por conflitar com o requisito de evolução para produção.

---

## Data Engineering Context

### Source Systems

| Source | Type | Volume Estimate | Current Freshness |
|--------|------|-----------------|-------------------|
| SUSEP (PDA 2024-2026) | CSV, dados abertos anonimizados de sinistros auto/rural/compreensivo | A confirmar no /define | Estático (atualização periódica pela SUSEP) — replay simulará tempo real |
| ANS | Dados abertos de saúde suplementar | A confirmar | Estático |
| Open Insurance Brasil | API/dados abertos do ecossistema Open Insurance | A confirmar | Estático ou API, a validar |

### Data Flow Sketch

```text
[SUSEP/ANS/Open Insurance — CSV/JSON estáticos]
        │
        ▼
[Python Producer (Docker) — republica linhas como eventos em ritmo configurável]
        │
        ▼
[Confluent Cloud — tópicos: claim-opened, policy-created, customer-updated, vehicle-tracking]
        │
        ▼
[Databricks Jobs — Spark Structured Streaming, SEM DLT/Lakeflow]
        │
        ▼
[Bronze Delta — raw, append-only, schema-on-read]
        │
        ▼
[Spark job — dedup, parsing, normalização, quality rules]
        │
        ▼
[Silver Delta]
        │
        ▼
[Spark job — agregações e regras de negócio, casos de uso (fraude, aprovação automática, SLA)]
        │
        ▼
[Gold Delta]
        │
        ├──▶ [Databricks SQL Warehouse → Dashboards]
        ├──▶ [MLflow — modelos de fraude, cancelamento, inadimplência, probabilidade de sinistro]
        ├──▶ [APIs externas / parceiros]
        └──▶ [Sistemas de decisão internos + filas para analistas humanos]
```

### Key Data Questions Explored

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | Qual o volume de dados esperado? | Não definido — volume de portfólio (baixo/médio), a validar com tamanho real dos datasets SUSEP/ANS | Afeta dimensionamento de compute, mas serverless absorve boa parte da variação |
| 2 | Qual SLA de atualização é necessário? | Tempo real (streaming), simulado via replay configurável dos dados públicos | Justifica Spark Structured Streaming em vez de batch |
| 3 | Quem consome a saída? | Decisão automática, analistas, BI, parceiros externos | Gold precisa suportar múltiplos padrões de acesso (SQL, API, dashboard) |

---

## Selected Approach

| Attribute | Value |
|-----------|-------|
| **Chosen** | Approach A — Workspace único, catálogos por ambiente + DAB + serverless |
| **User Confirmation** | 2026-07-08 |
| **Reasoning** | Melhor equilíbrio entre custo/simplicidade de portfólio e capacidade real de evoluir para produção, sem exigir múltiplos workspaces desde o início |

---

## Key Decisions Made

| # | Decision | Rationale | Alternative Rejected |
|---|----------|-----------|----------------------|
| 1 | 100% Databricks + Kafka + Spark (sem DLT/Lakeflow) | Decisão explícita do usuário — demonstrar domínio de Structured Streaming puro, não pipelines declarativos | Lakeflow/DLT para Silver (sugerido no context.md original, mas descartado) |
| 2 | Kafka hospedado no Confluent Cloud (free tier) | Databricks não hospeda Kafka nativamente; Confluent Cloud evita gerenciar servidor/Docker em produção | Kafka em Docker local/VM (mais barato mas menos realista para evolução a produção) |
| 3 | Dados públicos reais (SUSEP/ANS/Open Insurance) em vez de sintéticos | Usuário quer dados reais, não gerados por Faker | Dados 100% sintéticos |
| 4 | Escopo da fase 1 inclui TODOS os casos de uso (fraude, ML, SLA, auditoria, governança) | Decisão explícita do usuário: "pode incluir tudo" — objetivo é demonstrar o máximo do ecossistema Databricks desde já | YAGNI tradicional (cortar casos de uso avançados para uma fase 2) — considerado mas rejeitado pelo usuário |
| 5 | Ambiente único com catálogos separados por ambiente (não workspaces separados) | Portfólio precisa de baixo custo/fricção, mas evolução a produção fica viável trocando apenas rigor de RBAC | Workspaces físicos separados por ambiente |
| 6 | GitHub Actions para CI/CD | Mais natural para repositório público de portfólio, gratuito | Azure DevOps |

---

## Features Removed (YAGNI)

| Feature Suggested | Reason Removed | Can Add Later? |
|-------------------|----------------|----------------|
| _Nenhuma feature foi removida_ | O usuário revisou a proposta inicial de corte de escopo (fraude/ML/SLA/auditoria adiados) e decidiu explicitamente manter tudo na fase 1, priorizando demonstrar o máximo do ecossistema Databricks desde o início | N/A |

> Nota para `/define`: mesmo sem cortes de escopo, será importante propor uma **ordem de construção incremental** (ex: pipeline base → fraude → ML → SLA/auditoria) dentro da fase 1, para viabilizar entregas testáveis, mesmo que todos os itens estejam "dentro do escopo" da mesma fase.

---

## Incremental Validations

| Section | Presented | User Feedback | Adjusted? |
|---------|-----------|---------------|-----------|
| Topologia de ambientes (workspace único vs separados vs nenhum) | ✅ | Confirmou Approach A (workspace único, catálogos por ambiente) | Não |
| Fluxo de dados + corte YAGNI de casos de uso avançados | ✅ | Rejeitou o corte — pediu para incluir tudo no MVP | Sim — escopo do MVP expandido para incluir todos os casos de uso |
| Escopo do MVP (tudo no roadmap vs tudo na fase 1) | ✅ | Confirmou: tudo já na fase 1 | Não (apenas esclarecimento) |

**Minimum Validations:** 3 (mínimo de 2 excedido)

---

## Suggested Requirements for /define

### Problem Statement (Draft)

Construir uma plataforma de dados de seguros 100% Databricks (Kafka + Spark Structured Streaming, sem DLT/Lakeflow) que processe eventos de sinistros e underwriting em tempo real a partir de dados públicos reais (SUSEP/ANS/Open Insurance), cobrindo desde ingestão até detecção de fraude, aprovação automática, ML, monitoramento de SLA, auditoria e dashboards — servindo tanto como projeto de portfólio quanto como base evolutiva para um sistema de produção real.

### Target Users (Draft)

| User | Pain Point |
|------|------------|
| Recrutadores/entrevistadores técnicos (contexto portfólio) | Precisam avaliar rapidamente domínio real de Databricks/Spark/Kafka/streaming/CI-CD/governança em um projeto ponta a ponta |
| Analistas de sinistros/underwriting (contexto produção futura) | Precisam de casos sinalizados (fraude, SLA estourado) priorizados em filas/dashboards |
| Sistemas de decisão internos | Precisam de eventos e scores em tempo real para aprovação automática |
| Parceiros/corretoras externas | Precisam consumir dados via API |

### Success Criteria (Draft)

- [ ] Pipeline Bronze→Silver→Gold funcionando de ponta a ponta com dados reais (SUSEP no mínimo) fluindo via Kafka → Spark Structured Streaming → Delta
- [ ] CI/CD via GitHub Actions com deploy automatizado (Databricks Asset Bundles) para pelo menos dev e prod
- [ ] Unity Catalog configurado com catálogos por ambiente, governança básica (RLS/masking) aplicada em pelo menos uma tabela sensível
- [ ] Ao menos um caso de uso avançado funcionando em streaming (fraude ou aprovação automática) como prova de conceito do restante do roadmap
- [ ] Terraform provisionando os objetos de infraestrutura definidos (Unity Catalog, secrets, policies)
- [ ] Repositório público, documentado, pronto para portfólio

### Constraints Identified

- 100% Databricks + Kafka + Spark — sem DLT/Lakeflow declarativo
- Kafka apenas via Confluent Cloud (free tier)
- Dados públicos reais, sujeitos a LGPD (já anonimizados na fonte, mas tratamento deve respeitar isso)
- Orçamento de portfólio (compute serverless, tiers gratuitos sempre que possível)

### Out of Scope (Confirmado)

- Nenhum item foi formalmente excluído do escopo da fase 1 (ver seção YAGNI acima) — porém `/define` deve propor sequenciamento/priorização dentro da fase 1

---

## Session Summary

| Metric | Value |
|--------|-------|
| Questions Asked | 11 |
| Approaches Explored | 3 |
| Features Removed (YAGNI) | 0 (decisão explícita do usuário) |
| Validations Completed | 3 |
| Duration | ~1 sessão |

---

## Next Step

**Ready for:** `/define .claude/sdd/features/BRAINSTORM_INSURANCE_LAKEHOUSE_PLATFORM.md`
