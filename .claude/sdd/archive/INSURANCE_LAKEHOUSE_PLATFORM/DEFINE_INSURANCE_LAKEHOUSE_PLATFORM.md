# DEFINE: Insurance Lakehouse Platform

> Plataforma de dados de seguros 100% Databricks (Kafka + Spark Structured Streaming) que processa sinistros e underwriting em tempo real a partir de dados públicos reais, cobrindo desde ingestão até fraude, ML, governança e CI/CD.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | INSURANCE_LAKEHOUSE_PLATFORM |
| **Date** | 2026-07-08 |
| **Author** | define-agent |
| **Status** | ✅ Shipped |
| **Clarity Score** | 14/15 |

---

## Problem Statement

Não existe hoje uma plataforma de referência 100% Databricks que demonstre, de ponta a ponta e com padrões reais de produção (streaming, governança, CI/CD), o processamento em tempo real de eventos de sinistros e underwriting de seguros — desde a ingestão de dados públicos reais até decisões automatizadas (aprovação, triagem de fraude) e consumo por analistas, sistemas internos, dashboards e parceiros externos.

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| Recrutadores/entrevistadores técnicos | Avaliam candidatos a vagas de Data Engineering | Precisam avaliar rapidamente domínio real de Databricks/Spark/Kafka/streaming/CI-CD/governança em um projeto ponta a ponta, não só slides |
| Analistas de sinistros/underwriting | Revisam casos sinalizados (contexto de evolução futura para produção) | Hoje dependem de processos manuais/lentos para triagem de fraude e priorização de casos |
| Sistemas de decisão internos | Motores de regras/scoring automatizados | Precisam de eventos e scores em tempo real para aprovar/rejeitar automaticamente sinistros de baixo risco |
| Parceiros/corretoras externas | Consomem dados da seguradora via integração | Precisam de acesso confiável e governado aos dados via API, sem depender de exports manuais |

---

## Goals

> Nenhum item foi removido do escopo confirmado pelo usuário (ver BRAINSTORM). A priorização MUST/SHOULD/COULD abaixo reflete **ordem de construção** dentro da fase 1, não corte de funcionalidade — tudo listado deve ser entregue no roadmap da fase 1.

| Priority | Goal |
|----------|------|
| **MUST** | Pipeline Bronze→Silver→Gold funcional com dados reais fluindo via Kafka (Confluent Cloud) → Spark Structured Streaming → Delta, sem DLT/Lakeflow |
| **MUST** | CI/CD via GitHub Actions + Databricks Asset Bundles com deploy automatizado para pelo menos dev e prod |
| **MUST** | Unity Catalog configurado com catálogos/schemas por ambiente (dev/staging/prod) e governança básica (RLS/masking) em pelo menos uma tabela sensível |
| **MUST** | Terraform provisionando os objetos de infraestrutura (Unity Catalog, secrets, policies) |
| **SHOULD** | Pelo menos um caso de uso avançado em streaming funcionando (fraude OU aprovação automática) como prova de conceito do restante do roadmap |
| **SHOULD** | Modelo(s) de ML via MLflow para pelo menos um caso de uso (fraude, cancelamento, inadimplência ou probabilidade de sinistro) |
| **SHOULD** | Monitoramento de SLA com alertas para sinistros/underwriting que ultrapassam o prazo esperado |
| **COULD** | Detecção de documentos duplicados via hash |
| **COULD** | Auditoria completa via Change Data Feed + Time Travel |
| **COULD** | Exposição de dados via API externa para parceiros |
| **COULD** | Otimizações avançadas de performance (Z-Ordering, Liquid Clustering, tuning de Photon, Predictive Optimization) |

---

## Success Criteria

- [ ] Producer Python replica eventos de pelo menos 2 fontes públicas (SUSEP obrigatório; ANS e/ou Open Insurance Brasil como segunda fonte) para tópicos Kafka distintos (`claim-*`, `policy-*`, `customer-*`)
- [ ] Latência ponta a ponta (evento publicado no Kafka → disponível na Bronze) inferior a 2 minutos *(assunção — validar antes do /design, ver A-001)*
- [ ] Pipeline sustenta pelo menos 100 eventos/minuto sem falha ou perda de dados *(assunção de volume de portfólio — validar, ver A-001)*
- [ ] Deploy via GitHub Actions (dev→prod) executa em menos de 10 minutos, sem passos manuais
- [ ] 100% das tabelas Silver/Gold com testes de qualidade de dados automatizados (ex: not-null em chaves primárias, unicidade)
- [ ] Pelo menos 1 caso de uso avançado (fraude ou aprovação automática) com score/decisão calculado em streaming e visível em menos de 1 minuto após o evento
- [ ] Repositório público no GitHub com README, arquitetura documentada e pipeline reproduzível por terceiros

---

## Acceptance Tests

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Happy path — sinistro processado ponta a ponta | Producer publicando eventos válidos no tópico `claim-opened` | Um novo evento de sinistro é publicado no Kafka | O evento aparece na Gold com decisão/score calculado em menos de 1 minuto |
| AT-002 | Evento malformado não quebra o pipeline | Pipeline Spark Streaming rodando normalmente | Um evento com schema inválido é publicado no Kafka | O evento é isolado em uma área de quarentena (não é descartado silenciosamente nem derruba o job) |
| AT-003 | Pico de volume não causa perda de dados | Pipeline operando em regime normal (~100 eventos/min) | Uma rajada de eventos (ex: 10x o volume normal) é publicada em curto intervalo | Nenhum evento é perdido; o job aplica backpressure/escala sem falhar |
| AT-004 | CI/CD bloqueia merge com testes quebrados | PR aberto alterando um job Spark | GitHub Actions executa a suíte de testes automatizados | Se os testes falharem, o merge é bloqueado e o deploy não ocorre |
| AT-005 | Governança aplica mascaramento/RLS | Usuário sem permissão elevada autenticado no Unity Catalog | O usuário consulta a tabela com dado sensível (ex: dados de cliente) | O usuário recebe o dado mascarado ou nenhum acesso, conforme a policy configurada |

---

## Out of Scope

- Construir um sistema completo de administração de apólices (policy admin system) do zero — a plataforma consome/processa eventos, não substitui um core insurance system
- Uso de dados reais não anonimizados de clientes — apenas datasets públicos já anonimizados (SUSEP/ANS/Open Insurance)
- Múltiplos workspaces Databricks físicos por ambiente (Approach B do BRAINSTORM foi rejeitada — usar catálogos por ambiente em workspace único)
- Delta Live Tables / Lakeflow Declarative Pipelines — decisão explícita do usuário por Spark Structured Streaming puro
- Kafka self-hosted (Docker/VM) — apenas Confluent Cloud (free tier)
- Front-ends além de dashboards Databricks SQL (sem app mobile, sem portal de cliente dedicado)

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Technical | 100% Databricks + Kafka + Spark, sem DLT/Lakeflow declarativo | Toda orquestração de Silver/Gold via Databricks Workflows com jobs Spark explícitos, não pipelines declarativos |
| Technical | Kafka apenas via Confluent Cloud (free tier) | Limites de throughput/retenção do free tier podem restringir volume de teste e exigir monitoramento de cota |
| Resource | Orçamento de portfólio (tiers gratuitos, compute serverless) | Dimensionamento deve priorizar serverless/tiers gratuitos, evitando compute always-on |
| Compliance | Dados públicos sujeitos a LGPD (já anonimizados na fonte pela SUSEP/ANS) | Pipeline deve preservar a anonimização existente, sem tentar re-identificação ou enriquecimento que reverta o anonimato |

---

## Technical Context

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | `src/{ingestion,streaming,bronze,silver,gold,analytics,ml,monitoring}/`, `bundles/` (DAB), `terraform/`, `workflows/`, `tests/` | Estrutura sugerida no BRAINSTORM original; validar/ajustar no /design |
| **KB Domains** | spark-streaming, delta-lake, unity-catalog, databricks-asset-bundles, terraform, mlflow, kafka | Consultar `@spark-streaming-architect`, `@databricks-spark-expert`, `@ci-cd-specialist`, `@data-platform-security` no /design |
| **IaC Impact** | New resources | Unity Catalog (catálogos/schemas por ambiente), secrets (credenciais Confluent Cloud), Databricks Workflows/Jobs, cluster policies |

**Why This Matters:**

- **Location** → Design phase usa a estrutura de projeto correta, evita arquivos mal posicionados
- **KB Domains** → Design phase usa os padrões corretos de streaming/Delta/governança
- **IaC Impact** → Aciona o planejamento de infraestrutura (Terraform) desde o /design, evitando "funciona só localmente"

---

## Data Contract

### Source Inventory

| Source | Type | Volume | Freshness | Owner |
|--------|------|--------|-----------|-------|
| SUSEP (PDA 2024-2026) | CSV (dados abertos, anonimizados LGPD) | A confirmar no /design (spike de levantamento) | Estático — atualização periódica pela SUSEP; replay via producer simula tempo real | SUSEP (Superintendência de Seguros Privados) |
| ANS | CSV/dados abertos de saúde suplementar | A confirmar | Estático | ANS (Agência Nacional de Saúde Suplementar) |
| Open Insurance Brasil | API/dados abertos do ecossistema regulado | A confirmar (pode exigir credenciamento — ver Open Questions) | A confirmar | Open Insurance Brasil / BACEN |

### Schema Contract (rascunho — a validar no /design após levantamento dos datasets)

| Column | Type | Constraints | PII? |
|--------|------|-------------|------|
| claim_id / policy_id | STRING | NOT NULL, UNIQUE | Não (já anonimizado na fonte) |
| event_type | STRING | NOT NULL (ex: claim-opened, policy-created) | Não |
| event_timestamp | TIMESTAMP | NOT NULL | Não |
| amount | DECIMAL | >= 0 quando aplicável | Não |
| region / uf | STRING | Nullable | Não |
| vehicle_type / ramo_seguro | STRING | Nullable | Não |

### Freshness SLAs

| Layer | Target | Measurement |
|-------|--------|-------------|
| Bronze | Dentro de 2 minutos da publicação no Kafka *(assunção — validar)* | Comparação de timestamp evento vs. timestamp de escrita na Bronze |
| Silver | Dentro de 5 minutos após chegada na Bronze | Timestamp de conclusão do job incremental |
| Gold | Dentro de 10 minutos após atualização da Silver | Timestamp de conclusão do job de agregação |

### Completeness Metrics

- 100% dos eventos publicados no Kafka devem chegar à Bronze (zero perda); eventos malformados vão para quarentena, não são descartados silenciosamente
- Zero valores nulos em chaves primárias (`claim_id`, `policy_id`) nas camadas Silver e Gold

### Lineage Requirements

- Lineage automático via Unity Catalog, do tópico Kafka até a Gold
- Change Data Feed habilitado em todas as tabelas Delta desde o início (capacidade técnica é MUST, mesmo que o caso de uso de auditoria completa seja COULD) — evita retrofit caro depois

---

## Assumptions

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|-------------------|------------|
| A-001 | Volume de eventos fica na faixa de portfólio (dezenas a poucas centenas de eventos/minuto), não volume real de uma seguradora grande | Precisaria redimensionar compute e possivelmente sair do free tier do Confluent Cloud | [ ] |
| A-002 | Confluent Cloud free tier é suficiente para o volume e retenção necessários durante o desenvolvimento | Precisaria migrar para tier pago ou reconsiderar hospedagem do Kafka | [ ] |
| A-003 | Datasets da SUSEP/ANS/Open Insurance têm granularidade suficiente para simular eventos individuais (não apenas agregados) | Pipeline precisaria combinar dados reais (como referência estatística) com geração sintética de eventos individuais | [ ] |
| A-004 | Um workspace Databricks free/trial ou de baixo custo é suficiente para compute serverless + SQL Warehouse necessários | Pode ser necessário orçamento adicional ou ajuste para clusters compartilhados | [ ] |

**Note:** Validar as assunções A-001 a A-004 no início do /design ou como spike técnico antes de comprometer a arquitetura de capacity/custo.

---

## Clarity Score Breakdown

| Element | Score (0-3) | Notes |
|---------|-------------|-------|
| Problem | 3 | Claro, específico e acionável — extraído diretamente do BRAINSTORM com decisões técnicas já confirmadas pelo usuário |
| Users | 3 | 4 personas identificadas com papéis e dores específicas |
| Goals | 3 | MUST/SHOULD/COULD priorizados cobrindo todo o escopo confirmado (nada removido, apenas sequenciado) |
| Success | 2 | Critérios mensuráveis definidos, mas volume/latência exatos são assunções a validar, não números confirmados pelo usuário |
| Scope | 3 | Out of Scope explícito com fronteiras técnicas claras (mesmo sem corte de funcionalidades de negócio) |
| **Total** | **14/15** | |

**Minimum to proceed: 12/15** — atingido.

---

## Open Questions

1. Volume exato dos datasets SUSEP/ANS/Open Insurance (número de linhas, período coberto) — precisa de um spike de levantamento no início do /design.
2. Confirmar se o acesso ao Open Insurance Brasil exige credenciamento (ecossistema regulado pelo BACEN) antes de assumir acesso livre igual ao SUSEP/ANS.
3. Entre fraude e aprovação automática, qual deve ser o caso de uso avançado prioritário (SHOULD) da primeira entrega? Sugestão: fraude, por ser mais vistoso para portfólio — a confirmar no /design.

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-07-08 | define-agent | Versão inicial, extraída de BRAINSTORM_INSURANCE_LAKEHOUSE_PLATFORM.md |
| 1.1 | 2026-07-08 | ship-agent | Shipped e arquivado |

---

## Next Step

**Ready for:** `/design .claude/sdd/features/DEFINE_INSURANCE_LAKEHOUSE_PLATFORM.md`
