# DEFINE: Insurance Visualization Layer

> Camada de apresentação (AI/BI Dashboards + Databricks App + Genie) sobre as tabelas Gold já existentes da plataforma, publicada só no ambiente prod.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | INSURANCE_VISUALIZATION_LAYER |
| **Date** | 2026-07-09 |
| **Author** | jonataslimac@gmail.com |
| **Status** | Ready for Design |
| **Clarity Score** | 13/15 |

---

## Problem Statement

Hoje toda a plataforma (pipeline operacional Bronze/Silver/Gold, fraude, e o novo módulo regulatório SUSEP) só é consumível via SQL/notebook — não existe nenhuma camada de apresentação. Isso bloqueia dois públicos: usuários de negócio (gestores, analistas, executivos) que precisam de visibilidade self-service sobre SLA, custos, fraude e conformidade SUSEP sem escrever SQL; e avaliadores externos do projeto (recrutadores/portfólio), que não conseguem enxergar o valor da plataforma sem ler código.

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| Gestor/Executivo | Consome indicadores operacionais e de negócio | Não tem onde ver SLA/custos/fraude sem pedir uma query pra engenharia |
| Analista de negócio | Investiga sinistros/apólices/bancos/seguradoras | Não sabe SQL, precisa perguntar em linguagem natural |
| Recrutador/avaliador de portfólio | Avalia o projeto de fora | Não vê o ecossistema Databricks completo (Apps, AI/BI, Genie, UC) sem explorar código |
| Engenheiro de dados/plataforma | Mantém e audita a plataforma | Precisa de lineage/auditoria visível sem abrir o Unity Catalog manualmente |

---

## Goals

| Priority | Goal |
|----------|------|
| **MUST** | AI/BI Dashboard(s) cobrindo Pipeline Health, SLA, Qualidade de Dados, Fraude, Regras SUSEP e Bancos x Seguradoras, sobre toda a plataforma (operacional + regulatório) |
| **MUST** | Databricks App (Streamlit/Dash/Flask/Gradio) como interface principal, com painel de status (Streaming Online / Kafka Connected / Bronze / Silver / Gold / SLA / Custos / Alertas) e menus: Consultar Apólice, Consultar Cliente, Consultar Sinistro, Pesquisar Banco, Pesquisar Seguradora, Dashboard, Lineage, Auditoria |
| **MUST** | Toda a stack de visualização (Dashboard + App + Genie) existe/é deployada **só no target/catálogo prod** (`insurance_prod`) — nenhum recurso equivalente é criado em dev |
| **SHOULD** | Genie space em linguagem natural sobre as tabelas Gold, respondendo perguntas como "quantos sinistros foram processados hoje", "qual banco teve mais erros", "qual seguradora tem maior volume", "sinistros rejeitados", "tempo médio de processamento", "violações de regras SUSEP", "custo de processamento" |
| **COULD** | Página dedicada de custos (FinOps) detalhada por domínio/job dentro do App/Dashboard |

**Priority Guide:**
- **MUST** = MVP fails without this
- **SHOULD** = Important, but workaround exists (usuário consulta via SQL editor do Databricks)
- **COULD** = Nice-to-have, cut first if needed

---

## Success Criteria

- [ ] Pelo menos 1 AI/BI Dashboard publicado no workspace prod cobrindo os 6 temas citados (Pipeline Health, SLA, Qualidade de Dados, Fraude, Regras SUSEP, Bancos x Seguradoras), com no mínimo 1 visual por tema (6+ visuals no total)
- [ ] Databricks App publicado e acessível no workspace prod, com as 8 telas/menus listadas navegáveis (aceitável ter algumas como "em breve" no MVP, desde que o menu exista e não quebre)
- [ ] Genie space configurado sobre pelo menos `gold.claims`, `gold.regulatory_susep_claims` e `gold.regulatory_dq_summary`, respondendo corretamente a pelo menos 5 das 7 perguntas de exemplo listadas nos Goals
- [ ] Nenhum recurso de visualização (Dashboard/App/Genie) é criado ao rodar `databricks bundle deploy -t dev` — confirmado inspecionando o plano/saída do bundle

---

## Acceptance Tests

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Dashboard mostra dados reais | Tabelas Gold populadas em prod | Usuário abre o AI/BI Dashboard | Tiles de Pipeline Health/SLA/Fraude/SUSEP renderizam com dados recentes (não estado vazio) |
| AT-002 | Navegação do App | Databricks App deployado em prod | Usuário clica em "Consultar Sinistro" com um `claim_id` válido | Detalhes do sinistro são renderizados a partir das tabelas Gold |
| AT-003 | Pergunta em linguagem natural no Genie | Genie space configurado sobre as tabelas Gold | Usuário pergunta "Quantos sinistros foram processados hoje?" | Genie retorna uma contagem correta derivada das tabelas Gold |
| AT-004 | Isolamento do ambiente dev | `bundle.target = dev` | Roda `databricks bundle deploy -t dev` | Nenhum recurso de dashboard/App/Genie é criado nesse target |

---

## Out of Scope

- Autenticação/autorização granular dentro do App além do que o Unity Catalog já garante (masking/RLS já existentes são reaproveitados, não recriados)
- Refresh em tempo real dentro do dashboard (atualização é por schedule, não streaming ao vivo embutido no dashboard)
- App mobile nativo — só interface web
- Página de custos (FinOps) detalhada por linha de negócio — vira **COULD**/fast-follow
- Ambiente de staging para visualização — só "nenhum recurso" em dev e a stack completa em prod
- Novas fontes de dados — esta feature consome exclusivamente tabelas Gold já existentes, não introduz nenhum pipeline de ingestão novo

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Technical | Databricks Apps, AI/BI Dashboards e Genie só existem/rodam de verdade dentro de um workspace Databricks real — não são simuláveis localmente (mesma limitação de pytest/Spark local já documentada em `docs/ARCHITECTURE.md`) | Design/Build precisam de um plano de verificação alternativo (validação manual pós-deploy, não testável em CI) |
| Technical | Visualização só no target `prod` | `databricks.yml`/`resources/*.yml` precisam de um padrão de "recurso condicional por target" ainda não usado neste repo (hoje só `var.catalog` muda por target, todo o resto do bundle é idêntico entre dev/prod) |
| Resource | Orçamento adicional de infraestrutura para Databricks Apps/Genie ainda não confirmado | Precisa validar se cabe no workspace trial atual antes do Design finalizar a abordagem |

---

## Technical Context

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | `resources/` (novos arquivos DAB para dashboard/app/genie) + novo diretório `apps/insurance_platform_app/` para o código do Databricks App | Segue a convenção `resources/*.yml` já usada; o App precisa de um diretório próprio pro framework escolhido (Streamlit/Dash/Flask/Gradio) |
| **KB Domains** | Nenhum domínio de KB específico para Databricks Apps/AI-BI Dashboards/Genie foi localizado no plugin agentspec instalado — Design deve confirmar se algo se aplica antes de assumir ausência total | N/A ainda — verificar durante `/design` |
| **IaC Impact** | Novos recursos DAB (dashboard, app, genie space) + lógica condicional por target no bundle | Provavelmente exige um novo padrão "recurso só em prod" em `databricks.yml`/`resources/*.yml`, algo que o Terraform/DAB deste repo ainda não faz hoje |

---

## Data Contract

Não aplica no sentido tradicional — esta feature **não introduz nenhuma fonte de dados nova**. Ela consome exclusivamente tabelas Gold/monitoring já existentes e já povoadas pelos pipelines anteriores:

| Fonte (tabela Gold/monitoring) | Consumida por |
|---|---|
| `gold.claims` | Dashboard (Fraude, SLA), App (Consultar Sinistro), Genie |
| `gold.claims_region_agg` | Dashboard (Pipeline Health, Bancos x Seguradoras — proxy por região) |
| `gold.regulatory_susep_claims` | Dashboard (Regras SUSEP), App (Pesquisar Banco/Seguradora), Genie |
| `gold.regulatory_dq_summary` | Dashboard (Qualidade de Dados), Genie |
| `monitoring._pipeline_latency_results` | Dashboard (Pipeline Health) |
| `monitoring._regulatory_reconciliation_results` | Dashboard (Regras SUSEP / Bancos x Seguradoras) |
| `monitoring._dq_results` | Dashboard (Qualidade de Dados) |
| `monitoring._model_drift_results` | Dashboard (Fraude) |

Nenhuma dessas tabelas precisa de schema novo para esta feature — se o Design identificar necessidade de uma coluna/agregação adicional (ex.: para o Genie responder bem a "qual banco teve mais erros"), isso deve ser tratado como um ajuste pontual e documentado, não uma nova fonte.

---

## Assumptions

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|------------------|------------|
| A-001 | O workspace trial atual suporta Databricks Apps + Genie sem custo/licenciamento bloqueante | Precisaria reduzir o escopo desta feature só para AI/BI Dashboards | [ ] |
| A-002 | "Só em prod" é viável via lógica condicional simples no bundle (ex.: incluir blocos de resource só quando `bundle.target == prod`) | Pode exigir arquivos de resource totalmente separados por target, ou um bundle secundário | [ ] |
| A-003 | O Genie consegue responder as 7 perguntas de exemplo com o schema atual das tabelas Gold, sem precisar de novas colunas/views semânticas | Precisaria preparar uma camada semântica adicional (Metric Views) antes do Genie funcionar bem | [ ] |
| A-004 | Um único Databricks App cobre as 8 telas listadas no MVP sem precisar de autenticação própria (reaproveita identidade do workspace) | Precisaria de uma camada de auth própria no App, fora do escopo original | [ ] |

**Note:** Validar A-001 e A-002 primeiro — ambas bloqueiam a viabilidade técnica da restrição "só prod" antes do Design detalhar a abordagem.

---

## Clarity Score Breakdown

| Element | Score (0-3) | Notes |
|---------|-------------|-------|
| Problem | 3 | Específico: falta de camada de apresentação, dois públicos afetados nomeados (negócio + avaliadores externos) |
| Users | 3 | 4 personas com papel e pain point claros |
| Goals | 3 | MUST/SHOULD/COULD priorizados, entregáveis concretos nomeados pelo próprio usuário (telas do App, temas do dashboard, perguntas do Genie) |
| Success | 2 | Critérios têm números, mas "5 de 7 perguntas" e "6+ visuals" são metas propostas por mim, ainda não confirmadas explicitamente pelo usuário |
| Scope | 2 | Out of scope está claro, mas a combinação completa (Dashboard+App+Genie) ainda é um escopo grande para uma única feature — Design deve considerar dividir em sub-entregas |
| **Total** | **13/15** | |

**Scoring Guide:**
- 0 = Missing entirely
- 1 = Vague or incomplete
- 2 = Clear but missing details
- 3 = Crystal clear, actionable

**Minimum to proceed: 12/15**

---

## Open Questions

- A-001/A-002 precisam de validação técnica direta no workspace antes do Design fechar a abordagem de "só prod" (ver Assumptions).
- Os números exatos de Success Criteria (6+ visuals, 5 de 7 perguntas do Genie) foram propostos por mim a partir do conteúdo já detalhado pelo usuário — confirmar ou ajustar durante `/design` se não refletirem a expectativa real.
- Dado o tamanho da combinação completa, o Design deve avaliar se recomenda fatiar esta feature em sub-fases de build (ex.: Dashboard primeiro, depois App, depois Genie) mesmo mantendo esta única DEFINE como guarda-chuva.

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-07-09 | define-agent | Initial version |

---

## Next Step

**Ready for:** `/design .claude/sdd/features/DEFINE_INSURANCE_VISUALIZATION_LAYER.md`
