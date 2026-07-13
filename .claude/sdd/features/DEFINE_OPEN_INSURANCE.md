# DEFINE: Open Insurance

> Módulo que simula o consentimento do cliente para compartilhamento de dados com outras instituições (conceito Open Insurance/Open Finance), preparando os dados para Delta Sharing sem provisionar o Share em si.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | OPEN_INSURANCE |
| **Date** | 2026-07-13 |
| **Author** | jonataslimacostabr@gmail.com |
| **Status** | Ready for Design |
| **Clarity Score** | 14/15 |

---

## Problem Statement

Clientes segurados não têm hoje nenhum mecanismo, nem simulado, para consentir (ou revogar) o compartilhamento dos próprios dados com outras instituições financeiras/seguradoras — a plataforma cobre o reporte regulatório obrigatório (SUSEP, via `src/regulatory/`), mas não representa o fluxo voluntário de portabilidade de dados do cliente (Open Insurance/Open Finance).

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| Cliente/segurado (simulado) | Titular dos dados de sinistros/perfil | Não tem como expressar consentimento sobre quem acessa seus dados |
| Outra instituição (`insurer_a`/`insurer_b`/`insurer_c`, simulado) | Recipient de dados compartilhados | Não existe uma fonte de dados filtrada só pelos clientes que de fato consentiram |
| Time de engenharia/compliance | Mantém trilha de auditoria LGPD | Precisa saber quando cada cliente autorizou/revogou acesso, com histórico completo, não só o estado atual |

---

## Goals

| Priority | Goal |
|----------|------|
| **MUST** | Evento `consent-updated` fluindo por `bronze.consent_events` → `silver.consent_current` → `gold.customer_consent` |
| **MUST** | `silver.consent_current` mantém histórico completo (SCD2) de mudanças de status por cliente — não só o último estado |
| **MUST** | `gold.open_insurance_profile` com perfil sintético determinístico (mesmo `customer_id` sempre gera o mesmo perfil) |
| **MUST** | View `gold.open_insurance_shareable` expõe apenas clientes com `consent_status = GRANTED` no momento da consulta |
| **SHOULD** | Revogar consentimento remove o cliente da view no próximo refresh do job, sem intervenção manual |
| **COULD** | Documentação de como o Delta Share real seria configurado sobre `gold.open_insurance_shareable` (sem provisionar) |

**Priority Guide:**
- **MUST** = MVP fails without this
- **SHOULD** = Important, but workaround exists
- **COULD** = Nice-to-have, cut first if needed

---

## Success Criteria

- [ ] 100% dos eventos `consent-updated` publicados chegam em `bronze.consent_events`, mesmo SLA de latência dos demais tópicos (< 2 min, ver `pipeline_latency_monitor`)
- [ ] `silver.consent_current` produz >= 2 registros históricos para um mesmo `customer_id` que teve 2 mudanças de status (GRANTED → REVOKED), com só 1 marcado como vigente
- [ ] `gold.open_insurance_profile` gera exatamente 1 linha por `customer_id`, com valores idênticos entre execuções repetidas (determinístico)
- [ ] `gold.open_insurance_shareable` retorna 0 linhas para qualquer cliente cujo `consent_status` mais recente seja `REVOKED` ou inexistente
- [ ] Revogar consentimento remove o cliente da view no próximo run do job (sem necessidade de reprocessar histórico manualmente)

---

## Acceptance Tests

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Consentimento concedido aparece na view | Cliente sem consentimento prévio registrado | Evento `consent-updated` (GRANTED) é publicado e processado até Gold | Cliente aparece em `gold.open_insurance_shareable` com perfil e claims |
| AT-002 | Revogação remove da view | Cliente com consentimento ativo já refletido na view | Evento `consent-updated` (REVOKED) é publicado e processado | Cliente some de `gold.open_insurance_shareable` no próximo refresh |
| AT-003 | Histórico de consentimento é preservado (SCD2) | Cliente concede e depois revoga consentimento (2 eventos em momentos diferentes) | `silver.consent_current` processa os 2 eventos | Existem 2 registros históricos para o mesmo `customer_id`, com flag de vigente só no mais recente |
| AT-004 | Perfil sintético é determinístico | Gerador de perfil já rodou uma vez para um `customer_id` | O gerador roda novamente para o mesmo `customer_id` | Os mesmos valores de perfil são gerados (mesmo seed) |
| AT-005 | Cliente com consentimento mas sem claims | Cliente com `consent_status = GRANTED` mas nenhum registro em `gold.claims` | `gold.open_insurance_shareable` é consultada | Comportamento a decidir em Design — ver Open Questions (aparecer com claims vazio vs. ficar de fora) |

---

## Out of Scope

Explicitly NOT included in this feature:

- Provisionamento real do Delta Share + Recipients via Terraform (`databricks_share`/`databricks_recipient`) — fica documentado como próximo passo, não implementado
- Integração de verdade com um banco/seguradora externo real
- UI no Databricks App para o cliente gerenciar consentimento interativamente (toggle na tela) — o consentimento é simulado via evento Kafka, não via interface
- Qualquer mecanismo de distribuição de dados que não seja Delta Sharing (ex.: API REST custom)
- Alteração de perfil via evento/streaming — o perfil sintético é gerado em batch, determinístico, não é atualizado em tempo real

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Technical | Sem PII real — perfil sintético gerado a partir de dados já anonimizados (SUSEP/ANS) | Design não pode assumir nenhum dado cadastral real; gerador precisa ser claramente sintético/determinístico |
| Technical | Segue o padrão medallion (bronze/silver/gold) e o helper `src/common/delta_merge.py` já existentes | Não inventar uma camada de processamento paralela |
| Technical | **SCD2 é um padrão novo neste projeto** — não existe implementação de `is_current`/`valid_from`/`valid_to` em nenhum lugar do código hoje (confirmado por busca) | Design precisa especificar o mecanismo de SCD2 do zero, não apontar para um exemplo existente |
| Resource | Delta Share real fica fora do MVP (sem Terraform novo para isso) | Reduz escopo de infraestrutura desta fase |
| Operacional | **Workspace atual é trial com cota apertada de serverless compute** (RESOURCE_EXHAUSTED já confirmado num deploy real deste mesmo projeto, com os jobs contínuos existentes já perto do teto) | O job deste módulo **deve ser agendado (schedule), não contínuo** — adicionar mais um cluster serverless 24/7 provavelmente estoura a cota de novo |

---

## Technical Context

> Essential context for Design phase - prevents misplaced files and missed infrastructure needs.

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | `src/open_insurance/` (novo domínio, paralelo a `src/regulatory/`) | Gerador de perfil sintético entra em `src/ingestion/producer/datasets/` (mesmo padrão de `regulatory_feeds.py`); novo schema Kafka em `src/common/schemas.py` |
| **KB Domains** | Medallion architecture, SCD (Slowly Changing Dimensions), Delta Lake MERGE patterns | Consultar `src/common/delta_merge.py` como base para o MERGE do SCD2; padrão de geração sintética em `src/ingestion/producer/datasets/regulatory_feeds.py` |
| **IaC Impact** | Modify existing — novo(s) schema(s)/tabelas Gold em `terraform/unity_catalog.tf` (grants seguindo o padrão já usado); novo job em `resources/jobs.open_insurance.yml` (agendado, `environment_key: default` como os demais, ver constraint de cota acima) | Sem novo catálogo, sem novo warehouse |

**Why This Matters:**

- **Location** → Design phase usa a estrutura correta, mantendo o módulo isolado do regulatório obrigatório
- **KB Domains** → Design deve reaproveitar `delta_merge.py` em vez de escrever MERGE do zero
- **IaC Impact** → Job agendado (não contínuo) é obrigatório dado o histórico recente de `RESOURCE_EXHAUSTED` neste mesmo workspace

---

## Data Contract

### Source Inventory

| Source | Type | Volume | Freshness | Owner |
|--------|------|--------|-----------|-------|
| `consent-updated` (novo tópico Kafka) | Kafka (Confluent Cloud) | Baixo (~1 evento por cliente por mudança de consentimento) | Streaming, mesmo SLA dos demais tópicos (< 2 min Kafka→Bronze) |
| `gold.claims` (já existente) | Delta table interna | Já existente, sem mudança de volume | Batch/streaming já estabelecido |

### Schema Contract

| Column | Type | Constraints | PII? |
|--------|------|-------------|------|
| `customer_id` | STRING | NOT NULL | Não (ID interno já usado em toda a plataforma, não PII real) |
| `consent_status` | STRING | NOT NULL, valores `GRANTED`/`REVOKED` | Não |
| `target_institution` | STRING | NOT NULL (`insurer_a`/`insurer_b`/`insurer_c`) | Não |
| `scope` | ARRAY&lt;STRING&gt; | NOT NULL (ex.: `[claims, profile]`) | Não |
| `event_timestamp` | TIMESTAMP | NOT NULL | Não |

### Freshness SLAs

| Layer | Target | Measurement |
|-------|--------|-------------|
| Bronze (`consent_events`) | Dentro de 2 min do evento Kafka, mesmo padrão do restante do pipeline | Comparação de timestamp, igual ao `pipeline_latency_monitor` já existente |
| Gold (`customer_consent` / `open_insurance_shareable`) | Atualizado a cada execução do job agendado (frequência exata a decidir em Design, ex.: a cada 15-30 min) | Timestamp de última execução do job |

### Completeness Metrics

- 100% dos eventos `consent-updated` publicados devem chegar em `bronze.consent_events`, sem perda
- Zero `customer_id` nulo em `gold.customer_consent`

### Lineage Requirements

- Reaproveita o lineage já existente do Unity Catalog (Bronze → Silver → Gold) — nenhum mecanismo novo necessário

---

## Assumptions

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|-------------------|------------|
| A-001 | `customer_id` em `gold.claims` é estável e reaproveitável como chave para o gerador de perfil sintético e para o evento de consentimento | Perfil sintético ficaria inconsistente entre execuções | [ ] |
| A-002 | Volume de eventos de consentimento é baixo o suficiente para não precisar de otimização especial de throughput | Pipeline de consentimento precisaria de tuning de performance | [ ] |
| A-003 | Um job agendado (não contínuo) para este módulo é suficiente para os cenários de demo — não há necessidade real de refletir consentimento em tempo real | Se um recrutador/avaliador quiser ver o efeito da revogação "instantaneamente", precisaria esperar o próximo ciclo agendado | [ ] |

**Note:** Validar A-003 é especialmente importante dado o histórico recente de estouro de cota de serverless neste workspace.

---

## Clarity Score Breakdown

| Element | Score (0-3) | Notes |
|---------|-------------|-------|
| Problem | 3 | Claro, específico, com o gap identificado em relação ao módulo regulatório já existente |
| Users | 3 | 3 personas com pain points distintos |
| Goals | 3 | MUST/SHOULD/COULD priorizados, direto do brainstorm validado |
| Success | 2 | Critérios majoritariamente testáveis, mas AT-005 (edge case de cliente sem claims) ainda não decidido |
| Scope | 3 | Out of Scope explícito e validado (YAGNI já aplicado no brainstorm) |
| **Total** | **14/15** | |

**Minimum to proceed: 12/15** ✅

---

## Open Questions

1. **AT-005 (edge case):** cliente com `consent_status = GRANTED` mas sem nenhum registro em `gold.claims` — aparece na view com claims vazio/null (LEFT JOIN) ou fica de fora até ter pelo menos 1 claim (INNER JOIN)? Decidir em Design.
2. **Estrutura de schema:** as novas tabelas Gold (`customer_consent`, `open_insurance_profile`, view `open_insurance_shareable`) entram no schema `gold` já existente ou merecem um schema dedicado (`open_insurance`) dentro do catálogo? Decidir em Design considerando os grants já existentes em `terraform/unity_catalog.tf`.
3. **Frequência do job agendado:** dado que precisa ser agendado (não contínuo, ver Constraints), qual cadência faz sentido — mesma do `gold_aggregate` (10 min) ou mais espaçada, já que consentimento muda com bem menos frequência que sinistros?

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-07-13 | define-agent | Versão inicial, extraída de `BRAINSTORM_OPEN_INSURANCE.md` |

---

## Next Step

**Ready for:** `/design .claude/sdd/features/DEFINE_OPEN_INSURANCE.md`
