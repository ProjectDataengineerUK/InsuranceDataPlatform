# BRAINSTORM: Open Insurance

> Módulo de consentimento do cliente para compartilhamento de dados com outras instituições (conceito Open Insurance/Open Finance, regulação SUSEP), simulado sobre a plataforma já existente.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | OPEN_INSURANCE |
| **Date** | 2026-07-13 |
| **Author** | jonataslimacostabr@gmail.com |
| **Status** | Ready for Define |

---

## Initial Idea

**Raw Input:** "incluir o módulo. ideia: quando o cliente tem a possibilidade de compartilhar os dados com outros bancos. Open Insure."

**Context Gathered:**
- Plataforma já tem pipeline medallion completo (bronze/silver/gold) rodando sobre dados reais anonimizados (SUSEP sinistros, ANS beneficiários), via Kafka (Confluent Cloud) + Spark Structured Streaming.
- Já existe um módulo regulatório (`src/regulatory/`) que simula 3 instituições fictícias (`insurer_a`/`insurer_b`/`insurer_c`) reportando os mesmos sinistros reais em layouts heterogêneos, com reconciliação e export SUSEP-compliant (`gold.regulatory_susep_claims`).
- Tópico/schema `customer-updated` existe mas é raso (`customer_id`, `event_type`, `event_timestamp`, `region`, `source`) — não há perfil de cliente rico (nome/CPF/endereço) hoje, porque os datasets de origem são anonimizados.
- Padrão estabelecido para dados fictícios: geração determinística seedada (`src/ingestion/producer/datasets/regulatory_feeds.py`), reaproveitando os mesmos sinistros reais como base, com taxas de defeito nomeadas.

**Technical Context Observed (for Define):**

| Aspect | Observation | Implication |
|--------|-------------|--------------|
| Likely Location | `src/open_insurance/` (novo domínio, paralelo a `src/regulatory/`) | Consent logic e standardize ficam isolados do módulo regulatório obrigatório |
| Relevant Existing Patterns | `regulatory_feeds.py` (geração sintética determinística), `src/streaming/gold_aggregate.py` (padrão de agregação Gold), SCD2 já usado em outros pontos do projeto | Reaproveitar, não reinventar |
| IaC Patterns | Terraform já gerencia catálogo/schemas/grants/volumes (`terraform/unity_catalog.tf`) | Novo schema/tabelas Gold deste módulo devem seguir o mesmo padrão de grants |

---

## Discovery Questions & Answers

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | Qual o objetivo principal do módulo? | Simulação/demo (como o módulo regulatório), sem integração externa real | Escopo fica auto-contido na plataforma, sem depender de contas/parceiros externos reais |
| 2 | Como simular o consentimento do cliente? | Evento Kafka (`consent-updated`), fluindo bronze→silver→gold como os demais eventos | Segue o padrão medallion já estabelecido, não introduz um mecanismo paralelo |
| 3 | Como o "outro banco" de fato acessaria os dados? (esclarecido depois de confusão sobre o papel do Kafka) | Delta Sharing (mecanismo nativo Databricks para compartilhar tabelas entre organizações) | Kafka carrega só o consentimento; a distribuição de dados em si é um mecanismo separado |
| 4 | Quais dados ficam elegíveis para compartilhamento? | Perfil completo do cliente (não só claims) | Exige mais que `gold.claims` — precisa de um domínio de perfil |
| 5 | Como tratar o perfil "completo", já que hoje só existem campos finos? | Criar um dataset sintético de perfil (nome/idade/score fictícios) | Novo gerador de dados sintéticos, seguindo o padrão de `regulatory_feeds.py` |
| 6 | Quantas instituições fictícias participam do compartilhamento? | Reaproveitar `insurer_a`/`insurer_b`/`insurer_c` já existentes | Consistência entre módulo regulatório e Open Insurance, sem inventar identidades novas |
| 7 | Como tratar os Recipients do Delta Share, já que essas instituições não são contas Databricks reais separadas? | Só documentar o próximo passo, não provisionar Delta Share/Recipients agora (YAGNI) | Corta escopo de Terraform/infra do MVP — o MVP termina na view pronta para compartilhar |
| 8 | Onde deveria viver o código novo? | `src/open_insurance/` (novo domínio) + gerador de perfil em `src/ingestion/producer/datasets/` | Estrutura de pastas isolada do módulo regulatório |

**Minimum Questions:** 3 ✅ (8 feitas)

---

## Approaches Explored

### Approach A: Domínio Open Insurance completo, com pipeline próprio ⭐ Recomendado (Escolhido)

**Description:** Novo domínio `src/open_insurance/` com: (1) gerador de perfil sintético determinístico (seedado por `customer_id`, mesmo padrão de `regulatory_feeds.py`); (2) evento Kafka `consent-updated` → `bronze.consent_events` → `silver.consent_current` (SCD2, histórico completo) → `gold.customer_consent` (estado atual); (3) view `gold.open_insurance_shareable` juntando perfil sintético + `gold.claims`, filtrada por consentimento ativo. Delta Share real fica documentado como próximo passo, não implementado no MVP.

**Pros:**
- Segue exatamente os padrões já estabelecidos (medallion, SCD2, Kafka, geração sintética determinística)
- Reaproveita `gold.claims` e as 3 instituições fictícias já existentes
- Revogação de consentimento = sumir da view automaticamente, sem reconfigurar nada externo

**Cons:**
- Mais peças novas que uma versão mínima (gerador de perfil + SCD de consentimento)

**Why Recommended:** É o único approach que dá trilha de auditoria real de consentimento (GRANTED/REVOKED ao longo do tempo), essencial para uma feature que existe justamente para demonstrar controle do cliente sobre os próprios dados.

---

### Approach B: Versão enxuta, sem SCD de consentimento

**Description:** Mesmo fluxo, mas `gold.customer_consent` seria só o último estado (overwrite direto), sem `silver.consent_current` com histórico.

**Pros:**
- Menos uma tabela/etapa

**Cons:**
- Perde auditabilidade de consentimento (quem autorizou quando, revogou quando) — justamente o tipo de rastro que uma feature regulatória/LGPD deveria manter
- Pouca economia de esforço real para o que se perde

**Why not recommended:** O ganho de simplicidade não compensa perder o histórico de consentimento, que é o núcleo do valor demonstrado pela feature.

---

## Data Engineering Context

### Source Systems

| Source | Type | Volume Estimate | Current Freshness |
|--------|------|------------------|--------------------|
| Novo tópico Kafka `consent-updated` | Kafka (Confluent Cloud), publicado pelo producer existente | Baixo (1 evento por cliente por mudança de consentimento) | Streaming (mesmo padrão dos demais tópicos) |
| Gerador de perfil sintético | Batch, determinístico por `customer_id` | 1 linha por cliente único já visto em `gold.claims` | Recalculado quando o gerador roda, não é streaming |

### Data Flow Sketch

```text
[producer] --consent-updated--> [Kafka] --> bronze.consent_events
                                              --> silver.consent_current (SCD2: histórico GRANTED/REVOKED)
                                              --> gold.customer_consent (estado atual por cliente)

[gerador de perfil sintético, seed=customer_id] --> gold.open_insurance_profile

gold.claims + gold.customer_consent + gold.open_insurance_profile
  --> view gold.open_insurance_shareable (só customer_id com consent_status = GRANTED)
  --> [futuro, fora do MVP] Delta Share "open-insurance-share" (recipients: insurer_a/b/c)
```

### Key Data Questions Explored

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | O perfil sintético precisa ser realista/rico? | Sim — nome/idade/score de risco fictícios, não só IDs | Precisa de um gerador novo, não só reaproveitar campos existentes |
| 2 | O compartilhamento precisa refletir revogação imediatamente? | Sim, implícito no modelo de view sobre consentimento ativo | View sempre reflete o estado atual de `gold.customer_consent`, sem cache/snapshot |
| 3 | Delta Share precisa estar provisionado de verdade no MVP? | Não — decisão explícita de YAGNI | Reduz escopo de Terraform nesta fase |

---

## Selected Approach

| Attribute | Value |
|-----------|-------|
| **Chosen** | Approach A |
| **User Confirmation** | 2026-07-13 |
| **Reasoning** | Segue os padrões já validados do projeto e mantém trilha de auditoria de consentimento, que é o núcleo da feature |

---

## Key Decisions Made

| # | Decision | Rationale | Alternative Rejected |
|---|----------|-----------|------------------------|
| 1 | Consentimento via evento Kafka, não via tabela direta ou toggle no App | Mantém consistência com o padrão medallion já usado em todo o resto da plataforma | Tabela Gold seedada direto / toggle no Databricks App |
| 2 | Compartilhamento via Delta Sharing (conceitualmente), não API REST custom | É o mecanismo nativo do Databricks para esse exato caso de uso (compartilhar tabelas entre organizações) | API REST própria via Databricks App |
| 3 | Perfil sintético novo em vez de só reaproveitar campos existentes | Dados reais de origem são anonimizados, não há perfil rico pra reaproveitar | Compartilhar só `customer_id` + região + claims agregados |
| 4 | Reaproveitar `insurer_a`/`insurer_b`/`insurer_c` como recipients simulados | Consistência entre módulo regulatório e Open Insurance | Inventar instituições novas e distintas |

---

## Features Removed (YAGNI)

| Feature Suggested | Reason Removed | Can Add Later? |
|--------------------|------------------|------------------|
| Provisionamento real do Delta Share + Recipients via Terraform | `insurer_a/b/c` não são contas Databricks reais separadas — provisionar Recipients de verdade não teria um lado consumidor real pra validar | Sim — documentado como próximo passo natural pós-MVP |
| UI no Databricks App para o cliente gerenciar consentimento (toggle interativo) | Não foi pedido; o evento Kafka já cobre a simulação do consentimento sem precisar de tela nova | Sim — citado como alternativa descartada na Q2, pode virar fast-follow |

---

## Incremental Validations

| Section | Presented | User Feedback | Adjusted? |
|---------|-----------|-----------------|-----------|
| Esclarecimento Kafka vs. mecanismo de compartilhamento | ✅ | Usuário confirmou entendimento ("faz sentido") | Não — só esclarecido |
| Fluxo de dados completo (Kafka→bronze→silver SCD→gold consent + perfil sintético→view→Delta Share futuro) | ✅ | Aprovado implicitamente ao responder as perguntas seguintes sem contestar o fluxo | Não |
| YAGNI sobre Delta Share Recipients | ✅ | Optou por só documentar, não provisionar | Sim — MVP cortado para não incluir Terraform do Share |

**Minimum Validations:** 2 ✅ (3 feitas)

---

## Suggested Requirements for /define

### Problem Statement (Draft)

Clientes segurados não têm hoje nenhum mecanismo para consentir (ou revogar) o compartilhamento dos próprios dados com outras instituições financeiras/seguradoras — a plataforma só lida com o reporte regulatório obrigatório (SUSEP), não com o fluxo voluntário de Open Insurance/Open Finance.

### Target Users (Draft)

| User | Pain Point |
|------|------------|
| Cliente/segurado (simulado) | Não tem como expressar consentimento sobre quem acessa seus dados |
| Outra instituição (`insurer_a`/`b`/`c`, simulado) | Não tem uma fonte de dados de clientes que já consentiram o compartilhamento |
| Time de engenharia/compliance | Precisa de trilha auditável (LGPD) de quando cada cliente autorizou ou revogou acesso |

### Success Criteria (Draft)

- [ ] Evento `consent-updated` fluindo por `bronze.consent_events` → `silver.consent_current` → `gold.customer_consent`
- [ ] `silver.consent_current` mantém histórico completo (SCD2) de mudanças de status por cliente
- [ ] `gold.open_insurance_profile` com dados sintéticos determinísticos (mesmo `customer_id` sempre gera o mesmo perfil)
- [ ] View `gold.open_insurance_shareable` expõe apenas clientes com `consent_status = GRANTED` no momento da consulta
- [ ] Revogar consentimento remove o cliente da view no próximo refresh, sem intervenção manual

### Constraints Identified

- Sem PII real — perfil sintético gerado a partir de dados já anonimizados
- Delta Share em si (Terraform + Recipients) fica fora do MVP, só documentado como próximo passo
- Reaproveitar `insurer_a`/`insurer_b`/`insurer_c` como identidades, sem inventar instituições novas

### Out of Scope (Confirmed)

- Provisionamento real do Delta Share/Recipients via Terraform
- Integração de verdade com um banco/seguradora externo
- UI no Databricks App para o cliente gerenciar consentimento interativamente
- Qualquer mecanismo de distribuição de dados que não seja Delta Sharing (ex.: API REST custom)

---

## Session Summary

| Metric | Value |
|--------|-------|
| Questions Asked | 8 |
| Approaches Explored | 2 |
| Features Removed (YAGNI) | 2 |
| Validations Completed | 3 |
| Duration | ~1 sessão de diálogo |

---

## Next Step

**Ready for:** `/define .claude/sdd/features/BRAINSTORM_OPEN_INSURANCE.md`
