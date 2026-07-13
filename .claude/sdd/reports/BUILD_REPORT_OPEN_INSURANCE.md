# BUILD REPORT: Open Insurance

> Implementation report for Open Insurance (módulo de consentimento do cliente para compartilhamento de dados)

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | OPEN_INSURANCE |
| **Date** | 2026-07-13 |
| **Author** | build-agent |
| **DEFINE** | [DEFINE_OPEN_INSURANCE.md](../features/DEFINE_OPEN_INSURANCE.md) |
| **DESIGN** | [DESIGN_OPEN_INSURANCE.md](../features/DESIGN_OPEN_INSURANCE.md) |
| **Status** | Complete (com ressalva de verificação — ver Verification Results) |

---

## Summary

| Metric | Value |
|--------|-------|
| **Tasks Completed** | 10/10 |
| **Files Created** | 11 |
| **Files Modified** | 6 (incluindo `pyproject.toml`, não previsto no manifest original) |
| **Lines of Code** | ~1281 (novo + modificado, contando docs/config) |
| **Tests Passing** | Ver Verification Results — ambiente local sem PySpark/Java |
| **Agents Used** | 0 (build direto, sem delegação a subagentes) |

---

## Task Execution with Agent Attribution

| # | Task | Agent | Status | Notes |
|---|------|-------|--------|-------|
| 1 | `src/common/schemas.py` — `CONSENT_EVENT_SCHEMA` | (direct) | ✅ Complete | Chave revisada para `policy_id` durante o build (ver Deviations) |
| 2 | `src/common/scd2_merge.py` | (direct) | ✅ Complete | Lógica final difere do sketch ilustrativo do DESIGN — corrigida para não duplicar/reintroduzir eventos fora de ordem |
| 3 | `src/open_insurance/__init__.py` + `consent_scd.py` | (direct) | ✅ Complete | — |
| 4 | `src/open_insurance/profile_generator.py` | (direct) | ✅ Complete | Chave `policy_id`, não `customer_id` |
| 5 | `src/ingestion/producer/datasets/consent_events.py` | (direct) | ✅ Complete | Reaproveita `susep_loader.normalize_susep_claims` para obter `policy_id` reais |
| 6 | `config.yaml` + `main.py` (producer) | (direct) | ✅ Complete | Nova source `consent_simulator`, distinta do stub `open_insurance` já existente |
| 7 | `src/open_insurance/shareable_view.py` | (direct) | ✅ Complete | Guards de `tableExists` adicionados (não estavam no sketch do DESIGN) |
| 8 | `resources/jobs.open_insurance.yml` | (direct) | ✅ Complete | 2 jobs agendados (`*/15min`, `*/30min`), nenhum `continuous:` |
| 9 | `tests/unit/test_*.py` (5 arquivos) | (direct) | ✅ Complete | Inclui `test_consent_events.py`, não previsto no manifest original (seguindo precedente de `test_regulatory_feeds.py`) |
| 10 | `docs/ARCHITECTURE.md` + `pyproject.toml` (ruff per-file-ignores) | (direct) | ✅ Complete | `pyproject.toml` não estava no manifest — necessário pro padrão de bootstrap de `sys.path` já usado em todo o projeto |

**Legend:** ✅ Complete

**Nota sobre agentes:** todo o build foi feito diretamente (sem `Task`/subagentes), já que o escopo é contínuo com o padrão de código já estabelecido no projeto — usar os agentes especialistas listados no DESIGN teria exigido delegar contexto que eu já tinha carregado integralmente.

---

## Files Created

| File | Lines | Verified | Notes |
|------|-------|----------|-------|
| `src/common/scd2_merge.py` | 73 | ⚠️ Verificado por inspeção manual (traço linha a linha dos 4 cenários de teste) | Não executado via pytest neste ambiente |
| `src/open_insurance/__init__.py` | 0 | ✅ | Vazio, mesmo padrão de `src/regulatory/__init__.py` |
| `src/open_insurance/consent_scd.py` | 60 | ⚠️ Não executado (depende de PySpark) | Lógica revisada manualmente |
| `src/open_insurance/profile_generator.py` | 68 | ✅ Executado (import com stub de `spark_session`) | Determinismo e bounds confirmados de verdade |
| `src/open_insurance/shareable_view.py` | 106 | ⚠️ Não executado (depende de PySpark) | SQL revisado manualmente |
| `src/ingestion/producer/datasets/consent_events.py` | 58 | ✅ Executado de verdade (CSV de amostra) | Determinismo (exceto `event_timestamp`) confirmado |
| `resources/jobs.open_insurance.yml` | 93 | ✅ YAML válido (`python3 -c "import yaml"`) | Não validado via `databricks bundle validate` (CLI não instalado neste ambiente) |
| `tests/unit/test_scd2_merge.py` | 93 | ⏭️ Não executado | Requer PySpark/Delta/Java |
| `tests/unit/test_consent_scd.py` | 42 | ⏭️ Não executado | Requer PySpark/Delta/Java |
| `tests/unit/test_profile_generator.py` | 20 | ⏭️ Não executado via pytest, lógica confirmada manualmente | — |
| `tests/unit/test_shareable_view.py` | 73 | ⏭️ Não executado | Requer PySpark/Delta/Java |
| `tests/unit/test_consent_events.py` | 64 | ⏭️ Não executado via pytest, lógica confirmada manualmente | — |

## Files Modified

| File | Change |
|------|--------|
| `src/common/schemas.py` | Novo `CONSENT_EVENT_SCHEMA` (chave `policy_id`) + entrada em `SCHEMA_REGISTRY` |
| `src/ingestion/producer/config.yaml` | Novo tópico `consent_updated` + source `consent_simulator` |
| `src/ingestion/producer/main.py` | Registra `consent_simulator` em `SOURCE_LOADERS` |
| `pyproject.toml` | 3 novos per-file-ignores de `E402` (mesmo padrão dos demais entrypoints de job) |
| `docs/ARCHITECTURE.md` | Nova seção do módulo + bullet no roadmap |
| `terraform/*.tf` | **Nenhuma mudança** (Decision 4 do DESIGN — reaproveita schemas/grants/volume já existentes) |

---

## Verification Results

### Lint Check

```text
$ ruff check .
All checks passed!
```

**Status:** ✅ Pass

### Type Check

N/A — projeto não usa mypy (não configurado em `pyproject.toml`, consistente com o resto do repositório).

**Status:** ⏭️ Skipped (não configurado)

### Tests

Este ambiente de build **não tem Java nem PySpark/Delta instalados** (sem acesso a `sudo`/`apt`/`python3-venv` para instalá-los), então a suíte `pytest tests/unit` **não pôde ser executada de verdade** para os 3 arquivos que dependem de Spark (`test_scd2_merge.py`, `test_consent_scd.py`, `test_shareable_view.py`).

O que foi feito para compensar:

1. **`generate_profile` e `load_consent_events`** (lógica pura, sem Spark) foram executados de verdade fora do pytest (stub de `src.common.spark_session` via `sys.modules` para satisfazer o import sem precisar instalar PySpark) — determinismo, distinção entre chaves e limites de valores confirmados com dados reais.
2. **`merge_scd2_into_delta`** foi verificado por **traço manual linha a linha** dos 4 cenários de teste (criação inicial, fechamento de versão + inserção de nova, evento fora de ordem ignorado, reprocessamento idempotente) — resultado esperado bate com as asserções dos testes escritos, mas isso **não substitui execução real**.
3. **`shareable_view.py`** (SQL das views) foi revisado manualmente, sem execução.

```text
generate_profile: OK (determinístico, distinto por policy_id, dentro dos limites)
load_consent_events: OK (policy_id corretos, GRANTED sempre presente, determinístico exceto event_timestamp)
scd2_merge: verificado por inspeção manual, não executado
consent_scd / shareable_view: não executados
```

**Status:** ⚠️ **Parcial** — recomendo fortemente rodar `pytest tests/unit` no ambiente normal do projeto (com Java 17 + `pip install -r requirements-dev.txt`, ver README) ou deixar o CI (`.github/workflows/ci.yml`) validar no próximo push, **antes** de considerar este build pronto para produção.

---

## Issues Encountered

| # | Issue | Resolution | Time Impact |
|---|-------|------------|--------------|
| 1 | `gold.claims.customer_id` é sempre `NULL` (descoberto ao ler `ans_loader.py`/`susep_loader.py` durante o build) — o design original assumia `customer_id` como chave | Trocado para `policy_id` em todo o módulo (schema, SCD2, gerador de perfil, view) — já era uma limitação documentada em `docs/ARCHITECTURE.md` antes deste build, só não tinha sido conectada ao design da feature | +~15min |
| 2 | Sketch de `merge_scd2_into_delta` no DESIGN (Pattern 1) inseria incondicionalmente todas as linhas do batch, o que duplicaria/reintroduziria eventos fora de ordem como `is_current=true` | Reescrito com uma segunda etapa de anti-join que só insere chaves cuja versão candidata é de fato mais nova que a vigente | +~10min |
| 3 | `E402` do ruff nos 3 arquivos novos que seguem o padrão de bootstrap de `sys.path` (já usado em todo o projeto) | Adicionados ao `per-file-ignores` existente em `pyproject.toml`, mesmo padrão dos demais entrypoints | +~2min |
| 4 | Ambiente de build sem Java/PySpark e sem `sudo`/`python3-venv` para instalar | Verificação manual + parcial (ver Verification Results) em vez de execução completa da suíte | +~10min |

---

## Deviations from Design

| Deviation | Reason | Impact |
|-----------|--------|--------|
| Chave de join é `policy_id`, não `customer_id` (schema, SCD2, perfil, view) | `gold.claims.customer_id` é sempre `NULL` neste projeto (dados reais anonimizados) — descoberto ao inspecionar `ans_loader.py`/`susep_loader.py` durante o build, não durante o design | Nenhum impacto negativo — `policy_id` é a chave estável de fato disponível; a arquitetura geral do DESIGN (SCD2, LEFT JOIN, cadência agendada) continua válida sem mudança |
| `merge_scd2_into_delta` usa uma segunda etapa de anti-join (não estava no Pattern 1 ilustrativo do DESIGN) | O sketch original inseria toda linha do batch incondicionalmente, o que duplicaria versões "vigentes" para eventos fora de ordem | Corrige um bug de duplicação que o sketch teria introduzido; comportamento final continua fiel à intenção da Decision 2 |
| `shareable_view.py` verifica `tableExists` antes de criar as views | DESIGN não detalhava o comportamento nas primeiras execuções (antes de qualquer tabela base existir) | Evita falha em `CREATE VIEW` sobre tabela ausente nos primeiros runs — consistente com o padrão de degradação graciosa já usado no resto da plataforma |
| `test_consent_events.py` adicionado (não estava no File Manifest do DESIGN) | Precedente direto: `src/regulatory/regulatory_feeds.py` (módulo irmão) tem `test_regulatory_feeds.py` cobrindo exatamente esse tipo de gerador | Cobertura mais completa, sem escopo além do que o padrão já estabelecido no projeto pede |
| `pyproject.toml` modificado (não estava no File Manifest) | Necessário para os 3 arquivos novos que seguem o padrão de bootstrap de `sys.path` passarem no `ruff check .` | Nenhum, é config de lint consistente com o resto do projeto |

---

## Blockers (if any)

Nenhum blocker — mas ver a ressalva de verificação acima antes de mesclar/deployar.

---

## Acceptance Test Verification

| ID | Scenario | Status | Evidence |
|----|----------|--------|----------|
| AT-001 | Consentimento concedido aparece na view | ⚠️ Coberto por `test_shareable_view.py`, não executado | Traço manual da SQL confirma o comportamento esperado |
| AT-002 | Revogação remove da view | ⚠️ Coberto por `test_shareable_view.py` (via `p3` REVOKED), não executado | Traço manual confirma exclusão via `WHERE consent_status = 'GRANTED'` |
| AT-003 | Histórico de consentimento é preservado (SCD2) | ⚠️ Coberto por `test_scd2_merge.py`, não executado | Traço manual linha a linha (ver Verification Results) confirma 2 versões, `is_current` correto |
| AT-004 | Perfil sintético é determinístico | ✅ Pass | Executado de verdade — `generate_profile("policy-123")` chamado 2x produz resultado idêntico |
| AT-005 | Cliente com consentimento mas sem claims (edge case, resolvido em DESIGN Decision 6) | ⚠️ Coberto por `test_shareable_view.py` (`p2` sem claims), não executado | Traço manual do LEFT JOIN confirma `claim_id IS NULL` sem excluir a linha |

---

## Final Status

### Overall: 🔄 **COMPLETE COM RESSALVA** — código e testes escritos, lint passa, mas a suíte pytest completa (Spark) não foi executada neste ambiente

**Completion Checklist:**

- [x] Todos os arquivos do manifest (+ 2 extras justificados) criados/modificados
- [x] `ruff check .` passa
- [ ] Todos os testes executados e passando — **pendente**, rodar `pytest tests/unit` no ambiente normal do projeto ou aguardar o CI
- [x] Nenhum blocker
- [ ] Acceptance tests verificados por execução real — **verificados por inspeção manual**, não por execução
- [ ] Pronto para `/ship` — recomendo rodar a suíte real primeiro

---

## Next Step

**Recomendado antes de `/ship`:** rodar `pytest tests/unit -k "scd2 or consent_scd or shareable_view"` num ambiente com Java 17 + `pip install -r requirements-dev.txt`, ou abrir um PR e deixar `.github/workflows/ci.yml` validar. Se tudo passar, `/ship .claude/sdd/features/DEFINE_OPEN_INSURANCE.md`.

**Se algo falhar:** `/iterate DESIGN_OPEN_INSURANCE.md "{ajuste necessário}"`.
