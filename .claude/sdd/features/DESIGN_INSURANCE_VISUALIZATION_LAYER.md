# DESIGN: Insurance Visualization Layer

> Technical design for implementing the Insurance Visualization Layer

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | INSURANCE_VISUALIZATION_LAYER |
| **Date** | 2026-07-09 |
| **Author** | design-agent |
| **DEFINE** | [DEFINE_INSURANCE_VISUALIZATION_LAYER.md](./DEFINE_INSURANCE_VISUALIZATION_LAYER.md) |
| **Status** | Ready for Build |

---

## Architecture Overview

```text
┌──────────────────────────────────────────────────────────────────────────┐
│                    PROD-ONLY (insurance_prod catalog)                    │
│                                                                          │
│   ┌────────────────────┐   ┌───────────────────┐   ┌──────────────────┐ │
│   │ Databricks App      │   │ AI/BI Dashboard   │   │ Genie Space      │ │
│   │ (Streamlit)          │   │ (Lakeview,        │   │ (NL → SQL,       │ │
│   │ 8 telas + status     │   │ 1 arquivo, 6 abas)│   │ CLI ≥1.3.0/      │ │
│   │ panel                │   │                   │   │ direct engine)   │ │
│   └──────────┬───────────┘   └─────────┬─────────┘   └────────┬─────────┘ │
│              │                          │                      │         │
│              └──────────────┬───────────┴──────────┬───────────┘         │
│                              ▼                      ▼                    │
│                    Serverless SQL Warehouse (Terraform, prod-only)       │
│                              │                                           │
└──────────────────────────────┼───────────────────────────────────────────┘
                                ▼
                 gold.claims, gold.claims_region_agg,
                 gold.regulatory_susep_claims, gold.regulatory_dq_summary,
                 monitoring._pipeline_latency_results,
                 monitoring._regulatory_reconciliation_results,
                 monitoring._dq_results, monitoring._model_drift_results
                                │
                                ▼
                    (já existentes — nenhuma tabela nova)
```

Nada abaixo da linha "Serverless SQL Warehouse" muda nesta feature — ela é puramente uma camada de consumo sobre o Gold já existente. Tudo acima dessa linha só é deployado no target `prod`; o target `dev` (e `staging`, se usado) não ganha nenhum recurso de visualização.

---

## Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| Serverless SQL Warehouse | Motor de query compartilhado pelo Dashboard, Genie e pelo App | `databricks_sql_endpoint` (Terraform), serverless, prod-only via `count` |
| AI/BI Dashboard | Indicadores operacionais/executivos (Pipeline Health, SLA, DQ, Fraude, Regras SUSEP, Bancos x Seguradoras) | Lakeview (`.lvdash.json`), deployado via DAB `dashboards` resource |
| Databricks App | Interface principal — navegação, consultas, painel de status | Streamlit, deployado via DAB `apps` resource |
| Genie Space | Perguntas em linguagem natural sobre as tabelas Gold | DAB `genie_spaces` resource (requer CLI ≥1.3.0 + "direct" deployment engine) |
| Grants do App/Genie | Acesso de leitura às tabelas Gold/monitoring pro service principal do App | `databricks_grants` (Terraform), prod-only, bootstrap em 2 passos (ver Decision 5) |

---

## Key Decisions

### Decision 1: Escopo "só prod" via bloco de resources no nível do target

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-09 |

**Context:** A DEFINE exige que Dashboard/App/Genie existam só no target `prod`, mas hoje todo recurso DAB deste repo é global (carregado via `include: - resources/*.yml`, aplicado a todos os targets igualmente — só `var.catalog` varia por target).

**Choice:** Um novo arquivo `resources/visualization.yml` (ainda casando com o glob `resources/*.yml` já incluído em `databricks.yml`) declara os recursos `dashboards`/`apps`/`genie_spaces` dentro de um bloco `targets: prod: resources: ...`, não no nível `resources:` de topo.

**Rationale:** Confirmado na documentação do Databricks (Declarative Automation Bundles / DAB): "resources mapping can appear as a top-level mapping, or it can be a child of one or more of the targets" — definições em nível de target só se aplicam àquele target, e um identificador definido só lá não existe nos demais. Isso evita precisar de um segundo bundle root ou de arquivos de resource inteiramente separados por ambiente.

**Alternatives Rejected:**
1. Bundle/root_path secundário só para produção — duplicaria toda a configuração de jobs já existente, não só a nova.
2. Flag manual pós-deploy (deployar em todos os targets e depois deletar via script em dev) — frágil, não idempotente, e contradiz o modelo declarativo do resto do repo.

**Consequences:**
- `databricks bundle validate -t dev` não mostra esses recursos — comportamento esperado e desejado.
- Precisa confirmar em Build que a sintaxe exata (`targets.prod.resources.dashboards`/`.apps`/`.genie_spaces`) é aceita pela versão do CLI instalada via `databricks/setup-cli@main` no `deploy.yml` (sem pin de versão, então deve estar atualizada).

---

### Decision 2: Serverless SQL Warehouse novo, gerenciado por Terraform e só em prod

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-09 |

**Context:** Dashboard, Genie e o App precisam de um `warehouse_id` (SQL Warehouse) — este repo hoje só usa compute serverless via `spark_python_task` (jobs), nunca um SQL Warehouse.

**Choice:** Novo `terraform/warehouse.tf` cria um `databricks_sql_endpoint` serverless (`2X-Small`, `auto_stop_mins` curto), condicionado a `count = var.environment == "prod" ? 1 : 0`.

**Rationale:** Terraform já roda com state e apply **separados por ambiente** neste repo (`deploy.yml` faz `terraform apply` de dev e prod com `TF_VAR_environment` e caches de state distintos: `tfstate-dev-`/`tfstate-prod-`) — usar `count` condicionado a `var.environment` é o mecanismo mais simples e já é consistente com como o resto do Terraform deste repo varia por ambiente (embora hoje nada use `count` condicional, o padrão `var.environment` já existe em todo lugar). Mantém o warehouse dentro da fronteira Terraform já documentada (infra que muda raramente), sem introduzir um novo sistema.

**Alternatives Rejected:**
1. SQL Warehouse criado manualmente pela UI (como o catálogo foi, no início do projeto) — rejeitado porque, ao contrário do catálogo, não há um bloqueio conhecido de `terraform apply` para criar warehouses; não há motivo pra fugir de IaC aqui.
2. Warehouse clássico (não-serverless) — rejeitado por custo/gestão de cluster desnecessários para uma demo.

**Consequences:**
- Novo custo de compute (mesmo serverless, mesmo com `auto_stop_mins` curto) só em prod.
- Precisa confirmar em Build se a conta/workspace trial atual tem SQL Serverless habilitado (mesma classe de risco já vista com Unity Catalog Model Registry bloqueado por bucket policy nesta sessão) — ver Assumptions.

---

### Decision 3: Databricks App usa Streamlit, autenticado via service principal dedicado com bootstrap em 2 passos

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-09 |

**Context:** Cada Databricks App recebe um service principal **dedicado, criado automaticamente** no primeiro deploy (`DATABRICKS_CLIENT_ID`/`DATABRICKS_CLIENT_SECRET` injetados no runtime) — esse principal não existe antes do primeiro `databricks bundle deploy -t prod`, então não há como o Terraform conceder grants a ele na mesma rodada que cria o app.

**Choice:** Framework = Streamlit (multipage, um arquivo por tela). Autenticação/dados: o app usa o SQL Warehouse (Decision 2) via `valueFrom` no `app.yaml` (env var injetada, sem hardcode de credencial). O acesso às tabelas Gold é resolvido em **dois passos**: (1) primeiro `databricks bundle deploy -t prod` cria o app e mint a o service principal; (2) o `client_id` gerado é setado numa nova variável Terraform (`TF_VAR_app_service_principal_id`, opcional/vazia por padrão) e um novo `terraform apply` concede `SELECT` nos schemas `gold`/`monitoring` do catálogo prod a esse principal.

**Rationale:** Streamlit é o framework mais documentado para Databricks Apps e o que o próprio usuário citou primeiro. O bootstrap em 2 passos é o mesmo tipo de passo manual único já aceito neste repo para o catálogo Unity Catalog (criado uma vez pela UI, depois `terraform import`) — não é uma exceção nova ao padrão do projeto, é a mesma classe de problema (ordem de criação entre duas ferramentas).

**Alternatives Rejected:**
1. Assumir que o grant `catalog_owner = "account users"` já cobre o service principal do app — rejeitado por não ter confirmação: não está claro se o grupo embutido `account users` inclui automaticamente service principals provisionados por Databricks Apps. Fica como Assumption a validar em Build antes de decidir se o passo 2 é sequer necessário.
2. Dash/Flask/Gradio no lugar de Streamlit — rejeitado só por escopo (qualquer um resolveria; Streamlit tem mais exemplos oficiais de app.yaml).

**Consequences:**
- Um novo passo manual de deploy documentado no README (rodar `deploy-prod` duas vezes na primeira ativação: uma pra criar o app, outra — após setar `TF_VAR_app_service_principal_id` — pra conceder o grant).
- Se a Assumption acima for validada como verdadeira (o app já tem acesso via `account users`), o passo 2 e a nova variável Terraform podem ser removidos no Build sem afetar o resto do design.

---

### Decision 4: Dashboard e Genie são prototipados na UI e capturados via `bundle generate`, não escritos à mão

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-09 |

**Context:** O JSON de um dashboard Lakeview (`.lvdash.json`) e a definição de um Genie Space são artefatos complexos, tipicamente produzidos pela própria ferramenta (UI ou API), não destinados a serem escritos à mão do zero.

**Choice:** Fluxo de Build: (1) o dashboard/Genie space é criado e ajustado interativamente na UI do Databricks (usando o SQL Warehouse do Decision 2, no workspace prod); (2) `databricks bundle generate dashboard --existing-id <id>` / `databricks bundle generate genie-space --existing-id <id>` traz a definição pro bundle (YAML de resource + `.lvdash.json`/JSON), versionada em `resources/visualization.yml` + `resources/dashboards/insurance_platform.lvdash.json`.

**Rationale:** Essa é a forma documentada e suportada pela própria Databricks pra colocar dashboards/Genie sob controle de versão — confirmado via busca na documentação oficial (`databricks bundle generate dashboard`/`generate genie-space --existing-id`). Escrever esse JSON à mão arriscaria alucinar um schema não suportado.

**Alternatives Rejected:**
1. Gerar o `.lvdash.json` inteiramente por código/template — rejeitado: schema interno do Lakeview não é público/estável o suficiente pra hand-authoring confiável.

**Consequences:**
- O Build desta feature não é 100% "código primeiro" — exige uma etapa manual/interativa na UI do workspace prod antes de existir algo pra versionar. Isso é atípico pro resto do repo (tudo mais é gerado só por código) e deve ser documentado explicitamente no README como um passo humano necessário.
- O dashboard/Genie versionado no repo só reflete o estado no momento do `generate` — mudanças feitas depois na UI exigem rodar `generate` de novo (mesma limitação documentada pela Databricks).

---

### Decision 5: Um dashboard, seis abas — não seis dashboards separados

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-09 |

**Context:** O DEFINE lista 6 temas (Pipeline Health, SLA, Qualidade de Dados, Fraude, Regras SUSEP, Bancos x Seguradoras) e exige "pelo menos 1 dashboard".

**Choice:** Um único arquivo Lakeview com 6 abas/páginas internas, uma por tema, todas sobre o mesmo Warehouse.

**Rationale:** Simplifica deploy (1 resource DAB, 1 arquivo `.lvdash.json`) e navegação (usuário troca de aba em vez de link externo por dashboard) sem violar nenhum critério do DEFINE.

**Alternatives Rejected:**
1. 6 dashboards separados — rejeitado: mais recursos DAB pra manter sem benefício claro pro caso de uso.

**Consequences:**
- Todas as 6 abas dependem do mesmo Warehouse estar no ar; não há isolamento de falha entre temas (aceitável pro escopo de demo/portfólio).

---

### Decision 6: Genie fica marcado SHOULD, isolado do MUST (Dashboard + App)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-09 |

**Context:** O suporte a Genie Space via DAB é o mais novo dos três (CLI ≥1.3.0, exige o "direct deployment engine" — não Terraform mode; não há resource Terraform `databricks_genie_space`). É a peça com maior risco de não funcionar de primeira nesta sessão, no mesmo espírito de várias APIs novas que precisaram de workaround neste projeto (`mlflow.pyfunc.spark_udf`, UC Model Registry, etc.).

**Choice:** Build implementa e valida Dashboard + App primeiro (MUST, tecnologia mais madura); Genie é a última peça, com escopo claramente isolável — se falhar ou o "direct deployment engine" quebrar o deploy dos outros recursos (jobs, dashboard, app) no mesmo bundle, Genie pode ser removido do `resources/visualization.yml` sem afetar o resto.

**Rationale:** Mesmo padrão de risco já usado nesta sessão (ex.: `load_champion_model_udf` isolado em try/except amplo, nunca quebrando o scoring real). Isolar o componente mais arriscado protege o que já é MUST.

**Alternatives Rejected:**
1. Criar o Genie Space manualmente pela UI sem tentar a rota declarativa — rejeitado como primeira tentativa (contradiria o objetivo de portfólio "tudo como código"), mas documentado como fallback aceitável se a rota declarativa não funcionar.

**Consequences:**
- Se o "direct deployment engine" exigir uma mudança de configuração que afete o deploy dos jobs já existentes (Terraform-backed), Build deve isolar essa mudança e validar que os jobs continuam deployando antes de prosseguir.

---

## File Manifest

| # | File | Action | Purpose | Agent | Dependencies |
|---|------|--------|---------|-------|--------------|
| 1 | `terraform/warehouse.tf` | Create | Serverless SQL Warehouse, prod-only via `count` | @data-platform-security | None |
| 2 | `terraform/variables.tf` | Modify | Nova var opcional `app_service_principal_id` (default `""`) | @data-platform-security | None |
| 3 | `terraform/unity_catalog.tf` | Modify | Grant `SELECT` em `gold`/`monitoring` pro SP do app, `count` condicionado a `var.environment == "prod" && var.app_service_principal_id != ""` | @data-platform-security | 1, 2 |
| 4 | `resources/visualization.yml` | Create | `targets.prod.resources`: `dashboards`, `apps`, `genie_spaces` | @ci-cd-specialist | 1 |
| 5 | `resources/dashboards/insurance_platform.lvdash.json` | Create (via `bundle generate` após prototipar na UI) | Definição do dashboard Lakeview (6 abas) | (manual + @ci-cd-specialist) | 1, 4 |
| 6 | `apps/insurance_platform_app/app.yaml` | Create | Manifesto do Databricks App (`command`, `env` com `valueFrom` do warehouse) | @ci-cd-specialist | 1 |
| 7 | `apps/insurance_platform_app/app.py` | Create | Entrypoint Streamlit + navegação (8 telas) + painel de status | @python-developer | 6 |
| 8 | `apps/insurance_platform_app/queries.py` | Create | Funções puras de construção de query SQL (testáveis) + wrapper fino de execução via Databricks SQL Connector | @python-developer | 6 |
| 9 | `apps/insurance_platform_app/pages/*.py` | Create | 8 páginas Streamlit (Consultar Apólice/Cliente/Sinistro, Pesquisar Banco/Seguradora, Dashboard, Lineage, Auditoria) | @python-developer | 7, 8 |
| 10 | `apps/insurance_platform_app/requirements.txt` | Create | `streamlit`, `databricks-sql-connector` | @python-developer | None |
| 11 | `tests/unit/test_visualization_queries.py` | Create | Testes das funções puras de `queries.py` (sem warehouse real) | @test-generator | 8 |
| 12 | `docs/ARCHITECTURE.md` | Modify | Nova seção de Decision documentando o bootstrap em 2 passos e o escopo prod-only | (self) | None |
| 13 | `README.md` | Modify | Seção nova: como ativar a visualização (passos manuais: prototipar dashboard/Genie na UI, `bundle generate`, 2º apply do Terraform) | (self) | None |

**Total Files:** 13 (mais os arquivos gerados pelo `bundle generate`, que não são escritos à mão)

---

## Agent Assignment Rationale

> Agentes descobertos em `${CLAUDE_PLUGIN_ROOT}/agents/` (agentspec plugin já instalado neste ambiente).

| Agent | Files Assigned | Why This Agent |
|-------|----------------|-----------------|
| @data-platform-security | 1, 2, 3 | Especialista em RBAC/grants no Unity Catalog — é exatamente o tipo de mudança destes 3 arquivos (novo warehouse + grant condicional pro SP do app) |
| @ci-cd-specialist | 4, 5, 6 | Especialista em Databricks Asset Bundles/Terraform — resource YAML novo e manifesto do app são configuração de deploy, não lógica de negócio |
| @python-developer | 7, 8, 9, 10 | Código Python de aplicação (Streamlit, dataclasses/type hints já é a convenção deste repo) |
| @test-generator | 11 | Geração de testes pytest — mesmo padrão dos demais `tests/unit/test_*.py` do repo |
| (self / general) | 12, 13 | Documentação — segue o padrão já estabelecido nesta sessão de o próprio agente principal manter `docs/ARCHITECTURE.md`/`README.md` |

**Agent Discovery:** Escaneado `.claude/agents/` (cache local do plugin agentcode/agentspec) — nenhum agente específico de "Databricks Apps"/"Lakeview"/"Genie" foi encontrado; os agentes acima foram escolhidos pela especialização mais próxima (governança UC, CI/CD de Databricks, código Python de aplicação).

---

## Code Patterns

### Pattern 1: `app.yaml` referenciando o SQL Warehouse via `valueFrom`

```yaml
# apps/insurance_platform_app/app.yaml
command: ["streamlit", "run", "app.py"]

env:
  - name: WAREHOUSE_ID
    valueFrom: sql-warehouse
  - name: LOG_LEVEL
    value: "info"
```

```yaml
# resources/visualization.yml (dentro de targets.prod.resources.apps)
resources:
  apps:
    insurance_platform_app:
      name: insurance-platform-app
      source_code_path: ../apps/insurance_platform_app
      resources:
        - name: "sql-warehouse"
          sql_warehouse:
            id: ${resources.jobs... }  # placeholder — Build resolve o ID real do
                                       # databricks_sql_endpoint criado pelo Terraform
                                       # (provavelmente via variável de bundle, não
                                       # referência direta cross-tool)
          permission: "CAN_MANAGE"
```

### Pattern 2: `queries.py` — separar construção de query (testável) da execução (não testável localmente)

```python
# apps/insurance_platform_app/queries.py
from dataclasses import dataclass


@dataclass
class ClaimLookupQuery:
    sql: str
    params: dict


def build_claim_lookup_query(claim_id: str) -> ClaimLookupQuery:
    # Função pura — sem conexão real, 100% testável em tests/unit/ sem warehouse.
    return ClaimLookupQuery(
        sql="SELECT * FROM gold.claims WHERE claim_id = :claim_id",
        params={"claim_id": claim_id},
    )


def run_query(connection, query: ClaimLookupQuery):
    # Único ponto que toca o Databricks SQL Connector de verdade — não
    # coberto por unit test (mesma limitação já documentada: Spark/warehouse
    # real não roda neste sandbox).
    with connection.cursor() as cursor:
        cursor.execute(query.sql, query.params)
        return cursor.fetchall()
```

### Pattern 3: Warehouse condicional por ambiente no Terraform

```hcl
# terraform/warehouse.tf
resource "databricks_sql_endpoint" "visualization" {
  count = var.environment == "prod" ? 1 : 0

  name             = "insurance-visualization-${var.environment}"
  cluster_size     = "2X-Small"
  auto_stop_mins   = 10
  enable_serverless_compute = true
}
```

---

## Data Flow

```text
1. Usuário abre o Databricks App (prod) ou o AI/BI Dashboard ou pergunta no Genie
   │
   ▼
2. App/Dashboard/Genie disparam uma query SQL contra o Serverless SQL Warehouse
   │
   ▼
3. Warehouse lê diretamente das tabelas Gold/monitoring já existentes
   (nenhuma tabela nova, nenhum pipeline novo)
   │
   ▼
4. Resultado renderizado (tabela/gráfico no App e Dashboard; resposta em
   linguagem natural + tabela/gráfico no Genie)
```

---

## Integration Points

| External System | Integration Type | Authentication |
|-----------------|-------------------|------------------|
| Serverless SQL Warehouse | Databricks SQL Connector (App) / nativo (Dashboard, Genie) | Service principal dedicado do App (OAuth injetado); Dashboard/Genie usam a identidade do workspace |
| Unity Catalog (`gold`/`monitoring`, catálogo prod) | Leitura via SQL | Grants Terraform (bootstrap em 2 passos, ver Decision 3) |

---

## Testing Strategy

| Test Type | Scope | Files | Tools | Coverage Goal |
|-----------|-------|-------|-------|-----------------|
| Unit | Funções puras de construção de query (`build_*_query`) | `tests/unit/test_visualization_queries.py` | pytest | Todas as funções `build_*_query` |
| Manual (não automatizável) | Dashboard renderiza dados reais; App navega pelas 8 telas; Genie responde as 7 perguntas de exemplo | N/A — checklist manual pós-deploy em prod | Databricks UI | AT-001 a AT-003 do DEFINE |
| Bundle validation | Confirma que `dev`/`staging` não recebem os novos recursos | `databricks bundle validate -t dev` | Databricks CLI (CI) | AT-004 do DEFINE |

**Nota (mesma limitação já documentada no repo):** Dashboard/App/Genie só existem de verdade dentro de um workspace Databricks — não há como rodar nada disso neste sandbox local nem em CI puro. `tests/unit/test_visualization_queries.py` é o único teste automatizável desta feature; os demais critérios de aceite (AT-001, AT-002, AT-003) exigem validação manual pós-deploy, igual à já aceita para `scripts/measure_pipeline_latency.py` antes de rodar de verdade.

---

## Error Handling

| Error Type | Handling Strategy | Retry? |
|------------|---------------------|--------|
| Warehouse indisponível/hibernado | Serverless SQL Warehouse liga sob demanda (`auto_stop_mins`); primeira query de um período ocioso simplesmente demora mais (cold start) | Não — comportamento esperado do serverless |
| Grant do SP do app ainda não aplicado (entre passo 1 e passo 2 do bootstrap) | App mostra erro de permissão nas páginas de consulta; painel de status marca "Gold: erro de acesso" em vez de travar a UI inteira | Não — resolvido rodando o 2º `terraform apply` |
| Genie/direct engine não suportado pela versão do CLI no momento do deploy | Deploy falha só no resource `genie_spaces` — Build deve isolar esse bloco (ver Decision 6) pra não derrubar o deploy dos jobs/dashboard/app já funcionando | Não — fallback: remover o bloco, criar Genie manualmente pela UI |

---

## Configuration

| Config Key | Type | Default | Description |
|------------|------|---------|--------------|
| `app_service_principal_id` (Terraform var) | string | `""` | Client ID do service principal do Databricks App — vazio até o bootstrap passo 1 rodar; grant só é criado quando não-vazio |
| `WAREHOUSE_ID` (env var do App, via `valueFrom`) | string | (injetado) | ID do Serverless SQL Warehouse, resolvido pelo runtime do Databricks App |

---

## Security Considerations

- Nenhum dado novo, sensível ou pessoal é introduzido — a feature só consome tabelas Gold/monitoring já existentes, cujo masking/RLS (`customer_id`, região) já se aplica antes de chegar no Warehouse.
- O service principal do App só recebe `SELECT` (nunca `MODIFY`/`CREATE_TABLE`) nos schemas `gold`/`monitoring` — princípio de menor privilégio.
- `app_service_principal_id` não é um segredo (é um client ID, não uma credencial) — pode ficar como variável Terraform normal, não `sensitive`, mas ainda assim só populada manualmente (não commitada em texto claro em nenhum arquivo do repo — vive como `TF_VAR_app_service_principal_id` no ambiente do `deploy.yml`/GitHub Environment `production`, mesmo padrão dos demais secrets de prod).

---

## Observability

| Aspect | Implementation |
|--------|------------------|
| Logging | Streamlit App usa `logging` padrão (mesmo padrão de `logging.basicConfig` já usado em `main.py`/outros jobs deste repo) |
| Métricas | Nenhuma métrica nova — o painel de status do App lê diretamente das tabelas de monitoring já existentes (`monitoring._pipeline_latency_results`, `monitoring._dq_results`, `monitoring._model_drift_results`) |
| Alertas | Reaproveita o mesmo `SLA_WEBHOOK_URL`/`get_secret` já usado no resto do projeto — nenhum canal de alerta novo é necessário para esta feature |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-07-09 | design-agent | Initial version |

---

## Next Step

**Ready for:** `/build .claude/sdd/features/DESIGN_INSURANCE_VISUALIZATION_LAYER.md`
