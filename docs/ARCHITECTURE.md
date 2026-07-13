# Arquitetura — Insurance Lakehouse Platform

> Ver histórico completo de decisões em [`.claude/sdd/features/`](../.claude/sdd/features/): `BRAINSTORM_INSURANCE_LAKEHOUSE_PLATFORM.md`, `DEFINE_INSURANCE_LAKEHOUSE_PLATFORM.md`, `DESIGN_INSURANCE_LAKEHOUSE_PLATFORM.md`.

## Visão geral

Plataforma de dados de seguros 100% Databricks: ingestão de dados públicos reais (SUSEP, ANS) via Kafka (Confluent Cloud), processados com Spark Structured Streaming puro (sem DLT/Lakeflow) em uma arquitetura medallion (Bronze → Silver → Gold), com detecção de fraude em streaming, aprovação automática, governança via Unity Catalog e CI/CD via GitHub Actions + Databricks Asset Bundles + Terraform.

```text
[SUSEP CSV]  [ANS CSV]
      │            │
      └─────┬──────┘
            ▼
    Kafka Producer (GitHub Actions, cron 10 em 10 min)
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
- **Producer agendado no GitHub Actions, não always-on**: em vez de um processo contínuo (que exigiria hospedar um worker em algum lugar — custo/infra nova), `.github/workflows/producer.yml` publica em rajadas de até 8 min a cada 10 min, reusando os secrets `CONFLUENT_*` que já existem no repo. `PRODUCER_MAX_DURATION_SECONDS` faz o processo encerrar sozinho (flush gracioso) antes do timeout do job.
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
│   ├── fraud/           # score de fraude (streaming) + treino do modelo (MLflow, com gate champion/challenger)
│   ├── monitoring/      # alertas de SLA + monitor de drift de features do modelo
│   └── regulatory/      # standardize → reconcile → gold export do fluxo regulatório SUSEP (batch)
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

## Gate de CI/CD do modelo, monitor de drift e retrain automático (2026-07-09)

`train_model.py` não promove mais toda versão treinada — compara o `f1` do novo modelo contra o "champion" atual e só promove se o novo modelo for estritamente melhor. Versões não promovidas ficam com tags (`eval_f1`, `model_stage=rejected`) em vez de um alias `challenger`, que seria reatribuído em todo run independente de qualidade.

**Unity Catalog Model Registry bloqueado por bucket policy (2026-07-09):** a tentativa original usava `mlflow.set_registry_uri("databricks-uc")` + `registered_model_name` de 3 níveis. Isso falha com `MlflowException: ... AccessDenied ... explicit deny in a resource-based policy` ao tentar subir os artefatos da versão do modelo pro storage do UC (`s3://.../models/.../versions/...`) — um deny explícito na bucket policy do S3 gerenciado pelo metastore, não um grant de Unity Catalog (as roles `USE_SCHEMA`/`CREATE_MODEL` no schema `models` já estão corretas via Terraform). Deny explícito sempre vence IAM allow na avaliação da AWS; só um admin da conta AWS/Databricks pode corrigir a bucket policy, e não há esse acesso disponível agora. Workaround adotado: `mlflow.sklearn.log_model(...)` sem `registered_model_name` (loga só na run, sem tentar criar uma versão no Model Registry — esse caminho de escrita nunca é exercitado). "Champion" passa a ser uma tag (`model_stage=champion`) na run do MLflow, localizada via `MlflowClient.search_runs(filter_string="tags.model_stage = 'champion'")`, e a baseline de features referencia `champion_run_id` em vez de `model_name`/`model_version`. Quando o acesso de admin existir, migrar de volta pra alias no UC Model Registry é só trocar essas poucas funções — o restante do pipeline (gate, drift, retrain) não muda.

`src/monitoring/model_drift.py` (job `model_drift_monitor`, a cada 6h) recalcula as mesmas três features de `streaming_score.py::compute_fraud_score` sobre uma janela recente de `gold.claims` e compara contra a baseline (`monitoring._feature_baseline`, escrita por `train_model.py` só quando um modelo é promovido) via um z-score simples (sem scipy, mesmo estilo leve de estatística já usado no projeto). Resultado vai para `monitoring._model_drift_results`; drift nunca faz o job falhar, só fica registrado na tabela.

O retrain automático **não** usa `dbutils.jobs.taskValues`/`condition_task` do Databricks Jobs (não verificados sob compute serverless nesta sessão, dado quantas outras APIs precisaram de workaround). Em vez disso, `train_model.py` lê `monitoring._model_drift_results` ele mesmo a cada execução (`0 30 */6 * * ?`, 30min depois do `model_drift_monitor`) e só treina de verdade se: é a primeira execução (tabelas de baseline/drift ainda não existem), a última checagem sinalizou drift, ou `--force` foi passado.

**Importante:** isso não mudava o scoring ao vivo quando escrito — `fraud_score_stream` usava só a heurística de `streaming_score.py`. Ver seção seguinte para o shadow scoring adicionado depois.

## Shadow scoring do modelo campeão + monitor de latência automatizado (2026-07-09)

`fraud_score_stream` (`src/fraud/streaming_gold.py::load_champion_model_udf`) agora carrega o modelo mlflow marcado `model_stage=champion` via `mlflow.pyfunc.spark_udf` e escreve o resultado numa coluna extra `model_fraud_score` em `gold.claims` — **shadow mode**, não cutover: `apply_auto_approval`/`fraud_flag` continuam decidindo 100% pela heurística de `streaming_score.py`, o score do modelo é só observabilidade para comparar os dois antes de uma eventual troca futura. Motivo de ser shadow e não cutover: o modelo hoje treina sobre rótulo fraco derivado da própria heurística (f1=1.0, vazamento de rótulo documentado acima) — trocar a decisão real por esse modelo seria substituir uma heurística por uma cópia estatística da mesma heurística, não uma melhoria.

`mlflow.pyfunc.spark_udf` é uma API nova nesta sessão (nunca exercitada, ao contrário de `dbutils.secrets`/tracking URI já provados sob Spark Connect) — `load_champion_model_udf` está envolvida em `try/except` amplo: se não existir champion ainda (bootstrap) ou se essa API não funcionar neste compute serverless, retorna `None` e o job segue rodando só com a heurística, nunca quebra o scoring real. O modelo é carregado **uma vez por run do job** (não por micro-batch); como `fraud_score_stream` é um continuous job, um champion novo (promovido a cada ciclo de `fraud_model_training`) só é pego no próximo restart do job — não há hot-reload dentro de um run em andamento.

`scripts/measure_pipeline_latency.py` (que media AT-001/AT-003 mas nunca tinha rodado de verdade — inclusive tinha o mesmo bug de `.rdd.isEmpty()` já corrigido em outros 4 lugares nesta sessão, nunca pego porque nunca executou) agora roda como job agendado (`resources/jobs.pipeline_monitoring.yml`, a cada 15 min, alinhado à `--window-minutes` default), persiste cada checagem em `monitoring._pipeline_latency_results` e alerta via o mesmo secret `sla-webhook-url` quando alguma latência sai do SLA — mesmo padrão de `sla_alerts.py`/`model_drift.py`.

## Insurance Regulatory Data Platform — MVP do fluxo regulatório SUSEP (2026-07-09)

Módulo novo, deliberadamente separado do pipeline operacional (Kafka → Bronze → Silver → Gold de sinistros/apólices): modela o fluxo pelo qual seguradoras/bancos **padronizam, reconciliam e preparam** dados de sinistros antes do reporte à SUSEP — não o fluxo de contratação/emissão de apólice em si. `src/regulatory/` é um novo pacote de topo (irmão de `src/fraud/`, `src/streaming/`), já que representa um domínio de negócio distinto com sua própria sequência de estágios (standardize → reconcile → gold export), rodando em batch (`spark.read.table`, sem streaming/checkpoint) a cada 30 min via `resources/jobs.regulatory_pipeline.yml`, depois do job contínuo `regulatory_bronze_ingest`.

**Simulação sintética honesta:** não existe dataset público na granularidade "cru, pré-padronização, por seguradora" — a própria SUSEP só publica o resultado já padronizado (`S_AUTO`, já usado como fonte real em `susep_loader.py`). `src/ingestion/producer/datasets/regulatory_feeds.py` gera 3 fontes fictícias (`insurer_a`/`insurer_b`/`insurer_c`, layouts deliberadamente heterogêneos: snake_case PT-BR, UPPER_SNAKE, camelCase) a partir do **mesmo subconjunto de sinistros reais da SUSEP** (mesmo `dataset_path`/`sample_rows`/`random_seed=42` do source `susep` → `df.sample()` determinístico garante que as 3 fontes derivem do mesmo sample sem nenhuma coordenação extra). A premissa "múltiplas seguradoras reportando o mesmo sinistro" é uma simplificação intencional só para exercitar a lógica de reconciliação — na vida real, um sinistro de auto tem uma única seguradora de registro; não é uma alegação sobre sobreposição real entre seguradoras.

**Bronze com `required_fields` parametrizável:** `bronze_ingest.py::run_bronze_ingest`/`_is_malformed` ganharam um parâmetro opcional `required_fields: list[str] | None` (default preserva o comportamento antigo hardcoded pros 3 tópicos operacionais). O tópico único `regulatory-claim-report` (compartilhado pelas 3 fontes fictícias) passa `--required-fields external_reference_id` — Bronze só quarentena um registro se o JSON for inválido ou a própria chave de junção estiver ausente; qualquer outro defeito de negócio (data/valor ausente, código inválido) passa de propósito, porque capturar isso é o trabalho da Silver (`check_allowed_values`, `check_not_null` em `standardize.py`), não do Bronze.

**Reconciliação (`src/regulatory/reconcile.py`):** por `external_reference_id`, `amount_mismatch` quando o spread entre valores reportados excede `max(min_amount * 2%, R$ 5,00)`; `missing_in_source` quando uma fonte "ativa" (com relatos em algum lugar do batch) não aparece pra aquele sinistro. Resolução de conflito = **mediana** dos valores reportados — decisão de MVP documentada (robusta a uma única fonte discrepante, determinística, sem viés de "escolher uma seguradora"), não o que um processo regulatório real faria.

**Allow-list de região sem tabela oficial:** nenhuma tabela de códigos de região SUSEP foi confirmada neste repo (mesma limitação já documentada acima pro `susep_loader.py`). `standardize.py::_region_allowlist` usa um proxy data-driven: um valor de região só entra no allow-list se pelo menos 2 das (até 3) fontes concordarem nele pro mesmo `external_reference_id` — nenhum valor é hardcoded, incluindo o sentinel `"ZZ"` que `regulatory_feeds.py` usa pra simular código inválido.

**Sem masking/RLS novo:** este módulo não introduz nenhum campo com forma de identificador pessoal (CPF/CNPJ) — `external_reference_id` é um hash sintético, `source_system` é um rótulo de seguradora/banco, não dado pessoal. Consistente com a limitação já documentada de que nem SUSEP nem ANS expõem `customer_id` reutilizável.

**Fora de escopo neste slice:** exportação de arquivo plano (CSV) para um UC Volume — as colunas/tipos/ordem de `gold.regulatory_susep_claims` (`COD_APO`, `D_OCORR`, `INDENIZ`, `REGIAO`, `CAUSA`, literalmente os 5 campos já confirmados no layout oficial via `susep_loader.py`) já bastam para verificar fidelidade estrutural contra o SUSEP real, sem precisar de um novo UC Volume/grant Terraform. `gold.regulatory_dq_summary` é cumulativo (soma tudo desde sempre, sem conceito de `run_id`) — sumarização "por execução" de verdade é fast-follow.

**`None` virando `NaN` na serialização JSON (bug real, confirmado em produção, 2026-07-13):** `regulatory_feeds.py` usa `None` pra simular campo ausente (`MISSING_REQUIRED_FIELD_RATE`) em `CLAIM_AMOUNT` (insurer_b) e `amountCents` (insurer_c) — mas como a lista de dicts vira uma coluna pandas mista de números e `None`, `pd.DataFrame(rows)` coage a coluna inteira pra `float64`, convertendo `None` em `NaN` silenciosamente (confirmado localmente: 3-5 linhas por fonte, num sample de 300, viram `NaN`). `json.dumps` de um `float('nan')` produz o token JSON inválido `NaN` (não `null`), que o `from_json`/cast do lado Spark não trata como ausente — vira a string literal `"NaN"`, e o cast pra `DecimalType` em `standardize.py` quebra com `CAST_INVALID_INPUT`. Corrigido na origem mais robusta: `kafka_publisher.py::_sanitize_for_json` converte qualquer `float` NaN em `None` logo antes de serializar, cobrindo qualquer fonte (atual ou futura) que caia na mesma armadilha do pandas, não só `regulatory_feeds.py`.

**`standardize.py` usa `try_cast`, não `cast`, pros campos de valor/data (mesmo dia, 2026-07-13):** o fix acima impede `"NaN"` novo de entrar no Bronze, mas não limpa linhas **já gravadas** antes do fix — e este job sempre relê `bronze.regulatory_claims_raw` inteira a cada execução (sem streaming/checkpoint incremental), então uma linha poluída antiga derrubaria o job pra sempre. Trocado `cast`/`.cast(...)` por `try_cast(...)` (via `expr`) em `valor_indenizacao`/`CLAIM_AMOUNT`/`amountCents`/`occurrenceDate` — entrada malformada agora vira `NULL` (capturado por `check_not_null` como falha de DQ) em vez de derrubar o job inteiro. **Detalhe confirmado em CI real:** `try_cast(... AS DOUBLE)` não vira `NULL` pra `"NaN"` (DOUBLE aceita NaN como valor IEEE-754 legítimo, diferente de DECIMAL) — o `.cast("long")` seguinte então zera silenciosamente (`NaN.cast("long") == 0` em Java/Scala), então `occurrenceDate`/`amountCents` do insurer_c usam `nanvl(..., lit(None))` explicitamente antes do cast pra long.

**`reconcile.py::reconcile_claims` agora tolera `amount = NULL`:** consequência direta do `try_cast` acima — `amount` pode legitimamente ser `None` agora, e `float(None)` derrubava `reconcile_claims` com `TypeError` (confirmado em produção, mesmo dia). Uma fonte com `amount` ausente ainda conta pra `missing_in_source`, mas é excluída da comparação de valor (`amount_mismatch`); se **todas** as fontes de um sinistro tiverem `amount` nulo, só `missing_in_source` é avaliado (não há valor nenhum pra comparar).

**`fraud_score_stream` com `ignoreChanges` na leitura de `silver.claims` (2026-07-13):** `silver_transform.py` escreve em `silver.claims` via **MERGE** (`whenMatchedUpdateAll`/`whenNotMatchedInsertAll`), não só append. O Delta Streaming Source, por padrão, não processa como stream uma tabela de origem que sofre updates/merges — sem uma opção explícita, o comportamento observado em produção foi o job rodar "Bem-sucedida" processando **0 linhas**, mesmo com 35133 já escritas em `silver.claims` (`bronze`/`silver` sincronizados, `gold.claims` vazia). `streaming_gold.py::run_fraud_scoring_stream` agora lê com `.option("ignoreChanges", "true")` — arquivos reescritos pelo MERGE são reprocessados como linhas "novas", mas isso é seguro aqui: o `merge_into_delta` que escreve em `gold.claims` já deduplica por `claim_id`, então reprocessamento é idempotente.

**`fraud_score_stream` virou `schedule`, não resolveu com `ignoreChanges` sozinho (mesmo dia):** depois do fix acima, `gold.claims` continuava vazia. Inspecionando os arquivos `offsets/0,1,2` do checkpoint (`/Volumes/insurance_prod/gold/_checkpoints/claims`) diretamente: `reservoirVersion` avançava exatamente **+1 por execução** (1→2→3 em ~5h), enquanto `silver.claims` (escrito por outro job contínuo) já estava muito além disso — ou seja, a query processava só 1 versão do Delta por run antes de reiniciar, nunca alcançando o estado atual. Causa mais provável: **contenção de capacidade** — `bronze_ingest`, `silver_transform` e `fraud_score_stream`, todos `continuous`, disputando a mesma cota apertada de serverless deste workspace trial (mesmo tema de `RESOURCE_EXHAUSTED` documentado nas seções anteriores), com o compute deste job sendo reciclado antes de esvaziar o backlog. Convertido de `continuous` pra `schedule` (`*/5min`) em `resources/jobs.fraud_score.yml` — cada execução agendada ganha uma janela de compute dedicada pra processar tudo de uma vez, sem competir continuamente. Troca o alvo de latência original do DEFINE (score de fraude < 1 min) por um SLA mais realista (~5 min), que é o que a cota deste workspace de fato sustenta.

**Bug real no card "Gold (SUSEP export)" do app (mesmo dia):** `pages/status.py::STATUS_CHECKS` usava `D_OCORR` de `gold.regulatory_susep_claims` como coluna de frescor — mas `D_OCORR` é a **data do sinistro** (string `yyyyMMdd`, um dos 5 campos literais do layout oficial SUSEP), não um timestamp de processamento, e nunca deveria ter sido usada pra medir frescor do pipeline. `MAX(D_OCORR)` retorna uma string, e `.replace(tzinfo=UTC)` (que espera um `datetime`) quebrava com erro, confirmado em produção (card mostrava "✖ Erro"). Trocado pra `gold.regulatory_dq_summary._generated_at` — gerado pelo mesmo job (`gold_susep_export.py::run_gold_export_job`), timestamp de verdade.

## Insurance Visualization Layer — AI/BI Dashboard, Databricks App e Genie, só em prod (2026-07-09)

Camada de apresentação sobre o Gold já existente (operacional + regulatório) — nenhuma tabela nova, nenhum pipeline novo. Ver `.claude/sdd/features/DESIGN_INSURANCE_VISUALIZATION_LAYER.md` para o design completo; aqui ficam as decisões que sobrevivem além do documento de design.

**Escopo só em prod via `targets.prod.resources`:** `resources/visualization.yml` declara o recurso `apps` dentro de `targets: prod: resources:`, não no nível de topo (que é global a todos os targets, hoje só `var.catalog` varia por target). É o mecanismo suportado pelo Databricks Asset Bundles pra um recurso existir só num target específico — confirmado na documentação oficial antes de implementar. `databricks bundle validate -t dev` não deve mostrar nada deste módulo.

**Novo SQL Warehouse serverless, também só em prod:** `terraform/warehouse.tf` cria um `databricks_sql_endpoint` serverless via `count = var.environment == "prod" ? 1 : 0` — o mesmo mecanismo (`count` condicionado a `var.environment`) já usado em `secrets.tf`, só que invertido (lá é `== "dev"`, aqui é `== "prod"`). dev e prod compartilham o mesmo workspace Databricks, então não há risco de "already exists" entre os dois applies.

**Databricks App (Streamlit) sem grant Terraform explícito (revisado pós-deploy real, 2026-07-09):** a tentativa original previa um bootstrap em 2 passos — conceder `SELECT` em `gold`/`monitoring` pro service principal do app via um novo `terraform apply` depois de capturar seu `client_id`. Três formas diferentes de modelar esse grant (`databricks_grants` com `dynamic "grant"`, `databricks_grant` separado, `databricks_grant` com `depends_on`) falharam em applies reais contra o schema `gold` com variações de `"cannot create/update grant: permissions ... are [...], but have to be [...]"` — um erro de precondição/CAS do provider Databricks ao mexer num `databricks_grants` que já tem grants de outro principal no mesmo securable (schema `gold` já tinha `gold_read_only_analysts` pro `catalog_owner`). Testado o app de verdade contra `gold.regulatory_dq_summary`/`monitoring._regulatory_reconciliation_results`: os erros observados foram `TABLE_OR_VIEW_NOT_FOUND` (tabela não existe ainda), nunca `PERMISSION_DENIED` — ou seja, o service principal do app já consegue ler esses schemas por algum mecanismo fora deste Terraform (hipótese mais provável: o próprio Databricks Apps concede acesso básico de Unity Catalog ao service principal no catálogo/schema onde o app roda). Os resources de grant e a variável `app_service_principal_id` foram removidos — reavaliar só se um teste real mostrar `PERMISSION_DENIED` no futuro.

**Dashboard e Genie são prototipados na UI, não escritos à mão:** o JSON de um dashboard Lakeview (`.lvdash.json`) e a definição de um Genie Space são artefatos que a própria Databricks gera — não são hand-authored. Fluxo: (1) criar/ajustar o dashboard/Genie space interativamente na UI do workspace prod (usando o warehouse criado acima); (2) `databricks bundle generate dashboard --existing-id <id>` / `databricks bundle generate genie-space --existing-id <id>` traz a definição pro bundle, versionada em `resources/visualization.yml` + `resources/dashboards/*.lvdash.json`. Até esse passo rodar pela primeira vez, `resources/visualization.yml` só tem o recurso `apps` — os blocos `dashboards:`/`genie_spaces:` entram depois, no mesmo arquivo.

**Genie é a peça de maior risco, isolada do restante:** suporte a Genie Space via DAB exige Databricks CLI ≥1.3.0 e o "direct deployment engine" (não há resource Terraform `databricks_genie_space`) — é o mais novo dos três componentes. Fica marcado como SHOULD (não MUST) no DEFINE justamente por isso: se a rota declarativa não funcionar de primeira, o Genie Space pode ser criado manualmente pela UI sem bloquear o Dashboard/App, que já são MUST e usam tecnologia mais madura.

**`queries.py` separa construção de SQL (testável) de execução (não testável localmente):** `apps/insurance_platform_app/queries.py` expõe funções puras `build_*_query(...) -> SqlQuery` cobertas por `tests/unit/test_visualization_queries.py`; a única função que toca o Databricks SQL Connector de verdade (`get_connection`/`run_query`) segue a mesma limitação já documentada neste repo (nenhum warehouse real existe neste sandbox) — só validável manualmente pós-deploy.

**Limitações herdadas, não escondidas:** a tela "Consultar Cliente" existia no menu original (Success Criteria do DEFINE exigia as 8 telas navegáveis), mas `customer_id` é sempre nulo em `gold.claims` (limitação já documentada acima) — foi removida no pivô abaixo, já que não sustentava uma tela de verdade.

## Pivô do Databricks App para o core regulatório (2026-07-10)

O MVP original do app (Consultar Apólice/Cliente/Sinistro, Pesquisar Banco/Seguradora) era genérico demais — não refletia o objetivo real da plataforma, que é o fluxo regulatório SUSEP, não um CRUD de consulta. Reorientado por instrução direta do usuário: o app agora existe pra responder 3 perguntas de negócio — quais contratos estão aderentes/fora das regras SUSEP, qual a probabilidade de fraude, e a plataforma está saudável (pipeline, DataOps/MLOps, custo, performance Kafka/Databricks)?

**Conformidade por contrato, não só resumo agregado:** `gold_susep_export.py::build_gold_susep_claims` ganhou 2 colunas novas — `susep_compliant` (bool) e `compliance_issues` (string, `concat_ws`-separada por vírgula) — calculadas por `external_reference_id`: campo obrigatório ausente (`policy_id_ausente`/`data_ausente`/`valor_ausente`), código de causa fora da Tabela V oficial (`causa_invalida`, reaproveitando `CAUSE_LABELS` de `susep_loader.py` — não duplicado), ou discrepância de reconciliação (`reconciliacao:<tipo>`, join com `monitoring._regulatory_reconciliation_results` por `external_reference_id`). Região **não** entra nessa checagem: o allow-list de `REGIAO` em `standardize.py` é um proxy data-driven (consenso entre fontes) sem tabela oficial confirmada — usá-lo pra marcar "fora das regras" produziria falsos positivos. `build_dq_summary`/`gold.regulatory_dq_summary` continuam existindo para o resumo agregado por execução; as 2 colunas novas são o dado por-contrato que faltava para o app.

**Páginas do app trocadas por completo** (ver README.md pra tabela atual): removidas `consultar_apolice.py`/`consultar_cliente.py`/`consultar_sinistro.py`/`pesquisar_banco.py`/`pesquisar_seguradora.py`/`dashboard_link.py`/`auditoria.py`; `status.py` (renomeada "Visão Geral") ganhou freshness dos schemas regulatórios e resumo de conformidade; novas páginas `susep_compliance.py`, `fraud_probability.py`, `pipeline_monitoring.py`, `dataops_mlops_sentinel.py`, `performance.py`, `custos.py`; `lineage.py` mantida sem mudança.

**Custos via `system.billing.usage`:** system table nativa do Unity Catalog (não uma tabela deste pipeline) — precisa ser habilitada pelo account admin do workspace antes de existir. A página `custos.py` trata a ausência dela como um `try/except` que gera um aviso explicativo, não um erro — mesmo padrão de degradação graciosa já usado no resto do app (`TABLE_OR_VIEW_NOT_FOUND` em `status.py`). Não confirmado neste workspace trial se está habilitada.

**Performance Kafka/Databricks é o que os próprios jobs Spark medem, não a API do Confluent Cloud:** `performance.py` mostra taxa de quarentena por tópico (Bronze) e throughput vs. `replay.events_per_minute` (via `monitoring._pipeline_latency_results`) — não há integração com métricas nativas do Kafka (consumer lag, partition skew), documentado como fora de escopo/roadmap na própria página.

## Migração para workspace novo + `catalog_owner` dividido em duas variáveis (2026-07-13)

Migração para um novo workspace Databricks (trial): `databricks.yml` (`root_path` de staging/prod) e os secrets do GitHub Actions foram apontados pro workspace novo. Dois ajustes de Terraform surgiram de erros reais de deploy, e um terceiro de uma regressão introduzida pelo primeiro:

**`terraform/warehouse.tf` não cria mais um SQL Warehouse novo:** contas trial não permitem provisionar um segundo warehouse serverless via API — `databricks_sql_endpoint` ficou preso em "Still creating..." por 15+ minutos num deploy real (apply cancelado). O `output "sql_warehouse_id"` agora aponta pro warehouse serverless que já vem pré-provisionado no workspace ("Serverless Starter Warehouse"), sem `resource` novo.

**`catalog_owner` dividido em `catalog_owner` + `secrets_acl_principal`:** o workspace novo não tem o grupo `account users` atribuído a ele ainda (Account Console > Workspaces > Permissions, passo de admin de conta não feito) — isso quebrou `databricks_secret_acl` com `"User or Group account users does not exist"` (ACL de secret é um recurso de **workspace**, exige o principal já reconhecido lá). A correção original trocou `catalog_owner` inteiro pelo e-mail do usuário — resolveu o secret ACL, mas **quebrou o acesso do service principal do Databricks App**, que antes herdava `USE CATALOG`/`USE SCHEMA` implicitamente via `account users` (um principal de conta/metastore, que funciona em grants de Unity Catalog *sem* precisar estar atribuído ao workspace). Confirmado em produção: `INSUFFICIENT_PERMISSIONS: User does not have USE CATALOG on Catalog 'insurance_prod'` nas páginas de fraude e qualidade de dados do app. Fix definitivo: `catalog_owner` voltou a ser `"account users"` (usado em `databricks_catalog.owner` e todos os `databricks_grants` de schema/volume), e uma variável nova `secrets_acl_principal` (default = e-mail do usuário) ficou isolada só para `databricks_secret_acl`, que é o único recurso deste projeto que realmente precisa de um principal *workspace-scoped*.

**`system.billing.usage` (página Custos do app) continua não habilitado neste workspace novo** — mesma limitação já documentada acima pro workspace anterior, não uma regressão desta migração; requer account admin habilitar system tables e conceder `USE SCHEMA` em `system.billing`, fora do escopo deste Terraform.

**`monitoring` nunca teve `databricks_grants` próprio — lacuna antiga, só exposta agora:** diferente de `bronze`/`silver`/`gold`/`models`, o schema `monitoring` nunca teve um `databricks_grants` dedicado desde que foi criado. Passou despercebido enquanto o workspace anterior dava acesso implícito ao service principal do app; confirmado num deploy real neste workspace novo — `INSUFFICIENT_PERMISSIONS: USE SCHEMA on Schema 'insurance_prod.monitoring'` nas páginas de latência de pipeline e DataOps/MLOps Sentinela. Corrigido com `databricks_grants.monitoring_read_write` (mesmo padrão de privilégios de `bronze_read_write`/`silver_read_write`), sem o mesmo risco de conflito documentado no bloco "Databricks App sem grant Terraform explícito" acima — aquele conflito era sobre *adicionar* um grant pro service principal específico do app num schema que já tinha outro grant gerenciado; aqui é um `databricks_grants` novo, sem nada pré-existente para conflitar.

## Open Insurance — consentimento do cliente para compartilhamento de dados (2026-07-13)

Módulo novo, simulando o conceito de Open Insurance/Open Finance (regulação SUSEP): o cliente consente (ou revoga) o compartilhamento dos próprios dados com outras instituições. `src/open_insurance/` é um novo pacote de topo (irmão de `src/regulatory/`), deliberadamente separado do reporte regulatório obrigatório — este módulo é sobre consentimento voluntário do cliente, não obrigação legal de reporte. Ver `.claude/sdd/features/DESIGN_OPEN_INSURANCE.md` para o design completo (7 decisões documentadas); aqui ficam as que sobrevivem além do documento.

**Chave é `policy_id`, não `customer_id` (descoberto no build, não previsto no design original):** a limitação já documentada acima ("`customer_id` não disponível em nenhuma das duas fontes") se aplica também aqui — `gold.claims.customer_id` é sempre `NULL`. O módulo inteiro (evento de consentimento, perfil sintético, view compartilhável) usa `policy_id` como chave, não `customer_id`. `CONSENT_EVENT_SCHEMA` (`src/common/schemas.py`) reflete isso desde o início.

**Job agendado, não contínuo — lição direta do deploy no workspace novo:** `resources/jobs.open_insurance.yml` usa `schedule:` (ingestão a cada 15min, pipeline SCD2/perfil/view a cada 30min), nunca `continuous:`. O workspace trial atual já bateu em `RESOURCE_EXHAUSTED` na cota de serverless compute com os jobs contínuos existentes (`bronze_ingest`, `silver_transform`, `fraud_score_stream`, `regulatory_bronze_ingest`) rodando em paralelo em dev+prod — mais um cluster 24/7 não era uma opção. `src/streaming/bronze_ingest.py` é reaproveitado sem nenhuma mudança de código para a etapa de ingestão: ele já usa `trigger(availableNow=True)` internamente, então a diferença entre "job contínuo" e "job agendado" é inteiramente configuração do bundle (`continuous:` vs `schedule:`), não do pipeline PySpark em si.

**SCD2 é um padrão novo neste projeto:** nenhuma tabela usava `is_current`/`valid_from`/`valid_to` antes. `src/common/scd2_merge.py::merge_scd2_into_delta` implementa o padrão de 2 passos (fecha a versão vigente quando um evento mais novo chega para a mesma chave + insere a nova versão vigente), com proteção explícita contra eventos fora de ordem (um evento mais antigo que a versão já vigente não fecha nem duplica nada). `silver.consent_current` é a única tabela do projeto com histórico completo — é a trilha de auditoria LGPD do módulo.

**Perfil sintético sem nenhuma dependência nova:** `src/open_insurance/profile_generator.py::generate_profile` deriva um seed determinístico via hash SHA-256 do `policy_id` (mesmo espírito de `regulatory_feeds.py`), sorteando nome/idade/score de risco fictícios de listas fixas — sem adicionar `Faker` ou qualquer biblioteca nova ao `requirements.txt`.

**`gold.open_insurance_shareable` usa LEFT JOIN com `gold.claims`:** cliente com consentimento ativo mas sem nenhum sinistro ainda aparece na view (colunas de claims nulas) — um histórico limpo é, no mundo real de Open Insurance, um dado tão relevante quanto um histórico com sinistros. As views (`gold.customer_consent` e `gold.open_insurance_shareable`) só são criadas se as tabelas base já existirem (`spark.catalog.tableExists`) — nas primeiras execuções, antes do primeiro evento de consentimento ou do primeiro run do gerador de perfil, a criação é adiada silenciosamente para a próxima execução agendada, mesmo padrão de degradação graciosa já usado no resto da plataforma.

**Zero mudança em Terraform:** as tabelas novas (`bronze.consent_events`, `silver.consent_current`, `gold.customer_consent`, `gold.open_insurance_profile`, `gold.open_insurance_shareable`) ficam nos schemas de camada já existentes (mesmo padrão do módulo regulatório: domínio namespaced pelo nome da tabela, não por schema dedicado) — os grants já concedidos a `var.catalog_owner` em `terraform/unity_catalog.tf` já cobrem tudo. O checkpoint de streaming reaproveita o Volume `bronze._checkpoints` já provisionado.

**Distinto do stub `open_insurance` já existente em `config.yaml`:** a fonte simulada deste módulo se chama `consent_simulator` (tópico `consent-updated`) — nomes escolhidos de propósito para não colidir com a entrada `open_insurance` (`enabled: false`) já reservada para uma futura integração real com o sandbox Open Insurance Brasil (ver "Limitações conhecidas / roadmap" abaixo). São duas coisas diferentes: esta feature simula o consentimento e a distribuição via Delta Sharing; o stub existente é sobre ingerir dados reais de um sandbox externo, quando/se houver credenciamento.

**Delta Sharing real fica fora deste slice:** `gold.open_insurance_shareable` foi desenhada para ser a fonte de um Delta Share (`insurer_a`/`insurer_b`/`insurer_c` como recipients simulados), mas o Share em si (`databricks_share`/`databricks_recipient` via Terraform) não foi provisionado — essas instituições fictícias não são contas Databricks reais separadas, então não haveria um lado consumidor de verdade para validar. Quando uma integração real existir, o Share deve apontar só para a view, nunca para as tabelas base (`gold.claims`, `gold.open_insurance_profile` diretamente).

## Limitações conhecidas / roadmap

- Open Insurance Brasil como fonte de dados: pendente de credenciamento no diretório sandbox.
- Chaves de agrupamento exatas do CSV real da SUSEP (região/categoria/modelo) não foram confirmadas no dicionário consultado — `susep_loader.py` usa nomes de coluna candidatos (`REGIAO`, `UF`, etc.) com fallback para `"UNKNOWN"`; ajustar assim que o CSV real for inspecionado.
- Volume real dos datasets SUSEP/ANS ainda não validado (ver Assumptions A-001 a A-004 do DEFINE) — recomenda-se um spike antes de dimensionar compute para produção real.
- Modelo de fraude usa rótulo fraco (heurística) para bootstrap inicial; evoluir para rótulos supervisionados reais é um próximo passo natural.
- `customer_id` não disponível em nenhuma das duas fontes — qualquer caso de uso que dependa de histórico por cliente (ex: frequência de sinistros do `streaming_score.py`) fica limitado até uma fonte complementar ser identificada.
- Insurance Regulatory Data Platform: exportação de arquivo plano (CSV) pra UC Volume ainda não implementada; `gold.regulatory_dq_summary` é cumulativo, sem `run_id` por execução.
- Insurance Visualization Layer: dashboard e Genie space ainda não foram prototipados na UI nem capturados via `bundle generate` (só o Databricks App está com código pronto); página "Lineage" usa um formato de URL do Catalog Explorer não confirmado contra o workspace de produção; `system.billing.usage` (página Custos) não confirmado como habilitado neste workspace trial; discrepância de reconciliação por banco/seguradora individual (não só volume total) exigiria explodir `sources_reporting` em `monitoring._regulatory_reconciliation_results`, deixado como melhoria futura.
- RLS por região (`gold_aggregate.py::apply_governance`, ver seção acima) depende de grupos `insurance-region-<uf>` que ainda não existem na conta — até serem criados (fora do Terraform deste projeto, é administração de conta), só `insurance-data-team` enxerga `gold.claims`.
- Alertas de SLA/drift (`sla_alerts.py`, `model_drift.py`) só enviam via webhook se o secret opcional `SLA_WEBHOOK_URL` estiver configurado no GitHub (ver README, seção "Secrets necessários") — sem ele, caem no fallback de log, por design.
- Open Insurance: Delta Share real (`databricks_share`/`databricks_recipient`) não provisionado — só a view `gold.open_insurance_shareable` está pronta, documentada como a fonte que o Share usaria. Sendo 2 jobs agendados novos (mesmo em cota apertada de serverless), monitorar por `RESOURCE_EXHAUSTED` após o primeiro deploy real e, se necessário, espaçar ainda mais a cadência ou pausar em `dev`.
