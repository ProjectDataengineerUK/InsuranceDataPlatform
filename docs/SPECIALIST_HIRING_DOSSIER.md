# Insurance Data Platform — Dossiê de Especialista (Spark · Kafka · Databricks · Streaming)

> Documento de apoio a processo seletivo para **Engenheiro de Dados Especialista Sênior**, mapeando o que este repositório demonstra na prática contra 4 competências: Spark/PySpark, Kafka, Databricks e Databricks Streaming — e listando os gargalos técnicos reais enfrentados e resolvidos neste projeto.
>
> Versão web navegável (mesmo conteúdo, com diagrama de arquitetura e evidências em cards): gerada via `/artifact` — pedir a quem conduziu a sessão de desenvolvimento caso precise da versão HTML.

---

## Como usar este dossiê no processo (leia isto primeiro)

1. **Não peça pra recitar a arquitetura.** Peça pro candidato escolher 2 incidentes da seção [Gargalos reais & correção de especialista](#gargalos-reais--correção-de-especialista) e explicar como chegaria à mesma causa raiz **do zero, sem ver a resposta**.
2. **Cruze com o [Roteiro de entrevista](#roteiro-de-entrevista-baseado-nos-incidentes).** As perguntas já vêm calibradas pelo sinal esperado de um especialista — o gap entre a resposta do candidato e o sinal descrito é o dado mais honesto que você vai ter.
3. **Peça o "porquê", não o "o quê".** Qualquer candidato sênior sabe usar `cast("double")`. O diferencial é saber explicar **por que** `from_json` produz notação científica em primeiro lugar (ver INC-03) — a mesma lógica se aplica a cada incidente abaixo: a correção é trivial de copiar, o raciocínio que leva até ela não é.

---

## O diferencial

Qualquer engenheiro monta um pipeline Kafka → Spark → Databricks que funciona em condições ideais. O que separa um **especialista** é o que acontece quando ele **não** funciona: saber por que `.rdd.isEmpty()` quebra só sob Spark Connect, por que um `databricks_grants` aceita e depois reverte silenciosamente uma permissão concedida por outro resource, por que um `cast("long")` rejeita um valor que `cast("double")` aceita sem reclamar.

Este projeto documenta **9 incidentes reais** desse tipo — não hipóteses de entrevista, comportamento observado em execuções de produção, cada um com causa raiz identificada e corrigida.

---

## Arquitetura (resumo)

Terraform possui catálogo, schemas, grants, secrets e volumes; os jobs Spark possuem exclusivamente as tabelas Delta (via `saveAsTable`/`MERGE`). Fronteira deliberada — sem ela, dois sistemas de IaC disputam o mesmo objeto (causa real do INC-07).

```
Kafka (Confluent Cloud)          Bronze                          Silver                       Gold
├─ claim-opened            ──►   bronze.claims/policies/    ──►  silver.claims/policies  ──►  gold.claims (fraude +
├─ policy-created                customers                       silver.regulatory_claims      auto-aprovação)
├─ customer-updated               + bronze.regulatory_               (dedup + parsing            gold.regulatory_susep_claims
└─ regulatory-claim-report          claims_raw                        por fonte)                 gold.regulatory_dq_summary
                                  + _quarantine por tópico
                                    (schema inválido)

Monitoring: _dq_results · _model_drift_results · _pipeline_latency_results · _regulatory_reconciliation_results

Consumo: Databricks App (Streamlit, SQL Warehouse serverless, service principal próprio)
         + AI/BI Dashboard e Genie Space planejados via `databricks bundle generate`
```

---

## Competências demonstradas

### Spark & PySpark — nível especialista

Structured Streaming com `foreachBatch`, quarentena por schema, features de janela, escrita idempotente em Delta e a diferença de comportamento entre Spark clássico e **Spark Connect** sob compute serverless — a fonte da maioria dos bugs mais sutis deste projeto.

| Evidência | O que prova |
|---|---|
| `src/streaming/bronze_ingest.py` | Ingestão genérica orientada a schema (`SCHEMA_REGISTRY`), quarentena automática, `required_fields` variável — um único código atende 4 tópicos com contratos de dado diferentes |
| `src/common/delta_write.py::append_or_create` | Escrita Delta idempotente sob concorrência real (`AnalysisException` "already exists" → fallback `insertInto`), reusada em 6+ pontos do pipeline |
| `src/fraud/streaming_score.py` | Feature engineering com window functions em streaming: `claims_last_24h` via `Window.partitionBy("policy_id")`, `feature_amount_outlier` via z-score por região com piso mínimo de desvio padrão |
| `src/regulatory/standardize.py` | Ingestão de schema heterogêneo entre 3 fontes (PT-BR snake_case, UPPER_SNAKE, camelCase) num único tópico via superset schema + `from_json` tolerante |

```python
# serverless (Spark Connect) não aceita .rdd.isEmpty() nem
# .trigger(processingTime=...) — .isEmpty() puro + availableNow,
# restart via continuous job supre o streaming quase-contínuo.
if not valid_df.isEmpty():
    valid_df.write.format("delta").mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("_ingested_date") \
        .saveAsTable(bronze_table)
```

**Além do básico:** conhecimento fino da diferença semântica entre Spark clássico e Spark Connect; escrita concorrente em Delta tratada como problema de design, não de sorte; schema evolution real via superset tolerante; depuração de erros de cast até a causa binária (representação em notação científica), não tentativa e erro.

### Kafka — nível especialista

Produção via `confluent_kafka` contra Confluent Cloud, provisionamento idempotente de tópico, estratégia de chave por entidade, quarentena/DLQ e desligamento gracioso sob execução agendada.

| Evidência | O que prova |
|---|---|
| `src/ingestion/producer/kafka_publisher.py` | Diagnóstico de travamento de protocolo (KIP-714) que a maioria atribuiria a "rede instável" |
| `src/ingestion/producer/kafka_publisher.py::ensure_topic_exists` | Provisionamento idempotente via Admin API — não assume auto-criação (que falhou em produção real) |
| `kafka_publisher.py` + `producer.yml` | Chave = `claim_id or policy_id or customer_id or external_reference_id` (ordem por entidade); `max_duration_seconds` com `flush()` antes do timeout do GitHub Actions |
| `src/ingestion/producer/datasets/base_loader.py` | Replay determinístico sob concorrência real — `threading.Lock` por `dest_path` + escrita atômica via `os.replace()` |

```python
Producer({
    "bootstrap.servers": bootstrap_servers,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": api_key,
    "sasl.password": api_secret,
    # KIP-714: GetTelemetrySubscriptions trava contra alguns
    # clusters Confluent Cloud e derruba a conexão após ~6min.
    "enable.metrics.push": False,
})
```

**Além do básico:** debugging de protocolo, não só de aplicação; nunca assume "o tópico já existe" ou "o disco já está pronto"; particionamento com propósito (ordem por entidade); operação consciente de ambiente efêmero (CI runners).

### Databricks (plataforma & governança) — nível especialista

Unity Catalog, Databricks Asset Bundles, Databricks Apps e MLflow — incluindo dois incidentes de plataforma que só aparecem em uso real de produção.

| Evidência | O que prova |
|---|---|
| `terraform/unity_catalog.tf` | Fronteira de ownership IaC vs. jobs Spark deliberada — nenhuma tabela Delta declarada em Terraform |
| `resources/visualization.yml` + `databricks.yml` | DAB com escopo por target (`targets.prod.resources`) — Dashboard/App/Genie só existem em prod |
| `apps/insurance_platform_app/queries.py` | Databricks Apps autenticado via service principal nativo (`Config().authenticate`) — idêntico local e em produção |
| `src/fraud/train_model.py` | Diagnóstico correto de *explicit deny* de bucket policy S3, não erro de grant do Unity Catalog |

**Além do básico:** sabe onde termina Terraform e começa Spark/DAB, e por que essa fronteira existe; domina deploy por target do DAB; distingue erro de permissão do Unity Catalog de bloqueio de infraestrutura AWS por trás dele; workaround reversível e documentado em vez de pipeline quebrado esperando acesso de terceiros.

### Databricks Streaming — nível especialista

Jobs contínuos vs. agendados, trigger sob compute serverless, shadow scoring dentro do micro-batch e automação de SLA de latência ponta a ponta.

| Evidência | O que prova |
|---|---|
| `resources/jobs.bronze.yml` vs. `resources/jobs.regulatory_pipeline.yml` | Escolha deliberada entre job contínuo (ingestão/score) e agendado (reconciliação batch) |
| `src/streaming/bronze_ingest.py` | `.trigger(availableNow=True)` + restart via continuous job — contorno correto da falta de suporte a `processingTime` sob serverless |
| `src/fraud/streaming_gold.py` | Shadow scoring carregado 1x por execução (não por micro-batch), isolado por `try/except` — nunca decide `auto_approved`/`fraud_flag` |
| `scripts/measure_pipeline_latency.py` | SLA medido, não estimado: Kafka→Bronze < 2 min, Bronze→Gold < 1 min, com alerta automático |

**Além do básico:** escolhe entre streaming contínuo e batch pelo formato do problema, não por hábito; conhece restrições reais de trigger sob serverless; integra ML no streaming com disciplina de blast radius; trata SLA como métrica medida e alertada, não afirmação em slide.

---

## Gargalos reais & correção de especialista

### INC-01 — Spark Connect rejeita `.rdd.isEmpty()` sob compute serverless
- **Sintoma:** job falha/inconsistente ao checar DataFrame vazio antes de escrever.
- **Causa raiz:** Spark Connect não expõe `.rdd` como o Spark clássico — API idêntica na superfície, semântica diferente por baixo.
- **Correção:** `.isEmpty()` nativo do DataFrame em todos os pontos de escrita condicional (4+ arquivos).
- **Diferencial:** um mid-level troca o método quando vê o erro. Um especialista sabe *de antemão* que Spark Connect não é drop-in do Spark clássico — e audita todo o código por esse padrão antes de rodar de novo.

### INC-02 — `CANNOT_DETERMINE_TYPE` ao inferir schema de coluna 100% nula
- **Sintoma:** `createDataFrame` falha quando toda uma coluna é `None` numa lista de 1 linha — apareceu 3 vezes em arquivos diferentes.
- **Causa raiz:** Spark Connect não infere tipo sem nenhum valor não-nulo pra ancorar a inferência.
- **Correção:** schema explícito (`StructType`/DDL string) em vez de depender de inferência.
- **Diferencial:** reconhecer o mesmo bug reaparecendo em contextos diferentes e generalizar a correção, em vez de corrigir caso a caso.

### INC-03 — `CAST_INVALID_INPUT`: notação científica no `cast("long")`
- **Sintoma:** job de standardização regulatória falha em produção: `"1.5895008E12"` não converte pra `BIGINT`.
- **Causa raiz:** `from_json` serializa inteiros grandes em `StringType` usando notação científica — `cast("long")` só aceita dígitos decimais puros.
- **Correção:** `cast("double")` primeiro (aceita notação científica, exato nesta magnitude), depois `cast("long")`.
- **Diferencial:** o teste unitário original não reproduzia o formato real de serialização — passava, mas não provava nada. Corrigido pra fechar o gap entre "teste verde" e "funciona em produção".

### INC-04 — KIP-714 (client telemetry) trava conexão com Confluent Cloud
- **Sintoma:** producer trava por ~6 minutos e derruba a conexão, sem erro claro.
- **Causa raiz:** requisição `GetTelemetrySubscriptions` (KIP-714) não responde contra alguns clusters Confluent Cloud.
- **Correção:** `"enable.metrics.push": False` na config do producer.
- **Diferencial:** isolar um travamento de minutos até uma KIP específica do protocolo exige ler o comportamento do client abaixo da API pública.

### INC-05 — Tópico Kafka novo não é auto-criado pelo cluster
- **Sintoma:** `UnknownTopicOrPartitionException` em produção ao publicar num tópico recém-criado.
- **Causa raiz:** auto-criação de tópico desabilitada no cluster — suposição implícita do código original.
- **Correção:** `AdminClient.create_topics` idempotente antes de cada publicação, tolerante a "already exists".
- **Diferencial:** nunca assumir comportamento default de infraestrutura gerenciada sem verificar — e corrigir de forma idempotente pra suportar múltiplas fontes concorrentes.

### INC-06 — Corrida de download concorrente derruba 2 de 4 threads produtoras
- **Sintoma:** `ChunkedEncodingError`/`EmptyDataError` em 2 das 4 fontes ao rodar o producer real.
- **Causa raiz:** 4 threads baixavam o mesmo CSV de ~60MB simultaneamente — sobrecarga de conexão + escrita não-atômica em disco.
- **Correção:** `threading.Lock` por `dest_path` + escrita via arquivo temporário + `os.replace()` atômico.
- **Diferencial:** diagnosticar como corrida de concorrência a partir do padrão do erro (2 de 4 threads falhando ao mesmo tempo é a assinatura, não coincidência).

### INC-07 — `databricks_grants` reverte silenciosamente um grant de outro resource no mesmo objeto
- **Sintoma:** `terraform apply` falha 3 vezes seguidas com variações de `"permissions ... are [...], but have to be [...]"`.
- **Causa raiz:** `databricks_grants` (plural) é autoritativo por objeto — dois resources no mesmo securable competem, cada um revertendo o grant do outro.
- **Correção:** testado empiricamente contra o app real: o service principal já tinha acesso (erro real era `TABLE_OR_VIEW_NOT_FOUND`, nunca `PERMISSION_DENIED`) — o grant explícito foi removido por ser desnecessário.
- **Diferencial:** o instinto comum após 3 falhas seria insistir numa 4ª variação do mesmo padrão. O movimento de especialista foi parar, testar a hipótese contra o comportamento real, e descobrir que não era necessária — engenharia por evidência, não por tentativa e erro.

### INC-08 — Unity Catalog Model Registry bloqueado por *explicit deny* de bucket policy S3
- **Sintoma:** `MlflowException: AccessDenied` ao registrar versão de modelo no UC Model Registry.
- **Causa raiz:** deny explícito na bucket policy do S3 gerenciado pelo metastore — não um grant de Unity Catalog ausente.
- **Correção:** "campeão" do modelo rastreado via tag MLflow (`model_stage=champion`) em vez de alias no Model Registry — reversível quando o acesso AWS existir.
- **Diferencial:** saber que deny explícito sempre vence allow de IAM na avaliação da AWS — e reconhecer, pela mensagem de erro, que o bloqueio é de infraestrutura de nuvem, não do Unity Catalog em si.

### INC-09 — Bootstrap circular: Databricks App precisa existir antes de receber grant
- **Sintoma:** nenhum jeito de conceder acesso ao service principal do app antes do primeiro deploy — ele não existe até o app ser criado.
- **Causa raiz:** cada Databricks App minta um service principal dedicado só no primeiro `bundle deploy` — ordem de criação circular entre DAB e Terraform.
- **Correção:** mesmo padrão já usado pro catálogo Unity Catalog neste repo (bootstrap em 2 passos) — e no fim, dispensado pelo INC-07.
- **Diferencial:** reconhecer uma classe de problema (dependência circular entre duas ferramentas de deploy) e reaplicar um padrão já validado no mesmo repositório.

---

## Roteiro de entrevista baseado nos incidentes

| Pergunta | Sinal de nível especialista na resposta |
|---|---|
| "Seu job Spark funciona local mas falha só em compute serverless com um erro de atributo ausente. Como você investiga?" (→ INC-01) | Menciona Spark Connect como runtime diferente do Spark clássico, cita ao menos uma diferença concreta de superfície, propõe auditar o código pelo padrão — não só corrigir a linha. |
| "`createDataFrame` falha com erro de inferência de tipo numa lista de uma linha só. Por quê, e como evitar isso sistematicamente?" (→ INC-02) | Explica que coluna 100% nula não tem valor pra ancorar a inferência; propõe schema explícito como prática padrão, não correção pontual. |
| "Um `cast("long")` falha pra um valor que parece válido. O que você suspeita primeiro?" (→ INC-03) | Pergunta sobre a representação em string antes de assumir corrupção; reconhece notação científica como suspeito comum vindo de `from_json`. |
| "Seu producer Kafka trava por minutos sem erro claro. Por onde você começa?" (→ INC-04) | Vai direto pra configuração do client antes de suspeitar de rede/infra; cita telemetria/heartbeats como candidatos plausíveis. |
| "Publicar num tópico novo falha com 'unknown topic'. O que você verifica, e como corrige pra funcionar com múltiplos produtores concorrentes?" (→ INC-05) | Verifica a política de auto-criação antes de assumir bug de código; propõe Admin API idempotente, não criação manual única. |
| "Três `terraform apply` seguidos falham com variações do mesmo erro de permissão. Qual é o seu 4º passo?" (→ INC-07) | Não é "tentar de novo diferente" — é testar empiricamente se a mudança é sequer necessária, lendo a mensagem de erro com atenção ao verbo (Update vs Create, autoritativo vs aditivo). |
| "Um `AccessDenied` aparece numa chamada que deveria estar coberta pelos grants do Unity Catalog. Como você decide se é problema do Databricks ou da nuvem por trás?" (→ INC-08) | Sabe que deny explícito de IAM/bucket policy sempre vence allow — sinal de bloqueio de infraestrutura AWS, não de configuração do Unity Catalog. |

---

_Cada afirmação técnica acima é rastreável a um arquivo, commit ou execução real deste repositório — nenhum incidente foi hipotetizado para este documento._
