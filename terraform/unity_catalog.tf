resource "databricks_catalog" "insurance" {
  name    = "insurance_${var.environment}"
  comment = "Catálogo do Insurance Lakehouse Platform — ambiente ${var.environment}"

  owner = var.catalog_owner

  # storage_root é ForceNew e não é declarado aqui (o catálogo é criado via UI
  # usando o Default Storage do metastore, depois adotado via `terraform
  # import`) — sem isso, o plano tenta destruir e recriar o catálogo a cada
  # apply só porque o campo está ausente do config.
  lifecycle {
    ignore_changes = [storage_root]
  }
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.insurance.name
  name         = "bronze"
  comment      = "Camada raw — dados de eventos Kafka sem transformação"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.insurance.name
  name         = "silver"
  comment      = "Camada limpa — dedup, normalização e quality checks aplicados"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.insurance.name
  name         = "gold"
  comment      = "Camada analítica — agregações de negócio, fraude e aprovação automática"
}

resource "databricks_schema" "monitoring" {
  catalog_name = databricks_catalog.insurance.name
  name         = "monitoring"
  comment      = "Resultados de qualidade de dados (_dq_results) e observabilidade"
}

resource "databricks_schema" "models" {
  # O workspace tem o legacy Workspace Model Registry desabilitado — modelos
  # mlflow precisam ser registrados no Unity Catalog (nome de 3 níveis).
  catalog_name = databricks_catalog.insurance.name
  name         = "models"
  comment      = "Modelos mlflow registrados no Unity Catalog (ex.: fraud_classifier)"
}

resource "databricks_grants" "bronze_read_write" {
  schema = "${databricks_catalog.insurance.name}.${databricks_schema.bronze.name}"

  grant {
    principal  = var.catalog_owner
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
  }
}

resource "databricks_grants" "silver_read_write" {
  schema = "${databricks_catalog.insurance.name}.${databricks_schema.silver.name}"

  grant {
    principal  = var.catalog_owner
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
  }
}

resource "databricks_grants" "models_read_write" {
  schema = "${databricks_catalog.insurance.name}.${databricks_schema.models.name}"

  grant {
    principal  = var.catalog_owner
    privileges = ["USE_SCHEMA", "CREATE_MODEL", "EXECUTE"]
  }
}

resource "databricks_grants" "gold_read_only_analysts" {
  schema = "${databricks_catalog.insurance.name}.${databricks_schema.gold.name}"

  grant {
    principal  = var.catalog_owner
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
  }
}

// Volumes gerenciados para checkpoint do Structured Streaming — os jobs
// referenciam /Volumes/${var.catalog}/<schema>/_checkpoints/<nome> (ver
// resources/jobs.bronze.yml, jobs.silver.yml, jobs.fraud_score.yml). Um UC
// Volume não é criado implicitamente na primeira escrita como um diretório
// DBFS seria — sem isso o job falha com UC_VOLUME_NOT_FOUND.
resource "databricks_volume" "bronze_checkpoints" {
  catalog_name = databricks_catalog.insurance.name
  schema_name  = databricks_schema.bronze.name
  name         = "_checkpoints"
  volume_type  = "MANAGED"
  comment      = "Checkpoints do Structured Streaming de bronze_ingest (um subdiretório por tópico)"
}

resource "databricks_volume" "silver_checkpoints" {
  catalog_name = databricks_catalog.insurance.name
  schema_name  = databricks_schema.silver.name
  name         = "_checkpoints"
  volume_type  = "MANAGED"
  comment      = "Checkpoints do Structured Streaming de silver_transform (um subdiretório por tabela)"
}

resource "databricks_volume" "gold_checkpoints" {
  catalog_name = databricks_catalog.insurance.name
  schema_name  = databricks_schema.gold.name
  name         = "_checkpoints"
  volume_type  = "MANAGED"
  comment      = "Checkpoints do Structured Streaming de fraud_score_stream"
}

resource "databricks_grants" "bronze_checkpoints_read_write" {
  volume = "${databricks_catalog.insurance.name}.${databricks_schema.bronze.name}.${databricks_volume.bronze_checkpoints.name}"

  grant {
    principal  = var.catalog_owner
    privileges = ["READ_VOLUME", "WRITE_VOLUME"]
  }
}

resource "databricks_grants" "silver_checkpoints_read_write" {
  volume = "${databricks_catalog.insurance.name}.${databricks_schema.silver.name}.${databricks_volume.silver_checkpoints.name}"

  grant {
    principal  = var.catalog_owner
    privileges = ["READ_VOLUME", "WRITE_VOLUME"]
  }
}

resource "databricks_grants" "gold_checkpoints_read_write" {
  volume = "${databricks_catalog.insurance.name}.${databricks_schema.gold.name}.${databricks_volume.gold_checkpoints.name}"

  grant {
    principal  = var.catalog_owner
    privileges = ["READ_VOLUME", "WRITE_VOLUME"]
  }
}

// Grant do service principal do Databricks App pra gold/monitoring:
// REMOVIDO daqui de propósito (ver DESIGN_INSURANCE_VISUALIZATION_LAYER.md,
// Decision 3 — nota pós-deploy). 3 tentativas diferentes (databricks_grants
// com dynamic "grant", databricks_grant separado, e novamente databricks_grant
// com depends_on) falharam em applies reais contra este schema com
// variações do mesmo erro "cannot create/update grant: permissions ... are
// [...], but have to be [...]". Testado empiricamente: o app já consulta
// gold.regulatory_dq_summary e monitoring._regulatory_reconciliation_results
// sem erro de permissão (o erro real foi TABLE_OR_VIEW_NOT_FOUND, não
// PERMISSION_DENIED) — ou seja, o service principal do app já tem acesso por
// algum mecanismo fora deste Terraform (provavelmente concedido
// automaticamente pelo próprio Databricks Apps no catálogo/schema onde o
// app roda). Gerenciar esse grant explicitamente aqui só estava causando
// falhas de deploy sem necessidade real — reavaliar só se um teste real
// mostrar PERMISSION_DENIED no futuro.

// A tabela `claims` em Gold é criada pelos jobs Spark (saveAsTable), não pelo
// Terraform — gerenciar o schema da tabela em dois sistemas causaria conflito
// de ownership. A função de masking e o `ALTER TABLE ... SET MASK` em
// `customer_id` são aplicados por `sql/governance_setup.sql` (ver Decision 5
// do DESIGN: Terraform cobre catálogos/schemas/grants/secrets; tabelas e suas
// políticas de coluna são responsabilidade dos jobs/scripts de dados).
