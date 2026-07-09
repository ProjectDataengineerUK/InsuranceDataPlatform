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

  # Grant do service principal do Databricks App (ver
  # DESIGN_INSURANCE_VISUALIZATION_LAYER.md, Decision 3) — precisa estar
  # DENTRO deste mesmo resource, não num databricks_grant separado: essa
  # combinação foi tentada e falhou na prática (databricks_grants, plural, é
  # autoritativo por objeto — ele faz refresh lendo TODOS os grants reais do
  # schema e trata qualquer grant não declarado aqui como drift a reverter,
  # inclusive um adicionado por um databricks_grant separado no mesmo
  # securable). Um dynamic "grant" condicional é a única forma segura de ter
  # os dois grants nesse schema sob um único resource autoritativo.
  dynamic "grant" {
    for_each = var.app_service_principal_id != "" ? [var.app_service_principal_id] : []
    content {
      principal  = grant.value
      privileges = ["USE_SCHEMA", "SELECT"]
    }
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

// monitoring não tem um databricks_grants pré-existente (diferente de gold),
// então um databricks_grant (singular, aditivo) isolado aqui é seguro — não
// há outro resource autoritativo competindo pelo mesmo securable.
// depends_on força esta chamada a esperar a modificação de
// gold_read_only_analysts terminar antes de rodar: as duas mexendo em
// grants do MESMO principal em objetos diferentes, ao mesmo tempo, na
// mesma apply, foi o que causou a falha original (erro de precondição do
// provider ao ler o estado de permissões durante uma corrida).
resource "databricks_grant" "monitoring_read_visualization_app" {
  count      = var.environment == "prod" && var.app_service_principal_id != "" ? 1 : 0
  schema     = "${databricks_catalog.insurance.name}.${databricks_schema.monitoring.name}"
  principal  = var.app_service_principal_id
  privileges = ["USE_SCHEMA", "SELECT"]

  depends_on = [databricks_grants.gold_read_only_analysts]
}

// A tabela `claims` em Gold é criada pelos jobs Spark (saveAsTable), não pelo
// Terraform — gerenciar o schema da tabela em dois sistemas causaria conflito
// de ownership. A função de masking e o `ALTER TABLE ... SET MASK` em
// `customer_id` são aplicados por `sql/governance_setup.sql` (ver Decision 5
// do DESIGN: Terraform cobre catálogos/schemas/grants/secrets; tabelas e suas
// políticas de coluna são responsabilidade dos jobs/scripts de dados).
