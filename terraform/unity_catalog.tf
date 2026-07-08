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

resource "databricks_grants" "gold_read_only_analysts" {
  schema = "${databricks_catalog.insurance.name}.${databricks_schema.gold.name}"

  grant {
    principal  = var.catalog_owner
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
  }
}

// A tabela `claims` em Gold é criada pelos jobs Spark (saveAsTable), não pelo
// Terraform — gerenciar o schema da tabela em dois sistemas causaria conflito
// de ownership. A função de masking e o `ALTER TABLE ... SET MASK` em
// `customer_id` são aplicados por `sql/governance_setup.sql` (ver Decision 5
// do DESIGN: Terraform cobre catálogos/schemas/grants/secrets; tabelas e suas
// políticas de coluna são responsabilidade dos jobs/scripts de dados).
