resource "databricks_secret_scope" "insurance_platform" {
  name = "insurance-platform"
}

resource "databricks_secret" "confluent_bootstrap_servers" {
  scope        = databricks_secret_scope.insurance_platform.name
  key          = "confluent-bootstrap-servers"
  string_value = var.confluent_bootstrap_servers
}

resource "databricks_secret" "confluent_api_key" {
  scope        = databricks_secret_scope.insurance_platform.name
  key          = "confluent-api-key"
  string_value = var.confluent_api_key
}

resource "databricks_secret" "confluent_api_secret" {
  scope        = databricks_secret_scope.insurance_platform.name
  key          = "confluent-api-secret"
  string_value = var.confluent_api_secret
}

resource "databricks_secret" "sla_webhook_url" {
  # Só cria o secret se um webhook real foi configurado (TF_VAR_sla_webhook_url
  # / secret SLA_WEBHOOK_URL no GitHub) — sem isso, sla_alerts.py já cai no
  # fallback de log, então não há por que criar uma entrada vazia.
  count        = var.sla_webhook_url != "" ? 1 : 0
  scope        = databricks_secret_scope.insurance_platform.name
  key          = "sla-webhook-url"
  string_value = var.sla_webhook_url
}

resource "databricks_secret_acl" "insurance_platform_read" {
  scope      = databricks_secret_scope.insurance_platform.name
  principal  = var.catalog_owner
  permission = "READ"
}
