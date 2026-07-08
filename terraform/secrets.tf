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

resource "databricks_secret_acl" "insurance_platform_read" {
  scope      = databricks_secret_scope.insurance_platform.name
  principal  = var.catalog_owner
  permission = "READ"
}
