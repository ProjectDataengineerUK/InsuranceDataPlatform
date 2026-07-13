# dev e prod compartilham o mesmo workspace Databricks (só o catálogo Unity
# Catalog muda por ambiente) — o secret scope é um recurso de workspace, não
# de catálogo, então só o dev precisa criá-lo. Sem esse count, o apply do
# prod tenta recriar um scope que já existe e falha com "already exists".
resource "databricks_secret_scope" "insurance_platform" {
  count = var.environment == "dev" ? 1 : 0
  name  = "insurance-platform"
}

resource "databricks_secret" "confluent_bootstrap_servers" {
  count        = var.environment == "dev" ? 1 : 0
  scope        = databricks_secret_scope.insurance_platform[0].name
  key          = "confluent-bootstrap-servers"
  string_value = var.confluent_bootstrap_servers
}

resource "databricks_secret" "confluent_api_key" {
  count        = var.environment == "dev" ? 1 : 0
  scope        = databricks_secret_scope.insurance_platform[0].name
  key          = "confluent-api-key"
  string_value = var.confluent_api_key
}

resource "databricks_secret" "confluent_api_secret" {
  count        = var.environment == "dev" ? 1 : 0
  scope        = databricks_secret_scope.insurance_platform[0].name
  key          = "confluent-api-secret"
  string_value = var.confluent_api_secret
}

resource "databricks_secret" "sla_webhook_url" {
  # Só cria o secret se um webhook real foi configurado (TF_VAR_sla_webhook_url
  # / secret SLA_WEBHOOK_URL no GitHub) — sem isso, sla_alerts.py já cai no
  # fallback de log, então não há por que criar uma entrada vazia.
  count        = var.environment == "dev" && var.sla_webhook_url != "" ? 1 : 0
  scope        = databricks_secret_scope.insurance_platform[0].name
  key          = "sla-webhook-url"
  string_value = var.sla_webhook_url
}

resource "databricks_secret" "confluent_metrics_api_key" {
  # Mesmo padrão condicional de sla_webhook_url: o provider Databricks rejeita
  # string_value vazio (confirmado num apply real: "expected string_value to
  # not be an empty string"), então "criar sempre, mesmo vazio" não é viável.
  # Consequência real: o app em resources/visualization.yml referencia este
  # secret por nome fixo — os 3 GitHub secrets (CONFLUENT_METRICS_API_KEY/
  # SECRET/CONFLUENT_CLUSTER_ID) precisam existir com valor real ANTES do
  # primeiro deploy que inclui essa seção do app, senão o bundle deploy quebra
  # por secret inexistente (não por credencial ausente).
  count        = var.environment == "dev" && var.confluent_metrics_api_key != "" ? 1 : 0
  scope        = databricks_secret_scope.insurance_platform[0].name
  key          = "confluent-metrics-api-key"
  string_value = var.confluent_metrics_api_key
}

resource "databricks_secret" "confluent_metrics_api_secret" {
  count        = var.environment == "dev" && var.confluent_metrics_api_secret != "" ? 1 : 0
  scope        = databricks_secret_scope.insurance_platform[0].name
  key          = "confluent-metrics-api-secret"
  string_value = var.confluent_metrics_api_secret
}

resource "databricks_secret" "confluent_cluster_id" {
  count        = var.environment == "dev" && var.confluent_cluster_id != "" ? 1 : 0
  scope        = databricks_secret_scope.insurance_platform[0].name
  key          = "confluent-cluster-id"
  string_value = var.confluent_cluster_id
}

resource "databricks_secret_acl" "insurance_platform_read" {
  count      = var.environment == "dev" ? 1 : 0
  scope      = databricks_secret_scope.insurance_platform[0].name
  principal  = var.secrets_acl_principal
  permission = "READ"
}
