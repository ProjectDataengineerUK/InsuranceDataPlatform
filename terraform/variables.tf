variable "databricks_host" {
  description = "URL do workspace Databricks (ex: https://<workspace>.cloud.databricks.com)"
  type        = string
}

variable "databricks_token" {
  description = "Token de autenticação do service principal do ambiente"
  type        = string
  sensitive   = true
}

variable "environment" {
  description = "Ambiente de deploy (dev, staging, prod)"
  type        = string

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment deve ser um de: dev, staging, prod."
  }
}

variable "catalog_owner" {
  description = "Grupo ou usuário owner do catálogo Unity Catalog"
  type        = string
  default     = "account users"
}

variable "confluent_bootstrap_servers" {
  description = "Bootstrap servers do cluster Kafka no Confluent Cloud"
  type        = string
  sensitive   = true
}

variable "confluent_api_key" {
  description = "API Key do Confluent Cloud"
  type        = string
  sensitive   = true
}

variable "confluent_api_secret" {
  description = "API Secret do Confluent Cloud"
  type        = string
  sensitive   = true
}

variable "sla_webhook_url" {
  description = "Webhook (Slack/Teams/etc.) para alertas de SLA. Opcional — se vazio, sla_alerts.py só loga o alerta em vez de enviar."
  type        = string
  sensitive   = true
  default     = ""
}

variable "app_service_principal_id" {
  description = <<-EOT
    Client ID do service principal do Databricks App (insurance_platform_app).
    Vazio até o bootstrap em 2 passos rodar (ver DESIGN_INSURANCE_VISUALIZATION_LAYER.md,
    Decision 3): 1) `databricks bundle deploy -t prod` cria o app e minta o
    service principal; 2) o client_id gerado é setado aqui via TF_VAR e um
    novo `terraform apply` concede os grants de leitura. Não é segredo (é um
    client ID, não uma credencial), mas só é populado manualmente.
  EOT
  type        = string
  default     = ""
}
