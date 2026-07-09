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
