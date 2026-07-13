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
  description = "Grupo ou usuário owner do catálogo Unity Catalog e alvo dos grants de schema/volume"
  # "account users" é um principal de conta/metastore — funciona para grants
  # de Unity Catalog (catalog owner, schema/volume grants) SEM precisar estar
  # atribuído ao workspace, e cobre implicitamente o service principal do
  # Databricks App (confirmado: trocar isso pelo e-mail de um usuário
  # específico quebrou USE CATALOG para o app numa execução real —
  # INSUFFICIENT_PERMISSIONS, ver docs/ARCHITECTURE.md). Reservado
  # exclusivamente para esse propósito — não usar para o Secret ACL
  # (ver var.secrets_acl_principal).
  type    = string
  default = "account users"
}

variable "secrets_acl_principal" {
  description = "Principal com READ no secret scope insurance-platform (recurso de workspace, não de catálogo)"
  # Secret ACL é um recurso de WORKSPACE (API legada de permissions), não de
  # metastore/conta — precisa de um principal já reconhecido no workspace.
  # "account users" falha aqui com "User or Group account users does not
  # exist" enquanto o grupo não for atribuído ao workspace (Account Console >
  # Workspaces > Permissions, ainda não feito neste workspace novo) — usando
  # o e-mail do único usuário até isso ser resolvido.
  type    = string
  default = "jonataslimacostabr@gmail.com"
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
