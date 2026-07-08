terraform {
  required_version = ">= 1.5.0"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }

  # Backend local: o state (terraform.tfstate) é restaurado/salvo via
  # actions/cache no GitHub Actions (ver .github/workflows/deploy.yml), sem
  # depender de nenhum serviço externo (HCP Terraform, S3, etc.). Trade-off
  # aceito conscientemente: sem lock distribuído real (mitigado serializando o
  # workflow via `concurrency`) e sujeito à política de expiração do cache do
  # GitHub Actions (7 dias sem uso, ou 10GB por repositório) — documentado em
  # docs/ARCHITECTURE.md.
  backend "local" {
    path = "terraform.tfstate"
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}
