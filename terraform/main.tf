terraform {
  required_version = ">= 1.5.0"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }

  # Runners do GitHub Actions são efêmeros — um backend local perderia o state
  # a cada execução. HCP Terraform (Terraform Cloud) mantém o state remoto e
  # com lock, sem exigir infraestrutura própria de nenhum hyperscaler.
  cloud {
    organization = "insurance-lakehouse-platform"

    workspaces {
      tags = ["insurance-lakehouse-platform"]
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}
