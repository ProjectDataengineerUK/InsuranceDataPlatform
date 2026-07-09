# SQL Warehouse compartilhado pelo AI/BI Dashboard, Genie Space e pelo
# Databricks App (ver DESIGN_INSURANCE_VISUALIZATION_LAYER.md, Decision 2).
# Só existe em prod — dev e prod compartilham o mesmo workspace Databricks
# (mesmo padrão de count condicionado a var.environment já usado em
# secrets.tf, só que invertido: aqui só prod cria o recurso).
resource "databricks_sql_endpoint" "visualization" {
  count = var.environment == "prod" ? 1 : 0

  name                      = "insurance-visualization-${var.environment}"
  cluster_size              = "2X-Small"
  auto_stop_mins            = 10
  enable_serverless_compute = true
  max_num_clusters          = 1
  min_num_clusters          = 1
}

output "sql_warehouse_id" {
  # String vazia em dev/staging (nenhum warehouse criado) — consumido pelo
  # deploy.yml só na etapa de deploy-prod, via `terraform output -raw
  # sql_warehouse_id`, e passado pro `databricks bundle deploy -t prod` como
  # variável do bundle (ver resources/visualization.yml).
  value = var.environment == "prod" ? databricks_sql_endpoint.visualization[0].id : ""
}
