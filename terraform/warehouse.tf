# SQL Warehouse compartilhado pelo AI/BI Dashboard, Genie Space e pelo
# Databricks App (ver DESIGN_INSURANCE_VISUALIZATION_LAYER.md, Decision 2).
# Só existe em prod — dev e prod compartilham o mesmo workspace Databricks
# (mesmo padrão de count condicionado a var.environment já usado em
# secrets.tf, só que invertido: aqui só prod cria o recurso).
#
# O workspace novo (conta trial/serverless) não permite provisionar um
# segundo warehouse serverless via API — um `databricks_sql_endpoint` novo
# fica preso em "Still creating..." indefinidamente (confirmado: 15+ min sem
# completar num deploy real, apply cancelado). O workspace já vem com um
# warehouse serverless pré-provisionado ("Serverless Starter Warehouse"), que
# é reaproveitado aqui em vez de tentar criar um novo.
output "sql_warehouse_id" {
  # String vazia em dev/staging (nenhum warehouse usado) — consumido pelo
  # deploy.yml só na etapa de deploy-prod, via `terraform output -raw
  # sql_warehouse_id`, e passado pro `databricks bundle deploy -t prod` como
  # variável do bundle (ver resources/visualization.yml).
  value = var.environment == "prod" ? "ec7c51d2a3971eb0" : ""
}
