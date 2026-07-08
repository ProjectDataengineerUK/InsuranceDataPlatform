-- Executar manualmente (ou via job Databricks pós-deploy) depois que o job Gold
-- criar a tabela `claims` pela primeira vez. Requer privilégio de owner no catálogo.
-- Substitua {catalog} pelo catálogo do ambiente (insurance_dev/staging/prod).

CREATE OR REPLACE FUNCTION {catalog}.gold.mask_customer_id(customer_id STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('insurance-data-team') THEN customer_id
    ELSE sha2(customer_id, 256)
  END;

ALTER TABLE {catalog}.gold.claims
  ALTER COLUMN customer_id
  SET MASK {catalog}.gold.mask_customer_id;
