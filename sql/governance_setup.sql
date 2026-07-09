-- Mantido apenas como referência/fallback manual. Desde a automação do
-- masking, o job gold_aggregate (src/streaming/gold_aggregate.py,
-- apply_governance()) já reaplica este SQL a cada execução — não é mais
-- necessário rodar isto manualmente após um deploy normal.
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

-- RLS: insurance-data-team vê tudo; um grupo insurance-region-<uf> (não
-- provisionado ainda) só veria sinistros da própria região.
CREATE OR REPLACE FUNCTION {catalog}.gold.region_row_filter(region STRING)
RETURNS BOOLEAN
RETURN
  is_account_group_member('insurance-data-team')
  OR is_account_group_member(concat('insurance-region-', lower(region)));

ALTER TABLE {catalog}.gold.claims
  SET ROW FILTER {catalog}.gold.region_row_filter ON (region);
