from src.streaming.gold_aggregate import apply_governance


def test_apply_governance_skips_when_gold_claims_table_missing(spark):
    # gold.claims ainda não existe neste catálogo de teste — apply_governance
    # deve sair cedo sem tentar rodar DDL específico de Unity Catalog
    # (is_account_group_member/SET MASK), que não existe no Spark OSS local.
    apply_governance(spark, catalog="insurance_dev", gold_claims_table="no_such_gold_claims_table_xyz")

    assert not spark.catalog.tableExists("no_such_gold_claims_table_xyz")
