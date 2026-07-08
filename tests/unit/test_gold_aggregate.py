from src.streaming.gold_aggregate import apply_governance, run_gold_job


def test_apply_governance_skips_when_gold_claims_table_missing(spark):
    # gold.claims ainda não existe neste catálogo de teste — apply_governance
    # deve sair cedo sem tentar rodar DDL específico de Unity Catalog
    # (is_account_group_member/SET MASK), que não existe no Spark OSS local.
    apply_governance(spark, catalog="insurance_dev", gold_claims_table="no_such_gold_claims_table_xyz")

    assert not spark.catalog.tableExists("no_such_gold_claims_table_xyz")


def test_run_gold_job_skips_when_gold_claims_table_missing(spark):
    # Cenário de primeira execução: o job agendado (10 em 10 min) roda antes
    # de fraud_score_stream ter escrito o primeiro micro-batch em
    # gold.claims. Não deve lançar AnalysisException, só pular esta execução.
    run_gold_job(
        catalog="insurance_dev",
        gold_claims_table="no_such_gold_claims_table_for_run_job",
        gold_agg_table="no_such_gold_agg_table_for_run_job",
        results_table="no_such_results_table_for_run_job",
        run_date="2026-01-01",
    )

    assert not spark.catalog.tableExists("no_such_gold_agg_table_for_run_job")
