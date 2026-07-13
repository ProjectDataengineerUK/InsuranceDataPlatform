import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "apps" / "insurance_platform_app"))

from queries import (
    build_cost_usage_by_job_query,
    build_cost_usage_by_product_query,
    build_dq_results_query,
    build_dq_summary_query,
    build_fraud_probability_query,
    build_model_drift_query,
    build_pipeline_latency_query,
    build_reconciliation_query,
    build_sla_breach_query,
    build_source_system_volume_query,
    build_susep_claims_query,
    build_susep_compliance_summary_query,
    build_table_count_query,
    build_table_freshness_query,
)


def test_build_susep_claims_query_compliant():
    query = build_susep_claims_query(compliant=True)

    assert "gold.regulatory_susep_claims" in query.sql
    assert query.params == {"compliant": True, "row_limit": 200}


def test_build_susep_claims_query_non_compliant_custom_limit():
    query = build_susep_claims_query(compliant=False, row_limit=10)

    assert query.params == {"compliant": False, "row_limit": 10}


def test_build_susep_compliance_summary_query():
    query = build_susep_compliance_summary_query()

    assert "susep_compliant" in query.sql
    assert "GROUP BY susep_compliant" in query.sql


def test_build_source_system_volume_query():
    query = build_source_system_volume_query()

    assert "silver.regulatory_claims" in query.sql
    assert "source_system" in query.sql


def test_build_fraud_probability_query_default_limit():
    query = build_fraud_probability_query()

    assert "gold.claims" in query.sql
    assert "fraud_score" in query.sql
    assert query.params == {"row_limit": 100}


def test_build_sla_breach_query():
    query = build_sla_breach_query(sla_hours=48, row_limit=5)

    assert "auto_approved" in query.sql
    assert query.params == {"sla_hours": 48, "row_limit": 5}


def test_build_dq_summary_query_default_limit():
    query = build_dq_summary_query()

    assert "gold.regulatory_dq_summary" in query.sql
    assert query.params == {"row_limit": 50}


def test_build_reconciliation_query_custom_limit():
    query = build_reconciliation_query(row_limit=5)

    assert "monitoring._regulatory_reconciliation_results" in query.sql
    assert query.params == {"row_limit": 5}


def test_build_dq_results_query():
    query = build_dq_results_query(row_limit=20)

    assert "monitoring._dq_results" in query.sql
    assert query.params == {"row_limit": 20}


def test_build_model_drift_query():
    query = build_model_drift_query()

    assert "monitoring._model_drift_results" in query.sql
    assert query.params == {"row_limit": 50}


def test_build_pipeline_latency_query():
    query = build_pipeline_latency_query()

    assert "monitoring._pipeline_latency_results" in query.sql
    assert query.params == {"row_limit": 50}


def test_build_cost_usage_by_product_query():
    query = build_cost_usage_by_product_query(lookback_days=7)

    assert "system.billing.usage" in query.sql
    assert query.params == {"lookback_days": 7}


def test_build_cost_usage_by_job_query():
    query = build_cost_usage_by_job_query(lookback_days=7, row_limit=10)

    assert "usage_metadata.job_id" in query.sql
    assert query.params == {"lookback_days": 7, "row_limit": 10}


def test_build_table_freshness_query():
    query = build_table_freshness_query("gold.claims", "_scored_at")

    assert query.sql == "SELECT MAX(_scored_at) AS latest_ts FROM gold.claims"
    assert query.params == {}


def test_build_table_count_query():
    query = build_table_count_query("bronze.claims_quarantine")

    assert query.sql == "SELECT COUNT(*) AS total FROM bronze.claims_quarantine"
    assert query.params == {}
