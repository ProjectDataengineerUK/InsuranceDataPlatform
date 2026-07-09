import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "apps" / "insurance_platform_app"))

from queries import (
    build_claim_lookup_query,
    build_customer_lookup_query,
    build_dq_summary_query,
    build_policy_lookup_query,
    build_reconciliation_query,
    build_source_system_search_query,
    build_table_freshness_query,
)


def test_build_claim_lookup_query():
    query = build_claim_lookup_query("c1")

    assert "gold.claims" in query.sql
    assert query.params == {"claim_id": "c1"}


def test_build_policy_lookup_query():
    query = build_policy_lookup_query("p1")

    assert "gold.claims" in query.sql
    assert "policy_id" in query.sql
    assert query.params == {"policy_id": "p1"}


def test_build_customer_lookup_query():
    query = build_customer_lookup_query("cust1")

    assert "customer_id" in query.sql
    assert query.params == {"customer_id": "cust1"}


def test_build_source_system_search_query():
    query = build_source_system_search_query("insurer_b")

    assert "silver.regulatory_claims" in query.sql
    assert query.params == {"source_system": "insurer_b"}


def test_build_dq_summary_query_default_limit():
    query = build_dq_summary_query()

    assert "gold.regulatory_dq_summary" in query.sql
    assert query.params == {"row_limit": 50}


def test_build_reconciliation_query_custom_limit():
    query = build_reconciliation_query(row_limit=5)

    assert "monitoring._regulatory_reconciliation_results" in query.sql
    assert query.params == {"row_limit": 5}


def test_build_table_freshness_query():
    query = build_table_freshness_query("gold.claims", "_scored_at")

    assert query.sql == "SELECT MAX(_scored_at) AS latest_ts FROM gold.claims"
    assert query.params == {}
