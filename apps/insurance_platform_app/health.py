from datetime import UTC, datetime, timedelta
from typing import Any

GREEN = "green"
YELLOW = "yellow"
RED = "red"
UNKNOWN = "unknown"

STATUS_COLORS = {
    GREEN: "#2ecc71",
    YELLOW: "#f1c40f",
    RED: "#e74c3c",
    UNKNOWN: "#bdc3c7",
}

STATUS_LABELS = {
    GREEN: "🟢 OK",
    YELLOW: "🟡 Atenção",
    RED: "🔴 Fora do esperado",
    UNKNOWN: "⚪ Sem dado suficiente",
}

DEFAULT_FRESHNESS_MINUTES = 30


def freshness_status(latest_ts: datetime | None, threshold_minutes: int = DEFAULT_FRESHNESS_MINUTES) -> str:
    if latest_ts is None:
        return UNKNOWN
    age = datetime.now(UTC) - latest_ts.replace(tzinfo=UTC)
    return GREEN if age < timedelta(minutes=threshold_minutes) else RED


def compliance_status(compliant: int, non_compliant: int, yellow_at: float = 0.95, red_at: float = 0.80) -> str:
    total = compliant + non_compliant
    if total == 0:
        return UNKNOWN
    rate = compliant / total
    if rate >= yellow_at:
        return GREEN
    if rate >= red_at:
        return YELLOW
    return RED


def count_threshold_status(count: int, yellow_at: int = 1, red_at: int = 10) -> str:
    if count >= red_at:
        return RED
    if count >= yellow_at:
        return YELLOW
    return GREEN


def drift_status(rows: list[dict[str, Any]]) -> str:
    # Drift nunca falha o pipeline sozinho (ver model_drift.py) — o pior
    # status real é "atenção", nunca "vermelho".
    if not rows:
        return UNKNOWN
    return YELLOW if any(row["drift_detected"] for row in rows) else GREEN


def sla_status(within_sla: bool | None) -> str:
    if within_sla is None:
        return UNKNOWN
    return GREEN if within_sla else RED


def table_exists_status(exists: bool) -> str:
    return UNKNOWN if not exists else GREEN


# node_id -> (label, cluster, upstream_node_ids)
ARCHITECTURE_NODES: dict[str, tuple[str, str, list[str]]] = {
    "kafka_producer": ("Kafka Producer\n(GitHub Actions)", "operacional", []),
    "bronze_operational": ("Bronze\n(claims/policies/customers)", "operacional", ["kafka_producer"]),
    "silver_operational": ("Silver\n(dedup + DQ)", "operacional", ["bronze_operational"]),
    "gold_fraud": ("Gold\n(claims + fraud score)", "operacional", ["silver_operational"]),
    "fraud_model": ("Modelo de fraude\n(mlflow, shadow mode)", "mlops", ["gold_fraud"]),
    "model_drift": ("Monitor de drift", "mlops", ["fraud_model"]),
    "sla_breach": ("SLA de revisão manual", "operacional", ["gold_fraud"]),
    "pipeline_latency": ("Latência do pipeline", "operacional", ["bronze_operational", "gold_fraud"]),
    "bronze_regulatory": ("Bronze regulatório\n(insurer_a/b/c)", "regulatorio", []),
    "reconciliation": ("Reconciliação\nentre fontes", "regulatorio", ["bronze_regulatory"]),
    "susep_compliance": ("Conformidade SUSEP", "regulatorio", ["reconciliation"]),
    "dq_checks": ("Qualidade de dados\n(todas as camadas)", "regulatorio", ["bronze_regulatory"]),
    "open_insurance": ("Open Insurance\n(consentimento)", "open_insurance", []),
}


def build_architecture_dot(statuses: dict[str, str]) -> str:
    lines = ["digraph pipeline {", "  rankdir=LR;", '  node [shape=box, style="filled,rounded", fontname="Helvetica"];']

    clusters: dict[str, list[str]] = {}
    for node_id, (_, cluster, _) in ARCHITECTURE_NODES.items():
        clusters.setdefault(cluster, []).append(node_id)

    cluster_labels = {
        "operacional": "Pipeline operacional (Kafka → Bronze → Silver → Gold)",
        "mlops": "MLOps (fraude)",
        "regulatorio": "Pipeline regulatório (SUSEP)",
        "open_insurance": "Open Insurance",
    }

    for cluster_id, node_ids in clusters.items():
        lines.append(f"  subgraph cluster_{cluster_id} {{")
        lines.append(f'    label="{cluster_labels.get(cluster_id, cluster_id)}";')
        lines.append('    style="rounded"; color="#95a5a6";')
        for node_id in node_ids:
            label, _, _ = ARCHITECTURE_NODES[node_id]
            color = STATUS_COLORS[statuses.get(node_id, UNKNOWN)]
            lines.append(f'    {node_id} [label="{label}", fillcolor="{color}"];')
        lines.append("  }")

    for node_id, (_, _, upstream) in ARCHITECTURE_NODES.items():
        for upstream_id in upstream:
            lines.append(f"  {upstream_id} -> {node_id};")

    lines.append("}")
    return "\n".join(lines)
