import argparse
import statistics
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

# Job roda como spark_python_task via workspace files (sem empacotamento em
# wheel) — Databricks só põe o diretório do próprio script no sys.path, não a
# raiz do bundle. Sem isso, "from src..." abaixo falha com ModuleNotFoundError.
# Databricks executa o job via exec(compile(source, filename, 'exec')), que
# nao injeta __file__ nos globals — cai pro co_filename do frame atual.
_this_file = globals().get("__file__") or sys._getframe().f_code.co_filename
sys.path.insert(0, str(Path(_this_file).resolve().parents[2]))

from src.common.delta_write import append_or_create
from src.common.spark_session import get_spark

AMOUNT_TOLERANCE_PCT = 0.02
AMOUNT_TOLERANCE_ABS_FLOOR = 5.00


@dataclass
class ReconciliationResult:
    external_reference_id: str
    discrepancy_type: str
    sources_reporting: list[str]
    min_amount: float
    max_amount: float
    resolved_amount: float
    detected_at: datetime


def _is_amount_mismatch(min_amount: float, max_amount: float) -> bool:
    tolerance = max(min_amount * AMOUNT_TOLERANCE_PCT, AMOUNT_TOLERANCE_ABS_FLOOR)
    return (max_amount - min_amount) > tolerance


def reconcile_claims(rows: list[dict], active_sources: set[str]) -> list[ReconciliationResult]:
    by_claim: dict[str, list[dict]] = {}
    for row in rows:
        by_claim.setdefault(row["external_reference_id"], []).append(row)

    detected_at = datetime.now(UTC)
    results = []

    for external_reference_id, claim_rows in by_claim.items():
        # amount pode ser NULL (try_cast em standardize.py vira NULL pra
        # entrada malformada, ver docs/ARCHITECTURE.md) — uma fonte com
        # amount ausente ainda conta pra missing_in_source, mas não entra
        # na comparação de valor (não há o que comparar).
        amounts = [float(row["amount"]) for row in claim_rows if row["amount"] is not None]
        sources_reporting = sorted({row["source_system"] for row in claim_rows})

        if not amounts:
            missing_sources = active_sources - set(sources_reporting)
            if missing_sources:
                results.append(
                    ReconciliationResult(
                        external_reference_id=external_reference_id,
                        discrepancy_type="missing_in_source",
                        sources_reporting=sources_reporting,
                        min_amount=0.0,
                        max_amount=0.0,
                        resolved_amount=0.0,
                        detected_at=detected_at,
                    )
                )
            continue

        min_amount = min(amounts)
        max_amount = max(amounts)
        # MVP: mediana das fontes é a resolução de conflito — robusta a uma
        # única fonte discrepante, determinística, sem viés de "escolher uma
        # seguradora" arbitrariamente. Não é o que um processo regulatório
        # real faria (haveria fonte de verdade contratual), mas serve pro
        # propósito de demo de reconciliação deste slice.
        resolved_amount = statistics.median(amounts)

        if _is_amount_mismatch(min_amount, max_amount):
            results.append(
                ReconciliationResult(
                    external_reference_id=external_reference_id,
                    discrepancy_type="amount_mismatch",
                    sources_reporting=sources_reporting,
                    min_amount=min_amount,
                    max_amount=max_amount,
                    resolved_amount=resolved_amount,
                    detected_at=detected_at,
                )
            )

        missing_sources = active_sources - set(sources_reporting)
        if missing_sources:
            results.append(
                ReconciliationResult(
                    external_reference_id=external_reference_id,
                    discrepancy_type="missing_in_source",
                    sources_reporting=sources_reporting,
                    min_amount=min_amount,
                    max_amount=max_amount,
                    resolved_amount=resolved_amount,
                    detected_at=detected_at,
                )
            )

    return results


def run_reconcile_job(silver_table: str, results_table: str) -> int:
    spark = get_spark("regulatory-reconcile")
    silver_df = spark.read.table(silver_table)

    active_sources = {
        row["source_system"]
        for row in silver_df.select("source_system").distinct().collect()
    }
    rows = [
        row.asDict()
        for row in silver_df.select("external_reference_id", "source_system", "amount").collect()
    ]

    results = reconcile_claims(rows, active_sources)
    if results:
        results_df = spark.createDataFrame([asdict(result) for result in results])
        append_or_create(results_df, results_table)

    return len(results)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--results-table", required=True)
    args = parser.parse_args()

    discrepancy_count = run_reconcile_job(args.silver_table, args.results_table)
    print(f"regulatory reconciliation discrepancies found: {discrepancy_count}")


if __name__ == "__main__":
    main()
