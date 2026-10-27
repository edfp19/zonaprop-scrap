"""Gold refresh boundary: own Spark, stream typed results, and publish transactionally."""

from datetime import UTC, datetime
from pathlib import Path

from zona_oportunidades.listings.runner import create_local_spark

from .domain import ComparisonPolicy, GoldRunResult, Opportunity
from .repository import GoldRepository
from .transforms import comparable_statistics, current_listings


def refresh_gold(
    data_root: Path,
    dsn: str,
    policy: ComparisonPolicy,
    *,
    master: str | None = None,
    as_of: datetime | None = None,
) -> GoldRunResult:
    computed_at = as_of or datetime.now(UTC)
    silver_root = data_root / "silver" / "listing_observations"
    if not any(silver_root.glob("ingestion_date=*/run_id=*/_run_manifest.json")):
        raise ValueError(f"No hay corridas Silver publicadas en {silver_root}")
    spark = create_local_spark(master)
    try:
        silver = spark.read.parquet(str(silver_root))
        current = current_listings(silver, policy, computed_at).cache()
        try:
            statistics = comparable_statistics(current, policy)
            opportunities = (Opportunity(**row.asDict()) for row in statistics.toLocalIterator())
            return GoldRepository(dsn).replace(opportunities, policy.minimum_count, computed_at)
        finally:
            current.unpersist()
    finally:
        spark.stop()
