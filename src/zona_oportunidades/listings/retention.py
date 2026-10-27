"""Read-only five-week selection; publication and quarantine budgets belong to the caller."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import reduce
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

IDENTITY_COLUMNS = ("listing_id", "operation", "currency")
WEEK_SECONDS = 7 * 24 * 60 * 60
MONDAY_EPOCH_SECONDS = 4 * 24 * 60 * 60  # 1970-01-05 UTC


@dataclass(frozen=True)
class RetentionSelection:
    retained: "DataFrame"
    quarantine: "DataFrame"
    expired: "DataFrame"
    future: "DataFrame"


def utc_week_start(as_of: datetime) -> datetime:
    """Return Monday midnight, rejecting ambiguous naive clock values."""
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
    utc = as_of.astimezone(UTC)
    return utc.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=utc.weekday())


def weekly_observations(observations: "DataFrame", as_of: datetime) -> RetentionSelection:
    """Keep latest per identity/UTC week, including this week and four previous weeks.

    All original columns and types survive. Invalid keys/timestamps are quarantined
    individually, without grouping or deduplication. Flagged rows with usable keys
    participate normally. Future/expired rows are returned for explicit caller decisions.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    start = utc_week_start(as_of) - timedelta(weeks=4)
    valid_key = (
        reduce(
            lambda left, right: left & right,
            [F.col(name).isNotNull() & (F.length(F.trim(name)) > 0) for name in IDENTITY_COLUMNS],
        )
        & F.col("observed_at").isNotNull()
    )
    valid = observations.filter(valid_key)
    within_window = valid.filter(
        (F.col("observed_at") >= F.lit(start)) & (F.col("observed_at") <= F.lit(as_of))
    )
    # Epoch arithmetic makes the week independent of the Spark session time zone.
    week = F.floor((F.col("observed_at").cast("double") - MONDAY_EPOCH_SECONDS) / WEEK_SECONDS)
    # Full-row serialization breaks even duplicate lineage ties; identical rows are interchangeable.
    tie = F.to_json(F.struct(*[F.col(name) for name in observations.columns]))
    ordering = [F.col("observed_at").desc()]
    # Match Gold's same-observation preference before reducing several USD prices.
    for name in ("ingestion_date", "run_id", "source_observation_id"):
        if name in observations.columns:
            ordering.append(F.col(name).desc())
    if "price_index" in observations.columns:
        ordering.append(F.col("price_index").asc_nulls_last())
    if "observation_id" in observations.columns:
        ordering.append(F.col("observation_id").asc())
    window = Window.partitionBy(*IDENTITY_COLUMNS, week).orderBy(*ordering, tie.asc())
    # Selecting source-qualified columns avoids temporary-name collisions with future schemas.
    ranked = within_window.alias("source").select(
        "source.*", F.row_number().over(window).alias("__retention_rank")
    )
    retained = ranked.filter(F.col("__retention_rank") == 1).select(
        *[F.col(f"source.{name}") for name in observations.columns]
    )
    return RetentionSelection(
        retained=retained,
        quarantine=observations.filter(~valid_key),
        expired=valid.filter(F.col("observed_at") < F.lit(start)),
        future=valid.filter(F.col("observed_at") > F.lit(as_of)),
    )
