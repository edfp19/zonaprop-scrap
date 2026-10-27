"""Deterministic bounded catalogue planning, with no publication or deletion side effects."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from zona_oportunidades.listings.retention import IDENTITY_COLUMNS, utc_week_start

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass(frozen=True, order=True)
class CatalogueKey:
    listing_id: str
    operation: str
    currency: str


@dataclass(frozen=True)
class WeeklyKey:
    key: CatalogueKey
    week: datetime


@dataclass(frozen=True)
class HistoryFootprint:
    """Retained valid keys plus reserved slots for individually quarantined observations.

    Caller supplies only the five-week retained window. Unknown-timestamp quarantine
    consumes total slots; conservatively reserve it in each week's count as well.
    """

    keys: frozenset[WeeklyKey]
    quarantine_slots: int = 0


@dataclass(frozen=True)
class CataloguePolicy:
    max_current_keys: int
    max_weekly_keys: int
    max_history_keys: int
    max_age_days: int

    def __post_init__(self) -> None:
        if (
            min(
                self.max_current_keys,
                self.max_weekly_keys,
                self.max_history_keys,
                self.max_age_days,
            )
            < 0
        ):
            raise ValueError("catalogue limits and age must be nonnegative")


@dataclass(frozen=True)
class AdmissionDecision:
    key: CatalogueKey
    reason: str


@dataclass(frozen=True)
class CatalogueSelection:
    current: "DataFrame"
    admitted: tuple[CatalogueKey, ...]
    refreshed: tuple[CatalogueKey, ...]
    retired: tuple[CatalogueKey, ...]
    rejected: tuple[AdmissionDecision, ...]
    current_count: int
    history_count: int
    maximum_weekly_count: int


def select_catalogue(
    eligible: "DataFrame",
    existing: "DataFrame",
    history: HistoryFootprint,
    policy: CataloguePolicy,
    as_of: datetime,
) -> CatalogueSelection:
    """Refresh existing identities first, then admit in lexical identity order.

    Inputs use the same Silver schema. `eligible` has already passed current Gold
    eligibility; `existing` is the last admitted Silver frame, not historical peers.
    Missing sampled listings remain until age-ineligible. Existing slots are never
    evicted to fit a reduced budget: infeasible baselines raise ValueError.
    Caller must gate production use on measured byte capacity as well as these counts.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    start = utc_week_start(as_of) - timedelta(weeks=4)
    if history.quarantine_slots < 0:
        raise ValueError("quarantine_slots must be nonnegative")
    if any(
        item.week != utc_week_start(item.week) or not start <= item.week <= utc_week_start(as_of)
        for item in history.keys
    ):
        raise ValueError("history must contain UTC Monday keys in the retained window")
    if eligible.schema != existing.schema:
        raise ValueError("eligible and existing must have identical schemas")

    def metadata(frame: "DataFrame") -> dict[CatalogueKey, datetime]:
        result: dict[CatalogueKey, datetime] = {}
        for row in frame.select(
            *IDENTITY_COLUMNS, F.col("observed_at").cast("double").alias("epoch")
        ).collect():
            values = [row[name] for name in IDENTITY_COLUMNS]
            if any(value is None or not value.strip() for value in values):
                raise ValueError("catalogue inputs require valid identity keys")
            key = CatalogueKey(*values)
            epoch = row.epoch
            if epoch is None:
                raise ValueError("catalogue inputs require observed_at")
            timestamp = datetime.fromtimestamp(epoch, UTC)
            if key in result:
                raise ValueError("catalogue inputs must contain one current row per identity")
            if timestamp > as_of:
                raise ValueError("catalogue inputs cannot contain future observations")
            result[key] = timestamp
        return result

    old, incoming = metadata(existing), metadata(eligible)
    cutoff = as_of - timedelta(days=policy.max_age_days)
    incoming = {key: timestamp for key, timestamp in incoming.items() if timestamp >= cutoff}
    refreshed = tuple(
        sorted(key for key in old.keys() & incoming.keys() if incoming[key] >= old[key])
    )
    kept = {key: max(timestamp, incoming.get(key, timestamp)) for key, timestamp in old.items()}
    retired = tuple(sorted(key for key, timestamp in kept.items() if timestamp < cutoff))
    kept = {key: timestamp for key, timestamp in kept.items() if timestamp >= cutoff}
    footprint = set(history.keys)

    def add_history(key: CatalogueKey, timestamp: datetime) -> None:
        footprint.add(WeeklyKey(key, utc_week_start(timestamp)))

    for key, timestamp in kept.items():
        add_history(key, timestamp)

    def counts() -> tuple[int, int]:
        weekly: dict[datetime, int] = {}
        for item in footprint:
            weekly[item.week] = weekly.get(item.week, 0) + 1
        return (
            len(footprint) + history.quarantine_slots,
            max(weekly.values(), default=0) + history.quarantine_slots,
        )

    total, weekly = counts()
    if (
        len(kept) > policy.max_current_keys
        or total > policy.max_history_keys
        or weekly > policy.max_weekly_keys
    ):
        raise ValueError("existing catalogue/history exceeds limits; refuse publication")
    admitted: list[CatalogueKey] = []
    rejected: list[AdmissionDecision] = []
    for key in sorted(incoming.keys() - old.keys()):
        slot = WeeklyKey(key, utc_week_start(incoming[key]))
        is_new = slot not in footprint
        footprint.add(slot)
        total, weekly = counts()
        reason = (
            "current_capacity"
            if len(kept) >= policy.max_current_keys
            else "history_capacity"
            if total > policy.max_history_keys
            else "weekly_capacity"
            if weekly > policy.max_weekly_keys
            else None
        )
        if reason:
            if is_new:
                footprint.remove(slot)
            rejected.append(AdmissionDecision(key, reason))
        else:
            kept[key] = incoming[key]
            admitted.append(key)

    schema = "listing_id string, operation string, currency string"
    keys = eligible.sparkSession.createDataFrame(
        [(key.listing_id, key.operation, key.currency) for key in sorted(kept)], schema
    )
    combined = existing.unionByName(eligible).join(keys, list(IDENTITY_COLUMNS), "left_semi")
    tie = F.to_json(F.struct(*[F.col(name) for name in existing.columns]))
    window = Window.partitionBy(*IDENTITY_COLUMNS).orderBy(F.col("observed_at").desc(), tie.asc())
    current = (
        combined.withColumn("__catalogue_rank", F.row_number().over(window))
        .filter(F.col("__catalogue_rank") == 1)
        .drop("__catalogue_rank")
    )
    total, weekly = counts()
    return CatalogueSelection(
        current, tuple(admitted), refreshed, retired, tuple(rejected), len(kept), total, weekly
    )
