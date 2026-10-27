"""Capacity gates use verified account usage, never a local directory as a proxy.

JSON inputs are flat objects with the dataclass field names. Usage requires every
field; policy permits omitted fields to take the documented trial defaults.
Byte units are decimal MB/GB. Old SQL/object generations are already in usage.
"""

import json
import math
from dataclasses import dataclass, fields
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, overload


def _number(name: str, value: object, *, positive: bool = False) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or (value <= 0 if positive else value < 0)
    ):
        bound = "positive" if positive else "nonnegative"
        raise ValueError(f"{name} must be a finite {bound} number")


def _integer(name: str, value: object, *, positive: bool = False) -> None:
    _number(name, value, positive=positive)
    if not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")


@dataclass(frozen=True)
class CapacityPolicy:
    project_id: str
    warning_fraction: float = 0.60
    hard_fraction: float = 0.80
    max_usage_age_seconds: int = 3600
    max_records: int = 250
    max_bronze_bytes: int = 10_000_000
    silver_floor_bytes: int = 5_000_000
    max_failed_extracts: int = 2
    failed_bronze_reserve_bytes: int = 20_000_000
    batch_compute_budget_cu_hours: float = 20.0
    serving_compute_reserve_cu_hours: float = 60.0
    batch_transfer_budget_bytes: int = 1_000_000_000
    serving_transfer_reserve_bytes: int = 3_000_000_000

    def __post_init__(self) -> None:
        if not isinstance(self.project_id, str) or not self.project_id.strip():
            raise ValueError("project_id is required")
        for field in fields(self):
            if field.name == "project_id":
                continue
            value = getattr(self, field.name)
            if field.name.endswith(("bytes", "records", "extracts", "seconds")):
                _integer(field.name, value, positive=True)
            else:
                _number(field.name, value, positive=True)
        if not 0 < self.warning_fraction < self.hard_fraction < 1:
            raise ValueError("thresholds must satisfy 0 < warning < hard < 1")
        if self.max_records > 250 or self.max_bronze_bytes > 10_000_000:
            raise ValueError("trial cap is 250 records / 10 MB Bronze")
        if self.silver_floor_bytes < 5_000_000:
            raise ValueError("Silver cold-start floor must be at least 5 MB")
        if self.max_failed_extracts > 2 or self.failed_bronze_reserve_bytes > 20_000_000:
            raise ValueError("quarantine cap is two extracts / 20 MB")
        if self.failed_bronze_reserve_bytes < self.max_failed_extracts * self.max_bronze_bytes:
            raise ValueError("failure reserve cannot hold the configured failed extracts")


@dataclass(frozen=True)
class ProviderUsage:
    project_id: str
    observed_at: datetime
    verified: bool
    object_quota_bytes: int
    object_used_bytes: int
    sql_quota_bytes: int
    sql_used_bytes: int
    monthly_compute_quota_cu_hours: float
    monthly_compute_used_cu_hours: float
    monthly_transfer_quota_bytes: int
    monthly_transfer_used_bytes: int
    batch_compute_used_cu_hours: float
    batch_transfer_used_bytes: int

    def __post_init__(self) -> None:
        if not isinstance(self.project_id, str) or not self.project_id.strip():
            raise ValueError("usage project_id is required")
        if not isinstance(self.verified, bool):
            raise ValueError("verified must be a boolean")
        if not isinstance(self.observed_at, datetime) or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at requires a timezone")
        for field in fields(self):
            if field.name in {"project_id", "observed_at", "verified"}:
                continue
            value = getattr(self, field.name)
            if field.name.endswith("bytes"):
                _integer(field.name, value, positive="quota" in field.name)
            else:
                _number(field.name, value, positive="quota" in field.name)
        if self.batch_compute_used_cu_hours > self.monthly_compute_used_cu_hours:
            raise ValueError("batch compute exceeds project compute usage")
        if self.batch_transfer_used_bytes > self.monthly_transfer_used_bytes:
            raise ValueError("batch transfer exceeds project transfer usage")


@dataclass(frozen=True)
class RunMeasurement:
    source_rows: int
    silver_bytes: int

    def __post_init__(self) -> None:
        _integer("source_rows", self.source_rows, positive=True)
        _integer("silver_bytes", self.silver_bytes, positive=True)


@dataclass(frozen=True)
class JobEstimate:
    silver_bytes: int
    compaction_output_bytes: int
    sql_replacement_bytes: int
    sql_index_bytes: int
    sql_write_overhead_bytes: int
    compute_cu_hours: float
    transfer_bytes: int
    runner_input_bytes: int
    runner_staging_bytes: int
    runner_shuffle_bytes: int

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            positive = field.name in {
                "silver_bytes",
                "sql_replacement_bytes",
                "sql_index_bytes",
                "sql_write_overhead_bytes",
                "compute_cu_hours",
                "transfer_bytes",
                "runner_input_bytes",
                "runner_staging_bytes",
                "runner_shuffle_bytes",
            }
            if field.name.endswith("bytes"):
                _integer(field.name, value, positive=positive)
            else:
                _number(field.name, value, positive=positive)

    @property
    def runner_peak_bytes(self) -> int:
        return self.runner_input_bytes + self.runner_staging_bytes + self.runner_shuffle_bytes


def estimate_job(
    policy: CapacityPolicy,
    recent: tuple[RunMeasurement, ...],
    *,
    compaction_output_bytes: int,
    sql_replacement_bytes: int,
    sql_index_bytes: int,
    sql_write_overhead_bytes: int,
    compute_cu_hours: float,
    transfer_bytes: int,
    runner_input_bytes: int,
    runner_staging_bytes: int,
    runner_shuffle_bytes: int,
) -> JobEstimate:
    """Callers supply measured cost bounds; absent compute/disk costs are not invented."""
    sampled = max((run.silver_bytes / run.source_rows for run in recent), default=0)
    return JobEstimate(
        max(policy.silver_floor_bytes, math.ceil(2 * sampled * policy.max_records)),
        compaction_output_bytes,
        sql_replacement_bytes,
        sql_index_bytes,
        sql_write_overhead_bytes,
        compute_cu_hours,
        transfer_bytes,
        runner_input_bytes,
        runner_staging_bytes,
        runner_shuffle_bytes,
    )


@dataclass(frozen=True)
class RunnerCapacity:
    free_bytes: int
    failed_extracts: int = 0
    failed_bronze_bytes: int = 0

    def __post_init__(self) -> None:
        for field in fields(self):
            _integer(field.name, getattr(self, field.name))


@dataclass(frozen=True)
class ResourceDecision:
    resource: str
    used: float
    peak: float
    limit: float
    warning: bool
    allowed: bool


@dataclass(frozen=True)
class AdmissionDecision:
    allowed: bool
    reasons: tuple[str, ...]
    resources: tuple[ResourceDecision, ...] = ()


def assess_admission(
    policy: CapacityPolicy,
    usage: ProviderUsage | None,
    job: JobEstimate,
    runner: RunnerCapacity | None,
    *,
    now: datetime | None = None,
) -> AdmissionDecision:
    now = now or datetime.now(UTC)
    if now.utcoffset() is None:
        raise ValueError("now requires a timezone")
    reasons: list[str] = []
    if usage is None:
        return AdmissionDecision(False, ("missing verified provider usage",))
    if not usage.verified or usage.project_id != policy.project_id:
        reasons.append("provider usage is unverified or belongs to another project")
    age = now - usage.observed_at
    if age < timedelta(0) or age > timedelta(seconds=policy.max_usage_age_seconds):
        reasons.append("provider usage is stale or future-dated")
    if now.astimezone(UTC).strftime("%Y-%m") != usage.observed_at.astimezone(UTC).strftime("%Y-%m"):
        reasons.append("monthly usage is from a different billing month")
    if reasons:
        return AdmissionDecision(False, tuple(reasons))
    resources: list[ResourceDecision] = []

    def storage(name: str, used: int, peak: int, quota: int) -> None:
        allowed = peak <= quota * policy.hard_fraction
        resources.append(
            ResourceDecision(
                name, used, peak, quota, used >= quota * policy.warning_fraction, allowed
            )
        )
        if not allowed:
            reasons.append(f"{name} projected peak exceeds hard threshold")

    storage(
        "object_storage",
        usage.object_used_bytes,
        usage.object_used_bytes
        + job.silver_bytes
        + job.compaction_output_bytes
        + policy.failed_bronze_reserve_bytes,
        usage.object_quota_bytes,
    )
    storage(
        "sql_storage",
        usage.sql_used_bytes,
        usage.sql_used_bytes
        + job.sql_replacement_bytes
        + job.sql_index_bytes
        + job.sql_write_overhead_bytes,
        usage.sql_quota_bytes,
    )
    for name, used, cost, quota, batch_used, batch_budget, serving_reserve in (
        (
            "monthly_compute",
            usage.monthly_compute_used_cu_hours,
            job.compute_cu_hours,
            usage.monthly_compute_quota_cu_hours,
            usage.batch_compute_used_cu_hours,
            policy.batch_compute_budget_cu_hours,
            policy.serving_compute_reserve_cu_hours,
        ),
        (
            "monthly_transfer",
            usage.monthly_transfer_used_bytes,
            job.transfer_bytes,
            usage.monthly_transfer_quota_bytes,
            usage.batch_transfer_used_bytes,
            policy.batch_transfer_budget_bytes,
            policy.serving_transfer_reserve_bytes,
        ),
    ):
        # Project usage already includes serving consumption; reserve only its unspent budget.
        peak = used + cost + max(0, serving_reserve - (used - batch_used))
        allowed = peak <= quota * policy.hard_fraction and batch_used + cost <= batch_budget
        resources.append(
            ResourceDecision(
                name, used, peak, quota, used >= quota * policy.warning_fraction, allowed
            )
        )
        if not allowed:
            reasons.append(f"{name} budget or reserved headroom exhausted")
    if job.silver_bytes < policy.silver_floor_bytes:
        reasons.append("Silver estimate is below configured floor")
    if job.transfer_bytes < job.silver_bytes + job.compaction_output_bytes:
        reasons.append("transfer estimate cannot cover object output")
    if (
        job.runner_input_bytes < policy.max_bronze_bytes
        or job.runner_staging_bytes < job.silver_bytes + job.compaction_output_bytes
    ):
        reasons.append("runner estimate cannot cover input and staged output")
    if runner is None:
        reasons.append("missing runner disk and quarantine measurement")
    else:
        if job.runner_peak_bytes > runner.free_bytes:
            reasons.append("runner disk peak exceeds free bytes")
        if (
            runner.failed_extracts >= policy.max_failed_extracts
            or runner.failed_bronze_bytes + policy.max_bronze_bytes
            > policy.failed_bronze_reserve_bytes
        ):
            reasons.append("Bronze quarantine cannot hold another failed extract")
    return AdmissionDecision(not reasons, tuple(reasons), tuple(resources))


def _read_object(path: str | Path) -> dict[str, object]:
    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON field: {key}")
            result[key] = value
        return result

    value = json.loads(Path(path).read_text(encoding="utf-8"), object_pairs_hook=unique)
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return value


def load_policy(path: str | Path) -> CapacityPolicy:
    return _parse("policy", _read_object(path))


def load_usage(path: str | Path) -> ProviderUsage:
    return _parse("usage", _read_object(path))


@overload
def _parse(kind: Literal["policy"], value: dict[str, object]) -> CapacityPolicy: ...


@overload
def _parse(kind: Literal["usage"], value: dict[str, object]) -> ProviderUsage: ...


def _parse(
    kind: Literal["policy", "usage"], value: dict[str, object]
) -> CapacityPolicy | ProviderUsage:
    cls = CapacityPolicy if kind == "policy" else ProviderUsage
    if value.keys() - {field.name for field in fields(cls)}:
        raise ValueError(f"unknown {kind} fields")
    try:
        if kind == "usage":
            observed = value.get("observed_at")
            if not isinstance(observed, str):
                raise ValueError("observed_at must be an ISO timestamp")
            value = {**value, "observed_at": datetime.fromisoformat(observed)}
        return cls(**value)  # type: ignore[arg-type]
    except TypeError as error:
        raise ValueError(f"missing or invalid {kind} fields") from error
