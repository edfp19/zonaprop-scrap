import json
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from zona_oportunidades.operations.capacity import (
    AdmissionDecision,
    CapacityPolicy,
    JobEstimate,
    ProviderUsage,
    RunMeasurement,
    RunnerCapacity,
    assess_admission,
    estimate_job,
    load_policy,
    load_usage,
)

NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)
POLICY = CapacityPolicy(project_id="teaching")
USAGE = ProviderUsage(
    project_id="teaching",
    observed_at=NOW,
    verified=True,
    object_quota_bytes=1_000_000_000,
    object_used_bytes=100_000_000,
    sql_quota_bytes=1_000_000_000,
    sql_used_bytes=100_000_000,
    monthly_compute_quota_cu_hours=100,
    monthly_compute_used_cu_hours=10,
    monthly_transfer_quota_bytes=5_000_000_000,
    monthly_transfer_used_bytes=100_000_000,
    batch_compute_used_cu_hours=2,
    batch_transfer_used_bytes=10_000_000,
)
JOB = JobEstimate(
    silver_bytes=5_000_000,
    compaction_output_bytes=10_000_000,
    sql_replacement_bytes=30_000_000,
    sql_index_bytes=10_000_000,
    sql_write_overhead_bytes=20_000_000,
    compute_cu_hours=1,
    transfer_bytes=20_000_000,
    runner_input_bytes=10_000_000,
    runner_staging_bytes=15_000_000,
    runner_shuffle_bytes=30_000_000,
)
RUNNER = RunnerCapacity(free_bytes=100_000_000)


def decide(
    usage: ProviderUsage = USAGE, job: JobEstimate = JOB, runner: RunnerCapacity = RUNNER
) -> AdmissionDecision:
    return assess_admission(POLICY, usage, job, runner, now=NOW)


def test_reserves_cover_coexisting_generations_and_all_runner_files() -> None:
    result = decide()
    assert result.allowed
    resources = {resource.resource: resource for resource in result.resources}
    assert resources["object_storage"].peak == 135_000_000
    assert resources["sql_storage"].peak == 160_000_000
    assert resources["monthly_compute"].peak == 63
    assert resources["monthly_transfer"].peak == 3_030_000_000
    assert JOB.runner_peak_bytes == 55_000_000


@pytest.mark.parametrize("used, allowed", [(765_000_000, True), (765_000_001, False)])
def test_object_peak_at_hard_limit_is_allowed(used: int, allowed: bool) -> None:
    assert decide(replace(USAGE, object_used_bytes=used)).allowed is allowed


@pytest.mark.parametrize("used, allowed", [(740_000_000, True), (740_000_001, False)])
def test_sql_peak_not_final_snapshot_size(used: int, allowed: bool) -> None:
    assert decide(replace(USAGE, sql_used_bytes=used)).allowed is allowed


@pytest.mark.parametrize("used, warning", [(599_999_999, False), (600_000_000, True)])
def test_warning_is_inclusive(used: int, warning: bool) -> None:
    result = decide(replace(USAGE, object_used_bytes=used))
    assert result.allowed
    assert result.resources[0].warning is warning


@pytest.mark.parametrize("seconds, allowed", [(3600, True), (3601, False), (-1, False)])
def test_usage_freshness(seconds: int, allowed: bool) -> None:
    assert decide(replace(USAGE, observed_at=NOW - timedelta(seconds=seconds))).allowed is allowed


def test_missing_unverified_wrong_project_and_billing_month_fail_closed() -> None:
    assert not assess_admission(POLICY, None, JOB, RUNNER, now=NOW).allowed
    for usage in (replace(USAGE, verified=False), replace(USAGE, project_id="other")):
        assert not decide(usage).allowed
    old = replace(USAGE, observed_at=datetime(2026, 9, 30, 23, 59, tzinfo=UTC))
    result = assess_admission(POLICY, old, JOB, RUNNER, now=datetime(2026, 10, 1, tzinfo=UTC))
    assert not result.allowed
    assert any("billing month" in reason for reason in result.reasons)


@pytest.mark.parametrize("used, cost, allowed", [(79, 1, True), (80, 1, False), (79, 2, False)])
def test_compute_current_and_projected_threshold(used: float, cost: float, allowed: bool) -> None:
    assert (
        decide(
            replace(USAGE, monthly_compute_used_cu_hours=used), replace(JOB, compute_cu_hours=cost)
        ).allowed
        is allowed
    )


@pytest.mark.parametrize("batch, allowed", [(19, True), (19.01, False)])
def test_compute_batch_budget_preserves_serving_reserve(batch: float, allowed: bool) -> None:
    assert (
        decide(
            replace(USAGE, monthly_compute_used_cu_hours=30, batch_compute_used_cu_hours=batch)
        ).allowed
        is allowed
    )


@pytest.mark.parametrize("used, allowed", [(3_980_000_000, True), (4_000_000_000, False)])
def test_transfer_exhaustion_is_independent_of_compute(used: int, allowed: bool) -> None:
    assert decide(replace(USAGE, monthly_transfer_used_bytes=used)).allowed is allowed


def test_batch_transfer_budget() -> None:
    assert not decide(
        replace(
            USAGE, monthly_transfer_used_bytes=1_000_000_000, batch_transfer_used_bytes=990_000_000
        )
    ).allowed


def test_batch_budget_still_applies_when_project_quota_has_plenty_of_room() -> None:
    assert not decide(
        replace(
            USAGE,
            monthly_compute_quota_cu_hours=200,
            monthly_compute_used_cu_hours=60,
            batch_compute_used_cu_hours=20,
        )
    ).allowed
    assert not decide(
        replace(
            USAGE,
            monthly_transfer_quota_bytes=10_000_000_000,
            monthly_transfer_used_bytes=2_000_000_000,
            batch_transfer_used_bytes=POLICY.batch_transfer_budget_bytes,
        )
    ).allowed


def test_cost_estimates_can_exactly_cover_output_and_report_warning_boundaries() -> None:
    job = replace(JOB, transfer_bytes=JOB.silver_bytes + JOB.compaction_output_bytes)
    usage = replace(
        USAGE, monthly_compute_used_cu_hours=60, monthly_transfer_used_bytes=3_000_000_000
    )
    result = decide(usage, job)
    assert result.allowed is True
    resources = {item.resource: item for item in result.resources}
    for name, used, limit in (
        ("monthly_compute", 60, usage.monthly_compute_quota_cu_hours),
        ("monthly_transfer", 3_000_000_000, usage.monthly_transfer_quota_bytes),
    ):
        item = resources[name]
        assert item.warning is True and item.allowed is True
        assert item.used == used and item.limit == limit
    assert result.reasons == ()


def test_default_clock_is_aware_and_missing_usage_has_structured_reasons() -> None:
    result = assess_admission(POLICY, None, JOB, RUNNER)
    assert result.allowed is False
    assert isinstance(result.reasons, tuple) and result.reasons


@pytest.mark.parametrize("free, allowed", [(55_000_000, True), (54_999_999, False)])
def test_runner_peak(free: int, allowed: bool) -> None:
    assert decide(runner=replace(RUNNER, free_bytes=free)).allowed is allowed


def test_missing_runner_and_understated_estimates_block() -> None:
    assert not assess_admission(POLICY, USAGE, JOB, None, now=NOW).allowed
    for job in (
        replace(JOB, silver_bytes=4_999_999),
        replace(JOB, transfer_bytes=1),
        replace(JOB, runner_input_bytes=1),
        replace(JOB, runner_staging_bytes=1),
    ):
        assert not decide(job=job).allowed


@pytest.mark.parametrize(
    "count, size, allowed", [(1, 10_000_000, True), (2, 10_000_000, False), (1, 10_000_001, False)]
)
def test_quarantine_must_fit_another_failed_job(count: int, size: int, allowed: bool) -> None:
    assert decide(runner=RunnerCapacity(100_000_000, count, size)).allowed is allowed


def test_job_reserve_uses_worst_recent_sample_and_cold_start_floor() -> None:
    costs = asdict(JOB)
    costs.pop("silver_bytes")
    assert estimate_job(POLICY, (), **costs).silver_bytes == 5_000_000
    runs = (RunMeasurement(10, 200_000), RunMeasurement(100, 100_000))
    assert estimate_job(POLICY, runs, **costs).silver_bytes == 10_000_000


@pytest.mark.parametrize("value", [-1, True, float("nan"), float("inf"), "unknown", None])
def test_unsafe_usage_values_are_rejected(value: object) -> None:
    with pytest.raises(ValueError):
        replace(USAGE, object_used_bytes=value)


def test_usage_consistency_and_timestamp_validation() -> None:
    for changes in (
        {"batch_compute_used_cu_hours": 11},
        {"monthly_transfer_quota_bytes": 0},
        {"batch_transfer_used_bytes": 100_000_001},
        {"object_used_bytes": 1.5},
        {"observed_at": NOW.replace(tzinfo=None)},
        {"verified": "true"},
    ):
        with pytest.raises(ValueError):
            replace(USAGE, **changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"max_records": 251},
        {"max_bronze_bytes": 10_000_001},
        {"silver_floor_bytes": 1},
        {"max_failed_extracts": 3},
        {"failed_bronze_reserve_bytes": 19_999_999},
        {"warning_fraction": 0.8},
        {"project_id": ""},
        {"max_records": True},
    ],
)
def test_invalid_policy(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        replace(POLICY, **changes)


def test_json_contract_rejects_unknown_missing_duplicate_fields(tmp_path: Path) -> None:
    path = tmp_path / "input.json"
    path.write_text('{"project_id": "teaching"}')
    assert load_policy(path) == POLICY
    value = {**asdict(USAGE), "observed_at": NOW.isoformat()}
    path.write_text(json.dumps(value))
    assert load_usage(path) == USAGE
    for invalid in (
        {**value, "guess": 1},
        {k: v for k, v in value.items() if k != "verified"},
        {**value, "observed_at": "yesterday"},
        [],
    ):
        path.write_text(json.dumps(invalid))
        with pytest.raises(ValueError):
            load_usage(path)
    path.write_text('{"project_id":"one", "project_id":"two"}')
    with pytest.raises(ValueError, match="duplicate"):
        load_policy(path)
