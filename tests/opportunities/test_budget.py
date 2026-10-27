"""Budget reservations are durable, including failed jobs and reused usage snapshots."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest

from zona_oportunidades.operations.budget import claim_job
from zona_oportunidades.operations.capacity import CapacityPolicy, JobEstimate, ProviderUsage


def test_monthly_claims_cannot_reuse_or_exceed_budget(postgres_dsn: str) -> None:
    now = datetime.now(UTC)
    policy = CapacityPolicy("test", batch_compute_budget_cu_hours=2)
    job = JobEstimate(5_000_000, 25_000_000, 1, 1, 1, 1, 100, 1, 1, 1)
    usage = ProviderUsage(
        "test",
        now,
        True,
        5_000_000_000,
        0,
        1_000_000_000,
        0,
        100,
        0,
        5_000_000_000,
        0,
        0,
        0,
    )
    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        conn.execute(Path("infra/postgres/pipeline_budget.sql").read_text())
        claim_job(conn, policy, usage, job, "ingest", now=now)
        with pytest.raises(ValueError, match="already used"):
            claim_job(conn, policy, usage, job, "maintain", now=now)
        claim_job(
            conn,
            policy,
            replace(usage, observed_at=now + timedelta(seconds=1)),
            job,
            "maintain",
            now=now,
        )
        with pytest.raises(ValueError, match="exhausted"):
            claim_job(
                conn,
                policy,
                replace(usage, observed_at=now + timedelta(seconds=2)),
                job,
                "gc",
                now=now,
            )
        assert conn.execute("SELECT count(*) FROM ops.pipeline_budget_claims").fetchone() == (2,)
        # A different project's reservations do not consume this project's budget.
        claim_job(
            conn,
            replace(policy, project_id="other"),
            replace(usage, project_id="other"),
            job,
            "gc",
            now=now,
        )
