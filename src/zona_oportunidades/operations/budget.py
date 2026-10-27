"""Persist conservative job costs so repeated manual runs cannot reuse a quota snapshot."""

from datetime import UTC, datetime
from typing import Any

import psycopg

from .capacity import CapacityPolicy, JobEstimate, ProviderUsage


def claim_job(
    connection: psycopg.Connection[Any],
    policy: CapacityPolicy,
    usage: ProviderUsage,
    job: JobEstimate,
    mode: str,
    *,
    now: datetime,
) -> None:
    """Caller holds the storage lock; estimates remain charged even if a job fails.

    Application batch budgets use UTC calendar months; provider totals are checked
    independently. A fresh verified snapshot is required for every applied job.
    """
    month = now.astimezone(UTC).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    row = connection.execute(
        "SELECT COALESCE(sum(compute_cu_hours), 0), COALESCE(sum(transfer_bytes), 0) "
        "FROM ops.pipeline_budget_claims WHERE project_id = %s AND claimed_at >= %s",
        (policy.project_id, month),
    ).fetchone()
    if row is None:
        raise ValueError("batch budget history is unavailable")
    if (
        float(row[0]) + job.compute_cu_hours > policy.batch_compute_budget_cu_hours
        or int(row[1]) + job.transfer_bytes > policy.batch_transfer_budget_bytes
    ):
        raise ValueError("reserved monthly batch budget is exhausted")
    try:
        with connection.transaction():
            connection.execute(
                "INSERT INTO ops.pipeline_budget_claims "
                "(project_id, observed_at, claimed_at, mode, compute_cu_hours, transfer_bytes) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    policy.project_id,
                    usage.observed_at,
                    now,
                    mode,
                    job.compute_cu_hours,
                    job.transfer_bytes,
                ),
            )
    except psycopg.errors.UniqueViolation as error:
        raise ValueError("provider usage snapshot already used; verify fresh usage") from error
