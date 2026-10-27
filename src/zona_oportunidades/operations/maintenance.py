"""Expire operational metadata only after its data has been safely retired."""

from datetime import datetime, timedelta
from typing import Any

import psycopg


def prune_metadata(connection: psycopg.Connection[Any], *, now: datetime) -> None:
    """Caller holds storage_lock. Keep 90 days and every unresolved/active receipt."""
    cutoff = now - timedelta(days=90)
    with connection.transaction():
        connection.execute(
            "DELETE FROM ops.silver_source_receipts r USING ops.silver_generations g "
            "WHERE r.generation_id = g.generation_id AND g.state = 'deleted' "
            "AND r.published_at < %s AND g.created_at < %s",
            (cutoff, cutoff),
        )
        connection.execute(
            "DELETE FROM ops.silver_generations g WHERE g.state = 'deleted' "
            "AND g.created_at < %s AND NOT EXISTS "
            "(SELECT 1 FROM ops.silver_source_receipts r WHERE r.generation_id = g.generation_id)",
            (cutoff,),
        )
        connection.execute(
            "DELETE FROM ops.pipeline_runs p WHERE p.finished_at < %s "
            "AND p.status <> 'EN_PROGRESO' AND NOT EXISTS "
            "(SELECT 1 FROM ops.bronze_quarantine q "
            "WHERE q.run_id = p.run_id::text AND q.state <> 'deleted')",
            (cutoff,),
        )
        connection.execute(
            "DELETE FROM ops.bronze_quarantine WHERE state = 'deleted' AND created_at < %s",
            (cutoff,),
        )
        connection.execute(
            "DELETE FROM ops.pipeline_budget_claims WHERE claimed_at < %s",
            (cutoff,),
        )
