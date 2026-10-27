from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg

from zona_oportunidades.operations.maintenance import prune_metadata


def test_metadata_gc_preserves_active_and_unresolved_history(postgres_dsn: str) -> None:
    now = datetime.now(UTC)
    old = now - timedelta(days=100)
    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        for name in ("pipeline_storage", "bronze_quarantine", "pipeline_budget"):
            conn.execute(Path(f"infra/postgres/{name}.sql").read_text())
        for identity, state, when in (
            ("active", "active", old),
            ("old-deleted", "deleted", old),
            ("recent-deleted", "deleted", now),
            ("pending", "preparing", old),
        ):
            conn.execute(
                "INSERT INTO ops.silver_generations "
                "(generation_id, prefix, state, created_at, manifest) VALUES (%s,%s,%s,%s,'{}')",
                (identity, f"silver/{identity}", state, when),
            )
            conn.execute(
                "INSERT INTO ops.silver_source_receipts VALUES (%s,%s,%s,'{}')",
                (identity, identity, when),
            )
        for index, state in ((1, "preserved"), (2, "deleted")):
            identity = f"00000000-0000-0000-0000-{index:012d}"
            conn.execute(
                "INSERT INTO ops.pipeline_runs (run_id,started_at,finished_at,status) "
                "VALUES (%s,%s,%s,'FALLIDO')",
                (identity, old, old),
            )
            conn.execute(
                "INSERT INTO ops.bronze_quarantine "
                "(run_id,reserved_bytes,state,created_at,manifest,prefix) "
                "VALUES (%s,100,%s,%s,'{}',%s)",
                (identity, state, old, identity),
            )
        prune_metadata(conn, now=now)
        assert {
            row[0] for row in conn.execute("SELECT generation_id FROM ops.silver_generations")
        } == {"active", "recent-deleted", "pending"}
        assert conn.execute("SELECT count(*) FROM ops.silver_source_receipts").fetchone() == (3,)
        assert conn.execute("SELECT count(*) FROM ops.pipeline_runs").fetchone() == (1,)
        assert conn.execute("SELECT state FROM ops.bronze_quarantine").fetchone() == ("preserved",)
