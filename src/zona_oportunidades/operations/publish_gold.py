"""One-off publication of a small Gold snapshot computed from verified local Silver.

This does not activate cloud Silver, delete source evidence or enable ingestion.
"""

import base64
import gzip
import io
import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from pydantic import TypeAdapter

from zona_oportunidades.operations.storage import storage_lock
from zona_oportunidades.opportunities.domain import ComparisonPolicy, GoldRunResult, Opportunity
from zona_oportunidades.opportunities.repository import GoldRepository

MAX_BYTES = 1_000_000


@dataclass(frozen=True)
class GoldSnapshot:
    expected_gold_rows: int
    expected_newest_observation: datetime | None
    opportunities: tuple[Opportunity, ...]


def decode_snapshot(encoded: str) -> GoldSnapshot:
    if len(encoded) > 60_000:
        raise ValueError("Snapshot input exceeds the manual transfer bound")
    with gzip.GzipFile(fileobj=io.BytesIO(base64.b64decode(encoded, validate=True))) as archive:
        raw = archive.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError("Snapshot exceeds 1 MB")
    snapshot = TypeAdapter(GoldSnapshot).validate_json(raw)
    if not 0 < len(snapshot.opportunities) <= 250:
        raise ValueError("Manual publication requires between 1 and 250 rows")
    keys = {(row.listing_id, row.operation) for row in snapshot.opportunities}
    if len(keys) != len(snapshot.opportunities):
        raise ValueError("Duplicate listing/operation in snapshot")
    if any(row.collected_at.utcoffset() is None for row in snapshot.opportunities):
        raise ValueError("Observation timestamps require a timezone")
    return snapshot


def publish(snapshot: GoldSnapshot, dsn: str) -> GoldRunResult:
    with storage_lock(dsn) as connection, connection.transaction():
        connection.execute("SET LOCAL statement_timeout = '30s'")
        role = connection.execute("SELECT current_user").fetchone()
        if role != ("pipeline_writer",):
            raise ValueError("Use the restricted pipeline_writer role")
        previous = connection.execute(
            "SELECT count(*), max(collected_at), pg_database_size(current_database()) "
            "FROM gold.opportunities"
        ).fetchone()
        if previous is None or previous[:2] != (
            snapshot.expected_gold_rows,
            snapshot.expected_newest_observation,
        ):
            raise ValueError("Gold changed since preparation; prepare a fresh snapshot")
        if previous[2] > 100_000_000:
            raise ValueError("Database exceeds the small manual-refresh size bound")
        result = GoldRepository(dsn).replace(
            snapshot.opportunities,
            ComparisonPolicy().minimum_count,
            datetime.now(UTC),
            connection=connection,
        )
        count = connection.execute("SELECT count(*) FROM gold.opportunities").fetchone()
        if count != (len(snapshot.opportunities),):
            raise ValueError("Publication count mismatch")
        return result


def main() -> int:
    try:
        encoded = os.environ.get("GOLD_SNAPSHOT") or sys.stdin.read(60_001).strip()
        snapshot = decode_snapshot(encoded)
        dsn = os.environ.get("DATABASE_URL", "").strip()
        if not dsn:
            raise ValueError("Writer connection is required")
        result = publish(snapshot, dsn)
        print(json.dumps(asdict(result) | {"published": True}, default=str))
        return 0
    except Exception as error:
        # Driver messages can contain credentials; emit only the type and SQLSTATE.
        print(
            json.dumps(
                {
                    "published": False,
                    "error_type": type(error).__name__,
                    "sqlstate": getattr(error, "sqlstate", None),
                }
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
