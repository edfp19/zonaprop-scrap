"""Verify the direct production writer connection without changing application data."""

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime

from zona_oportunidades.operations.storage import storage_lock

REQUIRED_PERMISSIONS = ("SELECT", "INSERT", "UPDATE", "DELETE")


@dataclass(frozen=True)
class DatabaseCheckResult:
    role: str
    gold_rows: int
    newest_observation_at: datetime | None
    database_bytes: int
    gold_permissions: tuple[str, ...]
    ops_permissions: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return (
            self.role == "pipeline_writer"
            and self.gold_permissions == REQUIRED_PERMISSIONS
            and self.ops_permissions == REQUIRED_PERMISSIONS
        )


def check_database(dsn: str) -> DatabaseCheckResult:
    # The shared helper rejects pooled hosts and releases its session lock on exit.
    with storage_lock(dsn) as connection:
        connection.execute("SET default_transaction_read_only = on")
        connection.execute("SET statement_timeout = '15s'")
        row = connection.execute(
            "SELECT current_user, count(*), max(collected_at), "
            "pg_database_size(current_database()) FROM gold.opportunities"
        ).fetchone()
        if row is None:
            raise ValueError("Database check returned no result")
        permissions: list[tuple[str, ...]] = []
        for table in ("gold.opportunities", "ops.pipeline_runs"):
            allowed = []
            for permission in REQUIRED_PERMISSIONS:
                grant = connection.execute(
                    "SELECT has_table_privilege(current_user, %s, %s)", (table, permission)
                ).fetchone()
                if grant and grant[0]:
                    allowed.append(permission)
            permissions.append(tuple(allowed))
        return DatabaseCheckResult(
            str(row[0]), int(row[1]), row[2], int(row[3]), permissions[0], permissions[1]
        )


def main() -> int:
    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn:
        print(json.dumps({"passed": False, "error": "PIPELINE_DATABASE_URL is not configured"}))
        return 1
    try:
        result = check_database(dsn)
    except Exception as error:
        # Never serialize driver messages, which may contain connection credentials.
        print(
            json.dumps(
                {
                    "passed": False,
                    "error_type": type(error).__name__,
                    "sqlstate": getattr(error, "sqlstate", None),
                }
            )
        )
        return 1
    print(
        json.dumps(
            asdict(result) | {"passed": result.passed, "data_changed": False}, default=str, indent=2
        )
    )
    return 0 if result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
