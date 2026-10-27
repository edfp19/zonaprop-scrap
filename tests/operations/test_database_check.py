import json
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from zona_oportunidades.operations import database_check


@pytest.mark.parametrize(
    "role, denied, passed",
    [
        ("pipeline_writer", None, True),
        ("web_reader", None, False),
        ("pipeline_writer", ("gold.opportunities", "DELETE"), False),
        ("pipeline_writer", ("ops.pipeline_runs", "INSERT"), False),
    ],
)
def test_writer_check_rejects_wrong_identity_and_incomplete_grants(
    monkeypatch, role, denied, passed
):
    queries = []

    def execute(query, params=None):
        queries.append(query)
        assert query.startswith(("SET ", "SELECT "))
        if "has_table_privilege" in query:
            return SimpleNamespace(fetchone=lambda: (params != denied,))
        return SimpleNamespace(fetchone=lambda: (role, 96, datetime.now(UTC), 9_000_000))

    @contextmanager
    def lock(dsn):
        yield SimpleNamespace(execute=execute)

    monkeypatch.setattr(database_check, "storage_lock", lock)
    result = database_check.check_database("private")
    assert result.passed is passed and result.gold_rows == 96
    assert queries[0] == "SET default_transaction_read_only = on"


def test_driver_errors_do_not_expose_credentials(monkeypatch, capsys):
    monkeypatch.setenv("DATABASE_URL", "postgresql://writer:PRIVATE_PASSWORD@example/db")

    def fail(dsn):
        raise RuntimeError(f"Connection failed: {dsn}")

    monkeypatch.setattr(database_check, "check_database", fail)
    assert database_check.main() == 1
    output = capsys.readouterr().out
    assert "PRIVATE_PASSWORD" not in output
    assert json.loads(output)["error_type"] == "RuntimeError"
