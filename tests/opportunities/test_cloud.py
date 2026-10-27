from pathlib import Path

import psycopg
import pytest


def test_web_reader_can_read_but_cannot_replace_the_snapshot(postgres_dsn: str) -> None:
    # Roll back role creation/grants as well as data, leaving the local cluster unchanged.
    with psycopg.connect(postgres_dsn) as connection:
        connection.execute(Path("infra/postgres/web_reader.sql").read_text())
        connection.execute("SET LOCAL ROLE web_reader")
        assert connection.execute("SELECT count(*) FROM gold.opportunities").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM ops.pipeline_runs").fetchone() == (0,)
        with pytest.raises(psycopg.errors.InsufficientPrivilege), connection.transaction():
            connection.execute("DELETE FROM gold.opportunities")
        connection.rollback()
