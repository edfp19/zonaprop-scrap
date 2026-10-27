import os
import uuid
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from zona_oportunidades.shared.config import get_settings


@pytest.fixture()
def postgres_dsn() -> Iterator[str]:
    if os.environ.get("RUN_POSTGRES_TESTS") != "1":
        pytest.skip("Set RUN_POSTGRES_TESTS=1 with an accessible test PostgreSQL server")
    settings = get_settings()
    name = "test_gold_" + uuid.uuid4().hex
    with psycopg.connect(settings.postgres_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            with psycopg.connect(settings.postgres_dsn, dbname=name) as connection:
                dsn = make_conninfo(settings.postgres_dsn, dbname=name)
                schema = Path("infra/postgres/init.sql").read_text()
                # Exercise migration from the schema already present in phase-1 volumes.
                schema = schema.replace(
                    "listing_id TEXT NOT NULL,", "listing_id TEXT PRIMARY KEY,"
                ).replace(
                    "quality_flags TEXT,\n    PRIMARY KEY (listing_id, operation)",
                    "quality_flags TEXT",
                )
                connection.execute(schema)
                connection.execute(Path("infra/postgres/migrate_gold.sql").read_text())
            yield dsn
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
