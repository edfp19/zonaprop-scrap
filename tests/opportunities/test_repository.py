from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import psycopg
import pytest

from zona_oportunidades.opportunities.domain import Opportunity
from zona_oportunidades.opportunities.repository import GoldRepository


def candidate() -> Opportunity:
    return Opportunity(
        listing_id="42",
        operation="VENTA",
        property_type="DEPARTAMENTO",
        title="Depto",
        url="https://example.test/42",
        area_name="Palermo",
        parent_area_name="CABA",
        rooms=2,
        covered_area_m2=50,
        total_area_m2=60,
        price_usd=Decimal("80000"),
        price_per_m2_usd=1600,
        comparable_average_usd=Decimal("100000"),
        comparable_median_usd=Decimal("95000"),
        comparable_count=10,
        collected_at=datetime(2026, 10, 26, tzinfo=UTC),
        run_id="source-run",
        quality_flags=["coordinates_precision_unknown"],
        latitude=-34.6,
        longitude=-58.4,
        photo_url="https://images.example.test/property.jpg",
    )


def test_transactional_refresh_migration_and_failed_publish_preserve_snapshot(
    postgres_dsn: str,
) -> None:
    repository = GoldRepository(postgres_dsn)
    sale = candidate()
    rental = replace(sale, operation="ALQUILER", price_usd=Decimal("600"))
    result = repository.replace([sale, rental], 10, sale.collected_at)
    assert result.records == 2 and result.candidates == 2
    assert result.computed_at == sale.collected_at
    # Retrying replaces the snapshot, rather than duplicating it.
    assert repository.replace([sale, rental], 10, sale.collected_at).records == 2

    def interrupted() -> Iterator[Opportunity]:
        yield replace(sale, price_usd=Decimal("1"))
        raise RuntimeError("interrupted computation")

    with pytest.raises(RuntimeError, match="interrupted computation"):
        repository.replace(interrupted(), 10, sale.collected_at)
    with psycopg.connect(postgres_dsn) as connection:
        rows = connection.execute(
            "SELECT operation, price_usd FROM gold.opportunities ORDER BY operation"
        ).fetchall()
    assert rows == [("ALQUILER", 600.0), ("VENTA", 80000.0)]
    with psycopg.connect(postgres_dsn) as connection:
        assert connection.execute(
            "SELECT latitude, longitude, photo_url FROM gold.opportunities "
            "WHERE operation = 'VENTA'"
        ).fetchone() == (-34.6, -58.4, "https://images.example.test/property.jpg")
    assert repository.replace([], 10, sale.collected_at).records == 0
    with psycopg.connect(postgres_dsn) as connection:
        assert connection.execute("SELECT count(*) FROM gold.opportunities").fetchone() == (0,)


def test_repeatable_migration_preserves_phase_one_rows(postgres_dsn: str) -> None:
    from pathlib import Path

    repository = GoldRepository(postgres_dsn)
    sale = candidate()
    repository.replace([sale], 10, sale.collected_at)
    with psycopg.connect(postgres_dsn) as connection:
        connection.execute("ALTER TABLE gold.opportunities DROP CONSTRAINT opportunities_pkey")
        connection.execute("ALTER TABLE gold.opportunities ADD PRIMARY KEY (listing_id)")
        connection.execute("ALTER TABLE gold.opportunities DROP COLUMN latitude")
        migration = Path("infra/postgres/migrate_gold.sql").read_text()
        connection.execute(migration)
        connection.execute(migration)
        assert connection.execute("SELECT listing_id FROM gold.opportunities").fetchall() == [
            ("42",)
        ]
    assert (
        repository.replace(
            [sale, replace(sale, operation="ALQUILER")], 10, sale.collected_at
        ).records
        == 2
    )


def test_writer_can_publish_without_ddl_and_reader_keeps_select(postgres_dsn: str) -> None:
    from pathlib import Path

    # Roll back cluster-level role setup along with all test grants and writes.
    with psycopg.connect(postgres_dsn) as connection, connection.transaction(force_rollback=True):
        connection.execute(Path("infra/postgres/web_reader.sql").read_text())
        connection.execute(Path("infra/postgres/pipeline_writer.sql").read_text())
        connection.execute("SET LOCAL ROLE pipeline_writer")
        connection.execute("DELETE FROM gold.opportunities")
        connection.execute(
            "INSERT INTO ops.pipeline_runs (run_id, started_at, status) "
            "VALUES ('00000000-0000-0000-0000-000000000001', now(), 'EN_PROGRESO')"
        )
        connection.execute("UPDATE ops.pipeline_runs SET bronze_records = 1")
        for forbidden in (
            "ALTER TABLE gold.opportunities ADD COLUMN forbidden TEXT",
            "CREATE TABLE public.forbidden (id integer)",
            "CREATE TEMP TABLE forbidden (id integer)",
            "TRUNCATE ops.pipeline_runs",
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege), connection.transaction():
                connection.execute(forbidden)
        connection.execute("RESET ROLE")
        connection.execute("SET LOCAL ROLE web_reader")
        assert connection.execute("SELECT count(*) FROM ops.pipeline_runs").fetchone() == (1,)
        assert connection.execute("SELECT count(*) FROM gold.opportunities").fetchone() == (0,)
        with pytest.raises(psycopg.errors.InsufficientPrivilege), connection.transaction():
            connection.execute("DELETE FROM gold.opportunities")
