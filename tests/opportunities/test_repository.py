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
