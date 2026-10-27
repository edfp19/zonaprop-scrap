import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from zona_oportunidades.listings.runner import create_local_spark
from zona_oportunidades.opportunities.domain import ComparisonPolicy
from zona_oportunidades.opportunities.transforms import comparable_statistics, current_listings

AS_OF = datetime(2026, 10, 26, 12, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    session = create_local_spark()
    yield session
    session.stop()


def observation(listing_id: str, **changes: Any) -> dict[str, Any]:
    return {
        "listing_id": listing_id,
        "operation": "VENTA",
        "currency": "USD",
        "price": Decimal("100000.00"),
        "rooms": 2,
        "covered_area_m2": 50.0,
        "total_area_m2": 60.0,
        "posting_type": "PROPERTY",
        "property_type": "DEPARTAMENTO",
        "title": "Departamento",
        "listing_url": "https://example.test/" + listing_id,
        "location_id": "palermo",
        "location_name": "Palermo",
        "parent_area_name": "CABA",
        "observed_at": AS_OF - timedelta(days=1),
        "ingestion_date": "2026-10-25",
        "run_id": "run-a",
        "source_observation_id": listing_id + "-source",
        "observation_id": listing_id + "-price",
        "price_index": 0,
        "quality_flags": [],
        "latitude": -34.6,
        "longitude": -58.4,
        "geolocation_visibility": "APPROXIMATE",
        "raw_json": "{}",
    } | changes


def dataframe(spark, rows: list[dict[str, Any]]):
    return spark.createDataFrame(
        rows,
        """listing_id string, operation string, currency string, price decimal(18,2),
        rooms int, covered_area_m2 double, total_area_m2 double, posting_type string,
        property_type string, title string, listing_url string, location_id string,
        location_name string, parent_area_name string, observed_at timestamp,
        ingestion_date string, run_id string, source_observation_id string,
        observation_id string, price_index int, quality_flags array<string>,
        latitude double, longitude double, geolocation_visibility string, raw_json string""",
    )


def test_latest_selection_precedes_eligibility_and_does_not_weight_history(spark) -> None:
    rows = [
        observation("changed", price=Decimal("50000")),
        observation("changed", observed_at=AS_OF, price=Decimal("90000")),
        observation("invalid-newer"),
        observation("invalid-newer", observed_at=AS_OF, price=None),
        observation("currency-changed"),
        observation("currency-changed", observed_at=AS_OF, currency="ARS"),
        observation("multiple-currencies", currency="ARS", price_index=0),
        observation("multiple-currencies", currency="USD", price_index=1),
        observation("future", observed_at=AS_OF + timedelta(seconds=1)),
    ]
    current = current_listings(dataframe(spark, rows), ComparisonPolicy(), AS_OF).collect()
    assert {row.listing_id for row in current} == {"changed", "multiple-currencies"}
    assert next(row for row in current if row.listing_id == "changed").price == Decimal("90000")


def test_eligibility_freshness_and_quality_are_explicit(spark) -> None:
    rows = [
        observation("valid", quality_flags=["coordinates_precision_unknown"]),
        observation("boundary", observed_at=AS_OF - timedelta(days=35)),
        observation("stale", observed_at=AS_OF - timedelta(days=35, seconds=1)),
        observation("development", posting_type="DEVELOPMENT"),
        observation("house", property_type="CASA"),
        observation("ars", currency="ARS"),
        observation("zero", price=Decimal("0")),
        observation("negative", price=Decimal("-1")),
        observation("unknown-area", location_id=None),
        observation("unknown-rooms", rooms=None),
        observation("no-area", covered_area_m2=None),
        observation("nan", covered_area_m2=float("nan")),
        observation("inf", covered_area_m2=float("inf")),
        observation("corrupt", quality_flags=["malformed_json"]),
        observation("inconsistent", quality_flags=["covered_area_exceeds_total"]),
    ]
    current = current_listings(dataframe(spark, rows), ComparisonPolicy(), AS_OF).collect()
    assert {row.listing_id for row in current} == {"valid", "boundary"}


def test_comparables_exclude_self_and_respect_group_and_inclusive_area_bounds(spark) -> None:
    rows = [
        observation(
            "target",
            price=Decimal("80000"),
            raw_json=json.dumps(
                {
                    "payload": {
                        "visiblePictures": {
                            "pictures": [
                                {
                                    "url730x532": "javascript:alert(1)",
                                    "url360x266": "https://images.example.test/property.jpg",
                                }
                            ]
                        }
                    }
                }
            ),
        ),
        observation("low-bound", covered_area_m2=42.5, price=Decimal("100000")),
        observation("high-bound", covered_area_m2=57.5, price=Decimal("120000")),
        observation("too-small", covered_area_m2=42.49),
        observation("too-big", covered_area_m2=57.51),
        observation("different-rooms", rooms=3),
        observation("different-location", location_id="belgrano"),
        observation("rental", operation="ALQUILER"),
        observation("target", operation="ALQUILER", price=Decimal("600")),
    ]
    policy = ComparisonPolicy(minimum_count=2)
    current = current_listings(dataframe(spark, rows), policy, AS_OF)
    results = comparable_statistics(current, policy).collect()
    target = next(row for row in results if row.listing_id == "target" and row.operation == "VENTA")
    assert target.comparable_count == 2
    assert target.comparable_average_usd == Decimal("110000")
    assert target.comparable_median_usd == Decimal("110000")
    assert target.photo_url == "https://images.example.test/property.jpg"
    assert target.latitude == -34.6 and target.longitude == -58.4
    assert target.geolocation_visibility == "APPROXIMATE"
    isolated = next(row for row in results if row.listing_id == "different-location")
    assert isolated.comparable_count == 0
    assert isolated.comparable_average_usd is None
    assert isolated.comparable_median_usd is None
    assert isolated.photo_url is None
    assert len([row for row in results if row.listing_id == "target"]) == 2
