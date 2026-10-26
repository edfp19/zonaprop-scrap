import json
from pathlib import Path

import pytest

from zona_oportunidades.listings.application import ReplayBronzeListings
from zona_oportunidades.listings.domain import ReplayConflictError
from zona_oportunidades.listings.runner import create_local_spark


def _record(
    *,
    posting_type="PROPERTY",
    prices=None,
    operation="Venta",
    timestamp="2026-01-02T10:00:00+00:00",
):
    return {
        "schema_version": 1,
        "extractor_version": "fixture",
        "run_id": "run-a",
        "collected_at": timestamp,
        "source_page_url": "https://example.test/search",
        "requested_operation": "VENTA",
        "requested_region": "CABA",
        "requested_currency": "USD",
        "currency_context_verified": True,
        "payload": {
            "postingId": "42",
            "postingCode": "P42",
            "postingType": posting_type,
            "title": "Depto",
            "url": "/propiedad/42",
            "house": {"type": "Apartment"},
            "realEstateType": {"realEstateTypeId": "2", "name": "Departamentos"},
            "mainFeatures": {
                "CFT1": {"value": "2", "measure": None},
                "CFT3": {"value": "1", "measure": None},
                "CFT100": {"value": "50", "measure": "m²"},
                "CFT101": {"value": "40", "measure": "m²"},
            },
            "priceOperationTypes": [
                {
                    "operationType": {"name": operation, "operationTypeId": "1"},
                    "prices": prices
                    if prices is not None
                    else [{"amount": "100000", "currency": "USD"}],
                }
            ],
            "postingLocation": {
                "address": {"name": "Calle 1", "visibility": "EXACT"},
                "location": {
                    "locationId": "CABA",
                    "name": "CABA",
                    "label": "CIUDAD",
                    "depth": 1,
                    "parent": None,
                },
                "postingGeolocation": {
                    "visibility": "EXACT",
                    "geolocation": {"latitude": "-34.6", "longitude": "-58.4"},
                },
            },
        },
    }


@pytest.fixture()
def spark():
    pytest.importorskip("pyspark")
    session = create_local_spark()
    yield session
    session.stop()


def _bronze(root: Path, run_id="run-a", lines=()):
    path = (
        root / "bronze/listings/ingestion_date=2026-01-02" / f"run_id={run_id}" / "listings.jsonl"
    )
    path.parent.mkdir(parents=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def test_flattening_preserves_operations_prices_duplicates_and_history(tmp_path, spark):
    item = _record(
        prices=[{"amount": "100000", "currency": "USD"}, {"amount": "90000000", "currency": "ARS"}]
    )
    item["payload"]["priceOperationTypes"].append(
        {
            "operationType": {"name": "Alquiler", "operationTypeId": "2"},
            "prices": [{"amount": "500", "currency": "USD"}],
        }
    )
    path = _bronze(
        tmp_path,
        lines=[
            json.dumps(item),
            json.dumps(item),
            json.dumps(_record(timestamp="2026-01-03T10:00:00+00:00")),
        ],
    )
    result = ReplayBronzeListings(spark, tmp_path).replay(path)
    rows = spark.read.parquet(str(result.output_path)).collect()
    assert result.bronze_records == 3 and result.silver_records == 4
    assert any(row.currency == "ARS" and "currency_non_usd" in row.quality_flags for row in rows)
    assert any(row.raw_observation_count == 2 for row in rows)
    assert any("duplicate_source_observation" in row.quality_flags for row in rows)
    assert {row.operation_index for row in rows} == {0, 1}
    assert all(row.source_envelope_run_id == "run-a" for row in rows)
    assert all(row.property_type == "DEPARTAMENTO" for row in rows)
    assert all(row.listing_url == "https://www.zonaprop.com.ar/propiedad/42" for row in rows)


def test_no_price_malformed_invalid_values_and_types_are_retained(tmp_path, spark):
    property_row = _record(prices=[])
    bad = _record(posting_type="DEVELOPMENT")
    bad["payload"]["postingLocation"]["postingGeolocation"]["geolocation"] = {
        "latitude": "NaN",
        "longitude": "true",
    }
    unknown = _record(posting_type="OTHER", operation="Permuta")
    unknown["payload"]["priceOperationTypes"][0]["operationType"]["operationTypeId"] = "999"
    path = _bronze(
        tmp_path, lines=[json.dumps(property_row), json.dumps(bad), json.dumps(unknown), "not json"]
    )
    result = ReplayBronzeListings(spark, tmp_path).replay(path)
    rows = spark.read.parquet(str(result.output_path)).collect()
    flags = [flag for row in rows for flag in row.quality_flags]
    assert result.silver_records == 4
    assert "price_missing" in flags and "malformed_json" in flags
    assert "development_not_individual_comparable" in flags and "posting_type_unknown" in flags
    assert "coordinates_invalid_or_outside_world" in flags and "operation_unknown" in flags


def test_blank_nonfinite_and_type_corruption_are_flagged_without_null_booleans(tmp_path, spark):
    blank = _record(prices=[{"amount": "", "currency": "USD"}], timestamp="")
    blank["payload"]["mainFeatures"]["CFT1"]["value"] = "2.5"
    blank["payload"]["mainFeatures"]["CFT100"] = {"value": "Infinity", "measure": ""}
    blank["payload"]["mainFeatures"]["CFT101"] = {"value": "", "measure": None}
    blank["payload"]["postingLocation"]["postingGeolocation"]["visibility"] = None
    blank["payload"]["postingLocation"]["postingGeolocation"]["geolocation"] = {
        "latitude": "-34.6",
        "longitude": "",
    }
    corrupt_type = _record()
    corrupt_type["schema_version"] = "not-an-integer"
    corrupt_type["payload"]["url"] = "https://example.test/listing"
    path = _bronze(tmp_path, lines=[json.dumps(blank), json.dumps(corrupt_type)])
    result = ReplayBronzeListings(spark, tmp_path).replay(path)
    rows = spark.read.parquet(str(result.output_path)).collect()
    blank_row = next(
        row for row in rows if row.source_envelope_run_id == "run-a" and row.price is None
    )
    assert {
        "price_missing",
        "rooms_invalid",
        "total_area_unknown_unit",
        "covered_area_missing",
    } <= set(blank_row.quality_flags)
    assert "timestamp_missing" in blank_row.quality_flags
    assert blank_row.has_valid_coordinates is False
    assert blank_row.within_approx_coverage is False
    assert blank_row.map_eligible is False
    assert blank_row.latitude == -34.6 and blank_row.longitude is None
    assert any("malformed_json" in row.quality_flags for row in rows)
    assert any(row.listing_url == "https://example.test/listing" for row in rows)


def test_numeric_overflow_and_source_run_mismatch_are_retained_and_flagged(tmp_path, spark):
    overflow = _record(prices=[{"amount": "1e309", "currency": "USD"}])
    overflow["run_id"] = "different-source-run"
    overflow["payload"]["mainFeatures"]["CFT1"]["value"] = "999999999999999999999999"
    overflow["payload"]["mainFeatures"]["CFT100"]["value"] = "1e309"
    path = _bronze(tmp_path, lines=[json.dumps(overflow)])

    result = ReplayBronzeListings(spark, tmp_path).replay(path)
    row = spark.read.parquet(str(result.output_path)).first()

    assert row.source_envelope_run_id == "different-source-run"
    assert row.run_id == "run-a"
    assert {
        "price_invalid",
        "rooms_invalid",
        "total_area_invalid",
        "source_run_id_mismatch",
    } <= set(row.quality_flags)


def test_existing_matching_publish_is_idempotent_and_mismatch_is_not_overwritten(tmp_path, spark):
    path = _bronze(tmp_path, lines=[json.dumps(_record())])
    usecase = ReplayBronzeListings(spark, tmp_path)
    first = usecase.replay(path)
    second = usecase.replay(path)
    assert not first.already_processed and second.already_processed
    path.write_text(
        json.dumps(_record(timestamp="2026-01-04T10:00:00+00:00")) + "\n", encoding="utf-8"
    )
    with pytest.raises(ReplayConflictError):
        usecase.replay(path)
