from datetime import UTC, datetime, timedelta

import pytest

from zona_oportunidades.listings.retention import utc_week_start, weekly_observations
from zona_oportunidades.listings.runner import create_local_spark

AS_OF = datetime(2026, 10, 28, 12, tzinfo=UTC)
MONDAY = datetime(2026, 10, 26, tzinfo=UTC)
SCHEMA = """listing_id string, operation string, currency string, observed_at timestamp,
    observation_id string, raw_json string, run_id string, bronze_path string,
    quality_flags array<string>, raw_observation_count long"""


@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    session = create_local_spark()
    yield session
    session.stop()


def row(identity="a", when=AS_OF, observation="one", operation="VENTA", currency="USD"):
    return (identity, operation, currency, when, observation, observation, "run", "/bronze", [], 1)


def test_five_utc_monday_weeks_and_no_future_even_in_non_utc_session(spark):
    start = MONDAY - timedelta(weeks=4)
    frame = spark.createDataFrame(
        [
            row("start", start),
            row("expired", start - timedelta(microseconds=1)),
            row("now", AS_OF),
            row("future", AS_OF + timedelta(microseconds=1)),
            row("sunday", MONDAY - timedelta(microseconds=1)),
            row("sunday", MONDAY, "monday"),
        ],
        SCHEMA,
    )
    spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    try:
        result = weekly_observations(frame, AS_OF)
        retained = result.retained.collect()
        assert len(retained) == 4
        assert {item.listing_id for item in retained} == {"start", "now", "sunday"}
        assert [item.listing_id for item in result.expired.collect()] == ["expired"]
        assert [item.listing_id for item in result.future.collect()] == ["future"]
        assert result.retained.schema == frame.schema
        assert result.quarantine.count() == 0
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")


def test_latest_is_per_currency_operation_week_and_ties_preserve_lineage(spark):
    rows = [
        row(when=MONDAY, observation="old"),
        row(observation="z"),
        row(observation="a"),
        row(operation="ALQUILER"),
        row(currency="ARS"),
        row(when=MONDAY - timedelta(days=1), observation="previous-week"),
    ]
    frame = spark.createDataFrame(rows, SCHEMA)
    first = weekly_observations(frame.repartition(2), AS_OF).retained.collect()
    second = weekly_observations(
        spark.createDataFrame(list(reversed(rows)), SCHEMA).repartition(3), AS_OF
    ).retained.collect()
    assert {tuple(item)[:8] for item in first} == {tuple(item)[:8] for item in second}
    assert len(first) == 4
    assert "a" in {item.raw_json for item in first}
    selected = next(item for item in first if item.raw_json == "a")
    assert selected.run_id == "run" and selected.bronze_path == "/bronze"
    assert selected.raw_observation_count == 1 and selected.quality_flags == []
    assert "old" not in {item.observation_id for item in first}


def test_corrupt_identity_rows_are_individually_quarantined_not_collapsed(spark):
    rows = [
        row(None, observation="bad-a"),
        row(None, observation="bad-b"),
        row("", observation="blank"),
        row(operation=None),
        row(currency=None),
        row(when=None),
        row("flagged"),
    ]
    result = weekly_observations(spark.createDataFrame(rows, SCHEMA), AS_OF)
    assert result.quarantine.count() == 6
    assert {item.raw_json for item in result.quarantine.collect()} >= {"bad-a", "bad-b"}
    assert [item.listing_id for item in result.retained.collect()] == ["flagged"]


def test_week_requires_aware_clock():
    with pytest.raises(ValueError, match="timezone-aware"):
        utc_week_start(datetime(2026, 10, 28))


def test_same_observation_keeps_first_currency_price_as_gold_does(spark):
    frame = spark.createDataFrame(
        [row(observation="a") + (1,), row(observation="z") + (0,)],
        SCHEMA + ", price_index int",
    )
    retained = weekly_observations(frame, AS_OF).retained.collect()
    assert len(retained) == 1
    assert retained[0].price_index == 0


def test_actual_silver_projection_preserves_every_column_and_raw_lineage(spark, tmp_path):
    import json

    from zona_oportunidades.listings.transforms import listing_observations

    raw = json.dumps(
        {
            "schema_version": 1,
            "run_id": "source-run",
            "collected_at": AS_OF.isoformat(),
            "payload": {
                "postingId": "42",
                "priceOperationTypes": [
                    {
                        "operationType": {"name": "Venta", "operationTypeId": "1"},
                        "prices": [{"amount": "100", "currency": "USD"}],
                    }
                ],
            },
        }
    )
    path = tmp_path / "synthetic.jsonl"
    path.write_text(raw + "\n")
    silver = listing_observations(spark, str(path), "2026-10-28", "trusted-run")
    retained = weekly_observations(silver, AS_OF).retained
    assert retained.schema == silver.schema
    assert retained.collect() == silver.collect()
    selected = retained.first()
    assert selected.raw_json == raw
    assert selected.source_envelope_run_id == "source-run" and selected.run_id == "trusted-run"
    assert selected.bronze_path == str(path)
    assert selected.quality_flags  # Retention must not discard flagged observations.
