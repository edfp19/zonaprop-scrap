from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from zona_oportunidades.listings.runner import create_local_spark
from zona_oportunidades.opportunities.catalogue import (
    CatalogueKey,
    CataloguePolicy,
    HistoryFootprint,
    WeeklyKey,
    select_catalogue,
)

AS_OF = datetime(2026, 10, 28, 12, tzinfo=UTC)
WEEK = datetime(2026, 10, 26, tzinfo=UTC)
SCHEMA = """listing_id string, operation string, currency string, observed_at timestamp,
    price decimal(18,2), raw_json string, observation_id string"""


@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    session = create_local_spark()
    yield session
    session.stop()


def key(identity):
    return CatalogueKey(identity, "VENTA", "USD")


def frame(spark, *identities, when=AS_OF, price="100"):
    return spark.createDataFrame(
        [
            (identity, "VENTA", "USD", when, Decimal(price), '{"lineage":"kept"}', identity)
            for identity in identities
        ],
        SCHEMA,
    )


def footprint(*identities, week=WEEK, quarantine=0):
    return HistoryFootprint(
        frozenset(WeeklyKey(key(item), week) for item in identities), quarantine
    )


def test_refresh_at_cap_preserves_absent_slots_and_rejects_new_admissions(spark):
    existing = frame(spark, "a", "missing", when=AS_OF - timedelta(days=1))
    result = select_catalogue(
        frame(spark, "a", "b", price="50"),
        existing,
        footprint("a", "missing"),
        CataloguePolicy(2, 10, 50, 35),
        AS_OF,
    )
    rows = {item.listing_id: item for item in result.current.collect()}
    assert set(rows) == {"a", "missing"}
    assert rows["a"].price == Decimal("50") and rows["missing"].price == Decimal("100")
    assert result.refreshed == (key("a"),)
    assert result.retired == () and result.admitted == ()
    assert result.rejected[0].key == key("b")
    assert result.rejected[0].reason == "current_capacity"
    assert result.current_count == 2 and result.history_count == 2
    assert result.current.schema == existing.schema


def test_age_only_retirement_and_price_independent_deterministic_admission(spark):
    old = frame(spark, "expired", when=AS_OF - timedelta(days=35, microseconds=1))
    incoming = frame(spark, "b", price="1").unionByName(frame(spark, "a", price="999"))
    result = select_catalogue(
        incoming.repartition(2), old, footprint(), CataloguePolicy(1, 10, 50, 35), AS_OF
    )
    assert result.admitted == (key("a"),) and result.retired == (key("expired"),)
    assert [item.listing_id for item in result.current.collect()] == ["a"]
    reversed_prices = frame(spark, "a", price="1").unionByName(frame(spark, "b", price="999"))
    again = select_catalogue(
        reversed_prices, old, footprint(), CataloguePolicy(1, 10, 50, 35), AS_OF
    )
    assert again.admitted == result.admitted


@pytest.mark.parametrize(
    ("history", "policy", "reason"),
    [
        (footprint("retired"), CataloguePolicy(2, 1, 10, 35), "weekly_capacity"),
        (
            footprint("retired", week=WEEK - timedelta(weeks=1)),
            CataloguePolicy(2, 10, 1, 35),
            "history_capacity",
        ),
        (footprint(quarantine=1), CataloguePolicy(2, 10, 1, 35), "history_capacity"),
    ],
)
def test_historical_retired_keys_and_quarantine_consume_hard_budgets(
    spark, history, policy, reason
):
    result = select_catalogue(frame(spark, "new"), frame(spark), history, policy, AS_OF)
    assert result.current.count() == 0
    assert result.rejected[0].reason == reason
    assert result.history_count == 1


def test_refresh_cannot_silently_discard_history_to_fit_budget(spark):
    existing = frame(spark, "a", when=AS_OF - timedelta(weeks=1))
    with pytest.raises(ValueError, match="exceeds limits"):
        select_catalogue(
            frame(spark, "a"),
            existing,
            footprint("a", week=WEEK - timedelta(weeks=1)),
            CataloguePolicy(1, 1, 1, 35),
            AS_OF,
        )


def test_same_identity_history_cannot_inflate_current_peer_rows(spark):
    history = HistoryFootprint(
        frozenset(WeeklyKey(key("a"), WEEK - timedelta(weeks=offset)) for offset in range(5))
    )
    existing = frame(spark, "a", when=AS_OF - timedelta(days=1))
    result = select_catalogue(
        frame(spark, "a", "b"), existing, history, CataloguePolicy(2, 2, 6, 35), AS_OF
    )
    assert {item.listing_id for item in result.current.collect()} == {"a", "b"}
    assert result.current_count == 2 and result.history_count == 6


def test_freshness_boundary_and_stale_refresh_do_not_regress_current(spark):
    existing = frame(spark, "boundary", when=AS_OF - timedelta(days=35)).unionByName(
        frame(spark, "newer", price="200")
    )
    result = select_catalogue(
        frame(spark, "newer", when=AS_OF - timedelta(days=1), price="1"),
        existing,
        footprint(),
        CataloguePolicy(2, 2, 10, 35),
        AS_OF,
    )
    assert result.retired == () and result.refreshed == ()
    assert next(row for row in result.current.collect() if row.listing_id == "newer").price == 200


def test_invalid_limits_fail_before_spark():
    with pytest.raises(ValueError, match="nonnegative"):
        CataloguePolicy(-1, 1, 1, 35)


@pytest.mark.parametrize("invalid", ["null", "future", "duplicate"])
def test_catalogue_boundary_rejects_invalid_or_noncurrent_frames(spark, invalid):
    incoming = frame(spark, "a")
    if invalid == "null":
        incoming = frame(spark, None)
    elif invalid == "future":
        incoming = frame(spark, "a", when=AS_OF + timedelta(microseconds=1))
    else:
        incoming = incoming.unionByName(incoming)
    with pytest.raises(ValueError):
        select_catalogue(incoming, frame(spark), footprint(), CataloguePolicy(2, 2, 10, 35), AS_OF)


def test_aged_out_history_is_rejected_rather_than_silently_trimmed(spark):
    with pytest.raises(ValueError, match="retained window"):
        select_catalogue(
            frame(spark),
            frame(spark),
            footprint("old", week=WEEK - timedelta(weeks=5)),
            CataloguePolicy(2, 2, 10, 35),
            AS_OF,
        )
