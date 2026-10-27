"""Coordinator behavior with local fakes; no cloud account or live source required."""

import json
from contextlib import contextmanager
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from zona_oportunidades.operations import cloud
from zona_oportunidades.operations.capacity import (
    CapacityPolicy,
    JobEstimate,
    ProviderUsage,
    RunnerCapacity,
)
from zona_oportunidades.operations.storage import DatasetManifest, Generation
from zona_oportunidades.opportunities.catalogue import CataloguePolicy
from zona_oportunidades.opportunities.domain import GoldRunResult

NOW = datetime.now(UTC)
POLICY = cloud.CloudPolicy(
    CapacityPolicy("teaching"),
    CataloguePolicy(100, 250, 1250, 35),
    JobEstimate(
        silver_bytes=5_000_000,
        compaction_output_bytes=25_000_000,
        sql_replacement_bytes=10_000_000,
        sql_index_bytes=10_000_000,
        sql_write_overhead_bytes=20_000_000,
        compute_cu_hours=1,
        transfer_bytes=100_000_000,
        runner_input_bytes=40_000_000,
        runner_staging_bytes=60_000_000,
        runner_shuffle_bytes=100_000_000,
    ),
)
USAGE = ProviderUsage(
    "teaching",
    NOW,
    True,
    5_000_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000,
    100,
    10,
    5_000_000_000,
    100_000_000,
    2,
    10_000_000,
)
RUNNER = RunnerCapacity(1_000_000_000)


def arguments(**changes):
    return (
        dict(
            apply=True,
            policy=POLICY,
            usage=USAGE,
            runner=RUNNER,
            project_id="teaching",
            reclamation_verified=True,
            dsn="private-dsn",
            bucket="private-bucket",
            endpoint_url="https://storage.example.test",
            region="us-east-2",
            now=NOW,
        )
        | changes
    )


@pytest.mark.parametrize("mode", ["report", "seed", "ingest", "maintain", "gc"])
def test_default_dry_run_never_opens_remote_adapters(monkeypatch, mode):
    def forbidden(*args, **kwargs):
        pytest.fail("dry run opened a provider adapter")

    monkeypatch.setattr("zona_oportunidades.operations.storage.S3Store", forbidden)
    monkeypatch.setattr("zona_oportunidades.operations.storage.storage_lock", forbidden)
    result = cloud.execute(mode, **arguments(apply=False))
    assert result.allowed and not result.applied


@pytest.mark.parametrize("mode", ["seed", "ingest", "maintain", "gc"])
@pytest.mark.parametrize(
    "changes",
    [
        {"usage": None},
        {"usage": replace(USAGE, observed_at=NOW - timedelta(hours=2))},
        {"usage": replace(USAGE, verified=False)},
        {"usage": replace(USAGE, project_id="another-project")},
        {"project_id": "another-project"},
        {"reclamation_verified": False},
    ],
)
def test_unsafe_usage_and_attestation_block_before_remote_calls(monkeypatch, mode, changes):
    def forbidden(*args, **kwargs):
        pytest.fail("failed preflight opened a remote adapter")

    monkeypatch.setattr("zona_oportunidades.operations.storage.S3Store", forbidden)
    monkeypatch.setattr("zona_oportunidades.operations.storage.storage_lock", forbidden)
    with pytest.raises(ValueError):
        cloud.execute(mode, **arguments(**changes))


def test_ingestion_is_delegated_only_after_verified_capacity(monkeypatch):
    calls = []
    monkeypatch.setattr("zona_oportunidades.operations.storage.S3Store", lambda *args: "private")
    monkeypatch.setattr(
        "zona_oportunidades.operations.ingestion.ingest",
        lambda **kwargs: calls.append(kwargs) or cloud.CloudResult("ingest", True, True, ()),
    )
    assert cloud.execute("ingest", **arguments()).applied
    assert calls[0]["store"] == "private"
    assert calls[0]["policy"] == POLICY


def test_policy_requires_explicit_catalogue_caps_and_all_cost_estimates(tmp_path):
    value = asdict(POLICY)
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(value))
    assert cloud.load_cloud_policy(path) == POLICY
    for key in ("max_current_keys", "max_weekly_keys", "max_history_keys", "max_age_days"):
        path.write_text(
            json.dumps(
                value
                | {
                    "catalogue": {
                        name: item for name, item in value["catalogue"].items() if name != key
                    }
                }
            )
        )
        with pytest.raises(ValueError, match="catalogue caps are required"):
            cloud.load_cloud_policy(path)
    for invalid in (True, 0, -1, "100", None):
        path.write_text(
            json.dumps(value | {"catalogue": value["catalogue"] | {"max_current_keys": invalid}})
        )
        with pytest.raises(ValueError):
            cloud.load_cloud_policy(path)


def test_full_working_generation_and_gc_grace_are_hard_bounds():
    with pytest.raises(ValueError, match="25 MB"):
        replace(POLICY, max_working_bytes=25_000_001)
    with pytest.raises(ValueError, match="entire working generation"):
        replace(POLICY, job=replace(POLICY.job, compaction_output_bytes=24_999_999))
    with pytest.raises(ValueError, match="24 hours"):
        replace(POLICY, gc_grace_hours=23)


def test_gc_can_retire_data_under_storage_pressure_but_requires_remaining_budgets():
    usage = replace(
        USAGE, object_used_bytes=USAGE.object_quota_bytes, sql_used_bytes=USAGE.sql_quota_bytes
    )
    kwargs = dict(project_id="teaching", reclamation_verified=True, now=NOW)
    assert not cloud.preflight(POLICY, usage, RUNNER, **kwargs).allowed
    assert cloud.preflight(POLICY, usage, RUNNER, deletion_only=True, **kwargs).allowed
    assert cloud.preflight(
        POLICY, usage, replace(RUNNER, failed_extracts=2), deletion_only=True, **kwargs
    ).allowed
    assert not cloud.preflight(
        POLICY,
        replace(usage, monthly_compute_used_cu_hours=100),
        RUNNER,
        deletion_only=True,
        **kwargs,
    ).allowed
    assert not cloud.preflight(
        POLICY,
        replace(usage, verified=False),
        RUNNER,
        deletion_only=True,
        **kwargs,
    ).allowed


class FakeConnection:
    def __init__(self, events):
        self.events = events
        self.active = "old"
        self.gold = "last-good"

    @contextmanager
    def transaction(self):
        old = self.active, self.gold
        self.events.append("begin")
        try:
            yield
        except Exception:
            self.active, self.gold = old
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")


class FakeRegistry:
    def __init__(self, conn, events, *, fail_activate=False):
        self.conn, self.events = conn, events
        self.prepared = None
        self.fail_activate = fail_activate

    def prepare(self, manifest, prefix):
        self.events.append("prepare")
        self.prepared = Generation(manifest, prefix, "preparing")
        return self.prepared

    def activate(self, generation, replaces):
        assert replaces == ("old",)
        self.events.append("activate")
        self.conn.active = generation.generation_id
        if self.fail_activate:
            raise RuntimeError("activation failed")


class FakeStore:
    def __init__(self, events, *, failure=None):
        self.events, self.failure = events, failure

    def upload(self, directory, manifest):
        self.events.append("upload")
        if self.failure == "upload":
            raise RuntimeError("upload failed")
        return f"{manifest.kind}/{manifest.generation_id}"

    def download(self, generation, target):
        self.events.append("readback")
        if self.failure == "readback":
            raise RuntimeError("hash verification failed")
        return target


def manifest(identity):
    return DatasetManifest(identity, "silver", NOW, 1, ("source-run",), ())


def publish(monkeypatch, tmp_path, *, failure=None, bronze_ticket=None):
    events = []
    conn = FakeConnection(events)
    registry = FakeRegistry(conn, events, fail_activate=failure == "activate")
    store = FakeStore(events, failure=failure)

    def replace_gold(self, opportunities, minimum_count, computed_at, *, connection):
        assert connection is conn
        events.append("gold")
        connection.gold = "replacement"
        if failure == "gold":
            raise RuntimeError("Gold insertion failed")
        return GoldRunResult(1, 0, computed_at)

    monkeypatch.setattr(
        "zona_oportunidades.opportunities.repository.GoldRepository.replace", replace_gold
    )
    old = Generation(manifest("old"), "silver/old", "active")

    def invoke():
        return cloud.publish_prepared(
            tmp_path,
            manifest("new"),
            (SimpleNamespace(),),
            conn=conn,
            registry=registry,
            store=store,
            active=(old,),
            dsn="unused",
            minimum_count=10,
            as_of=NOW,
            bronze_ticket=bronze_ticket,
        )

    return invoke, events, conn, registry


def test_upload_and_readback_precede_atomic_gold_and_registry_switch(monkeypatch, tmp_path):
    invoke, events, conn, _ = publish(monkeypatch, tmp_path)
    assert invoke().records == 1
    assert events == ["prepare", "upload", "readback", "begin", "gold", "activate", "commit"]
    assert (conn.gold, conn.active) == ("replacement", "new")


@pytest.mark.parametrize("failure", ["upload", "readback", "gold", "activate"])
def test_failed_publication_keeps_old_gold_and_pointer(monkeypatch, tmp_path, failure):
    invoke, events, conn, registry = publish(monkeypatch, tmp_path, failure=failure)
    with pytest.raises(RuntimeError):
        invoke()
    assert (conn.gold, conn.active) == ("last-good", "old")
    assert registry.prepared.state == "preparing"
    assert "commit" not in events
    if failure in {"upload", "readback"}:
        assert "gold" not in events and "activate" not in events
    else:
        assert events[-1] == "rollback"


def test_empty_publication_cannot_touch_registry_or_gold(tmp_path):
    with pytest.raises(ValueError, match="empty publication"):
        cloud.publish_prepared(
            tmp_path,
            manifest("new"),
            (),
            conn=None,
            registry=None,
            store=None,
            active=(),
            dsn="unused",
            minimum_count=10,
            as_of=NOW,
        )


def test_bronze_promotion_failure_rolls_back_gold_and_registry(monkeypatch, tmp_path):
    def fail_promotion(conn, ticket):
        raise RuntimeError("promotion failed")

    monkeypatch.setattr("zona_oportunidades.operations.quarantine.promote", fail_promotion)
    invoke, events, conn, _ = publish(monkeypatch, tmp_path, bronze_ticket="reserved")
    with pytest.raises(RuntimeError, match="promotion failed"):
        invoke()
    assert (conn.gold, conn.active) == ("last-good", "old")
    assert events[-1] == "rollback"


def test_gc_never_marks_reclaimed_on_failed_object_verification():
    calls = []
    old = Generation(manifest("old"), "silver/old", "superseded")
    registry = SimpleNamespace(
        retired=lambda before: (old,),
        orphaned=lambda before: (),
        mark_deleted=lambda generation: calls.append("deleted-receipt"),
    )

    def failed_delete(generation):
        calls.append("delete")
        raise RuntimeError("object still present")

    with pytest.raises(RuntimeError, match="still present"):
        cloud.gc_generations(registry, SimpleNamespace(delete=failed_delete), before=NOW)
    assert calls == ["delete"]
    calls.clear()
    result = cloud.gc_generations(
        registry,
        SimpleNamespace(delete=lambda generation: calls.append("verified-absence")),
        before=NOW,
    )
    assert result == ("old",)
    assert calls == ["verified-absence", "deleted-receipt"]


def test_oversized_active_generation_refuses_download(monkeypatch):
    from zona_oportunidades.operations.storage import Artifact

    active = Generation(
        replace(manifest("old"), files=(Artifact("part.parquet", 25_000_001, "0" * 64),)),
        "silver/old",
        "active",
    )
    registry = SimpleNamespace(active=lambda: (active,))

    def forbidden(*args, **kwargs):
        pytest.fail("oversized generation downloaded")

    with pytest.raises(ValueError, match="download refused"):
        cloud._rewrite(
            "maintain",
            registry=registry,
            conn=None,
            store=SimpleNamespace(download=forbidden),
            policy=POLICY,
            usage=USAGE,
            runner=RUNNER,
            project_id="teaching",
            reclamation_verified=True,
            dsn="unused",
            silver_root=None,
            master=None,
            now=NOW,
        )


def test_gc_lock_covers_selection_delete_and_receipt(monkeypatch):
    events = []

    class FrozenClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW

    monkeypatch.setattr(cloud, "datetime", FrozenClock)

    @contextmanager
    def lock(dsn):
        events.append("lock")
        yield "connection"
        events.append("unlock")

    old = Generation(manifest("old"), "silver/old", "superseded")

    def retired(before):
        assert NOW - before >= timedelta(hours=23, minutes=59)
        events.append("retired")
        return (old,)

    monkeypatch.setattr("zona_oportunidades.operations.storage.storage_lock", lock)
    monkeypatch.setattr("zona_oportunidades.operations.budget.claim_job", lambda *a, **k: None)
    monkeypatch.setattr(
        "zona_oportunidades.operations.maintenance.prune_metadata", lambda *a, **k: None
    )
    monkeypatch.setattr("zona_oportunidades.operations.quarantine.cleanup_promoted", lambda *a: ())
    monkeypatch.setattr(
        "zona_oportunidades.operations.storage.DatasetRegistry",
        lambda conn: SimpleNamespace(
            retired=retired,
            orphaned=lambda before: (),
            mark_deleted=lambda generation: events.append("receipt"),
        ),
    )
    monkeypatch.setattr(
        "zona_oportunidades.operations.storage.S3Store",
        lambda *args: SimpleNamespace(delete=lambda generation: events.append("verified-delete")),
    )
    result = cloud.execute("gc", **arguments())
    assert result.deleted_generations == ("old",)
    assert events == ["lock", "retired", "verified-delete", "receipt", "unlock"]


def test_usage_expiring_while_acquiring_lock_blocks_before_registry_reads(monkeypatch):
    @contextmanager
    def lock(dsn):
        yield "connection"

    class Later(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW + timedelta(hours=2)

    monkeypatch.setattr(cloud, "datetime", Later)
    monkeypatch.setattr("zona_oportunidades.operations.storage.storage_lock", lock)
    monkeypatch.setattr("zona_oportunidades.operations.storage.S3Store", lambda *args: None)
    monkeypatch.setattr("zona_oportunidades.operations.storage.DatasetRegistry", lambda conn: None)
    with pytest.raises(ValueError, match="stale"):
        cloud.execute("maintain", **arguments())


def test_old_seed_replay_is_rejected_without_deleting_source(tmp_path):
    source = tmp_path / "ingestion_date=2020-01-01" / "run_id=old" / "_run_manifest.json"
    source.parent.mkdir(parents=True)
    source.write_text(json.dumps({"ingestion_date": "2020-01-01"}))
    with pytest.raises(ValueError, match="outside retained window"):
        cloud.seed_sources(tmp_path, NOW)
    assert source.is_file()


@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    from zona_oportunidades.listings.runner import create_local_spark

    session = create_local_spark()
    yield session
    session.stop()


def observations(spark, items):
    """Small real Silver shape: each tuple is identity, timestamp, price."""
    from decimal import Decimal

    schema = """listing_id string, operation string, currency string,
        observed_at timestamp, price decimal(18,2), rooms int, covered_area_m2 double,
        posting_type string, property_type string, title string, listing_url string,
        location_id string, location_name string, ingestion_date string, run_id string,
        source_observation_id string, observation_id string, price_index int,
        quality_flags array<string>, raw_json string"""
    return spark.createDataFrame(
        [
            (
                identity,
                "VENTA",
                "USD",
                when,
                Decimal(price) if price is not None else None,
                2,
                50.0,
                "PROPERTY",
                "DEPARTAMENTO",
                "Apartment",
                "https://example.test",
                "area",
                "Area",
                when.date().isoformat(),
                "source-run",
                f"source-{index}",
                f"row-{index}",
                0,
                [],
                '{"preserved":"source"}',
            )
            for index, (identity, when, price) in enumerate(items)
        ],
        schema,
    )


def gold_keys(spark, *identities):
    return spark.createDataFrame(
        [(identity, "VENTA", "USD") for identity in identities],
        "listing_id string, operation string, currency string",
    )


def test_maintenance_never_resurrects_old_valid_price_and_preserves_history_schema(spark):
    frame = observations(
        spark,
        [
            ("invalid-newer", NOW - timedelta(days=8), "100"),
            ("invalid-newer", NOW, None),
            ("still-valid", NOW, "200"),
        ],
    )
    result = cloud.plan_frames(
        frame,
        gold_keys(spark, "invalid-newer", "still-valid"),
        POLICY,
        cloud.ComparisonPolicy(),
        NOW,
    )
    assert {row.listing_id for row in result.current.collect()} == {"still-valid"}
    assert result.retained.count() == 3
    assert result.retained.schema == frame.schema
    assert all(row.raw_json == '{"preserved":"source"}' for row in result.retained.collect())


@pytest.mark.parametrize("kind", ["invalid-key", "future", "all-ineligible"])
def test_maintenance_unsafe_inputs_block_whole_publication(spark, kind):
    when = NOW + timedelta(seconds=1) if kind == "future" else NOW
    identity = None if kind == "invalid-key" else "listing"
    price = None if kind == "all-ineligible" else "100"
    frame = observations(spark, [(identity, when, price)])
    with pytest.raises(ValueError, match="quarantine|stale Gold"):
        cloud.plan_frames(frame, gold_keys(spark, "listing"), POLICY, cloud.ComparisonPolicy(), NOW)


def test_maintenance_history_cannot_shorten_to_fit_hard_caps(spark):
    frame = observations(
        spark,
        [
            ("a", NOW, "100"),
            ("a", NOW - timedelta(weeks=1), "90"),
        ],
    )
    limited = replace(POLICY, catalogue=CataloguePolicy(100, 100, 1, 35))
    with pytest.raises(ValueError, match="exceeds limits"):
        cloud.plan_frames(frame, gold_keys(spark, "a"), limited, cloud.ComparisonPolicy(), NOW)


def test_maintenance_refreshes_existing_at_cap_before_lexical_admission(spark):
    frame = observations(spark, [("new-a", NOW, "1"), ("existing-z", NOW, "500")])
    limited = replace(POLICY, catalogue=CataloguePolicy(1, 100, 500, 35))
    result = cloud.plan_frames(
        frame, gold_keys(spark, "existing-z"), limited, cloud.ComparisonPolicy(), NOW
    )
    assert [row.listing_id for row in result.current.collect()] == ["existing-z"]
    assert result.retained.count() == 2


def test_readback_rejects_duplicate_weekly_keys(spark):
    frame = observations(spark, [("a", NOW, "100"), ("a", NOW, "100")])
    with pytest.raises(ValueError, match="uniqueness"):
        cloud.validate_frame(frame, frame, 2, NOW)


def test_partitioned_run_and_flat_generation_preserve_lineage_when_combined(spark, tmp_path):
    original = observations(spark, [("old", NOW, "100")])
    compacted = observations(spark, [("new", NOW, "200")])
    run, generation = tmp_path / "run", tmp_path / "generation"
    original.write.partitionBy("posting_type").parquet(str(run))
    compacted.write.parquet(str(generation))
    combined = cloud._read_silver(spark, (run, generation), None)
    assert {row.listing_id for row in combined.collect()} == {"old", "new"}
    assert {row.posting_type for row in combined.collect()} == {"PROPERTY"}
    assert set(combined.columns) == set(original.columns)
    cloud.validate_frame(combined, combined, 2, NOW)


def test_source_receipts_preserve_original_hash_and_version_across_compaction(tmp_path):
    source = tmp_path / "source"
    first, second = tmp_path / "first", tmp_path / "second"
    for directory in (source, first, second):
        directory.mkdir()
    original = {"run_id": "retained", "input_sha256": "a" * 64, "transformation_version": "v1"}
    manifest_path = source / "_run_manifest.json"
    original_text = json.dumps(original | {"silver_records": 3})
    manifest_path.write_text(original_text)
    cloud.write_source_receipts((source,), first, ("retained",))
    cloud.write_source_receipts((first,), second, ("retained",))
    assert json.loads((second / "_source_receipts.json").read_text()) == {"sources": [original]}
    assert manifest_path.read_text() == original_text
    cloud.write_source_receipts((first,), second, ())
    assert json.loads((second / "_source_receipts.json").read_text()) == {"sources": []}


def test_missing_or_conflicting_original_receipts_block_publication(tmp_path):
    first, second, output = (tmp_path / name for name in ("one", "two", "output"))
    for directory in (first, second, output):
        directory.mkdir()
    receipt = {"run_id": "run", "input_sha256": "a" * 64, "transformation_version": "v1"}
    (first / "_run_manifest.json").write_text(json.dumps(receipt))
    (second / "_run_manifest.json").write_text(json.dumps(receipt | {"input_sha256": "b" * 64}))
    with pytest.raises(ValueError, match="conflicting"):
        cloud.write_source_receipts((first, second), output, ("run",))
    with pytest.raises(ValueError, match="no immutable source receipt"):
        cloud.write_source_receipts((first,), output, ("different-run",))
    assert not tuple(output.iterdir())
