"""Quarantine boundaries use a persistent fake DB and a private fake object store."""

import copy
import json
from contextlib import contextmanager
from pathlib import Path

import pytest

from zona_oportunidades.operations.quarantine import (
    QuarantineError,
    cleanup_promoted,
    preserve,
    promote,
    reserve,
)


class Result:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class Database:
    """Share this between callers to model reservations surviving runner exits."""

    autocommit = True

    def __init__(self):
        self.rows = {}

    @contextmanager
    def transaction(self):
        snapshot = copy.deepcopy(self.rows)
        try:
            yield
        except Exception:
            self.rows = snapshot
            raise

    def execute(self, query, params=()):
        if query.startswith("SELECT count(*)"):
            active = [row for row in self.rows.values() if row["state"] != "deleted"]
            return Result([(len(active), sum(row["bytes"] for row in active))])
        if query.startswith("SELECT reserved_bytes FROM"):
            row = self.rows.get(params[0])
            return Result([(row["bytes"],)] if row else [])
        if query.startswith("INSERT INTO"):
            self.rows[params[0]] = {
                "bytes": params[1],
                "state": "reserved",
                "manifest": None,
                "prefix": None,
            }
        elif query.startswith("SELECT reserved_bytes, state"):
            row = self.rows.get(params[0])
            return Result(
                [(row["bytes"], row["state"], row["manifest"], row["prefix"])] if row else []
            )
        elif query.startswith("UPDATE ops.bronze_quarantine SET manifest"):
            self.rows[params[2]].update(manifest=params[0].obj, prefix=params[1])
        elif query.startswith("UPDATE ops.bronze_quarantine SET state = %s"):
            self.rows[params[1]]["state"] = params[0]
        elif query.startswith("UPDATE ops.bronze_quarantine SET state = 'promoted'"):
            row = self.rows.get(params[0])
            if (
                row
                and row["bytes"] == params[1]
                and row["manifest"] is not None
                and row["state"] in ("preserved", "promoted")
            ):
                row["state"] = "promoted"
                return Result([(params[0],)])
        elif query.startswith("SELECT run_id, manifest, prefix"):
            return Result(
                [
                    (identity, row["manifest"], row["prefix"])
                    for identity, row in sorted(self.rows.items())
                    if row["state"] == "promoted"
                ]
            )
        elif query.startswith("UPDATE ops.bronze_quarantine SET state = 'deleted'"):
            assert self.rows[params[0]]["state"] == "promoted"
            self.rows[params[0]]["state"] = "deleted"
        else:
            raise AssertionError(query)
        return Result()


class Store:
    def __init__(self, failure=None):
        self.failure = failure
        self.objects = {}
        self.manifests = {}
        self.events = []

    def upload(self, directory, manifest):
        self.events.append("upload")
        if self.failure == "upload":
            raise RuntimeError("secret source contents and credentials")
        prefix = f"{manifest.kind}/{manifest.generation_id}"
        self.manifests[prefix] = manifest
        self.objects[prefix] = {
            item.path: (directory / item.path).read_bytes() for item in manifest.files
        }
        return prefix

    def download(self, generation, target):
        self.events.append("readback")
        if self.failure == "readback":
            raise RuntimeError("secret remote error")
        assert self.manifests[generation.prefix] == generation.manifest
        target.mkdir()
        for name, content in self.objects[generation.prefix].items():
            (target / name).write_bytes(content)
        return target

    def delete(self, generation):
        assert generation.state == "superseded"
        assert generation.manifest == self.manifests[generation.prefix]
        self.events.append("delete")
        if self.failure == "delete":
            raise RuntimeError("provider details")
        del self.objects[generation.prefix]


def bronze(root: Path, identity="run", content=b'{"id":"private"}\n', day="2026-10-27"):
    path = root / "bronze" / "listings" / f"ingestion_date={day}" / f"run_id={identity}"
    path.mkdir(parents=True)
    raw = path / "listings.jsonl"
    raw.write_bytes(content)
    metadata = root / "bronze" / "manifests" / f"{identity}.json"
    metadata.parent.mkdir(exist_ok=True)
    metadata.write_text(json.dumps({"run_id": identity}))
    return raw, metadata


def test_persisted_slots_cap_two_even_after_success_without_deletion(tmp_path):
    db, store = Database(), Store()
    first = reserve(db, "first")
    bronze(tmp_path, "first")
    preserve(db, store, first, data_root=tmp_path)
    promote(db, first)
    reserve(db, "crashed-runner")
    with pytest.raises(QuarantineError, match="full"):
        reserve(db, "third")
    assert cleanup_promoted(db, store) == ("first",)
    reserve(db, "third")
    assert db.rows["crashed-runner"]["state"] == "reserved"


def test_byte_capacity_reserves_maximum_not_actual_files():
    db = Database()
    reserve(db, "one", max_total_bytes=19, max_extract_bytes=10)
    with pytest.raises(QuarantineError, match="full"):
        reserve(db, "two", max_total_bytes=19, max_extract_bytes=10)
    reserve(db, "two", max_total_bytes=20, max_extract_bytes=10)


@pytest.mark.parametrize("failure", ["upload", "readback"])
def test_storage_failure_keeps_reservation_sources_and_immutable_retry(tmp_path, failure):
    db, store = Database(), Store(failure)
    ticket = reserve(db, "run")
    paths = bronze(tmp_path)
    with pytest.raises(QuarantineError, match="reservation and local files retained") as error:
        preserve(db, store, ticket, data_root=tmp_path)
    assert "secret" not in str(error.value)
    assert db.rows["run"]["state"] == "reserved"
    assert all(path.exists() for path in paths)
    assert cleanup_promoted(db, store) == ()
    original = db.rows["run"]["manifest"]
    store.failure = None
    generation = preserve(db, store, ticket, data_root=tmp_path)
    assert json.loads(generation.manifest.to_json()) == original
    assert db.rows["run"]["state"] == "preserved"


def test_exact_inventory_upload_readback_promotion_then_deletion(tmp_path):
    db, store = Database(), Store()
    ticket = reserve(db, "run")
    raw, metadata = bronze(tmp_path)
    bronze(tmp_path, "unrelated")
    (raw.parent / "unrelated.txt").write_text("ignore me")
    generation = preserve(db, store, ticket, data_root=tmp_path)
    assert store.events == ["upload", "readback"]
    assert generation.manifest.kind == "bronze"
    assert generation.manifest.source_runs == ("run",)
    assert store.objects["bronze/run"] == {
        "listings.jsonl": raw.read_bytes(),
        "collection_manifest.json": metadata.read_bytes(),
    }
    assert cleanup_promoted(db, store) == ()
    with db.transaction():
        promote(db, ticket)
    assert cleanup_promoted(db, store) == ("run",)
    assert cleanup_promoted(db, store) == ()
    assert raw.exists() and metadata.exists()


def test_no_raw_failure_marker_is_private_and_never_deleted_or_promoted(tmp_path):
    db, store = Database(), Store()
    ticket = reserve(db, "run")
    assert preserve(db, store, ticket, data_root=tmp_path) is None
    assert db.rows["run"]["state"] == "failed"
    marker = json.loads(store.objects["bronze/run"]["failure.json"])
    assert marker == {"run_id": "run", "reason": "no_bronze", "raw_present": False}
    with pytest.raises(QuarantineError, match="Only verified"):
        promote(db, ticket)
    assert cleanup_promoted(db, store) == ()
    reserve(db, "two")
    with pytest.raises(QuarantineError, match="full"):
        reserve(db, "three")


@pytest.mark.parametrize("kind", ["raw", "combined"])
def test_oversized_bronze_never_uploaded_or_removed(tmp_path, kind):
    db, store = Database(), Store()
    ticket = reserve(db, "run", max_extract_bytes=40)
    paths = bronze(tmp_path, content=b"x" * (41 if kind == "raw" else 30))
    with pytest.raises(QuarantineError):
        preserve(db, store, ticket, data_root=tmp_path)
    assert store.events == []
    assert all(path.exists() for path in paths)
    assert db.rows["run"]["state"] == "reserved"


@pytest.mark.parametrize("kind", ["ambiguous", "symlink", "incomplete"])
def test_untrusted_or_incomplete_run_never_uploaded(tmp_path, kind):
    db, store = Database(), Store()
    ticket = reserve(db, "run")
    raw, metadata = bronze(tmp_path)
    if kind == "ambiguous":
        bronze(tmp_path, day="2026-10-26")
    elif kind == "symlink":
        raw.unlink()
        raw.symlink_to(metadata)
    else:
        metadata.unlink()
    with pytest.raises(QuarantineError):
        preserve(db, store, ticket, data_root=tmp_path)
    assert store.events == []
    assert db.rows["run"]["state"] == "reserved"


def test_promotion_rollback_and_failed_delete_keep_slot(tmp_path):
    db, store = Database(), Store()
    ticket = reserve(db, "run")
    bronze(tmp_path)
    preserve(db, store, ticket, data_root=tmp_path)
    with pytest.raises(RuntimeError), db.transaction():
        promote(db, ticket)
        raise RuntimeError("Gold transaction failed")
    assert cleanup_promoted(db, store) == ()
    promote(db, ticket)
    store.failure = "delete"
    with pytest.raises(QuarantineError, match="slot retained"):
        cleanup_promoted(db, store)
    assert db.rows["run"]["state"] == "promoted"
    assert "bronze/run" in store.objects


def test_duplicate_run_and_invalid_identifier_cannot_collect_twice():
    db = Database()
    reserve(db, "run")
    with pytest.raises(QuarantineError, match="already"):
        reserve(db, "run")
    with pytest.raises(ValueError):
        reserve(db, "../run")
    with pytest.raises(ValueError):
        reserve(db, "other", max_extract_bytes=True)


def test_empty_real_bronze_is_distinct_from_missing_bronze_on_retry(tmp_path):
    db, store = Database(), Store()
    ticket = reserve(db, "run")
    bronze(tmp_path, content=b"")
    first = preserve(db, store, ticket, data_root=tmp_path)
    assert first is not None and first.manifest.rows == 0
    assert preserve(db, store, ticket, data_root=tmp_path) == first


def test_postgres_migration_reservation_and_atomic_promotion(tmp_path):
    """Optional isolated DB: QUARANTINE_TEST_DSN must allow CREATE DATABASE."""
    import os
    import uuid

    import psycopg
    from psycopg import sql

    dsn = os.environ.get("QUARANTINE_TEST_DSN")
    if not dsn:
        pytest.skip("Set QUARANTINE_TEST_DSN to an isolated PostgreSQL test server")
    name = "test_quarantine_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            with psycopg.connect(dsn, dbname=name, autocommit=True) as conn:
                conn.execute("CREATE SCHEMA ops")
                migration = Path("infra/postgres/bronze_quarantine.sql").read_text()
                conn.execute(migration)
                conn.execute(migration)
                ticket = reserve(conn, "first")
                reserve(conn, "second")
                with pytest.raises(QuarantineError, match="full"):
                    reserve(conn, "third")
                store = Store()
                bronze(tmp_path, "first")
                preserve(conn, store, ticket, data_root=tmp_path)
                with pytest.raises(RuntimeError), conn.transaction():
                    promote(conn, ticket)
                    raise RuntimeError("publication rolled back")
                assert cleanup_promoted(conn, store) == ()
                with conn.transaction():
                    promote(conn, ticket)
                assert cleanup_promoted(conn, store) == ("first",)
                reserve(conn, "third")
                assert conn.execute(
                    "SELECT state FROM ops.bronze_quarantine WHERE run_id = 'second'"
                ).fetchone() == ("reserved",)
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


def test_byte_cap_includes_remote_storage_manifest_overhead(tmp_path):
    db, store = Database(), Store()
    ticket = reserve(db, "run", max_extract_bytes=100)
    bronze(tmp_path, content=b"")
    with pytest.raises(QuarantineError):
        preserve(db, store, ticket, data_root=tmp_path)
    assert store.events == []
    assert db.rows["run"]["state"] == "reserved"
