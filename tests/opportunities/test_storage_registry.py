"""Registry/Gold atomicity and exclusion against an isolated temporary database."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest
from test_repository import candidate

from zona_oportunidades.operations.storage import (
    DatasetManifest,
    DatasetRegistry,
    Generation,
    StorageBusyError,
    StorageError,
    build_manifest,
    storage_lock,
)
from zona_oportunidades.opportunities.repository import GoldRepository


@pytest.fixture()
def registry_dsn(postgres_dsn: str) -> str:
    with psycopg.connect(postgres_dsn) as connection:
        # Roll back role creation after the fixture? Roles are cluster-wide, so use
        # the schema's current test role for its final grants instead of creating one.
        role = connection.execute("SELECT current_user").fetchone()[0]
        from psycopg import sql

        migration = Path("infra/postgres/pipeline_storage.sql").read_text()
        migration = migration.replace(
            "TO pipeline_writer", "TO " + sql.Identifier(role).as_string()
        )
        connection.execute(migration)
        connection.execute(migration)
    return postgres_dsn


@pytest.fixture()
def manifest(tmp_path: Path) -> DatasetManifest:
    (tmp_path / "part.parquet").write_bytes(b"PAR1-fixture-PAR1")
    return build_manifest(
        tmp_path, generation_id="first", kind="silver", rows=1, source_runs=("source-1",)
    )


def prepare(registry: DatasetRegistry, manifest: DatasetManifest) -> Generation:
    return registry.prepare(manifest, f"{manifest.kind}/{manifest.generation_id}")


def test_gold_and_registry_switch_commit_or_rollback_together(
    registry_dsn: str,
    manifest: DatasetManifest,
) -> None:
    sale = candidate()
    repository = GoldRepository(registry_dsn)
    repository.replace([sale], 10, sale.collected_at)
    with storage_lock(registry_dsn) as connection:
        registry = DatasetRegistry(connection)
        old = prepare(registry, manifest)
        registry.activate(old, ())
        new_manifest = replace(manifest, generation_id="second", source_runs=("source-2",))
        new = prepare(registry, new_manifest)
        assert new.state == "preparing"
        # Preparation is durable independently of the publication transaction.
        with psycopg.connect(registry_dsn) as observer:
            assert observer.execute(
                "SELECT state FROM ops.silver_generations WHERE generation_id = 'second'"
            ).fetchone() == ("preparing",)
        with pytest.raises(RuntimeError, match="interrupted"), connection.transaction():
            repository.replace(
                [replace(sale, title="changed")], 10, sale.collected_at, connection=connection
            )
            registry.activate(new, (old.generation_id,))
            raise RuntimeError("interrupted publication")
        assert [item.generation_id for item in registry.active()] == ["first"]
        assert connection.execute("SELECT title FROM gold.opportunities").fetchone() == ("Depto",)
        assert connection.execute("SELECT count(*) FROM ops.silver_source_receipts").fetchone() == (
            1,
        )
        with connection.transaction():
            repository.replace(
                [replace(sale, title="changed")], 10, sale.collected_at, connection=connection
            )
            registry.activate(new, (old.generation_id,))
            # Another reader keeps the last committed Gold until the whole switch commits.
            with psycopg.connect(registry_dsn) as observer:
                assert observer.execute("SELECT title FROM gold.opportunities").fetchone() == (
                    "Depto",
                )
                assert observer.execute(
                    "SELECT generation_id FROM ops.silver_generations WHERE state = 'active'"
                ).fetchall() == [("first",)]
        assert not connection.closed
        assert [item.generation_id for item in registry.active()] == ["second"]
        assert connection.execute("SELECT title FROM gold.opportunities").fetchone() == ("changed",)
        assert connection.execute("SELECT count(*) FROM ops.silver_source_receipts").fetchone() == (
            2,
        )
        retired = registry.retired(datetime.now(UTC) + timedelta(seconds=1))
        assert [item.generation_id for item in retired] == ["first"]
        registry.mark_deleted(retired[0])
        registry.mark_deleted(retired[0])
        assert registry.retired(datetime.now(UTC) + timedelta(seconds=1)) == ()
        # Deleting bytes/state preserves receipts for the separate 90-day audit policy.
        assert connection.execute("SELECT count(*) FROM ops.silver_source_receipts").fetchone() == (
            2,
        )


def test_invalid_switch_and_identifier_reuse_leave_old_generation_active(
    registry_dsn: str,
    manifest: DatasetManifest,
) -> None:
    with storage_lock(registry_dsn) as connection:
        registry = DatasetRegistry(connection)
        first = prepare(registry, manifest)
        assert prepare(registry, manifest) == first
        registry.activate(first, ())
        assert prepare(registry, manifest).state == "active"
        with pytest.raises(StorageError, match="immutable"):
            prepare(registry, replace(manifest, rows=99))
        with pytest.raises(StorageError, match="inactive"):
            registry.mark_deleted(first)
        second = prepare(registry, replace(manifest, generation_id="second"))
        with pytest.raises(StorageError, match="no longer active"):
            registry.activate(second, ("missing",))
        assert [item.generation_id for item in registry.active()] == ["first"]
        registry.activate(second, (first.generation_id,))
        registry.activate(second, (first.generation_id,))
        with pytest.raises(StorageError, match="Retry"):
            registry.activate(second, ("missing",))
        with pytest.raises(StorageError, match="Invalid"):
            registry.activate(first, ())
        assert [item.generation_id for item in registry.active()] == ["second"]
        with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
            connection.execute(
                "UPDATE ops.silver_generations SET prefix = 'changed' "
                "WHERE generation_id = 'second'"
            )
        with pytest.raises(psycopg.errors.RaiseException, match="transition"):
            connection.execute(
                "UPDATE ops.silver_generations SET state = 'preparing' "
                "WHERE generation_id = 'second'"
            )


def test_advisory_lock_excludes_readers_and_cleanup_and_releases_after_failure(
    registry_dsn: str,
) -> None:
    with pytest.raises(RuntimeError, match="failed computation"), storage_lock(registry_dsn):
        with pytest.raises(StorageBusyError), storage_lock(registry_dsn):
            pytest.fail("Concurrent process acquired the storage lock")
        raise RuntimeError("failed computation")
    with storage_lock(registry_dsn) as connection:
        assert connection.autocommit
        assert connection.execute("SELECT 1").fetchone() == (1,)


def test_gold_failure_uses_caller_transaction_without_closing_connection(
    registry_dsn: str,
) -> None:
    repository = GoldRepository(registry_dsn)
    sale = candidate()
    repository.replace([sale], 10, sale.collected_at)
    with psycopg.connect(registry_dsn, autocommit=True) as connection:
        with pytest.raises(psycopg.errors.UniqueViolation), connection.transaction():
            repository.replace([sale, sale], 10, sale.collected_at, connection=connection)
        assert not connection.closed
        assert connection.execute("SELECT listing_id FROM gold.opportunities").fetchall() == [
            ("42",)
        ]


def test_registry_writer_grants_allow_publication_without_ddl(
    registry_dsn: str,
    manifest: DatasetManifest,
) -> None:
    with psycopg.connect(registry_dsn) as connection, connection.transaction(force_rollback=True):
        connection.execute(Path("infra/postgres/pipeline_writer.sql").read_text())
        connection.execute(Path("infra/postgres/pipeline_storage.sql").read_text())
        connection.execute("SET LOCAL ROLE pipeline_writer")
        registry = DatasetRegistry(connection)
        first = prepare(registry, manifest)
        registry.activate(first, ())
        second = prepare(registry, replace(manifest, generation_id="second"))
        registry.activate(second, (first.generation_id,))
        registry.mark_deleted(registry.retired(datetime.now(UTC) + timedelta(seconds=1))[0])
        assert [item.generation_id for item in registry.active()] == ["second"]
        for forbidden in (
            "ALTER TABLE ops.silver_generations ADD COLUMN forbidden TEXT",
            "CREATE TABLE ops.forbidden (id integer)",
            "UPDATE ops.silver_source_receipts SET run_id = 'changed'",
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege), connection.transaction():
                connection.execute(forbidden)


def test_lock_releases_even_when_caller_leaves_an_aborted_transaction(registry_dsn: str) -> None:
    with pytest.raises(psycopg.errors.DivisionByZero), storage_lock(registry_dsn) as connection:
        connection.execute("BEGIN")
        connection.execute("SELECT 1 / 0")
    with storage_lock(registry_dsn) as connection:
        assert connection.execute("SELECT 1").fetchone() == (1,)
