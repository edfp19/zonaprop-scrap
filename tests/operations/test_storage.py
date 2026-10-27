"""Object publication failure tests use an in-memory S3 boundary, never a bucket."""

import hashlib
import io
import json
import sys
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import Mock

import pytest

from zona_oportunidades.operations.storage import (
    Artifact,
    DatasetManifest,
    Generation,
    S3Store,
    StorageError,
    build_manifest,
    storage_lock,
)


class FakeClientError(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}


class MemoryS3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.writes: list[str] = []
        self.deletes: list[str] = []
        self.fail_upload: str | None = None
        self.fail_delete: str | None = None
        self.delete_404 = False
        self.corrupt_upload = False
        self.race_content: bytes | None = None
        self.inventory_calls = 0

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        if Key not in self.objects:
            raise FakeClientError("NoSuchKey")
        content = self.objects[Key]
        return {"Body": io.BytesIO(content), "ContentLength": len(content)}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, IfNoneMatch: str) -> None:
        assert IfNoneMatch == "*"
        if self.fail_upload == Key:
            raise RuntimeError("interrupted upload")
        if self.race_content is not None:
            self.objects[Key] = self.race_content
        if Key in self.objects:
            raise FakeClientError("PreconditionFailed")
        self.objects[Key] = b"corrupt" if self.corrupt_upload else Body
        self.writes.append(Key)

    def list_objects_v2(
        self, *, Bucket: str, Prefix: str, MaxKeys: int, ContinuationToken: str = "0"
    ) -> dict[str, Any]:
        self.inventory_calls += 1
        keys = sorted(key for key in self.objects if key.startswith(Prefix))
        start = int(ContinuationToken)
        page = keys[start : start + MaxKeys]
        return {
            "Contents": [{"Key": key} for key in page],
            "IsTruncated": start + len(page) < len(keys),
            "NextContinuationToken": str(start + len(page)),
        }

    def delete_object(self, *, Bucket: str, Key: str) -> None:
        if self.fail_delete == Key:
            raise RuntimeError("interrupted deletion")
        self.objects.pop(Key, None)
        self.deletes.append(Key)
        if self.delete_404:
            raise FakeClientError("404")


@pytest.fixture()
def s3(monkeypatch: pytest.MonkeyPatch) -> tuple[S3Store, MemoryS3]:
    # Inject the SDK boundary so the optional cloud extra is unnecessary for unit tests.
    client = MemoryS3()
    boto3 = ModuleType("boto3")
    boto3.client = Mock(return_value=client)  # type: ignore[attr-defined]
    config = ModuleType("botocore.config")
    config.Config = Mock()  # type: ignore[attr-defined]
    errors = ModuleType("botocore.exceptions")
    errors.ClientError = FakeClientError  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "boto3", boto3)
    monkeypatch.setitem(sys.modules, "botocore.config", config)
    monkeypatch.setitem(sys.modules, "botocore.exceptions", errors)
    store = S3Store("private", "http://objects.test", "test-region")
    boto3.client.assert_called_once_with(  # type: ignore[attr-defined]
        "s3",
        endpoint_url="http://objects.test",
        region_name="test-region",
        config=config.Config.return_value,  # type: ignore[attr-defined]
    )
    config.Config.assert_called_once_with(s3={"addressing_style": "path"})  # type: ignore[attr-defined]
    return store, client


@pytest.fixture()
def source(tmp_path: Path) -> tuple[Path, DatasetManifest]:
    directory = tmp_path / "source"
    (directory / "operation=sale").mkdir(parents=True)
    (directory / "operation=sale/part.parquet").write_bytes(b"PAR1-teaching-fixture-PAR1")
    (directory / "_SUCCESS").touch()
    return directory, build_manifest(
        directory, generation_id="run-1", kind="silver", rows=3, source_runs=("source-1",)
    )


def test_round_trip_idempotency_and_manifest_is_published_last(
    s3: tuple[S3Store, MemoryS3], source: tuple[Path, DatasetManifest], tmp_path: Path
) -> None:
    store, client = s3
    directory, manifest = source
    assert DatasetManifest.from_json(manifest.to_json()) == manifest
    prefix = store.upload(directory, manifest)
    assert client.writes[-1] == f"{prefix}/manifest.json"
    written = list(client.writes)
    assert store.upload(directory, manifest) == prefix
    assert client.writes == written
    generation = Generation(manifest, prefix, "active")
    output = store.download(generation, tmp_path / "download")
    for artifact in manifest.files:
        assert (output / artifact.path).read_bytes() == (directory / artifact.path).read_bytes()
    with pytest.raises(StorageError, match="active"):
        store.delete(generation)
    store.delete(replace(generation, state="superseded"))
    assert not client.objects
    assert client.deletes[-1] == f"{prefix}/manifest.json"
    store.delete(replace(generation, state="deleted"))


def test_interrupted_upload_keeps_manifest_absent_and_retry_is_safe(
    s3: tuple[S3Store, MemoryS3], source: tuple[Path, DatasetManifest]
) -> None:
    store, client = s3
    directory, manifest = source
    prefix = "silver/run-1"
    client.fail_upload = f"{prefix}/{manifest.files[-1].path}"
    with pytest.raises(RuntimeError, match="interrupted"):
        store.upload(directory, manifest)
    assert f"{prefix}/manifest.json" not in client.objects
    client.fail_upload = None
    store.upload(directory, manifest)
    assert f"{prefix}/manifest.json" in client.objects


def test_partial_upload_can_be_cleaned_from_exact_registry_inventory(
    s3: tuple[S3Store, MemoryS3], source: tuple[Path, DatasetManifest]
) -> None:
    store, client = s3
    directory, manifest = source
    prefix = "silver/run-1"
    artifact = manifest.files[-1]
    client.objects[f"{prefix}/{artifact.path}"] = (directory / artifact.path).read_bytes()
    client.objects["silver/another-run/keep"] = b"other generation"
    store.delete(Generation(manifest, prefix, "preparing"))
    assert client.objects == {"silver/another-run/keep": b"other generation"}


@pytest.mark.parametrize("mismatch", ["content", "extra", "missing", "manifest"])
def test_download_and_delete_fail_closed_before_any_deletion(
    mismatch: str,
    s3: tuple[S3Store, MemoryS3],
    source: tuple[Path, DatasetManifest],
    tmp_path: Path,
) -> None:
    store, client = s3
    directory, manifest = source
    prefix = store.upload(directory, manifest)
    artifact = f"{prefix}/{manifest.files[-1].path}"
    if mismatch == "content":
        client.objects[artifact] = b"x" * len(client.objects[artifact])
    elif mismatch == "extra":
        client.objects[f"{prefix}/unexpected"] = b"unknown"
    elif mismatch == "missing":
        client.objects.pop(artifact)
    else:
        client.objects[f"{prefix}/manifest.json"] = replace(manifest, rows=99).to_json().encode()
    generation = Generation(manifest, prefix, "superseded")
    with pytest.raises(StorageError):
        store.download(generation, tmp_path / "download")
    if mismatch == "missing":
        # An absent artifact is also a legitimate interrupted deletion retry.
        store.delete(generation)
        assert not client.objects
    else:
        with pytest.raises(StorageError):
            store.delete(generation)
        assert not client.deletes


def test_upload_refuses_existing_different_objects_and_metadata(
    s3: tuple[S3Store, MemoryS3], source: tuple[Path, DatasetManifest]
) -> None:
    store, client = s3
    directory, manifest = source
    prefix = store.upload(directory, manifest)
    before = dict(client.objects)
    with pytest.raises(StorageError, match="manifest differs"):
        store.upload(directory, replace(manifest, rows=4))
    assert client.objects == before
    artifact_key = f"{prefix}/{manifest.files[-1].path}"
    client.objects[artifact_key] = b"x" * len(client.objects[artifact_key])
    with pytest.raises(StorageError):
        store.upload(directory, manifest)


@pytest.mark.parametrize("failure", ["corruption", "race"])
def test_upload_readback_and_conditional_write_detect_bad_bytes(
    failure: str, s3: tuple[S3Store, MemoryS3], source: tuple[Path, DatasetManifest]
) -> None:
    store, client = s3
    directory, manifest = source
    client.corrupt_upload = failure == "corruption"
    client.race_content = b"racing writer" if failure == "race" else None
    with pytest.raises(StorageError):
        store.upload(directory, manifest)
    assert "silver/run-1/manifest.json" not in client.objects


def test_interrupted_delete_keeps_manifest_and_can_be_retried(
    s3: tuple[S3Store, MemoryS3], source: tuple[Path, DatasetManifest]
) -> None:
    store, client = s3
    directory, manifest = source
    prefix = store.upload(directory, manifest)
    generation = Generation(manifest, prefix, "superseded")
    client.fail_delete = f"{prefix}/{manifest.files[-1].path}"
    with pytest.raises(RuntimeError, match="interrupted"):
        store.delete(generation)
    assert f"{prefix}/manifest.json" in client.objects
    client.fail_delete = None
    store.delete(generation)
    assert not client.objects


@pytest.mark.parametrize(
    "path",
    [
        "/absolute",
        "../escape",
        "a/../escape",
        "a//b",
        "a\\b",
        "./part",
        "part/",
        "manifest.json",
        "a\npart",
    ],
)
def test_manifest_rejects_unsafe_paths(path: str, source: tuple[Path, DatasetManifest]) -> None:
    _, manifest = source
    item = replace(manifest.files[0], path=path)
    with pytest.raises(ValueError):
        DatasetManifest.from_json(replace(manifest, files=(item,)).to_json())


@pytest.mark.parametrize(
    "field,value",
    [
        ("rows", True),
        ("rows", -1),
        ("generation_id", "../escape"),
        ("kind", "silver/x"),
        ("created_at", "2026-10-27T00:00:00"),
        ("source_runs", ["run", "run"]),
        ("files", []),
        ("files", [{"path": "part", "bytes": -1, "sha256": "0" * 64}]),
        ("files", [{"path": "part", "bytes": 1, "sha256": "bad"}]),
    ],
)
def test_manifest_validates_boundary_types(
    field: str, value: object, source: tuple[Path, DatasetManifest]
) -> None:
    _, manifest = source
    payload = json.loads(manifest.to_json())
    payload[field] = value
    with pytest.raises(ValueError):
        DatasetManifest.from_json(payload)


def test_symlinks_and_changed_local_inventory_are_rejected(
    s3: tuple[S3Store, MemoryS3],
    source: tuple[Path, DatasetManifest],
    tmp_path: Path,
) -> None:
    store, client = s3
    directory, manifest = source
    (directory / "extra").write_bytes(b"unlisted")
    with pytest.raises(StorageError, match="inventory"):
        store.upload(directory, manifest)
    assert not client.writes
    (directory / "extra").unlink()
    (directory / "link").symlink_to(tmp_path)
    with pytest.raises(ValueError, match="Symlink"):
        build_manifest(directory, generation_id="run", kind="silver", rows=3, source_runs=())


def test_bounded_inventory_rejects_large_unknown_prefix(
    s3: tuple[S3Store, MemoryS3],
    source: tuple[Path, DatasetManifest],
) -> None:
    store, client = s3
    directory, manifest = source
    client.objects.update({f"silver/run-1/unknown-{n}": b"x" for n in range(5000)})
    with pytest.raises(StorageError, match="Unexpected|inventory"):
        store.upload(directory, manifest)
    assert client.inventory_calls <= 3
    assert not client.writes


def test_prefix_mismatch_and_nonempty_target_never_touch_existing_files(
    s3: tuple[S3Store, MemoryS3],
    source: tuple[Path, DatasetManifest],
    tmp_path: Path,
) -> None:
    store, _ = s3
    directory, manifest = source
    prefix = store.upload(directory, manifest)
    wrong = Generation(manifest, "another/prefix", "superseded")
    with pytest.raises(StorageError, match="prefix"):
        store.delete(wrong)
    with pytest.raises(StorageError, match="empty"):
        store.download(Generation(manifest, prefix, "active"), directory)
    link = tmp_path / "link"
    link.symlink_to(directory, target_is_directory=True)
    with pytest.raises(StorageError, match="empty"):
        store.download(Generation(manifest, prefix, "active"), link)


def test_duplicate_and_overlapping_paths_rejected(source: tuple[Path, DatasetManifest]) -> None:
    _, manifest = source
    item = Artifact("part", 0, hashlib.sha256(b"").hexdigest())
    for files in ((item, item), (item, replace(item, path="part/child"))):
        with pytest.raises(ValueError, match="Duplicate|overlap"):
            DatasetManifest.from_json(replace(manifest, files=files).to_json())


def test_delete_accepts_not_found_responses_but_verifies_absence(
    s3: tuple[S3Store, MemoryS3],
    source: tuple[Path, DatasetManifest],
) -> None:
    store, client = s3
    directory, manifest = source
    prefix = store.upload(directory, manifest)
    client.delete_404 = True
    store.delete(Generation(manifest, prefix, "superseded"))
    assert not client.objects


def test_inventory_paginates_large_known_generation_with_manifest_bound(
    s3: tuple[S3Store, MemoryS3],
    source: tuple[Path, DatasetManifest],
    tmp_path: Path,
) -> None:
    store, client = s3
    _, manifest = source
    content = b"PAR1-many-parts-PAR1"
    files = tuple(
        Artifact(f"part-{n:04}.parquet", len(content), hashlib.sha256(content).hexdigest())
        for n in range(1002)
    )
    manifest = replace(manifest, files=files)
    prefix = "silver/run-1"
    client.objects.update({f"{prefix}/{item.path}": content for item in files})
    client.objects[f"{prefix}/manifest.json"] = manifest.to_json().encode()
    output = store.download(Generation(manifest, prefix, "active"), tmp_path / "many")
    assert len(list(output.iterdir())) == 1002
    assert client.inventory_calls == 2


def test_lock_rejects_known_transaction_pooler_before_connecting() -> None:
    with (
        pytest.raises(ValueError, match="direct PostgreSQL"),
        storage_lock("postgresql://writer@project-pooler.example.test/catalogue"),
    ):
        pytest.fail("Pooler accepted for a session advisory lock")
