"""Immutable object generations and a PostgreSQL publication pointer.

The caller holds storage_lock throughout reads/publication/cleanup. Registry methods
use its connection; wrap Gold replacement and activate in one transaction.
"""

import hashlib
import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.types.json import Jsonb

MAX_FILES = 10_000
MAX_MANIFEST_BYTES = 2_000_000
MANIFEST_NAME = "manifest.json"
LOCK_ID = 627419368105


class StorageError(RuntimeError):
    """A generation could not be verified safely."""


class StorageBusyError(StorageError):
    """Another coordinated reader or writer holds the storage lock."""


def _relative(value: object) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ValueError("Expected a safe relative POSIX path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in ("", ".", "..") for part in value.split("/")):
        raise ValueError("Expected a safe relative POSIX path")
    if any(ord(char) < 32 for char in value):
        raise ValueError("Control characters are not allowed in paths")
    return value


def _identifier(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value):
        raise ValueError("Invalid generation identifier or kind")
    return value


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Expected an ISO timestamp")
    timestamp = datetime.fromisoformat(value)
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("Timestamp must include a timezone")
    return timestamp


@dataclass(frozen=True)
class Artifact:
    path: str
    bytes: int
    sha256: str


@dataclass(frozen=True)
class DatasetManifest:
    generation_id: str
    kind: str
    created_at: datetime
    rows: int
    source_runs: tuple[str, ...]
    files: tuple[Artifact, ...]

    def to_json(self) -> str:
        value = asdict(self)
        value["created_at"] = self.created_at.isoformat()
        return json.dumps(value, sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, data: str | bytes | dict[str, Any]) -> "DatasetManifest":
        if isinstance(data, (str, bytes)):
            if len(data) > MAX_MANIFEST_BYTES:
                raise ValueError("Manifest exceeds metadata limit")
            value = json.loads(data)
        else:
            value = data
        fields = {"generation_id", "kind", "created_at", "rows", "source_runs", "files"}
        if not isinstance(value, dict) or set(value) != fields:
            raise ValueError("Invalid manifest fields")
        generation_id = _identifier(value["generation_id"])
        kind = _identifier(value["kind"])
        rows = value["rows"]
        if type(rows) is not int or rows < 0:
            raise ValueError("Manifest rows must be a nonnegative integer")
        sources = value["source_runs"]
        if not isinstance(sources, list) or len(sources) > MAX_FILES:
            raise ValueError("Invalid source run inventory")
        source_runs = tuple(_identifier(source) for source in sources)
        if len(set(source_runs)) != len(source_runs):
            raise ValueError("Duplicate source runs")
        raw_files = value["files"]
        if not isinstance(raw_files, list) or not 0 < len(raw_files) <= MAX_FILES:
            raise ValueError("Invalid file inventory")
        files = []
        for item in raw_files:
            if not isinstance(item, dict) or set(item) != {"path", "bytes", "sha256"}:
                raise ValueError("Invalid artifact fields")
            path = _relative(item["path"])
            if path == MANIFEST_NAME:
                raise ValueError("manifest.json is reserved")
            size, digest = item["bytes"], item["sha256"]
            if type(size) is not int or size < 0:
                raise ValueError("Invalid artifact size")
            if not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest):
                raise ValueError("Invalid artifact checksum")
            files.append(Artifact(path, size, digest))
        paths = [item.path for item in files]
        if len(paths) != len(set(paths)):
            raise ValueError("Duplicate artifact paths")
        if any(str(parent) in paths for path in paths for parent in PurePosixPath(path).parents):
            raise ValueError("Artifact paths overlap")
        result = cls(
            generation_id, kind, _timestamp(value["created_at"]), rows, source_runs, tuple(files)
        )
        if len(result.to_json().encode()) > MAX_MANIFEST_BYTES:
            raise ValueError("Manifest exceeds metadata limit")
        return result


def _artifact(path: Path, relative: str) -> Artifact:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            size += len(chunk)
            digest.update(chunk)
    return Artifact(relative, size, digest.hexdigest())


def build_manifest(
    directory: Path, *, generation_id: str, kind: str, rows: int, source_runs: tuple[str, ...]
) -> DatasetManifest:
    directory = Path(directory)
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("Generation must be a real directory")
    files = []
    for path in sorted(directory.rglob("*")):
        if path.is_symlink():
            raise ValueError("Symlinks are not allowed in a generation")
        if path.is_file():
            relative = path.relative_to(directory).as_posix()
            files.append(_artifact(path, relative))
            if len(files) > MAX_FILES:
                raise ValueError("Generation exceeds file limit")
    manifest = DatasetManifest(
        generation_id, kind, datetime.now(UTC), rows, source_runs, tuple(files)
    )
    return DatasetManifest.from_json(manifest.to_json())


@dataclass(frozen=True)
class Generation:
    manifest: DatasetManifest
    prefix: str
    state: str

    @property
    def generation_id(self) -> str:
        return self.manifest.generation_id


class S3Store:
    def __init__(self, bucket: str, endpoint_url: str | None, region: str | None) -> None:
        import boto3
        from botocore.config import Config

        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region,
            config=Config(s3={"addressing_style": "path"}),
        )

    @staticmethod
    def _prefix(manifest: DatasetManifest) -> str:
        return f"{manifest.kind}/{manifest.generation_id}"

    def _get(self, key: str, limit: int) -> bytes | None:
        from botocore.exceptions import ClientError

        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            if error.response["Error"]["Code"] in ("NoSuchKey", "404", "NotFound"):
                return None
            raise
        body = response["Body"]
        try:
            if response["ContentLength"] > limit:
                raise StorageError(f"Unexpected object size: {key}")
            content: bytes = body.read(limit + 1)
            if len(content) > limit or len(content) != response["ContentLength"]:
                raise StorageError(f"Incomplete or oversized object: {key}")
            return content
        finally:
            body.close()

    def _put(self, key: str, content: bytes) -> None:
        from botocore.exceptions import ClientError

        existing = self._get(key, len(content))
        if existing is not None:
            if existing != content:
                raise StorageError(f"Immutable object differs: {key}")
            return
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=content, IfNoneMatch="*")
        except ClientError as error:
            if error.response["Error"]["Code"] not in ("PreconditionFailed", "412"):
                raise
        if self._get(key, len(content)) != content:
            raise StorageError(f"Upload verification failed: {key}")

    def _inventory(self, prefix: str, expected: set[str], *, complete: bool) -> set[str]:
        # Page count is bounded by the manifest, never by the size of an unknown prefix.
        response = self.client.list_objects_v2(
            Bucket=self.bucket, Prefix=prefix + "/", MaxKeys=min(len(expected) + 1, 1000)
        )
        keys = {item["Key"] for item in response.get("Contents", [])}
        if not keys <= expected:
            raise StorageError("Object inventory differs from manifest")
        if response.get("IsTruncated"):
            # Larger valid generations need a bounded number of pages, never a prefix sweep.
            pages = 1
            while response.get("IsTruncated"):
                if pages > (len(expected) // 1000) + 1:
                    raise StorageError("Object inventory exceeds manifest")
                response = self.client.list_objects_v2(
                    Bucket=self.bucket,
                    Prefix=prefix + "/",
                    MaxKeys=1000,
                    ContinuationToken=response["NextContinuationToken"],
                )
                keys.update(item["Key"] for item in response.get("Contents", []))
                pages += 1
                if not keys <= expected:
                    raise StorageError("Unexpected objects in generation")
        if not keys <= expected or (complete and keys != expected):
            raise StorageError("Object inventory differs from manifest")
        return keys

    @staticmethod
    def _validate(generation: Generation) -> DatasetManifest:
        manifest = DatasetManifest.from_json(generation.manifest.to_json())
        if generation.prefix != S3Store._prefix(manifest):
            raise StorageError("Generation prefix does not match manifest")
        return manifest

    def upload(self, directory: Path, manifest: DatasetManifest) -> str:
        manifest = DatasetManifest.from_json(manifest.to_json())
        directory = Path(directory)
        if directory.is_symlink():
            raise StorageError("Generation directory must not be a symlink")
        actual = build_manifest(
            directory,
            generation_id=manifest.generation_id,
            kind=manifest.kind,
            rows=manifest.rows,
            source_runs=manifest.source_runs,
        )
        if actual.files != manifest.files:
            raise StorageError("Local inventory differs from manifest")
        prefix = self._prefix(manifest)
        metadata = manifest.to_json().encode()
        manifest_key = f"{prefix}/{MANIFEST_NAME}"
        existing = self._get(manifest_key, MAX_MANIFEST_BYTES)
        if existing is not None and existing != metadata:
            raise StorageError("Immutable manifest differs")
        expected = {f"{prefix}/{item.path}" for item in manifest.files} | {manifest_key}
        self._inventory(prefix, expected, complete=False)
        for item in manifest.files:
            content = (directory / item.path).read_bytes()
            if len(content) != item.bytes or hashlib.sha256(content).hexdigest() != item.sha256:
                raise StorageError("Local artifact changed during upload")
            self._put(f"{prefix}/{item.path}", content)
        # The completion marker is visible only after all artifact reads verified.
        self._put(manifest_key, metadata)
        self._inventory(prefix, expected, complete=True)
        return prefix

    def _remote_manifest(self, generation: Generation) -> tuple[DatasetManifest, bytes | None]:
        manifest = self._validate(generation)
        data = self._get(f"{generation.prefix}/{MANIFEST_NAME}", MAX_MANIFEST_BYTES)
        if data is not None and DatasetManifest.from_json(data) != manifest:
            raise StorageError("Remote manifest differs from registry")
        return manifest, data

    def _read_artifact(self, prefix: str, item: Artifact) -> bytes:
        content = self._get(f"{prefix}/{item.path}", item.bytes)
        if (
            content is None
            or len(content) != item.bytes
            or hashlib.sha256(content).hexdigest() != item.sha256
        ):
            raise StorageError(f"Artifact verification failed: {item.path}")
        return content

    def download(self, generation: Generation, target_dir: Path) -> Path:
        manifest, metadata = self._remote_manifest(generation)
        if metadata is None:
            raise StorageError("Generation has no completion manifest")
        prefix = generation.prefix
        expected = {f"{prefix}/{item.path}" for item in manifest.files}
        expected.add(f"{prefix}/{MANIFEST_NAME}")
        self._inventory(prefix, expected, complete=True)
        target = Path(target_dir)
        if target.exists() and (target.is_symlink() or any(target.iterdir())):
            raise StorageError("Download target must be an empty directory")
        if any(parent.is_symlink() for parent in (target, *target.parents)):
            raise StorageError("Download target must not traverse symlinks")
        target.mkdir(parents=True, exist_ok=True)
        for item in manifest.files:
            content = self._read_artifact(prefix, item)
            destination = target / item.path
            destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open("xb") as output:
                output.write(content)
        return target

    def delete(self, generation: Generation) -> None:
        from botocore.exceptions import ClientError

        if generation.state not in ("preparing", "superseded", "deleted"):
            raise StorageError("Cannot delete an active or unknown generation")
        manifest, metadata = self._remote_manifest(generation)
        prefix = generation.prefix
        manifest_key = f"{prefix}/{MANIFEST_NAME}"
        expected = {f"{prefix}/{item.path}" for item in manifest.files} | {manifest_key}
        present = self._inventory(prefix, expected, complete=False)
        # Verify every remaining artifact before deleting any, including on a retry.
        for item in manifest.files:
            if f"{prefix}/{item.path}" in present:
                self._read_artifact(prefix, item)
        for key in sorted(present - {manifest_key}) + ([manifest_key] if metadata else []):
            try:
                self.client.delete_object(Bucket=self.bucket, Key=key)
            except ClientError as error:
                if error.response["Error"]["Code"] not in ("NoSuchKey", "404", "NotFound"):
                    raise
            if self._get(key, MAX_MANIFEST_BYTES if key == manifest_key else 0) is not None:
                raise StorageError(f"Object remains visible after deletion: {key}")
        if self._inventory(prefix, expected, complete=False):
            raise StorageError("Objects remain after deletion")


@contextmanager
def storage_lock(dsn: str) -> Iterator[psycopg.Connection[Any]]:
    """Use a direct PostgreSQL host, never a transaction-pooler connection."""
    host = str(conninfo_to_dict(dsn).get("host") or "")
    if "-pooler." in host or "-pooler," in host or host.endswith("-pooler"):
        raise ValueError("Storage lock requires a direct PostgreSQL host, not a pooler")
    with psycopg.connect(dsn, autocommit=True) as connection:
        row = connection.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_ID,)).fetchone()
        if row is None or not row[0]:
            raise StorageBusyError("Another pipeline process holds the storage lock")
        try:
            yield connection
        finally:
            if not connection.closed:
                # An abandoned/failed caller transaction must not block the unlock.
                connection.rollback()
                connection.execute("SELECT pg_advisory_unlock(%s)", (LOCK_ID,))


class DatasetRegistry:
    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self.connection = connection

    def _select(
        self, predicate: str, params: tuple[Any, ...] = (), *, lock: bool = False
    ) -> tuple[Generation, ...]:
        rows = self.connection.execute(
            "SELECT manifest, prefix, state FROM ops.silver_generations WHERE "
            + predicate
            + " ORDER BY created_at, generation_id"
            + (" FOR UPDATE" if lock else ""),
            params,
        ).fetchall()
        return tuple(Generation(DatasetManifest.from_json(row[0]), row[1], row[2]) for row in rows)

    def active(self) -> tuple[Generation, ...]:
        return self._select("state = 'active'")

    def prepare(self, manifest: DatasetManifest, prefix: str) -> Generation:
        manifest = DatasetManifest.from_json(manifest.to_json())
        generation = Generation(manifest, prefix, "preparing")
        S3Store._validate(generation)
        self.connection.execute(
            "INSERT INTO ops.silver_generations (generation_id, created_at, manifest, prefix) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (generation_id) DO NOTHING",
            (
                manifest.generation_id,
                manifest.created_at,
                Jsonb(json.loads(manifest.to_json())),
                prefix,
            ),
        )
        existing = self._select("generation_id = %s", (manifest.generation_id,))[0]
        if existing.manifest != manifest or existing.prefix != prefix:
            raise StorageError("Generation identifier already has different immutable metadata")
        return existing

    def activate(self, generation: Generation, replaces: tuple[str, ...]) -> None:
        # Also atomic when called alone; nested transactions use a savepoint.
        with self.connection.transaction():
            existing = self._select("generation_id = %s", (generation.generation_id,), lock=True)
            if (
                not existing
                or existing[0].manifest != generation.manifest
                or existing[0].prefix != generation.prefix
            ):
                raise StorageError("Generation must be prepared before activation")
            if existing[0].state == "active":
                retired = self._select("generation_id = ANY(%s)", (list(replaces),))
                if len(retired) != len(set(replaces)) or any(
                    item.state not in ("superseded", "deleted") for item in retired
                ):
                    raise StorageError("Retry does not match superseded generations")
                return
            if existing[0].state != "preparing" or generation.generation_id in replaces:
                raise StorageError("Invalid generation activation")
            old = self._select("generation_id = ANY(%s)", (list(replaces),), lock=True)
            if len(old) != len(set(replaces)) or any(item.state != "active" for item in old):
                raise StorageError("Replacement generation is no longer active")
            self.connection.execute(
                "UPDATE ops.silver_generations SET state = 'superseded', retired_at = now() "
                "WHERE generation_id = ANY(%s)",
                (list(replaces),),
            )
            self.connection.execute(
                "UPDATE ops.silver_generations SET state = 'active' WHERE generation_id = %s",
                (generation.generation_id,),
            )
            for run_id in generation.manifest.source_runs:
                self.connection.execute(
                    "INSERT INTO ops.silver_source_receipts (run_id, generation_id, manifest) "
                    "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                    (
                        run_id,
                        generation.generation_id,
                        Jsonb(json.loads(generation.manifest.to_json())),
                    ),
                )

    def retired(self, before: datetime) -> tuple[Generation, ...]:
        if before.tzinfo is None:
            raise ValueError("Retirement cutoff must include a timezone")
        return self._select("state = 'superseded' AND retired_at < %s", (before,))

    def orphaned(self, before: datetime) -> tuple[Generation, ...]:
        """A locked, unactivated reservation is safe to retire after its grace period."""
        if before.tzinfo is None:
            raise ValueError("Orphan cutoff must include a timezone")
        return self._select("state = 'preparing' AND created_at < %s", (before,))

    def mark_deleted(self, generation: Generation) -> None:
        result = self.connection.execute(
            "UPDATE ops.silver_generations SET state = 'deleted' "
            "WHERE generation_id = %s AND prefix = %s AND manifest = %s "
            "AND state IN ('superseded', 'deleted', 'preparing')",
            (
                generation.generation_id,
                generation.prefix,
                Jsonb(json.loads(generation.manifest.to_json())),
            ),
        )
        if result.rowcount != 1:
            raise StorageError("Only an inactive known generation may be marked deleted")
