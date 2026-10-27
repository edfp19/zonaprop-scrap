"""Private Bronze recovery slots; every caller must hold the storage_lock session.

Reserve in autocommit mode before collecting. Preserve before the publication
transaction; promote inside that transaction only after Silver/Gold verification.
No function deletes local Bronze. Failed slots require explicit investigation.
"""

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from zona_oportunidades.operations.storage import (
    DatasetManifest,
    Generation,
    S3Store,
    build_manifest,
)


class QuarantineError(RuntimeError):
    """Safe operational error containing neither source data nor provider errors."""


@dataclass(frozen=True)
class QuarantineTicket:
    run_id: str
    max_extract_bytes: int  # Includes both artifacts and the immutable storage manifest.


def reserve(
    conn: psycopg.Connection[Any],
    run_id: str,
    *,
    max_extracts: int = 2,
    max_total_bytes: int = 20_000_000,
    max_extract_bytes: int = 10_000_000,
) -> QuarantineTicket:
    """Persist a maximum-sized slot before any source collection starts."""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", run_id):
        raise ValueError("Invalid quarantine run identifier")
    if any(type(n) is not int or n < 1 for n in (max_extracts, max_total_bytes, max_extract_bytes)):
        raise ValueError("Quarantine limits must be positive integers")
    if not conn.autocommit:
        raise QuarantineError("Reservation requires autocommit before collection")
    with conn.transaction():
        existing = conn.execute(
            "SELECT reserved_bytes FROM ops.bronze_quarantine WHERE run_id = %s", (run_id,)
        ).fetchone()
        if existing is not None:
            raise QuarantineError("Run already has a quarantine ticket; inspect before retrying")
        usage = conn.execute(
            "SELECT count(*), COALESCE(sum(reserved_bytes), 0) "
            "FROM ops.bronze_quarantine WHERE state <> 'deleted'"
        ).fetchone()
        if (
            usage is None
            or usage[0] >= max_extracts
            or usage[1] + max_extract_bytes > max_total_bytes
        ):
            raise QuarantineError("Private Bronze quarantine is full; collection refused")
        conn.execute(
            "INSERT INTO ops.bronze_quarantine (run_id, reserved_bytes) VALUES (%s, %s)",
            (run_id, max_extract_bytes),
        )
    return QuarantineTicket(run_id, max_extract_bytes)


def _trusted(path: Path, root: Path) -> None:
    if not path.is_relative_to(root) or any(item.is_symlink() for item in (path, *path.parents)):
        raise QuarantineError("Bronze path is not trusted")


def _sources(root: Path, run_id: str) -> tuple[Path, ...]:
    base = root / "bronze" / "listings"
    _trusted(base, root)
    matches = []
    for partition in base.glob("ingestion_date=*"):
        _trusted(partition, root)
        try:
            day = partition.name.removeprefix("ingestion_date=")
            if date.fromisoformat(day).isoformat() != day:
                continue
        except ValueError:
            continue
        directory = partition / f"run_id={run_id}"
        _trusted(directory, root)
        if directory.exists():
            matches.append(directory)
    if len(matches) > 1:
        raise QuarantineError("Ambiguous Bronze run directories")
    if not matches:
        return ()
    paths = (matches[0] / "listings.jsonl", root / "bronze" / "manifests" / f"{run_id}.json")
    for path in paths:
        _trusted(path, root)
        if not path.is_file():
            raise QuarantineError("Bronze run is incomplete; local files retained")
    return paths


def preserve(
    conn: psycopg.Connection[Any], store: S3Store, ticket: QuarantineTicket, *, data_root: Path
) -> Generation | None:
    """Verify private immutable recovery bytes; never expose exception contents.

    A no-raw marker records absence, not invented observations. An object-storage
    outage leaves the persisted reservation and source files intact, but cannot
    promise durability on a disposable runner. Last committed Gold remains safe.
    """
    if not conn.autocommit:
        raise QuarantineError("Preserve requires autocommit before publication")
    row = conn.execute(
        "SELECT reserved_bytes, state, manifest, prefix FROM ops.bronze_quarantine "
        "WHERE run_id = %s",
        (ticket.run_id,),
    ).fetchone()
    if (
        row is None
        or row[0] != ticket.max_extract_bytes
        or row[1] not in ("reserved", "preserved", "failed")
    ):
        raise QuarantineError("Ticket is not an outstanding quarantine reservation")
    if row[1] in ("preserved", "failed") and row[2] is not None:
        manifest = DatasetManifest.from_json(row[2])
        return Generation(manifest, row[3], row[1]) if row[1] == "preserved" else None
    try:
        sources = _sources(Path(data_root).absolute(), ticket.run_id)
        with TemporaryDirectory(prefix="bronze-quarantine-") as temporary:
            staging = Path(temporary) / "upload"
            staging.mkdir(mode=0o700)
            total = 0
            rows = 0
            if sources:
                for source, name in zip(
                    sources, ("listings.jsonl", "collection_manifest.json"), strict=True
                ):
                    with source.open("rb") as stream:
                        content = stream.read(ticket.max_extract_bytes - total + 1)
                    total += len(content)
                    if total > ticket.max_extract_bytes:
                        raise QuarantineError(
                            "Bronze exceeds quarantine byte allowance; files retained"
                        )
                    (staging / name).write_bytes(content)
                    if name == "listings.jsonl":
                        rows = len(content.splitlines())
            else:
                (staging / "failure.json").write_text(
                    json.dumps(
                        {"run_id": ticket.run_id, "reason": "no_bronze", "raw_present": False}
                    )
                )
            manifest = build_manifest(
                staging,
                generation_id=ticket.run_id,
                kind="bronze",
                rows=rows,
                source_runs=(ticket.run_id,),
            )
            if (
                sum(item.bytes for item in manifest.files) + len(manifest.to_json().encode())
                > ticket.max_extract_bytes
            ):
                raise QuarantineError("Bronze exceeds quarantine byte allowance; files retained")
            if row[2] is not None:
                original = DatasetManifest.from_json(row[2])
                if original.files != manifest.files or original.rows != manifest.rows:
                    raise QuarantineError("Reserved Bronze inventory changed; files retained")
                manifest = original
            prefix = f"bronze/{ticket.run_id}"
            # Persist the exact inventory before upload, so an interrupted retry is immutable.
            conn.execute(
                "UPDATE ops.bronze_quarantine SET manifest = %s, prefix = %s WHERE run_id = %s",
                (Jsonb(json.loads(manifest.to_json())), prefix, ticket.run_id),
            )
            generation = Generation(manifest, prefix, "preserved" if sources else "failed")
            if store.upload(staging, manifest) != prefix:
                raise QuarantineError("Unexpected Bronze quarantine prefix")
            store.download(generation, Path(temporary) / "readback")
            conn.execute(
                "UPDATE ops.bronze_quarantine SET state = %s WHERE run_id = %s",
                (generation.state, ticket.run_id),
            )
            return generation if sources else None
    except Exception:
        raise QuarantineError(
            "Private Bronze preservation failed; reservation and local files retained"
        ) from None


def promote(conn: psycopg.Connection[Any], ticket: QuarantineTicket) -> None:
    """Call in the verified Silver/Gold transaction; no implicit commit here."""
    row = conn.execute(
        "UPDATE ops.bronze_quarantine SET state = 'promoted' "
        "WHERE run_id = %s AND reserved_bytes = %s "
        "AND state IN ('preserved', 'promoted') AND manifest IS NOT NULL RETURNING run_id",
        (ticket.run_id, ticket.max_extract_bytes),
    ).fetchone()
    if row is None:
        raise QuarantineError("Only verified preserved Bronze can be promoted")


def cleanup_promoted(conn: psycopg.Connection[Any], store: S3Store) -> tuple[str, ...]:
    """Delete only exact inventories made eligible by corresponding publication."""
    rows = conn.execute(
        "SELECT run_id, manifest, prefix FROM ops.bronze_quarantine "
        "WHERE state = 'promoted' ORDER BY run_id"
    ).fetchall()
    deleted = []
    for run_id, raw_manifest, prefix in rows:
        try:
            manifest = DatasetManifest.from_json(raw_manifest)
            if (manifest.kind, manifest.generation_id, manifest.source_runs, prefix) != (
                "bronze",
                run_id,
                (run_id,),
                f"bronze/{run_id}",
            ):
                raise QuarantineError("Invalid quarantine deletion inventory")
            store.delete(Generation(manifest, prefix, "superseded"))
        except Exception:
            raise QuarantineError(
                "Private Bronze deletion could not be verified; slot retained"
            ) from None
        conn.execute(
            "UPDATE ops.bronze_quarantine SET state = 'deleted' WHERE run_id = %s "
            "AND state = 'promoted'",
            (run_id,),
        )
        deleted.append(run_id)
    return tuple(deleted)
