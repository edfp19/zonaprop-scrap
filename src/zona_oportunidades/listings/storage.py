"""Filesystem publication protocol for one immutable Silver run."""

import hashlib
import json
import os
import re
import shutil
import tempfile
from datetime import date as calendar_date
from pathlib import Path
from typing import TYPE_CHECKING

from .domain import BronzePartition, BronzePathError, ReplayConflictError, SilverRunResult

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def bronze_partition(path: Path, data_root: Path) -> BronzePartition:
    """Validate the collector's partitioned layout and return its trusted context."""
    path = path.resolve()
    root = (data_root / "bronze" / "listings").resolve()
    try:
        relative = path.relative_to(root)
    except ValueError as error:
        raise BronzePathError(f"Bronze fuera de {root}: {path}") from error
    parts = relative.parts
    if (
        len(parts) != 3
        or not parts[0].startswith("ingestion_date=")
        or not parts[1].startswith("run_id=")
    ):
        raise BronzePathError(
            "Se esperaba bronze/listings/ingestion_date=YYYY-MM-DD/run_id=ID/archivo.jsonl"
        )
    date, run_id = parts[0].removeprefix("ingestion_date="), parts[1].removeprefix("run_id=")
    try:
        calendar_date.fromisoformat(date)
    except ValueError as error:
        raise BronzePathError(f"Fecha de ingestión inválida: {date}") from error
    if (
        not run_id
        or not re.fullmatch(r"[A-Za-z0-9_-]+", run_id)
        or path.suffix != ".jsonl"
        or not path.is_file()
    ):
        raise BronzePathError(f"Archivo Bronze inválido: {path}")
    return BronzePartition(path=path, ingestion_date=date, run_id=run_id)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class AtomicSilverPublisher:
    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root

    def final_path(self, partition: BronzePartition) -> Path:
        return (
            self.data_root
            / "silver"
            / "listing_observations"
            / f"ingestion_date={partition.ingestion_date}"
            / f"run_id={partition.run_id}"
        )

    def existing(
        self, partition: BronzePartition, digest: str, version: str
    ) -> SilverRunResult | None:
        output = self.final_path(partition)
        manifest_path = output / "_run_manifest.json"
        if not output.exists():
            return None
        if not manifest_path.is_file():
            raise ReplayConflictError(f"Salida Silver existente sin manifiesto: {output}")
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ReplayConflictError(f"Manifiesto Silver inválido: {manifest_path}") from error
        if not isinstance(manifest, dict):
            raise ReplayConflictError(f"Manifiesto Silver inválido: {manifest_path}")
        required = {"bronze_records", "silver_records", "input_sha256", "transformation_version"}
        if not required.issubset(manifest) or not (output / "_SUCCESS").is_file():
            raise ReplayConflictError(f"Salida Silver existente incompleta: {output}")
        if (
            manifest.get("run_id") != partition.run_id
            or manifest.get("ingestion_date") != partition.ingestion_date
        ):
            raise ReplayConflictError(f"Manifiesto Silver no coincide con la partición: {output}")
        for field in ("bronze_records", "silver_records"):
            value = manifest.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ReplayConflictError(f"Conteo inválido en manifiesto Silver: {output}")
        actual_inventory = self._parquet_inventory(output)
        expected_inventory = manifest.get("parquet_inventory")
        if expected_inventory is not None and (
            not isinstance(expected_inventory, list) or expected_inventory != actual_inventory
        ):
            raise ReplayConflictError(f"Inventario Parquet Silver no coincide: {output}")
        if (
            manifest.get("input_sha256") != digest
            or manifest.get("transformation_version") != version
        ):
            raise ReplayConflictError(
                "La corrida Silver existente tiene otro input o versión; no se sobrescribe."
            )
        return SilverRunResult(
            partition.run_id, manifest["bronze_records"], manifest["silver_records"], output, True
        )

    @staticmethod
    def _parquet_inventory(output: Path) -> list[dict[str, int | str]]:
        parts = sorted(path for path in output.rglob("part-*.parquet") if path.is_file())
        if not parts:
            raise ReplayConflictError(f"Salida Silver sin archivos Parquet: {output}")
        inventory = []
        for part in parts:
            try:
                with part.open("rb") as source:
                    header = source.read(4)
                    source.seek(-4, os.SEEK_END)
                    footer = source.read(4)
            except (OSError, ValueError) as error:
                raise ReplayConflictError(f"Archivo Parquet Silver ilegible: {part}") from error
            if header != b"PAR1" or footer != b"PAR1":
                raise ReplayConflictError(f"Archivo Parquet Silver truncado: {part}")
            inventory.append(
                {
                    "path": str(part.relative_to(output)),
                    "bytes": part.stat().st_size,
                    "sha256": file_sha256(part),
                }
            )
        return inventory

    def publish(
        self,
        frame: "DataFrame",
        partition: BronzePartition,
        digest: str,
        version: str,
        bronze_records: int,
    ) -> SilverRunResult:
        output = self.final_path(partition)
        staging_root = self.data_root / "silver" / "_staging"
        staging_root.mkdir(parents=True, exist_ok=True)
        staging_root_for_run = Path(
            tempfile.mkdtemp(prefix=f"{partition.run_id}-{digest[:12]}-", dir=staging_root)
        )
        staging = staging_root_for_run / "output"
        try:
            silver_records = frame.count()
            # Staging is a sibling outside listing_observations, preventing recursive reads.
            frame.write.mode("errorifexists").partitionBy("posting_type").parquet(str(staging))
            manifest = {
                "run_id": partition.run_id,
                "ingestion_date": partition.ingestion_date,
                "source_path": str(partition.path),
                "input_sha256": digest,
                "transformation_version": version,
                "bronze_records": bronze_records,
                "silver_records": silver_records,
                "parquet_inventory": self._parquet_inventory(staging),
            }
            (staging / "_run_manifest.json").write_text(
                json.dumps(manifest, indent=2), encoding="utf-8"
            )
            output.parent.mkdir(parents=True, exist_ok=True)
            if output.exists():
                existing = self.existing(partition, digest, version)
                if existing is not None:
                    return existing
                raise ReplayConflictError(f"Salida Silver ya existe: {output}")
            try:
                os.rename(staging, output)
            except OSError:
                if output.exists():
                    existing = self.existing(partition, digest, version)
                    if existing is not None:
                        return existing
                raise
            return SilverRunResult(partition.run_id, bronze_records, silver_records, output, False)
        finally:
            if staging_root_for_run.exists():
                shutil.rmtree(staging_root_for_run)
