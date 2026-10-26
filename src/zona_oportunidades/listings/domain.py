"""Small Spark-independent contracts for the listings replay."""

from dataclasses import dataclass
from pathlib import Path


class ReplayConflictError(RuntimeError):
    """A published run belongs to a different Bronze input or transformer."""


class BronzePathError(ValueError):
    """The supplied file is not in the supported Bronze partition layout."""


@dataclass(frozen=True)
class BronzePartition:
    path: Path
    ingestion_date: str
    run_id: str


@dataclass(frozen=True)
class SilverRunResult:
    run_id: str
    bronze_records: int
    silver_records: int
    output_path: Path
    already_processed: bool
