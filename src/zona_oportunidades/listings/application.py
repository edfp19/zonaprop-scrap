"""Reusable replay use case; the caller supplies the Spark lifecycle."""

from pathlib import Path
from typing import TYPE_CHECKING

from .domain import ReplayConflictError, SilverRunResult
from .storage import AtomicSilverPublisher, bronze_partition, file_sha256
from .transforms import TRANSFORMATION_VERSION, listing_observations

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class ReplayBronzeListings:
    def __init__(self, spark: "SparkSession", data_root: Path) -> None:
        self.spark = spark
        self.data_root = Path(data_root)

    def replay(self, bronze_path: Path) -> SilverRunResult:
        partition = bronze_partition(Path(bronze_path), self.data_root)
        if partition.path.stat().st_size == 0:
            raise ValueError(f"El archivo Bronze está vacío: {partition.path}")
        digest = file_sha256(partition.path)
        publisher = AtomicSilverPublisher(self.data_root)
        existing = publisher.existing(partition, digest, TRANSFORMATION_VERSION)
        if existing is not None:
            actual_records = self.spark.read.parquet(str(existing.output_path)).count()
            if actual_records != existing.silver_records:
                raise ReplayConflictError(
                    f"Conteo Parquet Silver no coincide con manifiesto: {existing.output_path}"
                )
            return existing
        with partition.path.open(encoding="utf-8") as bronze_file:
            bronze_records = sum(1 for _ in bronze_file)
        observations = listing_observations(
            self.spark, str(partition.path), partition.ingestion_date, partition.run_id
        )
        return publisher.publish(
            observations, partition, digest, TRANSFORMATION_VERSION, bronze_records
        )
