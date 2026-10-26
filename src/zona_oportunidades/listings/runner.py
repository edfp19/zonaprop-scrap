"""Spark session boundary kept out of API/dashboard imports."""

import os
from pathlib import Path
from typing import TYPE_CHECKING

from .application import ReplayBronzeListings
from .domain import SilverRunResult

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def create_local_spark(master: str | None = None) -> "SparkSession":
    """Create the owned Spark session with settings suitable for local replay."""
    try:
        from pyspark.sql import SparkSession
    except ImportError as error:
        raise RuntimeError(
            "PySpark no está instalado. Instale la dependencia Spark del pipeline."
        ) from error
    try:
        return (
            SparkSession.builder.master(master or os.environ.get("SPARK_MASTER", "local[2]"))
            .appName("zona-oportunidades-listings")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as error:
        raise RuntimeError(
            "No se pudo iniciar Spark. Verifique Java 17 y la configuración de SPARK_MASTER."
        ) from error


def run_bronze_file(
    bronze_path: Path, data_root: Path, *, master: str | None = None
) -> SilverRunResult:
    spark = create_local_spark(master)
    try:
        return ReplayBronzeListings(spark, Path(data_root)).replay(Path(bronze_path))
    finally:
        spark.stop()
