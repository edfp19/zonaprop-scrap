"""Spark selects current eligible observations and computes leave-one-out peer statistics."""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from .domain import ComparisonPolicy

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def current_listings(
    observations: "DataFrame", policy: ComparisonPolicy, as_of: datetime
) -> "DataFrame":
    """Select latest first so an invalid newer posting cannot resurrect an older good price."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    window = Window.partitionBy("listing_id", "operation").orderBy(
        F.col("observed_at").desc_nulls_last(),
        F.col("ingestion_date").desc(),
        F.col("run_id").desc(),
        F.col("source_observation_id").desc(),
        F.when(F.col("currency") == "USD", 0).otherwise(1),
        F.col("price_index").asc_nulls_last(),
        F.col("observation_id").asc(),
    )
    latest = (
        observations.filter(F.col("observed_at") <= F.lit(as_of))
        .withColumn("_latest", F.row_number().over(window))
        .filter(F.col("_latest") == 1)
        .drop("_latest")
    )
    # Precision/coverage warnings do not invalidate price comparisons; keep them visible.
    blocking_flags = F.array(
        *[
            F.lit(flag)
            for flag in (
                "malformed_json",
                "missing_envelope_schema_version",
                "unsupported_envelope_schema",
                "source_run_id_missing",
                "source_run_id_mismatch",
                "covered_area_exceeds_total",
                "operation_requested_mismatch",
            )
        ]
    )
    return latest.filter(
        (F.col("observed_at") >= F.lit(as_of - timedelta(days=policy.max_age_days)))
        & (F.col("posting_type") == "PROPERTY")
        & (F.col("property_type") == "DEPARTAMENTO")
        & F.col("operation").isin("VENTA", "ALQUILER")
        & (F.col("currency") == "USD")
        & (F.col("price") > 0)
        & (F.col("rooms") > 0)
        & (F.col("covered_area_m2") > 0)
        & ~F.isnan("covered_area_m2")
        & (F.col("covered_area_m2") < float("inf"))
        & (F.length(F.trim("listing_id")) > 0)
        & (F.length(F.trim("title")) > 0)
        & (F.length(F.trim("listing_url")) > 0)
        & (F.length(F.trim("location_id")) > 0)
        & (F.length(F.trim("location_name")) > 0)
        & (F.size(F.array_intersect("quality_flags", blocking_flags)) == 0)
    )


def comparable_statistics(current: "DataFrame", policy: ComparisonPolicy) -> "DataFrame":
    """Compare distinct listings in the same source area, operation, type, and room count."""
    from pyspark.sql import functions as F

    target, peer = current.alias("target"), current.alias("peer")
    target_area = F.col("target.covered_area_m2").cast("decimal(18,6)")
    peer_area = F.col("peer.covered_area_m2").cast("decimal(18,6)")
    tolerance = F.lit(Decimal(str(policy.area_tolerance)))
    matches = (
        (F.col("target.listing_id") != F.col("peer.listing_id"))
        & (F.col("target.operation") == F.col("peer.operation"))
        & (F.col("target.property_type") == F.col("peer.property_type"))
        & (F.col("target.location_id") == F.col("peer.location_id"))
        & (F.col("target.rooms") == F.col("peer.rooms"))
        & peer_area.between(
            target_area * (1 - tolerance),
            target_area * (1 + tolerance),
        )
    )
    statistics = (
        target.join(peer, matches, "left")
        .groupBy(
            F.col("target.listing_id").alias("listing_id"),
            F.col("target.operation").alias("operation"),
        )
        .agg(
            F.count("peer.listing_id").alias("comparable_count"),
            F.avg("peer.price").alias("comparable_average_usd"),
            F.percentile("peer.price", F.lit(0.5))
            .cast("decimal(22,6)")
            .alias("comparable_median_usd"),
        )
    )
    photo_large = F.get_json_object("raw_json", "$.payload.visiblePictures.pictures[0].url730x532")
    photo_small = F.get_json_object("raw_json", "$.payload.visiblePictures.pictures[0].url360x266")
    photo = F.when(photo_large.rlike("^https?://"), photo_large).when(
        photo_small.rlike("^https?://"), photo_small
    )
    return current.join(statistics, ["listing_id", "operation"]).select(
        "listing_id",
        "operation",
        "property_type",
        "title",
        F.col("listing_url").alias("url"),
        F.col("location_name").alias("area_name"),
        "parent_area_name",
        "rooms",
        "covered_area_m2",
        "total_area_m2",
        F.col("price").alias("price_usd"),
        (F.col("price") / F.col("covered_area_m2")).alias("price_per_m2_usd"),
        "comparable_average_usd",
        "comparable_median_usd",
        "comparable_count",
        F.col("observed_at").alias("collected_at"),
        "run_id",
        "quality_flags",
        "latitude",
        "longitude",
        "geolocation_visibility",
        photo.alias("photo_url"),
    )
