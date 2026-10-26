"""DataFrame-only transformation from immutable Bronze lines to Silver observations."""

from typing import TYPE_CHECKING

from .schemas import bronze_listing_schema

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.column import Column

TRANSFORMATION_VERSION = "checkpoint2-listings-v2"
SILVER_SCHEMA_VERSION = "1"
MIN_LATITUDE, MAX_LATITUDE = -35.3, -34.2
MIN_LONGITUDE, MAX_LONGITUDE = -59.3, -57.3
VALID_POSTING_TYPES = ("PROPERTY", "DEVELOPMENT")
VALID_SQUARE_METRE_UNITS = ("m²", "m2", "mts2", "mt2")


def _parse_and_explode(spark: "SparkSession", bronze_path: str) -> "DataFrame":
    """Parse explicit JSON and retain one row for each source op/price combination."""
    from pyspark.sql import functions as F

    raw = spark.read.text(bronze_path).select(F.col("value").alias("raw_json"))
    deduplicated = raw.groupBy("raw_json").agg(F.count(F.lit(1)).alias("raw_observation_count"))
    parsed = deduplicated.withColumn(
        "record",
        F.from_json(
            F.col("raw_json"),
            bronze_listing_schema(),
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ),
    ).select("raw_json", "raw_observation_count", "record.*")
    operations = parsed.select(
        "*",
        F.posexplode_outer("payload.priceOperationTypes").alias(
            "operation_index", "operation_entry"
        ),
    )
    return operations.select(
        "*", F.posexplode_outer("operation_entry.prices").alias("price_index", "price_entry")
    )


def _safe_double(expression: str) -> "Column":
    """Cast only finite numeric text; booleans and infinities become unusable nulls."""
    from pyspark.sql import functions as F

    candidate = F.expr(f"try_cast({expression} as double)")
    raw = F.lower(F.trim(F.expr(expression)))
    return F.when(
        candidate.isNotNull()
        & ~F.isnan(candidate)
        & (F.abs(candidate) != F.lit(float("inf")))
        & ~raw.isin("true", "false", "infinity", "+infinity", "-infinity", "inf", "+inf", "-inf"),
        candidate,
    )


def _normalized_operation(name: "Column", operation_id: "Column") -> "Column":
    """Normalize the source operation once for flags and the Silver projection."""
    from pyspark.sql import functions as F

    normalized_name = F.upper(F.trim(name))
    return F.when((normalized_name == "VENTA") | (operation_id == "1"), "VENTA").when(
        normalized_name.isin("ALQUILER", "RENT") | (operation_id == "2"), "ALQUILER"
    )


def _numeric_columns() -> dict[str, "Column"]:
    """Build normalized numeric columns without treating empty values as invalid values."""
    from pyspark.sql import functions as F

    feature = lambda code: F.col("payload.mainFeatures").getItem(code)  # noqa: E731
    room_value = F.trim(feature("CFT1").value)
    room_number = F.expr("try_cast(payload.mainFeatures['CFT1'].value as double)")
    rooms = F.when(
        room_number.isNotNull()
        & ~F.isnan(room_number)
        & (F.abs(room_number) != F.lit(float("inf")))
        & (room_number == F.floor(room_number))
        & (room_number >= -(2**31))
        & (room_number <= 2**31 - 1),
        room_number.cast("int"),
    )
    total_raw = F.trim(feature("CFT100").value)
    covered_raw = F.trim(feature("CFT101").value)
    total = _safe_double("payload.mainFeatures['CFT100'].value")
    covered = _safe_double("payload.mainFeatures['CFT101'].value")
    return {
        "feature": feature,
        "room_value": room_value,
        "rooms": rooms,
        "bedrooms": F.expr("try_cast(payload.mainFeatures['CFT2'].value as int)"),
        "bathrooms": F.expr("try_cast(payload.mainFeatures['CFT3'].value as int)"),
        "total_raw": total_raw,
        "covered_raw": covered_raw,
        "total": total,
        "covered": covered,
    }


def _coordinates() -> dict[str, "Column"]:
    """Keep independently valid coordinate components while making pair booleans total."""
    from pyspark.sql import functions as F

    latitude_raw = F.trim(F.col("payload.postingLocation.postingGeolocation.geolocation.latitude"))
    longitude_raw = F.trim(
        F.col("payload.postingLocation.postingGeolocation.geolocation.longitude")
    )
    latitude = _safe_double("payload.postingLocation.postingGeolocation.geolocation.latitude")
    longitude = _safe_double("payload.postingLocation.postingGeolocation.geolocation.longitude")
    latitude_world = latitude.isNotNull() & (latitude >= -90) & (latitude <= 90)
    longitude_world = longitude.isNotNull() & (longitude >= -180) & (longitude <= 180)
    valid_pair = F.coalesce(latitude_world & longitude_world, F.lit(False))
    in_screen = (
        valid_pair
        & (latitude >= MIN_LATITUDE)
        & (latitude <= MAX_LATITUDE)
        & (longitude >= MIN_LONGITUDE)
        & (longitude <= MAX_LONGITUDE)
    )
    return {
        "latitude_raw": latitude_raw,
        "longitude_raw": longitude_raw,
        "latitude": F.when(latitude_world, latitude),
        "longitude": F.when(longitude_world, longitude),
        "valid_pair": valid_pair,
        "in_screen": F.coalesce(in_screen, F.lit(False)),
    }


def _quality_flags(numbers: dict[str, "Column"], coords: dict[str, "Column"]) -> "Column":
    """Return a non-null array of flags, preserving missing versus invalid distinctions."""
    from pyspark.sql import functions as F

    feature = numbers["feature"]
    amount_raw = F.trim(F.col("price_entry.amount"))
    amount = F.expr("try_cast(price_entry.amount as decimal(18,2))")
    currency = F.upper(F.trim(F.col("price_entry.currency")))
    posting_type = F.upper(F.trim(F.col("payload.postingType")))
    operation_name = F.col("operation_entry.operationType.name")
    operation_id = F.col("operation_entry.operationType.operationTypeId")
    operation = _normalized_operation(operation_name, operation_id)
    total_unit = F.lower(F.trim(feature("CFT100").measure))
    covered_unit = F.lower(F.trim(feature("CFT101").measure))
    location = F.col("payload.postingLocation.location")
    timestamp_raw = F.trim(F.col("collected_at"))
    timestamp = F.try_to_timestamp(F.col("collected_at"))
    listing_url = F.trim(F.col("payload.url"))
    missing_lat = coords["latitude_raw"].isNull() | (coords["latitude_raw"] == "")
    missing_lon = coords["longitude_raw"].isNull() | (coords["longitude_raw"] == "")
    entries = F.array(
        F.when(F.col("_corrupt_record").isNotNull(), "malformed_json"),
        F.when(F.col("schema_version").isNull(), "missing_envelope_schema_version").when(
            F.col("schema_version") != 1, "unsupported_envelope_schema"
        ),
        F.when(F.col("payload").isNull(), "payload_missing"),
        F.when(F.col("run_id").isNull() | (F.trim(F.col("run_id")) == ""), "source_run_id_missing"),
        F.when(
            F.col("run_id").isNotNull()
            & (F.trim(F.col("run_id")) != "")
            & (F.col("run_id") != F.col("trusted_run_id")),
            "source_run_id_mismatch",
        ),
        F.when(timestamp_raw.isNull() | (timestamp_raw == ""), "timestamp_missing").when(
            timestamp.isNull(), "timestamp_invalid"
        ),
        F.when(posting_type.isNull() | (posting_type == ""), "posting_type_missing").when(
            ~posting_type.isin(*VALID_POSTING_TYPES), "posting_type_unknown"
        ),
        F.when(posting_type == "DEVELOPMENT", "development_not_individual_comparable"),
        F.when(
            F.col("payload.postingId").isNull() | (F.trim(F.col("payload.postingId")) == ""),
            "listing_id_missing",
        ),
        F.when(listing_url.isNull() | (listing_url == ""), "listing_url_missing").when(
            ~listing_url.rlike("^https?://") & ~listing_url.startswith("/"),
            "listing_url_invalid",
        ),
        F.when(
            F.col("price_entry").isNull() | amount_raw.isNull() | (amount_raw == ""),
            "price_missing",
        )
        .when(amount.isNull(), "price_invalid")
        .when(amount <= 0, "price_nonpositive"),
        F.when(currency.isNull() | (currency == ""), "currency_missing").when(
            currency != "USD", "currency_non_usd"
        ),
        F.when(
            ~F.coalesce(F.col("currency_context_verified"), F.lit(False)), "unverified_usd_context"
        ),
        F.when(F.col("operation_entry").isNull() | operation.isNull(), "operation_unknown"),
        F.when(
            operation.isNotNull() & (operation != F.upper(F.trim(F.col("requested_operation")))),
            "operation_requested_mismatch",
        ),
        F.when(
            feature("CFT1").isNull()
            | numbers["room_value"].isNull()
            | (numbers["room_value"] == ""),
            "rooms_missing",
        )
        .when(numbers["rooms"].isNull(), "rooms_invalid")
        .when(numbers["rooms"] <= 0, "rooms_nonpositive"),
        F.when(
            feature("CFT100").isNull()
            | numbers["total_raw"].isNull()
            | (numbers["total_raw"] == ""),
            "total_area_missing",
        )
        .when(
            total_unit.isNull() | (total_unit == "") | ~total_unit.isin(*VALID_SQUARE_METRE_UNITS),
            "total_area_unknown_unit",
        )
        .when(numbers["total"].isNull(), "total_area_invalid")
        .when(numbers["total"] <= 0, "total_area_nonpositive"),
        F.when(
            feature("CFT101").isNull()
            | numbers["covered_raw"].isNull()
            | (numbers["covered_raw"] == ""),
            "covered_area_missing",
        )
        .when(
            covered_unit.isNull()
            | (covered_unit == "")
            | ~covered_unit.isin(*VALID_SQUARE_METRE_UNITS),
            "covered_area_unknown_unit",
        )
        .when(numbers["covered"].isNull(), "covered_area_invalid")
        .when(numbers["covered"] <= 0, "covered_area_nonpositive"),
        F.when(
            (numbers["covered"] > numbers["total"])
            & numbers["covered"].isNotNull()
            & numbers["total"].isNotNull(),
            "covered_area_exceeds_total",
        ),
        F.when(
            location.isNull()
            | F.col("payload.postingLocation.location.locationId").isNull()
            | F.col("payload.postingLocation.location.name").isNull(),
            "location_missing",
        ),
        F.when(missing_lat | missing_lon, "coordinates_missing_pair").when(
            ~coords["valid_pair"], "coordinates_invalid_or_outside_world"
        ),
        F.when(coords["valid_pair"] & ~coords["in_screen"], "coordinates_outside_approx_coverage"),
        F.when(
            coords["valid_pair"]
            & F.col("payload.postingLocation.postingGeolocation.visibility").isNull(),
            "coordinates_precision_unknown",
        ),
        F.when(F.col("raw_observation_count") > 1, "duplicate_source_observation"),
    )
    return F.filter(entries, lambda flag: flag.isNotNull())


def _projection(
    rows: "DataFrame", bronze_path: str, ingestion_date: str, source_run_id: str
) -> "DataFrame":
    """Select the stable flat Silver contract and derive no business score."""
    from pyspark.sql import functions as F

    rows = rows.withColumn("trusted_run_id", F.lit(source_run_id))
    numbers, coords = _numeric_columns(), _coordinates()
    flags = _quality_flags(numbers, coords)
    source_id = F.sha2("raw_json", 256)
    raw_type = F.upper(F.trim(F.col("payload.postingType")))
    posting_type = F.when(raw_type.isin(*VALID_POSTING_TYPES), raw_type).otherwise("UNKNOWN")
    raw_operation_name = F.col("operation_entry.operationType.name")
    raw_operation_id = F.col("operation_entry.operationType.operationTypeId")
    operation = _normalized_operation(raw_operation_name, raw_operation_id)
    currency = F.upper(F.trim(F.col("price_entry.currency")))
    price = F.expr("try_cast(price_entry.amount as decimal(18,2))")
    real_estate_name = F.upper(F.trim(F.col("payload.realEstateType.name")))
    property_type = F.when(
        (F.col("payload.realEstateType.realEstateTypeId") == "2")
        | real_estate_name.isin("DEPARTAMENTOS", "DEPARTAMENTO"),
        "DEPARTAMENTO",
    )
    source_url = F.trim(F.col("payload.url"))
    listing_url = F.when(source_url.rlike("^https?://"), source_url).when(
        source_url.startswith("/"), F.concat(F.lit("https://www.zonaprop.com.ar"), source_url)
    )
    total_unit_ok = F.lower(F.trim(numbers["feature"]("CFT100").measure)).isin(
        *VALID_SQUARE_METRE_UNITS
    )
    covered_unit_ok = F.lower(F.trim(numbers["feature"]("CFT101").measure)).isin(
        *VALID_SQUARE_METRE_UNITS
    )
    return rows.select(
        source_id.alias("source_observation_id"),
        F.sha2(
            F.concat_ws(
                "|",
                source_id,
                F.lit(source_run_id),
                F.coalesce("operation_index", F.lit(-1)),
                F.coalesce("price_index", F.lit(-1)),
            ),
            256,
        ).alias("observation_id"),
        F.lit("zonaprop").alias("source"),
        F.col("payload.postingId").alias("listing_id"),
        F.col("payload.postingCode").alias("posting_code"),
        posting_type.alias("posting_type"),
        property_type.alias("property_type"),
        F.col("payload.house.type").alias("source_house_type"),
        F.col("payload.realEstateType.realEstateTypeId").alias("real_estate_type_id"),
        F.col("payload.realEstateType.name").alias("real_estate_type_name"),
        F.col("payload.title").alias("title"),
        listing_url.alias("listing_url"),
        F.col("source_page_url"),
        raw_operation_id.alias("source_operation_id"),
        raw_operation_name.alias("source_operation_name"),
        operation.alias("operation"),
        currency.alias("currency"),
        price.alias("price"),
        "operation_index",
        "price_index",
        numbers["rooms"].alias("rooms"),
        numbers["bedrooms"].alias("bedrooms"),
        numbers["bathrooms"].alias("bathrooms"),
        F.when(total_unit_ok & (numbers["total"] > 0), numbers["total"]).alias("total_area_m2"),
        F.when(covered_unit_ok & (numbers["covered"] > 0), numbers["covered"]).alias(
            "covered_area_m2"
        ),
        F.col("payload.postingLocation.location.locationId").alias("location_id"),
        F.col("payload.postingLocation.location.name").alias("location_name"),
        F.col("payload.postingLocation.location.label").alias("location_label"),
        F.col("payload.postingLocation.location.depth").alias("location_depth"),
        F.col("payload.postingLocation.location.parent.locationId").alias("parent_area_id"),
        F.col("payload.postingLocation.location.parent.name").alias("parent_area_name"),
        F.get_json_object("raw_json", "$.payload.postingLocation.location").alias(
            "location_hierarchy_json"
        ),
        F.col("payload.postingLocation.address.name").alias("address"),
        F.col("payload.postingLocation.address.visibility").alias("address_visibility"),
        F.col("payload.postingLocation.postingGeolocation.visibility").alias(
            "geolocation_visibility"
        ),
        coords["latitude"].alias("latitude"),
        coords["longitude"].alias("longitude"),
        coords["valid_pair"].alias("has_valid_coordinates"),
        coords["in_screen"].alias("within_approx_coverage"),
        coords["in_screen"].alias("map_eligible"),
        flags.alias("quality_flags"),
        F.try_to_timestamp(F.col("collected_at")).alias("observed_at"),
        F.col("run_id").alias("source_envelope_run_id"),
        F.lit(source_run_id).alias("run_id"),
        "requested_operation",
        "requested_region",
        "requested_currency",
        "currency_context_verified",
        F.col("schema_version").alias("source_schema_version"),
        "extractor_version",
        F.lit(SILVER_SCHEMA_VERSION).alias("silver_schema_version"),
        F.lit(ingestion_date).alias("ingestion_date"),
        "raw_observation_count",
        "raw_json",
        F.lit(bronze_path).alias("bronze_path"),
        F.lit(TRANSFORMATION_VERSION).alias("transformation_version"),
    )


def listing_observations(
    spark: "SparkSession", bronze_path: str, ingestion_date: str, source_run_id: str
) -> "DataFrame":
    """Produce flat observations with explicit schema, flags, and Bronze traceability."""
    return _projection(
        _parse_and_explode(spark, bronze_path), bronze_path, ingestion_date, source_run_id
    )
