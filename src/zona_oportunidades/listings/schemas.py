"""Explicit source schema.  JSON inference is deliberately never used."""


def bronze_listing_schema():
    """Return the known collector envelope and relevant Zonaprop payload fields."""
    from pyspark.sql.types import (  # lazy: importing the public API needs no Spark
        ArrayType,
        BooleanType,
        IntegerType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    feature = StructType([StructField("value", StringType()), StructField("measure", StringType())])
    price = StructType([StructField("amount", StringType()), StructField("currency", StringType())])
    operation = StructType(
        [StructField("name", StringType()), StructField("operationTypeId", StringType())]
    )
    price_operation = StructType(
        [StructField("operationType", operation), StructField("prices", ArrayType(price))]
    )
    location = StructType(
        [
            StructField("locationId", StringType()),
            StructField("name", StringType()),
            StructField("label", StringType()),
            StructField("depth", IntegerType()),
            StructField(
                "parent",
                StructType(
                    [
                        StructField("locationId", StringType()),
                        StructField("name", StringType()),
                        StructField("label", StringType()),
                        StructField("depth", IntegerType()),
                    ]
                ),
            ),
        ]
    )
    payload = StructType(
        [
            StructField("postingId", StringType()),
            StructField("postingCode", StringType()),
            StructField("postingType", StringType()),
            StructField("title", StringType()),
            StructField("url", StringType()),
            StructField(
                "realEstateType",
                StructType(
                    [
                        StructField("realEstateTypeId", StringType()),
                        StructField("name", StringType()),
                    ]
                ),
            ),
            StructField("house", StructType([StructField("type", StringType())])),
            StructField("mainFeatures", MapType(StringType(), feature)),
            StructField("priceOperationTypes", ArrayType(price_operation)),
            StructField(
                "postingLocation",
                StructType(
                    [
                        StructField(
                            "address",
                            StructType(
                                [
                                    StructField("name", StringType()),
                                    StructField("visibility", StringType()),
                                ]
                            ),
                        ),
                        StructField("location", location),
                        StructField(
                            "postingGeolocation",
                            StructType(
                                [
                                    StructField("visibility", StringType()),
                                    StructField(
                                        "geolocation",
                                        StructType(
                                            [
                                                StructField("latitude", StringType()),
                                                StructField("longitude", StringType()),
                                            ]
                                        ),
                                    ),
                                ]
                            ),
                        ),
                    ]
                ),
            ),
        ]
    )
    return StructType(
        [
            StructField("schema_version", IntegerType()),
            StructField("extractor_version", StringType()),
            StructField("run_id", StringType()),
            StructField("collected_at", StringType()),
            StructField("source_page_url", StringType()),
            StructField("requested_operation", StringType()),
            StructField("requested_region", StringType()),
            StructField("requested_currency", StringType()),
            StructField("currency_context_verified", BooleanType()),
            StructField("payload", payload),
            # Required by Spark PERMISSIVE parsing. The original line remains raw_json too.
            StructField("_corrupt_record", StringType()),
        ]
    )
