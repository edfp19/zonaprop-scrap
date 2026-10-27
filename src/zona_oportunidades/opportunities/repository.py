"""Publish the complete Gold serving snapshot in one PostgreSQL transaction."""

import json
from collections.abc import Iterable
from dataclasses import asdict
from datetime import datetime

import psycopg

from .domain import GoldRunResult, Opportunity, assess_price


class GoldRepository:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def replace(
        self, opportunities: Iterable[Opportunity], minimum_count: int, computed_at: datetime
    ) -> GoldRunResult:
        records, candidates = 0, 0
        with psycopg.connect(self.dsn) as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                ALTER TABLE gold.opportunities
                    ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION,
                    ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION,
                    ADD COLUMN IF NOT EXISTS photo_url TEXT,
                    ADD COLUMN IF NOT EXISTS geolocation_visibility TEXT;
                """
            )
            # Existing Compose volumes retain the phase-1 schema; migrate its single-column key.
            cursor.execute(
                """
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conrelid = 'gold.opportunities'::regclass
                          AND contype = 'p' AND cardinality(conkey) = 1
                    ) THEN
                        ALTER TABLE gold.opportunities DROP CONSTRAINT opportunities_pkey;
                        ALTER TABLE gold.opportunities ADD PRIMARY KEY (listing_id, operation);
                    END IF;
                END $$;
                """
            )
            # DELETE is transactional and lets readers continue seeing the old committed snapshot.
            cursor.execute("DELETE FROM gold.opportunities")
            for opportunity in opportunities:
                assessment = assess_price(
                    opportunity.price_usd,
                    opportunity.comparable_average_usd,
                    opportunity.comparable_count,
                    minimum_count,
                )
                values = asdict(opportunity) | asdict(assessment)
                values["quality_flags"] = json.dumps(opportunity.quality_flags)
                cursor.execute(
                    """
                    INSERT INTO gold.opportunities (
                        listing_id, operation, property_type, title, url, area_name,
                        parent_area_name, rooms, covered_area_m2, total_area_m2, price_usd,
                        price_per_m2_usd, comparable_average_usd, comparable_median_usd,
                        comparable_count, discount_percent, has_sufficient_data,
                        is_opportunity, collected_at, run_id, quality_flags,
                        latitude, longitude, photo_url, geolocation_visibility
                    ) VALUES (
                        %(listing_id)s, %(operation)s, %(property_type)s, %(title)s, %(url)s,
                        %(area_name)s, %(parent_area_name)s, %(rooms)s, %(covered_area_m2)s,
                        %(total_area_m2)s, %(price_usd)s, %(price_per_m2_usd)s,
                        %(comparable_average_usd)s, %(comparable_median_usd)s,
                        %(comparable_count)s, %(discount_percent)s, %(has_sufficient_data)s,
                        %(is_opportunity)s, %(collected_at)s, %(run_id)s, %(quality_flags)s,
                        %(latitude)s, %(longitude)s, %(photo_url)s, %(geolocation_visibility)s
                    )
                    """,
                    values,
                )
                records += 1
                candidates += assessment.is_opportunity
        return GoldRunResult(records, candidates, computed_at)
