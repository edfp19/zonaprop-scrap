r"""Publish Gold transactionally; runtime connections need DML rights only.

One-time local upgrade for an existing phase-1 Compose volume, as its owner::

    docker compose exec -T postgres sh -c \
      'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
      < infra/postgres/migrate_gold.sql

This repeatable migration preserves rows and upgrades the key and map columns.
Fresh volumes already receive the current schema from init.sql. For a hosted
installation, run init.sql, migrate_gold.sql and pipeline_writer.sql as the
owner through a direct connection; set the writer password privately afterward.
The runtime writer must never be responsible for schema upgrades.
"""

import json
from collections.abc import Iterable
from dataclasses import asdict
from datetime import datetime
from typing import Any

import psycopg

from .domain import GoldRunResult, Opportunity, assess_price


class GoldRepository:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def replace(
        self,
        opportunities: Iterable[Opportunity],
        minimum_count: int,
        computed_at: datetime,
        *,
        connection: psycopg.Connection[Any] | None = None,
    ) -> GoldRunResult:
        if connection is None:
            with psycopg.connect(self.dsn) as owned_connection:
                return self.replace(
                    opportunities, minimum_count, computed_at, connection=owned_connection
                )
        records, candidates = 0, 0
        with connection.cursor() as cursor:
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
