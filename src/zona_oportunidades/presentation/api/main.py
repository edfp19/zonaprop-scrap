import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Literal

import psycopg
from fastapi import FastAPI, Query
from psycopg.rows import dict_row
from pydantic import BaseModel

from zona_oportunidades.shared.config import get_settings

app = FastAPI(
    title="API de Oportunidades Inmobiliarias",
    description="Estado del pipeline y, desde el checkpoint Gold, oportunidades comparables.",
    version="0.1.0",
)


@contextmanager
def database() -> Iterator[psycopg.Connection[dict[str, Any]]]:
    settings = get_settings()
    with psycopg.connect(settings.postgres_dsn, row_factory=dict_row) as connection:
        yield connection


@app.get("/salud")
def health() -> dict[str, str]:
    return {"estado": "ok"}


@app.get("/estado-pipeline")
def pipeline_status() -> dict[str, Any]:
    with database() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT run_id, started_at, finished_at, status, bronze_records,
                   silver_records, gold_records, error_message
            FROM ops.pipeline_runs
            ORDER BY started_at DESC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
    if row is None:
        return {"estado": "SIN_EJECUCIONES", "mensaje": "La primera recolección aún no comenzó."}
    return dict(row)


class OpportunityView(BaseModel):
    listing_id: str
    operation: str
    property_type: str
    title: str
    url: str
    area_name: str
    parent_area_name: str | None
    rooms: int
    covered_area_m2: float
    total_area_m2: float | None
    price_usd: float
    price_per_m2_usd: float | None
    comparable_average_usd: float | None
    comparable_median_usd: float | None
    comparable_count: int
    discount_percent: float | None
    has_sufficient_data: bool
    is_opportunity: bool
    collected_at: datetime
    run_id: str
    quality_flags: list[str]
    latitude: float | None = None
    longitude: float | None = None
    photo_url: str | None = None
    geolocation_visibility: str | None = None


class OpportunityPage(BaseModel):
    etapa: str = "GOLD"
    disponible: bool = True
    total: int
    evaluados: int
    resultados: list[OpportunityView]


class MapSnapshot(BaseModel):
    evaluados: int
    resultados: list[OpportunityView]


def _opportunity_views(rows: list[dict[str, Any]]) -> list[OpportunityView]:
    return [
        OpportunityView(**(row | {"quality_flags": json.loads(row["quality_flags"] or "[]")}))
        for row in rows
    ]


@app.get("/api/mapa", include_in_schema=False)
@app.get("/mapa")
def map_properties() -> MapSnapshot:
    """The small teaching dataset, including above-average and thin-sample comparisons."""
    with database() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT * FROM gold.opportunities ORDER BY listing_id, operation")
        rows = cursor.fetchall()
    return MapSnapshot(evaluados=len(rows), resultados=_opportunity_views(rows))


@app.get("/oportunidades")
def opportunities(
    operation: Literal["VENTA", "ALQUILER"] | None = None,
    area: str | None = Query(default=None, min_length=1, max_length=200),
    rooms: int | None = Query(default=None, ge=1, le=100),
    only_opportunities: bool = True,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> OpportunityPage:
    filters = """
        WHERE (%(operation)s::text IS NULL OR operation = %(operation)s)
          AND (%(area)s::text IS NULL OR area_name = %(area)s)
          AND (%(rooms)s::integer IS NULL OR rooms = %(rooms)s)
          AND (NOT %(only_opportunities)s OR is_opportunity)
    """
    parameters = {
        "operation": operation,
        "area": area,
        "rooms": rooms,
        "only_opportunities": only_opportunities,
        "limit": limit,
        "offset": offset,
    }
    with database() as connection, connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cursor.execute("SELECT count(*) AS evaluated FROM gold.opportunities")
        evaluated = cursor.fetchone()
        assert evaluated is not None
        cursor.execute("SELECT count(*) AS total FROM gold.opportunities " + filters, parameters)
        count = cursor.fetchone()
        assert count is not None
        cursor.execute(
            "SELECT * FROM gold.opportunities "
            + filters
            + " ORDER BY discount_percent DESC NULLS LAST, listing_id, operation "
            "LIMIT %(limit)s OFFSET %(offset)s",
            parameters,
        )
        rows = cursor.fetchall()
    return OpportunityPage(
        total=count["total"],
        evaluados=evaluated["evaluated"],
        resultados=_opportunity_views(rows),
    )
