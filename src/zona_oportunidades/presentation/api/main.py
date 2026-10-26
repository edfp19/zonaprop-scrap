from contextlib import contextmanager
from typing import Any

import psycopg
from fastapi import FastAPI
from psycopg.rows import dict_row

from zona_oportunidades.shared.config import get_settings

app = FastAPI(
    title="API de Oportunidades Inmobiliarias",
    description="Estado del pipeline y, desde el checkpoint Gold, oportunidades comparables.",
    version="0.1.0",
)


@contextmanager
def database():
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


@app.get("/oportunidades")
def opportunities_not_available() -> dict[str, object]:
    return {
        "etapa": "BRONZE",
        "disponible": False,
        "resultados": [],
        "mensaje": "Las oportunidades estarán disponibles después del checkpoint Gold.",
    }
