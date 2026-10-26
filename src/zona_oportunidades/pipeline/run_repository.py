from datetime import UTC, datetime

import psycopg


class PipelineRunRepository:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def start(self, run_id: str) -> None:
        with psycopg.connect(self.dsn) as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ops.pipeline_runs
                SET status = 'FALLIDO', finished_at = %s,
                    error_message = 'La ejecución fue interrumpida antes de finalizar.'
                WHERE status = 'EN_PROGRESO'
                """,
                (datetime.now(UTC),),
            )
            cursor.execute(
                """
                INSERT INTO ops.pipeline_runs (run_id, started_at, status)
                VALUES (%s, %s, 'EN_PROGRESO')
                """,
                (run_id, datetime.now(UTC)),
            )

    def succeed(self, run_id: str, bronze_records: int, warning: str | None = None) -> None:
        with psycopg.connect(self.dsn) as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ops.pipeline_runs
                SET status = 'EXITOSO', finished_at = %s, bronze_records = %s,
                    error_message = %s
                WHERE run_id = %s
                """,
                (datetime.now(UTC), bronze_records, warning, run_id),
            )

    def fail(self, run_id: str, message: str) -> None:
        with psycopg.connect(self.dsn) as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ops.pipeline_runs
                SET status = 'FALLIDO', finished_at = %s, error_message = %s
                WHERE run_id = %s
                """,
                (datetime.now(UTC), message[:4000], run_id),
            )
