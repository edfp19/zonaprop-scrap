from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock

import psycopg
import pytest

from zona_oportunidades.collection.application.collect_listings import CollectionResult
from zona_oportunidades.listings.domain import SilverRunResult
from zona_oportunidades.opportunities.domain import GoldRunResult
from zona_oportunidades.pipeline import entrypoint
from zona_oportunidades.shared.config import Settings


@pytest.mark.parametrize("gold_fails", [False, True])
def test_stage_counts_survive_a_gold_failure(
    postgres_dsn: str, monkeypatch, gold_fails: bool, tmp_path: Path
) -> None:
    class TestSettings(Settings):
        @property
        def postgres_dsn(self) -> str:
            return postgres_dsn

    monkeypatch.setattr(entrypoint, "get_settings", TestSettings)
    monkeypatch.setattr(entrypoint, "ZonapropSeleniumSource", Mock())
    monkeypatch.setattr(
        entrypoint.CollectListings,
        "execute",
        Mock(return_value=CollectionResult(30, Path("fixture.jsonl"), ())),
    )
    silver_path = tmp_path / "silver"
    silver_path.mkdir()
    (silver_path / "part.parquet").write_bytes(b"fixture")
    monkeypatch.setattr(
        entrypoint,
        "run_bronze_file",
        Mock(return_value=SilverRunResult("fixture", 30, 29, silver_path, False)),
    )
    gold = Mock(return_value=GoldRunResult(25, 3, datetime.now(UTC)))
    if gold_fails:
        gold.side_effect = RuntimeError("Gold unavailable")
    monkeypatch.setattr(entrypoint, "refresh_gold", gold)
    entrypoint.run_collection()
    with psycopg.connect(postgres_dsn) as connection:
        row = connection.execute(
            "SELECT status, bronze_records, silver_records, gold_records, error_message "
            "FROM ops.pipeline_runs"
        ).fetchone()
    if gold_fails:
        assert row == ("FALLIDO", 30, 29, 0, "Gold unavailable")
    else:
        assert row == ("EXITOSO", 30, 29, 25, None)


def test_start_does_not_fail_another_worker_and_interrupt_is_explicit(postgres_dsn: str) -> None:
    from zona_oportunidades.pipeline.run_repository import PipelineRunRepository

    runs = PipelineRunRepository(postgres_dsn)
    first = "00000000-0000-0000-0000-000000000001"
    second = "00000000-0000-0000-0000-000000000002"
    runs.start(first)
    runs.start(second)
    with psycopg.connect(postgres_dsn) as connection:
        assert connection.execute(
            "SELECT count(*) FROM ops.pipeline_runs WHERE status = 'EN_PROGRESO'"
        ).fetchone() == (2,)
    runs.interrupt(first)
    with psycopg.connect(postgres_dsn) as connection:
        assert connection.execute(
            "SELECT status FROM ops.pipeline_runs WHERE run_id = %s", (second,)
        ).fetchone() == ("EN_PROGRESO",)
        status, message = connection.execute(
            "SELECT status, error_message FROM ops.pipeline_runs WHERE run_id = %s", (first,)
        ).fetchone()
    assert status == "FALLIDO"
    assert "interrumpida" in message
