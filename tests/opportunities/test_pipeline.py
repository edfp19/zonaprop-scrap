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
    postgres_dsn: str, monkeypatch, gold_fails: bool
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
    monkeypatch.setattr(
        entrypoint,
        "run_bronze_file",
        Mock(return_value=SilverRunResult("fixture", 30, 29, Path("silver"), False)),
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
