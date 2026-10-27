"""Ingestion safety checks use fake source/Spark/DB boundaries, never the live site."""

from pathlib import Path
from unittest.mock import Mock

import pytest

from zona_oportunidades.collection.application.collect_listings import CollectionResult
from zona_oportunidades.collection.domain.models import CollectionError, SearchTarget
from zona_oportunidades.listings.domain import SilverRunResult
from zona_oportunidades.pipeline import entrypoint
from zona_oportunidades.shared.config import Settings


@pytest.fixture()
def stages(monkeypatch, tmp_path: Path):
    settings = Settings(_env_file=None, data_root=tmp_path, pipeline_max_silver_bytes=10)
    runs = Mock()
    monkeypatch.setattr(entrypoint, "PipelineRunRepository", Mock(return_value=runs))
    monkeypatch.setattr(entrypoint, "get_settings", lambda: settings)
    monkeypatch.setattr(entrypoint, "ZonapropSeleniumSource", Mock())
    collection = Mock(return_value=CollectionResult(1, tmp_path / "bronze.jsonl", (), 9))
    monkeypatch.setattr(entrypoint.CollectListings, "execute", collection)
    output = tmp_path / "silver/listing_observations/ingestion_date=2026-10-27/run_id=test"
    output.mkdir(parents=True)
    (output / "part.parquet").write_bytes(b"12345")
    (output / "_run_manifest.json").write_bytes(b"12345")
    silver = Mock(return_value=SilverRunResult("test", 1, 1, output, False))
    monkeypatch.setattr(entrypoint, "run_bronze_file", silver)
    gold = Mock()
    monkeypatch.setattr(entrypoint, "refresh_gold", gold)
    return settings, runs, collection, silver, gold


def test_collect_silver_reports_actual_bytes_and_defers_gold(stages) -> None:
    settings, runs, _, silver, gold = stages
    result = entrypoint.collect_silver(settings, run_id="test")
    assert result.run_id == "test"
    assert result.silver == silver.return_value
    assert result.silver_bytes == 10
    assert result.silver_files == 2
    runs.record_bronze.assert_called_once_with("test", 1)
    runs.record_silver.assert_called_once_with("test", 1)
    runs.succeed.assert_not_called()
    runs.fail.assert_not_called()
    gold.assert_not_called()


@pytest.mark.parametrize("empty", [False, True])
def test_incomplete_collection_cannot_publish_silver_or_gold(stages, empty: bool) -> None:
    settings, runs, collection, silver, gold = stages
    error = CollectionError(SearchTarget("VENTA", "CABA", "https://example.test"), 2, "failed")
    collection.return_value = CollectionResult(
        0 if empty else 1, settings.data_root / "bronze.jsonl", () if empty else (error,)
    )
    with pytest.raises(RuntimeError, match="Incomplete collection"):
        entrypoint.collect_silver(settings, run_id="test")
    runs.fail.assert_called_once()
    silver.assert_not_called()
    gold.assert_not_called()


@pytest.mark.parametrize("empty", [False, True])
def test_rejected_silver_is_quarantined_before_future_gold(stages, empty: bool) -> None:
    settings, runs, _, silver, gold = stages
    if empty:
        previous = silver.return_value
        silver.return_value = SilverRunResult("test", 1, 0, previous.output_path, False)
    else:
        settings.pipeline_max_silver_bytes = 9
    with pytest.raises(RuntimeError, match="Silver rejected"):
        entrypoint.collect_silver(settings, run_id="test")
    assert not silver.return_value.output_path.exists()
    assert (settings.data_root / "quarantine/silver/test/part.parquet").read_bytes() == b"12345"
    runs.fail.assert_called_once()
    gold.assert_not_called()


def test_interrupted_collection_records_only_its_interruption(stages) -> None:
    settings, runs, collection, silver, gold = stages
    collection.side_effect = InterruptedError("stop")
    with pytest.raises(InterruptedError):
        entrypoint.collect_silver(settings, run_id="test")
    runs.interrupt.assert_called_once_with("test")
    runs.fail.assert_not_called()
    silver.assert_not_called()
    gold.assert_not_called()


@pytest.mark.parametrize("mode", ["collect-silver", "gold"])
def test_finite_command_has_nonzero_failure_status(monkeypatch, mode: str) -> None:
    monkeypatch.setattr(entrypoint, "get_settings", lambda: Settings(_env_file=None))
    monkeypatch.setattr(entrypoint, "signal", Mock())
    monkeypatch.setattr(entrypoint, "collect_silver", Mock(side_effect=RuntimeError("failure")))
    monkeypatch.setattr(entrypoint, "run_gold", Mock(side_effect=RuntimeError("failure")))
    assert entrypoint.main([mode]) == 1


def test_compatibility_wrapper_reports_failure(stages) -> None:
    _, runs, _, _, gold = stages
    gold.side_effect = RuntimeError("publication failed")
    assert entrypoint.run_collection() is False
    runs.fail.assert_called_once()
    runs.succeed.assert_not_called()


@pytest.mark.parametrize("mode", ["collect-silver", "gold"])
def test_finite_command_returns_json_and_success(stages, monkeypatch, capsys, mode: str) -> None:
    import json
    from datetime import UTC, datetime

    from zona_oportunidades.opportunities.domain import GoldRunResult

    _, runs, _, _, gold = stages
    monkeypatch.setattr(entrypoint, "signal", Mock())
    gold.return_value = GoldRunResult(1, 0, datetime.now(UTC))
    assert entrypoint.main([mode]) == 0
    report = json.loads(capsys.readouterr().out)
    if mode == "collect-silver":
        assert report["silver_bytes"] == 10
        assert report["collection"]["bronze_bytes"] == 9
        runs.succeed.assert_not_called()
        gold.assert_not_called()
    else:
        assert report["records"] == 1
