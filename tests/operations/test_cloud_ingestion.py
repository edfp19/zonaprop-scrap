from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from zona_oportunidades.operations import cloud, ingestion
from zona_oportunidades.operations.capacity import CapacityPolicy, JobEstimate, RunnerCapacity
from zona_oportunidades.opportunities.catalogue import CataloguePolicy


@pytest.mark.parametrize("failure", [None, "collection", "publication", "cleanup"])
def test_private_recovery_brackets_collection_and_publication(monkeypatch, tmp_path, failure):
    events = []
    locked = False

    @contextmanager
    def lock(dsn):
        nonlocal locked
        assert not locked
        locked = True
        try:
            yield "connection"
        finally:
            locked = False

    def collect(settings, *, run_id):
        assert not locked
        assert settings.collection_max_pages == 1
        assert settings.collection_max_bronze_bytes <= 9_000_000
        events.append("collect")
        if failure == "collection":
            raise RuntimeError("collection failed")
        return SimpleNamespace(
            collection=SimpleNamespace(records=30),
            silver=SimpleNamespace(output_path=tmp_path, silver_records=30),
        )

    def rewrite(*args, **kwargs):
        assert locked and kwargs["bronze_ticket"] == "ticket"
        events.append("publish")
        if failure == "publication":
            raise RuntimeError("publication failed")
        return cloud.CloudResult("ingest", True, True, (), gold_rows=30)

    def cleanup(*args):
        assert locked
        events.append("cleanup")
        if failure == "cleanup" and "publish" in events:
            raise RuntimeError("cleanup failed")

    monkeypatch.setattr(ingestion, "storage_lock", lock)
    monkeypatch.setattr(cloud, "require_capacity", lambda *a, **k: None)
    monkeypatch.setattr(ingestion, "claim_job", lambda *a, **k: events.append("claim"))
    monkeypatch.setattr(
        ingestion, "DatasetRegistry", lambda conn: SimpleNamespace(active=lambda: (1,))
    )
    monkeypatch.setattr(ingestion, "cleanup_promoted", cleanup)
    monkeypatch.setattr(ingestion, "reserve", lambda *a, **k: events.append("reserve") or "ticket")
    monkeypatch.setattr(ingestion, "preserve", lambda *a, **k: events.append("preserve"))
    monkeypatch.setattr(ingestion, "collect_silver", collect)
    monkeypatch.setattr(cloud, "_rewrite", rewrite)
    monkeypatch.setattr(
        ingestion,
        "PipelineRunRepository",
        lambda dsn: SimpleNamespace(
            succeed=lambda *a, **k: events.append("succeed"),
            fail=lambda *a: events.append("fail"),
        ),
    )
    policy = cloud.CloudPolicy(
        CapacityPolicy("test"),
        CataloguePolicy(100, 250, 1250, 35),
        JobEstimate(5_000_000, 25_000_000, 1, 1, 1, 1, 100, 1, 1, 1),
    )
    kwargs = dict(
        policy=policy,
        usage=object(),
        runner=RunnerCapacity(1_000_000_000, 0, 0),
        project_id="test",
        reclamation_verified=True,
        dsn="unused",
        store=None,
        data_root=tmp_path,
        master=None,
        now=datetime.now(UTC),
    )
    if failure in {"collection", "publication"}:
        with pytest.raises(RuntimeError, match=failure):
            ingestion.ingest(**kwargs)
    else:
        result = ingestion.ingest(**kwargs)
        assert result.applied
        assert result.allowed == (failure is None)
    assert events[:4] == ["claim", "cleanup", "reserve", "collect"]
    assert events[4] == "preserve"
    if failure == "collection":
        assert "publish" not in events
    elif failure == "publication":
        assert events[-1] == "fail" and "succeed" not in events
    else:
        assert events[-3:] == ["publish", "succeed", "cleanup"]
