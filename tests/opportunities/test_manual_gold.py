import base64
import gzip
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

import psycopg
import pytest
from pydantic import TypeAdapter
from test_repository import candidate

from zona_oportunidades.operations import publish_gold
from zona_oportunidades.operations.publish_gold import GoldSnapshot, decode_snapshot
from zona_oportunidades.opportunities.repository import GoldRepository


def encode(snapshot):
    return base64.b64encode(gzip.compress(TypeAdapter(GoldSnapshot).dump_json(snapshot))).decode()


def test_snapshot_boundary_preserves_contract_and_rejects_duplicates_or_oversize():
    snapshot = GoldSnapshot(0, None, (candidate(),))
    assert decode_snapshot(encode(snapshot)) == snapshot
    with pytest.raises(ValueError, match="Duplicate"):
        decode_snapshot(encode(replace(snapshot, opportunities=(candidate(), candidate()))))
    with pytest.raises(ValueError, match="1 MB"):
        decode_snapshot(base64.b64encode(gzip.compress(b"x" * 1_000_001)).decode())
    with pytest.raises(ValueError):
        decode_snapshot(encode(replace(snapshot, opportunities=())))


@pytest.mark.parametrize("fail_write", [False, True])
def test_manual_publication_commits_or_rolls_back_as_writer(postgres_dsn, monkeypatch, fail_write):
    sale = candidate()
    GoldRepository(postgres_dsn).replace([sale], 10, sale.collected_at)
    with psycopg.connect(postgres_dsn) as conn, conn.transaction(force_rollback=True):
        conn.execute(Path("infra/postgres/pipeline_writer.sql").read_text())
        conn.execute("SET LOCAL ROLE pipeline_writer")

        @contextmanager
        def lock(dsn):
            yield conn

        monkeypatch.setattr(publish_gold, "storage_lock", lock)
        snapshot = GoldSnapshot(1, sale.collected_at, (replace(sale, title="Fresh listing"),))
        if fail_write:
            original = GoldRepository.replace

            def fail_after_write(*args, **kwargs):
                original(*args, **kwargs)
                raise RuntimeError("interrupted before commit")

            monkeypatch.setattr(GoldRepository, "replace", fail_after_write)
            with pytest.raises(RuntimeError, match="interrupted"):
                publish_gold.publish(snapshot, "unused")
            assert conn.execute("SELECT title FROM gold.opportunities").fetchone() == (sale.title,)
        else:
            assert publish_gold.publish(snapshot, "unused").records == 1
            assert conn.execute("SELECT title FROM gold.opportunities").fetchone() == (
                "Fresh listing",
            )
        with pytest.raises(ValueError, match="changed since preparation"):
            publish_gold.publish(replace(snapshot, expected_gold_rows=99), "unused")
