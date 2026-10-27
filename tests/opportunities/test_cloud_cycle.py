"""Exercise seed and compaction with real Spark/PostgreSQL and local object storage."""

import json
import shutil
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest
from test_transforms import dataframe, observation

from zona_oportunidades.listings.runner import create_local_spark
from zona_oportunidades.listings.storage import AtomicSilverPublisher
from zona_oportunidades.operations.capacity import ProviderUsage, RunnerCapacity
from zona_oportunidades.operations.cloud import _rewrite, load_cloud_policy
from zona_oportunidades.operations.storage import DatasetRegistry, build_manifest, storage_lock


def test_seed_then_maintenance_preserves_gold_and_original_receipts(postgres_dsn, tmp_path):
    pytest.importorskip("pyspark")
    now = datetime.now(UTC)
    day = now.date().isoformat()
    root = tmp_path / "silver"
    source = root / f"ingestion_date={day}" / "run_id=run-a"
    spark = create_local_spark()
    try:
        dataframe(
            spark, [observation("42", observed_at=now, ingestion_date=day)]
        ).write.partitionBy("posting_type").parquet(str(source))
    finally:
        spark.stop()
    (source / "_run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "run-a",
                "ingestion_date": day,
                "input_sha256": "a" * 64,
                "transformation_version": "v1",
                "parquet_inventory": AtomicSilverPublisher._parquet_inventory(source),
            }
        )
    )
    with psycopg.connect(postgres_dsn) as conn:
        conn.execute(Path("infra/postgres/pipeline_storage.sql").read_text())
    policy = load_cloud_policy(Path("infra/pipeline/policy.example.json"))
    policy = replace(policy, capacity=replace(policy.capacity, project_id="test"))
    usage = ProviderUsage(
        "test", now, True, 5_000_000_000, 0, 1_000_000_000, 0, 100, 0, 5_000_000_000, 0, 0, 0
    )

    class LocalObjectStore:
        def upload(self, directory, manifest):
            prefix = f"{manifest.kind}/{manifest.generation_id}"
            shutil.copytree(directory, tmp_path / "objects" / prefix)
            return prefix

        def download(self, generation, target):
            shutil.copytree(tmp_path / "objects" / generation.prefix, target)
            expected = generation.manifest
            actual = build_manifest(
                target,
                generation_id=expected.generation_id,
                kind=expected.kind,
                rows=expected.rows,
                source_runs=expected.source_runs,
            )
            assert actual.files == expected.files
            return target

    store = LocalObjectStore()
    with storage_lock(postgres_dsn) as conn:
        registry = DatasetRegistry(conn)
        kwargs = dict(
            registry=registry,
            conn=conn,
            store=store,
            policy=policy,
            usage=usage,
            runner=RunnerCapacity(1_000_000_000, 0, 0),
            project_id="test",
            reclamation_verified=True,
            dsn=postgres_dsn,
            silver_root=root,
            master=None,
            now=now,
        )
        seed = _rewrite("seed", **kwargs)
        maintained = _rewrite("maintain", **kwargs)
        assert seed.applied and maintained.applied
        assert seed.silver_rows == maintained.silver_rows == 1
        assert seed.gold_rows == maintained.gold_rows == 1
        assert seed.generation_id != maintained.generation_id
        assert [item.generation_id for item in registry.active()] == [maintained.generation_id]
        assert conn.execute("SELECT listing_id FROM gold.opportunities").fetchone() == ("42",)
        output = store.download(registry.active()[0], tmp_path / "verified")
        receipts = json.loads((output / "_source_receipts.json").read_text())["sources"]
        assert receipts == [
            {"run_id": "run-a", "input_sha256": "a" * 64, "transformation_version": "v1"}
        ]
