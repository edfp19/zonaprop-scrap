"""Bounded collection overlaps maintenance; shared state changes use the storage lock."""

import uuid
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import SecretStr

from zona_oportunidades.operations.budget import claim_job
from zona_oportunidades.operations.capacity import ProviderUsage, RunnerCapacity
from zona_oportunidades.operations.quarantine import cleanup_promoted, preserve, reserve
from zona_oportunidades.operations.storage import DatasetRegistry, S3Store, storage_lock
from zona_oportunidades.pipeline.entrypoint import collect_silver
from zona_oportunidades.pipeline.run_repository import PipelineRunRepository
from zona_oportunidades.shared.config import get_settings

if TYPE_CHECKING:
    from zona_oportunidades.operations.cloud import CloudPolicy, CloudResult


def ingest(
    *,
    policy: "CloudPolicy",
    usage: ProviderUsage | None,
    runner: RunnerCapacity,
    project_id: str | None,
    reclamation_verified: bool,
    dsn: str,
    store: S3Store,
    data_root: Path | None,
    master: str | None,
    now: datetime,
) -> "CloudResult":
    from zona_oportunidades.operations.cloud import _rewrite, require_capacity

    settings = get_settings()
    root = data_root or settings.data_root
    if policy.capacity.max_bronze_bytes <= 1_000_000:
        raise ValueError("Bronze budget needs at least 1 MB for collection/storage manifests")
    settings = settings.model_copy(
        update={
            "data_root": root,
            "database_url": SecretStr(dsn),
            "collection_max_pages": 1,
            "collection_max_records": min(
                settings.collection_max_records, policy.capacity.max_records
            ),
            # The private slot includes raw bytes plus both evidence and storage manifests.
            "collection_max_bronze_bytes": min(
                settings.collection_max_bronze_bytes, policy.capacity.max_bronze_bytes - 1_000_000
            ),
            "pipeline_max_silver_bytes": min(
                settings.pipeline_max_silver_bytes, policy.job.silver_bytes
            ),
        }
    )
    run_id = str(uuid.uuid4())
    with storage_lock(dsn) as conn:
        require_capacity(
            policy,
            usage,
            runner,
            project_id=project_id,
            reclamation_verified=reclamation_verified,
            now=datetime.now(UTC),
        )
        if not DatasetRegistry(conn).active():
            raise ValueError("seed the existing Silver before collecting new data")
        assert usage is not None
        claim_job(conn, policy.capacity, usage, policy.job, "ingest", now=now)
        cleanup_promoted(conn, store)
        ticket = reserve(
            conn,
            run_id,
            max_extracts=policy.capacity.max_failed_extracts,
            max_total_bytes=policy.capacity.failed_bronze_reserve_bytes,
            max_extract_bytes=policy.capacity.max_bronze_bytes,
        )

    runs = PipelineRunRepository(dsn)
    try:
        # Selenium and the new run's local Spark work do not hold the shared-state lock.
        prepared = collect_silver(settings, run_id=run_id)
    except Exception:
        with storage_lock(dsn) as conn:
            preserve(conn, store, ticket, data_root=root)
        raise

    committed = False
    try:
        with storage_lock(dsn) as conn:
            preserve(conn, store, ticket, data_root=root)
            result = _rewrite(
                "ingest",
                registry=DatasetRegistry(conn),
                conn=conn,
                store=store,
                policy=policy,
                usage=usage,
                runner=runner,
                project_id=project_id,
                reclamation_verified=reclamation_verified,
                dsn=dsn,
                silver_root=None,
                master=master,
                now=now,
                extra_paths=(prepared.silver.output_path,),
                bronze_ticket=ticket,
            )
            committed = True
            try:
                runs.succeed(
                    run_id,
                    prepared.collection.records,
                    silver_records=prepared.silver.silver_records,
                    gold_records=result.gold_rows,
                )
                cleanup_promoted(conn, store)
            except Exception:
                # Do not report that the atomic Gold/registry commit rolled back.
                return replace(
                    result,
                    allowed=False,
                    reasons=("publication committed; bookkeeping or promoted cleanup needs retry",),
                )
            return result
    except Exception:
        if not committed:
            runs.fail(run_id, "cloud publication failed; private Bronze retained")
        raise
