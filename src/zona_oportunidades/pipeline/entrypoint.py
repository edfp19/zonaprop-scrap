"""Finite collection/Gold commands and the default local Compose loop.

collect_silver leaves its ops run in progress: a cloud coordinator must persist
and verify the returned Silver files before calling run_gold and runs.succeed.
"""

import argparse
import json
import logging
import signal
import time
import uuid
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from zona_oportunidades.collection.application.collect_listings import (
    CollectionResult,
    CollectListings,
)
from zona_oportunidades.collection.domain.models import SEARCH_TARGETS
from zona_oportunidades.collection.infrastructure.jsonl_repository import JsonlBronzeRepository
from zona_oportunidades.collection.infrastructure.selenium_source import ZonapropSeleniumSource
from zona_oportunidades.listings.domain import SilverRunResult
from zona_oportunidades.listings.runner import run_bronze_file
from zona_oportunidades.opportunities.domain import ComparisonPolicy, GoldRunResult
from zona_oportunidades.opportunities.runner import refresh_gold
from zona_oportunidades.pipeline.run_repository import PipelineRunRepository
from zona_oportunidades.shared.config import Settings, get_settings
from zona_oportunidades.shared.logging import configure_logging

logger = logging.getLogger(__name__)
keep_running = True


@dataclass(frozen=True)
class CollectedSilverRun:
    run_id: str
    collection: CollectionResult
    silver: SilverRunResult
    silver_bytes: int
    silver_files: int


def request_stop(_signal: int, _frame: object) -> None:
    global keep_running
    keep_running = False
    raise InterruptedError("Pipeline stop requested")


def collect_silver(
    settings: Settings | None = None, *, run_id: str | None = None
) -> CollectedSilverRun:
    """Prepare bounded Silver; reject any incomplete collection before Spark/Gold.

    Failures raise and mark only this run failed. Successful runs stay in progress
    until the caller verifies persistence and records the Gold publication.
    silver_bytes/files include all files, including the immutable run manifest.
    """
    settings = settings or get_settings()
    run_id = run_id or str(uuid.uuid4())
    runs = PipelineRunRepository(settings.postgres_dsn)
    runs.start(run_id)
    try:
        offset = datetime.now(UTC).date().toordinal() % len(SEARCH_TARGETS)
        targets = SEARCH_TARGETS[offset:] + SEARCH_TARGETS[:offset]
        result = CollectListings(
            source=ZonapropSeleniumSource(
                settings.selenium_url, settings.collection_page_timeout_seconds
            ),
            repository=JsonlBronzeRepository(settings.data_root),
            targets=targets,
            max_pages=settings.collection_max_pages,
            delay_seconds=settings.collection_delay_seconds,
            batch_pages=settings.collection_batch_pages,
            batch_cooldown_seconds=settings.collection_batch_cooldown_seconds,
            max_records=settings.collection_max_records,
            max_bronze_bytes=settings.collection_max_bronze_bytes,
        ).execute(run_id)
        runs.record_bronze(run_id, result.records)
        if result.records == 0 or result.errors:
            raise RuntimeError(
                f"Incomplete collection: {result.records} records, {len(result.errors)} errors; "
                "Bronze retained as failure evidence"
            )
        silver = run_bronze_file(
            result.bronze_path, settings.data_root, master=settings.spark_master
        )
        runs.record_silver(run_id, silver.silver_records)
        files = [path for path in silver.output_path.rglob("*") if path.is_file()]
        silver_bytes = sum(path.stat().st_size for path in files)
        if (
            not files
            or silver.silver_records == 0
            or silver_bytes > settings.pipeline_max_silver_bytes
        ):
            # Keep failed bytes out of the local Gold reader's recursive Silver root.
            quarantine = settings.data_root / "quarantine" / "silver" / run_id
            quarantine.parent.mkdir(parents=True, exist_ok=True)
            silver.output_path.rename(quarantine)
            raise RuntimeError(
                f"Silver rejected: {silver.silver_records} records, {silver_bytes} bytes "
                f"(limit {settings.pipeline_max_silver_bytes}); evidence at {quarantine}"
            )
        return CollectedSilverRun(run_id, result, silver, silver_bytes, len(files))
    except InterruptedError:
        runs.interrupt(run_id)
        raise
    except Exception as exc:
        runs.fail(run_id, str(exc))
        raise


def run_gold(settings: Settings | None = None) -> GoldRunResult:
    """Publish Gold from the caller's verified local Silver dataset."""
    settings = settings or get_settings()
    return refresh_gold(
        settings.data_root,
        settings.postgres_dsn,
        ComparisonPolicy(
            minimum_count=settings.comparable_min_count,
            area_tolerance=settings.comparable_area_tolerance,
        ),
        master=settings.spark_master,
    )


def run_collection() -> bool:
    """Compatibility wrapper for Compose: finish a local cycle, return its success."""
    settings = get_settings()
    prepared: CollectedSilverRun | None = None
    try:
        prepared = collect_silver(settings)
        gold = run_gold(settings)
        PipelineRunRepository(settings.postgres_dsn).succeed(
            prepared.run_id,
            prepared.collection.records,
            silver_records=prepared.silver.silver_records,
            gold_records=gold.records,
        )
        logger.info("Bronze→Silver→Gold complete: %s", prepared.run_id)
        return True
    except Exception as exc:
        logger.exception("Falló la ejecución Bronze→Silver→Gold")
        if prepared is not None:
            runs = PipelineRunRepository(settings.postgres_dsn)
            if isinstance(exc, InterruptedError):
                runs.interrupt(prepared.run_id)
            else:
                runs.fail(prepared.run_id, str(exc))
        return False


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode", nargs="?", choices=("loop", "collect-silver", "gold"), default="loop"
    )
    args = parser.parse_args(argv)
    settings = get_settings()
    configure_logging(settings.log_level)
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        if args.mode == "collect-silver":
            print(json.dumps(asdict(collect_silver(settings)), default=str))
            return 0
        if args.mode == "gold":
            print(json.dumps(asdict(run_gold(settings)), default=str))
            return 0
        while keep_running:
            run_collection()
            deadline = time.monotonic() + settings.pipeline_interval_seconds
            while keep_running and time.monotonic() < deadline:
                time.sleep(min(1, max(0, deadline - time.monotonic())))
        return 0
    except InterruptedError:
        logger.warning("Pipeline interrupted")
        return 130
    except Exception:
        logger.exception("Pipeline command failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
