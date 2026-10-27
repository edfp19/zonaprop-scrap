"""One bounded, isolated capacity trial; no ops, cloud, or Gold publication.

Dry-run: python -m zona_oportunidades.operations.pilot
Live trial: python -m zona_oportunidades.operations.pilot --collect --silver
"""

import argparse
import json
import logging
import os
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Literal
from uuid import uuid4

from zona_oportunidades.collection.application.collect_listings import (
    CollectionLimitExceeded,
    CollectListings,
)
from zona_oportunidades.collection.domain.models import (
    SEARCH_TARGETS,
    CollectedListing,
    SearchTarget,
    SourceAccessBlocked,
)
from zona_oportunidades.collection.domain.ports import ListingSource
from zona_oportunidades.collection.infrastructure.jsonl_repository import JsonlBronzeRepository

MAX_RECORDS = 250
MAX_BRONZE_BYTES = 9_000_000
TOTAL_RESERVE_BYTES = 10_000_000


@dataclass(frozen=True)
class FileUsage:
    bytes: int
    files: int


def file_usage(root: Path) -> FileUsage:
    files = [path for path in root.rglob("*") if path.is_file()]
    return FileUsage(sum(path.stat().st_size for path in files), len(files))


@dataclass(frozen=True)
class PilotReport:
    run_id: str
    status: Literal["full_target", "blocked", "incomplete"]
    representative: bool
    targets_planned: int
    targets_attempted: int
    targets_completed: int
    records: int
    bronze: FileUsage
    silver: FileUsage
    total: FileUsage
    collection_seconds: float
    spark_seconds: float | None
    wall_seconds: float
    silver_records: int | None
    failure_kind: str | None
    report_path: str
    baseline: str = "Four partial 30-row jobs; not a representative full-target baseline."


class MeasuredSource:
    """Count only nonempty successful targets; leave browser ownership to CollectListings."""

    def __init__(self, source: ListingSource) -> None:
        self.source = source
        self.attempted = 0
        self.completed = 0

    def collect_page(self, target: SearchTarget, page: int, run_id: str) -> list[CollectedListing]:
        self.attempted += 1
        rows = self.source.collect_page(target, page, run_id)
        if not rows:
            raise RuntimeError("Pilot target returned no records")
        self.completed += 1
        return rows

    def close(self) -> None:
        self.source.close()


def run_pilot(
    data_root: Path,
    source_factory: Callable[[], ListingSource],
    *,
    silver: bool = False,
) -> PilotReport:
    """Collect into a new private directory and replay only complete eight-target runs."""
    started = time.monotonic()
    run_id = str(uuid4())
    root = data_root / "pilot" / run_id
    root.mkdir(parents=True, exist_ok=False)
    repository = JsonlBronzeRepository(root)
    source: MeasuredSource | None = None
    status: Literal["full_target", "blocked", "incomplete"] = "incomplete"
    failure_kind: str | None = None
    bronze_path: Path | None = None
    spark_seconds: float | None = None
    silver_records: int | None = None
    records = 0
    try:
        source = MeasuredSource(source_factory())
        result = CollectListings(
            source,
            repository,
            SEARCH_TARGETS,
            max_pages=1,
            delay_seconds=10,
            batch_pages=1,
            batch_cooldown_seconds=60,
            max_records=MAX_RECORDS,
            max_bronze_bytes=MAX_BRONZE_BYTES,
        ).execute(run_id)
        records = result.records
        bronze_path = result.bronze_path
        if not result.errors and source.completed == len(SEARCH_TARGETS):
            status = "full_target"
        else:
            failure_kind = "TargetCollectionIncomplete"
    except SourceAccessBlocked:
        status = "blocked"
        failure_kind = "SourceAccessBlocked"
    except CollectionLimitExceeded:
        failure_kind = "CollectionLimitExceeded"
    except Exception as error:
        # Exception text may include source rows, driver URLs, or credentials.
        failure_kind = type(error).__name__
    collection_seconds = time.monotonic() - started

    manifest_path = root / "bronze" / "manifests" / f"{run_id}.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        records = int(manifest["record_count"])
    else:
        repository.save_listings(run_id, [])
        repository.save_manifest(run_id, {"record_count": 0, "failure_kind": failure_kind})

    if status == "full_target" and silver and bronze_path is not None:
        from zona_oportunidades.listings.runner import run_bronze_file

        spark_started = time.monotonic()
        try:
            silver_records = run_bronze_file(bronze_path, root).silver_records
        except Exception as error:
            status = "incomplete"
            failure_kind = type(error).__name__
        finally:
            spark_seconds = round(time.monotonic() - spark_started, 3)

    report_path = root / "report.json"
    # Reserve space for the small safe report itself, independently of raw Bronze.
    if file_usage(root).bytes + 16_384 > TOTAL_RESERVE_BYTES:
        status = "incomplete"
        failure_kind = "PilotTotalReserveExceeded"
    report = PilotReport(
        run_id=run_id,
        status=status,
        representative=status == "full_target",
        targets_planned=len(SEARCH_TARGETS),
        targets_attempted=source.attempted if source else 0,
        targets_completed=source.completed if source else 0,
        records=records,
        bronze=file_usage(root / "bronze"),
        silver=file_usage(root / "silver"),
        total=file_usage(root),
        collection_seconds=round(collection_seconds, 3),
        spark_seconds=spark_seconds,
        wall_seconds=round(time.monotonic() - started, 3),
        silver_records=silver_records,
        failure_kind=failure_kind,
        report_path=str(report_path),
    )
    artifacts = report.total
    while True:
        serialized = json.dumps(asdict(report), indent=2) + "\n"
        total = FileUsage(artifacts.bytes + len(serialized.encode("utf-8")), artifacts.files + 1)
        if report.total == total:
            break
        report = replace(report, total=total)
    report_path.write_text(serialized, encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--collect", action="store_true", help="Contact the live public source once"
    )
    parser.add_argument("--silver", action="store_true", help="Replay only a full successful trial")
    args = parser.parse_args()
    if not args.collect:
        print(
            json.dumps(
                {
                    "status": "dry_run",
                    "targets": len(SEARCH_TARGETS),
                    "pages_per_target": 1,
                    "max_records": MAX_RECORDS,
                    "max_bronze_bytes": MAX_BRONZE_BYTES,
                    "total_reserve_bytes": TOTAL_RESERVE_BYTES,
                    "delay_seconds": 10,
                    "batch_pages": 1,
                    "batch_cooldown_seconds": 60,
                    "source_timeout_seconds": 45,
                    "browser_sessions": 1,
                    "silver_requested": args.silver,
                },
                indent=2,
            )
        )
        return 0

    from zona_oportunidades.collection.infrastructure.selenium_source import ZonapropSeleniumSource

    logging.getLogger("zona_oportunidades.collection").setLevel(logging.CRITICAL)
    report = run_pilot(
        Path(os.environ.get("DATA_ROOT", "data")),
        lambda: ZonapropSeleniumSource(
            os.environ.get("SELENIUM_URL", "http://localhost:4444/wd/hub"), timeout_seconds=45
        ),
        silver=args.silver,
    )
    print(json.dumps(asdict(report), indent=2))
    return 0 if report.status == "full_target" else 1


if __name__ == "__main__":
    raise SystemExit(main())
