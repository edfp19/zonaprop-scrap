import json
import logging
import math
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from zona_oportunidades.collection.domain.models import (
    CollectedListing,
    CollectionError,
    SearchTarget,
    SourceAccessBlocked,
)
from zona_oportunidades.collection.domain.ports import BronzeRepository, ListingSource

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CollectionResult:
    records: int
    bronze_path: Path
    errors: tuple[CollectionError, ...]
    bronze_bytes: int = 0


class CollectionLimitExceeded(RuntimeError):
    """Collection exceeded a hard budget; saved Bronze is failure evidence only."""


class CollectListings:
    def __init__(
        self,
        source: ListingSource,
        repository: BronzeRepository,
        targets: tuple[SearchTarget, ...],
        max_pages: int,
        delay_seconds: float,
        max_records: int = 250,
        max_bronze_bytes: int = 10_000_000,
        batch_pages: int = 1,
        batch_cooldown_seconds: float = 60,
    ) -> None:
        if max_records < 1 or max_bronze_bytes < 1:
            raise ValueError("Collection budgets must be positive")
        if type(batch_pages) is not int or batch_pages < 1:
            raise ValueError("Batch pages must be a positive integer")
        if any(
            not math.isfinite(value) or value < 0
            for value in (delay_seconds, batch_cooldown_seconds)
        ):
            raise ValueError("Collection delays must be finite and nonnegative")
        self.max_records = max_records
        self.max_bronze_bytes = max_bronze_bytes
        self.source = source
        self.repository = repository
        self.targets = targets
        self.max_pages = max_pages
        self.delay_seconds = delay_seconds
        self.batch_pages = batch_pages
        self.batch_cooldown_seconds = batch_cooldown_seconds

    def execute(self, run_id: str) -> CollectionResult:
        listings: list[CollectedListing] = []
        bronze_bytes = 0
        attempted_records = 0
        attempted_bytes = 0
        limit_error: CollectionLimitExceeded | None = None
        errors: list[CollectionError] = []
        blocked_error: SourceAccessBlocked | None = None
        fatal_error: Exception | None = None
        requested_pages = 0
        started_at = time.time()

        def persist(*, finished: bool) -> Path:
            path = self.repository.save_listings(run_id, listings)
            complete = bool(listings) and not errors and fatal_error is None
            self.repository.save_manifest(
                run_id,
                {
                    "run_id": run_id,
                    "status": ("complete" if complete else "failed") if finished else "in_progress",
                    "duration_seconds": round(time.time() - started_at, 3),
                    "record_count": len(listings),
                    "bronze_bytes": bronze_bytes,
                    "attempted_records": attempted_records,
                    "attempted_bytes": attempted_bytes,
                    "max_records": self.max_records,
                    "max_bronze_bytes": self.max_bronze_bytes,
                    "requested_pages": requested_pages,
                    "batch_pages": self.batch_pages,
                    "batch_cooldown_seconds": self.batch_cooldown_seconds,
                    "delay_seconds": self.delay_seconds,
                    "error_count": len(errors),
                    "failure_message": str(fatal_error) if fatal_error else None,
                    "errors": [
                        {
                            "target": asdict(error.target),
                            "page": error.page,
                            "message": error.message,
                        }
                        for error in errors
                    ],
                },
            )
            return path

        try:
            for target in self.targets:
                for page in range(1, self.max_pages + 1):
                    if requested_pages:
                        delay = self.delay_seconds
                        if requested_pages % self.batch_pages == 0:
                            delay = max(delay, self.batch_cooldown_seconds)
                        logger.info("Waiting %.0fs before the next search page", delay)
                        time.sleep(delay)
                    requested_pages += 1
                    try:
                        page_listings = self.source.collect_page(target, page, run_id)
                        page_bytes = sum(
                            len(
                                json.dumps(
                                    item.as_dict(), ensure_ascii=False, separators=(",", ":")
                                ).encode("utf-8")
                            )
                            + 1
                            for item in page_listings
                        )
                        attempted_records = len(listings) + len(page_listings)
                        attempted_bytes = bronze_bytes + page_bytes
                        if (
                            attempted_records > self.max_records
                            or attempted_bytes > self.max_bronze_bytes
                        ):
                            limit_error = CollectionLimitExceeded(
                                f"Collection budget exceeded: {attempted_records} records "
                                f"(limit {self.max_records}), {attempted_bytes} Bronze bytes "
                                f"(limit {self.max_bronze_bytes})"
                            )
                            errors.append(CollectionError(target, page, str(limit_error)))
                            break
                        listings.extend(page_listings)
                        bronze_bytes += page_bytes
                        logger.info(
                            "Recolectados %s avisos de %s/%s, página %s",
                            len(page_listings),
                            target.operation,
                            target.region,
                            page,
                        )
                    except SourceAccessBlocked as exc:
                        logger.error("La fuente bloqueó la sesión automatizada: %s", exc)
                        errors.append(CollectionError(target, page, str(exc)))
                        blocked_error = exc
                        break
                    except InterruptedError:
                        raise
                    except Exception as exc:  # una búsqueda no debe cancelar las demás
                        logger.exception("Falló la recolección de %s", target.url)
                        errors.append(CollectionError(target, page, str(exc)))
                    if requested_pages % self.batch_pages == 0:
                        persist(finished=False)
                if blocked_error or limit_error:
                    break
        except Exception as exc:
            fatal_error = exc
        finally:
            try:
                self.source.close()
            except Exception as exc:
                fatal_error = fatal_error or exc

        bronze_path = persist(finished=True)
        if fatal_error:
            raise fatal_error
        if limit_error:
            raise limit_error
        if blocked_error:
            raise blocked_error
        if not listings:
            raise RuntimeError("La recolección no produjo avisos; consulte el manifiesto Bronze")
        return CollectionResult(len(listings), bronze_path, tuple(errors), bronze_bytes)
