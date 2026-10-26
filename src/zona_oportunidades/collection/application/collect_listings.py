import logging
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from zona_oportunidades.collection.domain.models import (
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


class CollectListings:
    def __init__(
        self,
        source: ListingSource,
        repository: BronzeRepository,
        targets: tuple[SearchTarget, ...],
        max_pages: int,
        delay_seconds: float,
    ) -> None:
        self.source = source
        self.repository = repository
        self.targets = targets
        self.max_pages = max_pages
        self.delay_seconds = delay_seconds

    def execute(self, run_id: str) -> CollectionResult:
        listings = []
        errors: list[CollectionError] = []
        blocked_error: SourceAccessBlocked | None = None
        started_at = time.time()
        try:
            for target in self.targets:
                for page in range(1, self.max_pages + 1):
                    try:
                        page_listings = self.source.collect_page(target, page, run_id)
                        listings.extend(page_listings)
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
                    except Exception as exc:  # una búsqueda no debe cancelar las demás
                        logger.exception("Falló la recolección de %s", target.url)
                        errors.append(CollectionError(target, page, str(exc)))
                    time.sleep(self.delay_seconds)
                if blocked_error:
                    break
        finally:
            self.source.close()

        bronze_path = self.repository.save_listings(run_id, listings)
        self.repository.save_manifest(
            run_id,
            {
                "run_id": run_id,
                "duration_seconds": round(time.time() - started_at, 3),
                "record_count": len(listings),
                "error_count": len(errors),
                "errors": [
                    {"target": asdict(error.target), "page": error.page, "message": error.message}
                    for error in errors
                ],
            },
        )
        if not listings:
            if blocked_error:
                raise blocked_error
            raise RuntimeError("La recolección no produjo avisos; consulte el manifiesto Bronze")
        return CollectionResult(len(listings), bronze_path, tuple(errors))
