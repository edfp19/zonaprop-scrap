import logging
import signal
import time
import uuid
from datetime import UTC, datetime

from zona_oportunidades.collection.application.collect_listings import CollectListings
from zona_oportunidades.collection.domain.models import SEARCH_TARGETS
from zona_oportunidades.collection.infrastructure.jsonl_repository import JsonlBronzeRepository
from zona_oportunidades.collection.infrastructure.selenium_source import ZonapropSeleniumSource
from zona_oportunidades.listings.runner import run_bronze_file
from zona_oportunidades.pipeline.run_repository import PipelineRunRepository
from zona_oportunidades.shared.config import get_settings
from zona_oportunidades.shared.logging import configure_logging

logger = logging.getLogger(__name__)
keep_running = True


def request_stop(_signal: int, _frame: object) -> None:
    global keep_running
    keep_running = False


def run_collection() -> None:
    settings = get_settings()
    run_id = str(uuid.uuid4())
    runs = PipelineRunRepository(settings.postgres_dsn)
    runs.start(run_id)
    try:
        offset = datetime.now(UTC).date().toordinal() % len(SEARCH_TARGETS)
        targets = SEARCH_TARGETS[offset:] + SEARCH_TARGETS[:offset]
        use_case = CollectListings(
            source=ZonapropSeleniumSource(
                settings.selenium_url, settings.collection_page_timeout_seconds
            ),
            repository=JsonlBronzeRepository(settings.data_root),
            targets=targets,
            max_pages=settings.collection_max_pages,
            delay_seconds=settings.collection_delay_seconds,
        )
        result = use_case.execute(run_id)
        runs.record_bronze(run_id, result.records)
        silver_result = run_bronze_file(
            result.bronze_path, settings.data_root, master=settings.spark_master
        )
        warning = None
        if result.errors:
            warning = f"La recolección finalizó con {len(result.errors)} error(es) parcial(es)."
        runs.succeed(
            run_id,
            result.records,
            warning,
            silver_records=silver_result.silver_records,
        )
        logger.info(
            "Checkpoint Bronze→Silver completo: %s Bronze en %s; %s Silver en %s "
            "(%s errores parciales)",
            result.records,
            result.bronze_path,
            silver_result.silver_records,
            silver_result.output_path,
            len(result.errors),
        )
    except Exception as exc:
        logger.exception("Falló la ejecución Bronze→Silver %s", run_id)
        runs.fail(run_id, str(exc))


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    while keep_running:
        run_collection()
        deadline = time.monotonic() + settings.pipeline_interval_seconds
        while keep_running and time.monotonic() < deadline:
            time.sleep(min(1, max(0, deadline - time.monotonic())))


if __name__ == "__main__":
    main()
