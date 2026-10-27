import json
from pathlib import Path

import pytest

from zona_oportunidades.collection.application.collect_listings import CollectListings
from zona_oportunidades.collection.domain.models import (
    CollectedListing,
    SearchTarget,
    SourceAccessBlocked,
)
from zona_oportunidades.collection.infrastructure.jsonl_repository import JsonlBronzeRepository


class FakeSource:
    closed = False

    def collect_page(self, target: SearchTarget, page: int, run_id: str) -> list[CollectedListing]:
        return [
            CollectedListing(
                schema_version=1,
                extractor_version="test",
                run_id=run_id,
                collected_at="2026-10-26T00:00:00+00:00",
                source_page_url=target.url,
                requested_operation=target.operation,
                requested_region=target.region,
                requested_currency="USD",
                currency_context_verified=True,
                payload={"postingId": f"listing-{page}"},
            )
        ]

    def close(self) -> None:
        self.closed = True


class BlockedSource(FakeSource):
    def collect_page(self, target: SearchTarget, page: int, run_id: str) -> list[CollectedListing]:
        raise SourceAccessBlocked("verificación de acceso")


def test_collection_persists_jsonl_and_manifest(tmp_path: Path) -> None:
    source = FakeSource()
    target = SearchTarget("VENTA", "CABA", "https://example.com/list.html")
    use_case = CollectListings(
        source=source,
        repository=JsonlBronzeRepository(tmp_path),
        targets=(target,),
        max_pages=2,
        delay_seconds=0,
    )

    result = use_case.execute("00000000-0000-0000-0000-000000000001")

    assert result.records == 2
    assert source.closed
    records = [json.loads(line) for line in result.bronze_path.read_text().splitlines()]
    assert [record["payload"]["postingId"] for record in records] == ["listing-1", "listing-2"]
    manifest_path = tmp_path / "bronze" / "manifests" / "00000000-0000-0000-0000-000000000001.json"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["record_count"] == 2
    assert manifest["error_count"] == 0


def test_access_block_stops_other_targets_and_writes_manifest(tmp_path: Path) -> None:
    source = BlockedSource()
    targets = (
        SearchTarget("VENTA", "CABA", "https://example.com/one.html"),
        SearchTarget("VENTA", "GBA_NORTE", "https://example.com/two.html"),
    )
    use_case = CollectListings(
        source=source,
        repository=JsonlBronzeRepository(tmp_path),
        targets=targets,
        max_pages=2,
        delay_seconds=0,
    )

    with pytest.raises(SourceAccessBlocked):
        use_case.execute("00000000-0000-0000-0000-000000000002")

    assert source.closed
    manifest_path = tmp_path / "bronze" / "manifests" / "00000000-0000-0000-0000-000000000002.json"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["record_count"] == 0
    assert manifest["error_count"] == 1


class IntermittentSource(FakeSource):
    def __init__(self, failure: type[Exception]) -> None:
        self.failure = failure
        self.calls = 0

    def collect_page(self, target: SearchTarget, page: int, run_id: str) -> list[CollectedListing]:
        self.calls += 1
        if page >= 2:
            raise self.failure("page failed")
        return super().collect_page(target, page, run_id)


def test_partial_error_count_counts_failed_pages(tmp_path: Path) -> None:
    source = IntermittentSource(RuntimeError)
    result = CollectListings(
        source,
        JsonlBronzeRepository(tmp_path),
        (SearchTarget("VENTA", "CABA", "https://example.test"),),
        3,
        0,
    ).execute("partial")
    manifest = json.loads((tmp_path / "bronze/manifests/partial.json").read_text())
    assert result.records == 1
    assert len(result.errors) == manifest["error_count"] == 2
    assert [error["page"] for error in manifest["errors"]] == [2, 3]
    assert result.bronze_bytes == result.bronze_path.stat().st_size


def test_block_after_success_preserves_evidence_and_raises(tmp_path: Path) -> None:
    source = IntermittentSource(SourceAccessBlocked)
    with pytest.raises(SourceAccessBlocked):
        CollectListings(
            source,
            JsonlBronzeRepository(tmp_path),
            (SearchTarget("VENTA", "CABA", "https://example.test"),),
            3,
            0,
        ).execute("blocked-partial")
    manifest = json.loads((tmp_path / "bronze/manifests/blocked-partial.json").read_text())
    assert manifest["record_count"] == 1
    assert manifest["error_count"] == 1
    assert source.calls == 2
    assert source.closed


@pytest.mark.parametrize("budget", ["rows", "bytes"])
def test_collection_budget_stops_without_saving_overflow(tmp_path: Path, budget: str) -> None:
    from zona_oportunidades.collection.application.collect_listings import CollectionLimitExceeded

    source = FakeSource()
    target = SearchTarget("VENTA", "CABA", "https://example.test")
    listing = source.collect_page(target, 1, "limited")[0]
    one_row_bytes = (
        len(
            json.dumps(listing.as_dict(), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        )
        + 1
    )
    with pytest.raises(CollectionLimitExceeded):
        CollectListings(
            source,
            JsonlBronzeRepository(tmp_path),
            (target,),
            4,
            0,
            max_records=1 if budget == "rows" else 250,
            max_bronze_bytes=one_row_bytes if budget == "bytes" else 10_000_000,
        ).execute("limited")
    manifest = json.loads((tmp_path / "bronze/manifests/limited.json").read_text())
    assert manifest["record_count"] == 1
    assert manifest["bronze_bytes"] == one_row_bytes
    assert manifest["attempted_records"] == 2
    assert manifest["attempted_bytes"] == one_row_bytes * 2
    assert manifest["error_count"] == 1
    bronze = next((tmp_path / "bronze/listings").rglob("listings.jsonl"))
    assert bronze.stat().st_size == one_row_bytes
    assert source.closed


def test_empty_collection_records_failure_evidence(tmp_path: Path) -> None:
    class EmptySource(FakeSource):
        def collect_page(
            self, target: SearchTarget, page: int, run_id: str
        ) -> list[CollectedListing]:
            return []

    with pytest.raises(RuntimeError, match="no produjo avisos"):
        CollectListings(
            EmptySource(),
            JsonlBronzeRepository(tmp_path),
            (SearchTarget("VENTA", "CABA", "https://example.test"),),
            1,
            0,
        ).execute("empty")
    manifest = json.loads((tmp_path / "bronze/manifests/empty.json").read_text())
    assert manifest["record_count"] == manifest["bronze_bytes"] == 0


@pytest.mark.parametrize("during_close", [False, True])
def test_interruption_keeps_bronze_evidence_and_is_not_a_partial_error(
    tmp_path: Path, during_close: bool
) -> None:
    class InterruptedSource(FakeSource):
        def collect_page(
            self, target: SearchTarget, page: int, run_id: str
        ) -> list[CollectedListing]:
            if page == 2 and not during_close:
                raise InterruptedError("stop")
            return super().collect_page(target, page, run_id)

        def close(self) -> None:
            super().close()
            if during_close:
                raise InterruptedError("stop")

    source = InterruptedSource()
    with pytest.raises(InterruptedError):
        CollectListings(
            source,
            JsonlBronzeRepository(tmp_path),
            (SearchTarget("VENTA", "CABA", "https://example.test"),),
            2,
            0,
        ).execute("interrupted")
    manifest = json.loads((tmp_path / "bronze/manifests/interrupted.json").read_text())
    assert manifest["record_count"] == (2 if during_close else 1)
    assert manifest["error_count"] == 0
    assert manifest["failure_message"] == "stop"
    assert source.closed
