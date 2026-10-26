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
