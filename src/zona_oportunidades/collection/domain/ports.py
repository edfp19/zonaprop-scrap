from collections.abc import Iterable
from pathlib import Path
from typing import Protocol

from zona_oportunidades.collection.domain.models import CollectedListing, SearchTarget


class ListingSource(Protocol):
    def collect_page(
        self, target: SearchTarget, page: int, run_id: str
    ) -> list[CollectedListing]: ...

    def close(self) -> None: ...


class BronzeRepository(Protocol):
    def save_listings(self, run_id: str, listings: Iterable[CollectedListing]) -> Path: ...

    def save_manifest(self, run_id: str, manifest: dict[str, object]) -> Path: ...
