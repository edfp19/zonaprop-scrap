import json
import os
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

from zona_oportunidades.collection.domain.models import CollectedListing


class JsonlBronzeRepository:
    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root

    def save_listings(self, run_id: str, listings: Iterable[CollectedListing]) -> Path:
        date = datetime.now(UTC).date().isoformat()
        directory = (
            self.data_root / "bronze" / "listings" / f"ingestion_date={date}" / f"run_id={run_id}"
        )
        directory.mkdir(parents=True, exist_ok=True)
        destination = directory / "listings.jsonl"
        temporary = destination.with_suffix(".jsonl.tmp")
        with temporary.open("w", encoding="utf-8") as output:
            for listing in listings:
                output.write(
                    json.dumps(listing.as_dict(), ensure_ascii=False, separators=(",", ":"))
                )
                output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(destination)
        return destination

    def save_manifest(self, run_id: str, manifest: dict[str, object]) -> Path:
        directory = self.data_root / "bronze" / "manifests"
        directory.mkdir(parents=True, exist_ok=True)
        destination = directory / f"{run_id}.json"
        destination.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return destination
