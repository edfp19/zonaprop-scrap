import json
from pathlib import Path

import pytest

from zona_oportunidades.collection.domain.models import (
    CollectedListing,
    SearchTarget,
    SourceAccessBlocked,
)
from zona_oportunidades.operations.pilot import main, run_pilot


class FakeSource:
    def __init__(self, failure: str = "", rows: int = 1) -> None:
        self.failure = failure
        self.rows = rows
        self.calls = 0
        self.closed = False

    def collect_page(self, target: SearchTarget, page: int, run_id: str) -> list[CollectedListing]:
        self.calls += 1
        assert page == 1
        if self.calls == 2:
            if self.failure == "blocked":
                raise SourceAccessBlocked("private source details")
            if self.failure == "error":
                raise RuntimeError("private source details")
            if self.failure == "empty":
                return []
        return [
            CollectedListing(
                1,
                "fake",
                run_id,
                "2026-10-27T00:00:00Z",
                target.url,
                target.operation,
                target.region,
                "USD",
                True,
                {"postingId": str(index)},
            )
            for index in range(self.rows)
        ]

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def no_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "zona_oportunidades.collection.application.collect_listings.time.sleep", lambda _: None
    )


def test_full_trial_is_isolated_and_counts_all_targets(tmp_path: Path) -> None:
    source = FakeSource()
    report = run_pilot(tmp_path, lambda: source)
    assert report.status == "full_target"
    assert report.representative
    assert report.targets_completed == report.targets_attempted == 8
    assert report.records == 8
    assert report.bronze.files == 2
    assert report.bronze.bytes > 0
    assert report.spark_seconds is None
    assert source.closed
    path = Path(report.report_path)
    assert path.parent.parent == tmp_path / "pilot"
    assert not (tmp_path / "silver").exists()
    assert json.loads(path.read_text())["records"] == 8


@pytest.mark.parametrize("failure", ["blocked", "error", "empty"])
def test_incomplete_preserves_evidence_without_starting_spark(
    tmp_path: Path, failure: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Incomplete collections must not start Spark")

    monkeypatch.setattr("zona_oportunidades.listings.runner.run_bronze_file", forbidden)
    source = FakeSource(failure)
    report = run_pilot(tmp_path, lambda: source, silver=True)
    assert report.status == ("blocked" if failure == "blocked" else "incomplete")
    assert not report.representative
    assert report.records == (1 if failure == "blocked" else 7)
    assert source.calls == (2 if failure == "blocked" else 8)
    assert source.closed
    assert report.bronze.files == 2
    assert report.spark_seconds is None
    assert "private source details" not in Path(report.report_path).read_text()


def test_row_budget_is_not_successful_truncation(tmp_path: Path) -> None:
    source = FakeSource(rows=126)
    report = run_pilot(tmp_path, lambda: source)
    assert report.status == "incomplete"
    assert report.failure_kind == "CollectionLimitExceeded"
    assert report.records == 126
    assert source.calls == 2
    assert source.closed


def test_initialization_failure_leaves_safe_report_and_manifest(tmp_path: Path) -> None:
    def fail() -> FakeSource:
        raise RuntimeError("secret driver URL")

    report = run_pilot(tmp_path, fail)
    assert report.status == "incomplete"
    assert report.records == report.targets_attempted == 0
    assert report.bronze.files == 2
    assert "secret driver URL" not in Path(report.report_path).read_text()


def test_default_dry_run_does_not_initialize_browser(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr("sys.argv", ["pilot"])
    assert main() == 0
    assert json.loads(capsys.readouterr().out)["status"] == "dry_run"


def test_byte_ceiling_stops_before_saving_oversized_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("zona_oportunidades.operations.pilot.MAX_BRONZE_BYTES", 100)
    source = FakeSource()
    report = run_pilot(tmp_path, lambda: source)
    assert report.status == "incomplete"
    assert report.failure_kind == "CollectionLimitExceeded"
    assert report.records == 0
    assert source.calls == 1


def test_full_trial_replays_only_into_pilot_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zona_oportunidades.listings.domain import SilverRunResult
    from zona_oportunidades.operations.pilot import file_usage

    def replay(bronze_path: Path, data_root: Path) -> SilverRunResult:
        assert data_root.parent == tmp_path / "pilot"
        assert bronze_path.is_relative_to(data_root)
        output = data_root / "silver" / "fake.parquet"
        output.parent.mkdir()
        output.write_bytes(b"fake parquet")
        return SilverRunResult(data_root.name, 8, 8, output, False)

    monkeypatch.setattr("zona_oportunidades.listings.runner.run_bronze_file", replay)
    report = run_pilot(tmp_path, FakeSource, silver=True)
    assert report.status == "full_target"
    assert report.spark_seconds is not None
    assert report.silver_records == 8
    assert report.silver.bytes == 12
    assert report.total == file_usage(Path(report.report_path).parent)
