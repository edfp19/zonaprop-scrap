import json
from pathlib import Path

import pytest

from zona_oportunidades.operations.__main__ import main
from zona_oportunidades.operations.report import local_report, report_json


def test_report_counts_actual_files_and_classifies_local_not_cloud(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    bronze.mkdir()
    (bronze / "input.jsonl").write_bytes(b"{}\n\n{}\n")
    silver = tmp_path / "silver" / "run"
    silver.mkdir(parents=True)
    (silver / "part.parquet").write_bytes(b"measured-parquet-bytes")
    (silver / "_run_manifest.json").write_text(
        json.dumps({"run_id": "sample", "bronze_records": 2, "silver_records": 1})
    )
    (silver / "_staging").write_bytes(b"staged")
    archive = tmp_path / "silver" / "_archive"
    archive.mkdir()
    (archive / "_run_manifest.json").write_text("invalid JSON")
    result = local_report(tmp_path)
    assert result.bronze_bytes == 7
    assert result.bronze_source_rows == 2
    assert result.runs[0].source_rows == 2
    assert result.runs[0].parquet_bytes == 22
    assert result.silver_bytes == sum(p.stat().st_size for p in silver.iterdir()) + 12
    assert result.manifest_count == 2
    assert result.invalid_manifest_count == 1
    assert result.gold_status == "not_requested"
    document = json.loads(report_json(result))
    assert document["classification"] == "local_baseline_only"
    assert document["cloud_admission_allowed"] is False
    assert len(document["missing_cloud_gates"]) == 3


def test_gold_connection_errors_do_not_expose_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(dsn: str) -> None:
        raise RuntimeError(f"secret database URL: {dsn}")

    monkeypatch.setattr("zona_oportunidades.operations.report.measure_gold", fail)
    result = local_report(tmp_path, gold_dsn="postgres://user:secret@example/database")
    assert result.gold_status == "measurement_failed"
    assert "secret" not in report_json(result)
    assert not result.cloud_admission_allowed


def test_cli_is_read_only_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr("sys.argv", ["operations", "capacity-report", "--data-root", str(tmp_path)])
    assert main() == 0
    assert json.loads(capsys.readouterr().out)["classification"] == "local_baseline_only"
    assert list(tmp_path.iterdir()) == []
