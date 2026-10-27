import json
import shutil
from dataclasses import asdict
from pathlib import Path

import pytest

from zona_oportunidades.operations import storage_check


class LocalStore:
    def __init__(self, root: Path, failure: str | None = None):
        self.root, self.failure = root, failure
        self.deleted = []

    def upload(self, source, manifest):
        prefix = f"{manifest.kind}/{manifest.generation_id}"
        shutil.copytree(source, self.root / prefix)
        if self.failure == "upload":
            raise RuntimeError("private connection details must not appear")
        return prefix

    def download(self, generation, target):
        if self.failure == "readback":
            raise RuntimeError("private connection details must not appear")
        shutil.copytree(self.root / generation.prefix, target)
        if self.failure == "corrupt":
            (target / "connection-check.txt").write_bytes(b"corrupt")
        return target

    def delete(self, generation):
        assert generation.manifest.kind == "storage-check"
        assert generation.state == "preparing"
        if self.failure == "delete":
            raise RuntimeError("private connection details must not appear")
        shutil.rmtree(self.root / generation.prefix)
        self.deleted.append(generation.prefix)


def test_successful_roundtrip_is_unique_cleaned_up_and_does_not_attest_quota(tmp_path):
    store = LocalStore(tmp_path)
    first = storage_check.check_storage(store)
    second = storage_check.check_storage(store)
    assert first.passed and second.passed
    assert first.prefix != second.prefix
    assert first.payload_bytes == len(storage_check.PAYLOAD) < 100
    assert not first.quota_reclamation_verified
    assert store.deleted == [first.prefix, second.prefix]
    assert not list((tmp_path / "storage-check").iterdir())


@pytest.mark.parametrize("failure", ["upload", "readback", "corrupt", "delete"])
def test_failed_check_reports_stage_without_leaking_errors_and_attempts_cleanup(tmp_path, failure):
    store = LocalStore(tmp_path, failure)
    result = storage_check.check_storage(store)
    assert not result.passed and not result.quota_reclamation_verified
    assert result.failed_stage == ("readback" if failure == "corrupt" else failure)
    assert result.deletion_verified == (failure != "delete")
    assert "private connection details" not in json.dumps(asdict(result))
    if failure != "delete":
        assert not (tmp_path / result.prefix).exists()
    else:
        assert (tmp_path / result.prefix).exists()


def test_quoted_endpoint_fails_before_opening_client(monkeypatch, capsys):
    monkeypatch.setenv("S3_ENDPOINT_URL", '"https://storage.example.test"')
    monkeypatch.setenv("S3_BUCKET", "private")
    monkeypatch.setenv("S3_REGION", "us-east-2")

    def forbidden(*args):
        pytest.fail("An invalid endpoint must not open a storage client")

    monkeypatch.setattr(storage_check, "S3Store", forbidden)
    assert storage_check.main() == 1
    result = json.loads(capsys.readouterr().out)
    assert not result["passed"] and result["failed_stage"] == "configuration"


@pytest.mark.parametrize("failure, exit_code", [(None, 0), ("delete", 1)])
def test_cli_exit_code_follows_verified_result(tmp_path, monkeypatch, capsys, failure, exit_code):
    monkeypatch.setenv("S3_ENDPOINT_URL", "https://storage.example.test")
    monkeypatch.setenv("S3_BUCKET", "private")
    monkeypatch.setenv("S3_REGION", "us-east-2")
    monkeypatch.setattr(storage_check, "S3Store", lambda *args: LocalStore(tmp_path, failure))
    assert storage_check.main() == exit_code
    result = json.loads(capsys.readouterr().out)
    assert result["passed"] == (exit_code == 0)
