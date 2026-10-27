"""Tiny, manual S3 compatibility check; never publishes Gold or attests quota recovery."""

import json
import os
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit
from uuid import uuid4

from zona_oportunidades.operations.storage import Generation, S3Store, StorageError, build_manifest

PAYLOAD = b"Zonaprop educational storage connection check.\n"


@dataclass(frozen=True)
class StorageCheckResult:
    prefix: str
    payload_bytes: int = len(PAYLOAD)
    upload_verified: bool = False
    readback_verified: bool = False
    deletion_verified: bool = False
    failed_stage: str | None = None
    provider_error_code: str | None = None
    quota_reclamation_verified: bool = False

    @property
    def passed(self) -> bool:
        return self.upload_verified and self.readback_verified and self.deletion_verified


def _error_code(error: Exception) -> str | None:
    # Provider messages may contain connection details; publish only known error codes.
    response = getattr(error, "response", None)
    if isinstance(response, dict):
        value = response.get("Error", {})
        code = value.get("Code") if isinstance(value, dict) else None
        if isinstance(code, str) and code in {
            "AccessDenied",
            "InvalidAccessKeyId",
            "SignatureDoesNotMatch",
            "NoSuchBucket",
            "NoSuchKey",
            "NotImplemented",
            "PreconditionFailed",
            "403",
            "404",
            "501",
        }:
            return str(code)
    return None


def check_storage(store: S3Store) -> StorageCheckResult:
    """Use a unique test prefix outside the dataset registry and delete only its inventory."""
    identity = uuid4().hex
    result = StorageCheckResult(f"storage-check/{identity}")
    with TemporaryDirectory(prefix="storage-check-") as temporary:
        source = Path(temporary) / "source"
        source.mkdir()
        (source / "connection-check.txt").write_bytes(PAYLOAD)
        manifest = build_manifest(
            source,
            generation_id=identity,
            kind="storage-check",
            rows=0,
            source_runs=(),
        )
        generation = Generation(manifest, result.prefix, "preparing")
        stage = "upload"
        try:
            if store.upload(source, manifest) != result.prefix:
                raise StorageError("Unexpected test prefix")
            result = replace(result, upload_verified=True)
            stage = "readback"
            target = store.download(generation, Path(temporary) / "readback")
            if (target / "connection-check.txt").read_bytes() != PAYLOAD:
                raise StorageError("Test payload readback mismatch")
            result = replace(result, readback_verified=True)
        except Exception as error:
            result = replace(result, failed_stage=stage, provider_error_code=_error_code(error))
        finally:
            # Also clean up partial uploads, using the same exact immutable inventory.
            try:
                store.delete(generation)
                result = replace(result, deletion_verified=True)
            except Exception as error:
                if result.failed_stage is None:
                    result = replace(
                        result,
                        failed_stage="delete",
                        provider_error_code=_error_code(error),
                    )
    return result


def main() -> int:
    bucket = os.environ.get("S3_BUCKET", "")
    endpoint = os.environ.get("S3_ENDPOINT_URL", "")
    region = os.environ.get("S3_REGION", "")
    try:
        parsed = urlsplit(endpoint)
        if (
            endpoint != endpoint.strip()
            or parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or any(char in endpoint for char in ('"', "'", "\n", "\r", " "))
        ):
            raise ValueError("S3_ENDPOINT_URL must be an HTTPS URL without quotes or whitespace")
        if not bucket or not region or bucket != bucket.strip() or region != region.strip():
            raise ValueError("S3_BUCKET and S3_REGION must be set without surrounding whitespace")
    except ValueError:
        print(
            json.dumps(
                {
                    "passed": False,
                    "failed_stage": "configuration",
                    "error": "Check S3_BUCKET, S3_REGION and the unquoted HTTPS S3_ENDPOINT_URL",
                }
            )
        )
        return 1
    try:
        result = check_storage(S3Store(bucket, endpoint, region))
    except Exception as error:
        print(
            json.dumps(
                {
                    "passed": False,
                    "failed_stage": "client_setup",
                    "provider_error_code": _error_code(error),
                }
            )
        )
        return 1
    print(json.dumps(asdict(result) | {"passed": result.passed}, indent=2))
    return 0 if result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
