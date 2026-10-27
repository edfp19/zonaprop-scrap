"""Read-only local baseline. This report never grants cloud admission."""

import json
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class LocalRun:
    run_id: str
    source_rows: int
    silver_rows: int
    parquet_bytes: int
    archived: bool


@dataclass(frozen=True)
class GoldMeasurement:
    rows: int
    table_bytes: int
    index_bytes: int
    total_relation_bytes: int
    database_bytes: int


@dataclass(frozen=True)
class CapacityReport:
    classification: str
    cloud_admission_allowed: bool
    missing_cloud_gates: tuple[str, ...]
    bronze_bytes: int
    bronze_source_rows: int
    silver_bytes: int
    manifest_count: int
    invalid_manifest_count: int
    runner_free_bytes: int
    runs: tuple[LocalRun, ...]
    gold: GoldMeasurement | None
    gold_status: str


def measure_gold(dsn: str) -> GoldMeasurement:
    """Explicit opt-in connection; caller must keep DSN and driver errors private."""
    import psycopg

    with (
        psycopg.connect(
            dsn, connect_timeout=5, options="-c default_transaction_read_only=on"
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute(
            "SELECT count(*), pg_table_size('gold.opportunities'), "
            "pg_indexes_size('gold.opportunities'), "
            "pg_total_relation_size('gold.opportunities'), "
            "pg_database_size(current_database()) FROM gold.opportunities"
        )
        row = cursor.fetchone()
    if row is None:
        raise ValueError("Gold size query returned no result")
    return GoldMeasurement(*(int(value) for value in row))


def local_report(data_root: Path, *, gold_dsn: str | None = None) -> CapacityReport:
    """Count all local files, including archived generations and staging bytes.

    Manifest row counts are reported as declared, not as a Parquet integrity check.
    Bronze source rows are counted from actual nonblank JSONL lines.
    """
    bronze = data_root / "bronze"
    silver = data_root / "silver"
    bronze_files = tuple(path for path in bronze.rglob("*") if path.is_file())
    silver_files = tuple(path for path in silver.rglob("*") if path.is_file())
    source_rows = 0
    for path in bronze_files:
        if path.suffix == ".jsonl":
            with path.open("rb") as source:
                source_rows += sum(bool(line.strip()) for line in source)
    runs: list[LocalRun] = []
    invalid = 0
    manifests = [path for path in silver_files if path.name == "_run_manifest.json"]
    for path in manifests:
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(manifest, dict):
                raise ValueError("manifest is not an object")
            run_id, rows, silver_rows = (
                manifest["run_id"],
                manifest["bronze_records"],
                manifest["silver_records"],
            )
            if not isinstance(run_id, str) or not run_id.strip():
                raise ValueError("invalid run identity")
            if any(type(value) is not int or value < 0 for value in (rows, silver_rows)):
                raise ValueError("invalid row counts")
            runs.append(
                LocalRun(
                    run_id,
                    rows,
                    silver_rows,
                    sum(part.stat().st_size for part in path.parent.rglob("*.parquet")),
                    "_archive" in path.relative_to(silver).parts,
                )
            )
        except (ValueError, KeyError, TypeError, UnicodeError):
            invalid += 1
    gold = None
    gold_status = "not_requested"
    if gold_dsn is not None:
        try:
            gold = measure_gold(gold_dsn)
            gold_status = "measured_database_not_provider_quota"
        except Exception:
            # Driver exceptions can contain the DSN or host. Do not serialize them.
            gold_status = "measurement_failed"
    return CapacityReport(
        "local_baseline_only",
        False,
        (
            "verified project-specific provider quotas and usage with current observed_at",
            "bounded compute, transfer, SQL replacement and runner job estimates",
            "failed Bronze quarantine measurement",
        ),
        sum(path.stat().st_size for path in bronze_files),
        source_rows,
        sum(path.stat().st_size for path in silver_files),
        len(manifests),
        invalid,
        shutil.disk_usage(data_root).free,
        tuple(sorted(runs, key=lambda run: (run.archived, run.run_id))),
        gold,
        gold_status,
    )


def report_json(report: CapacityReport) -> str:
    return json.dumps(asdict(report), indent=2, allow_nan=False)
