"""Manual cloud coordinator; no schedule and no implicit provider usage collector.

The initial implementation rewrites one complete, bounded five-week generation
(maximum 25 MB), rather than maintaining a partition index. Increase scope only
when measured capacity supports it. Raw extracts receive a private reservation
before collection and verified persistence before publication.
"""

import argparse
import json
import re
import shutil
import tempfile
import uuid
from collections.abc import Sequence
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, cast

from zona_oportunidades.listings.retention import IDENTITY_COLUMNS, utc_week_start
from zona_oportunidades.operations.capacity import (
    AdmissionDecision,
    CapacityPolicy,
    JobEstimate,
    ProviderUsage,
    ResourceDecision,
    RunnerCapacity,
    _parse,
    _read_object,
    assess_admission,
    load_usage,
)
from zona_oportunidades.opportunities.catalogue import CataloguePolicy
from zona_oportunidades.opportunities.domain import ComparisonPolicy, GoldRunResult, Opportunity

if TYPE_CHECKING:
    from psycopg import Connection
    from pyspark.sql import DataFrame, SparkSession

    from zona_oportunidades.operations.quarantine import QuarantineTicket
    from zona_oportunidades.operations.storage import (
        DatasetManifest,
        DatasetRegistry,
        Generation,
        S3Store,
    )


@dataclass(frozen=True)
class CloudPolicy:
    capacity: CapacityPolicy
    catalogue: CataloguePolicy
    job: JobEstimate
    max_working_bytes: int = 25_000_000
    gc_grace_hours: int = 24

    def __post_init__(self) -> None:
        if type(self.max_working_bytes) is not int or not 0 < self.max_working_bytes <= 25_000_000:
            raise ValueError("working generation must be between 1 and 25 MB")
        if type(self.gc_grace_hours) is not int or self.gc_grace_hours < 24:
            raise ValueError("GC grace must be at least 24 hours")
        if self.job.compaction_output_bytes < self.max_working_bytes:
            raise ValueError("compaction reserve must cover the entire working generation")
        for name, value in asdict(self.catalogue).items():
            if type(value) is not int or value <= 0:
                raise ValueError(f"catalogue {name} must be an explicit positive integer")


@dataclass(frozen=True)
class SourceReceipt:
    run_id: str
    input_sha256: str
    transformation_version: str


@dataclass(frozen=True)
class FramePlan:
    retained: "DataFrame"
    current: "DataFrame"


@dataclass(frozen=True)
class CloudResult:
    mode: str
    applied: bool
    allowed: bool
    reasons: tuple[str, ...]
    generation_id: str | None = None
    silver_rows: int = 0
    gold_rows: int = 0
    deleted_generations: tuple[str, ...] = ()
    resources: tuple[ResourceDecision, ...] = ()


def load_cloud_policy(path: Path) -> CloudPolicy:
    """All count caps and cost estimates are operator supplied, never inferred."""
    value = _read_object(path)
    required = {"capacity", "catalogue", "job"}
    if not required <= value.keys() or value.keys() - required - {
        "max_working_bytes",
        "gc_grace_hours",
    }:
        raise ValueError("policy requires capacity, catalogue and job objects")
    for name in required:
        if not isinstance(value[name], dict):
            raise ValueError(f"{name} must be a JSON object")
    try:
        catalogue = cast(dict[str, object], value["catalogue"])
        job = cast(dict[str, object], value["job"])
        working = value.get("max_working_bytes", 25_000_000)
        grace = value.get("gc_grace_hours", 24)
        if type(working) is not int or type(grace) is not int:
            raise ValueError("working bytes and grace hours must be integers")
        return CloudPolicy(
            capacity=_parse("policy", cast(dict[str, object], value["capacity"])),
            catalogue=CataloguePolicy(**catalogue),  # type: ignore[arg-type]
            job=JobEstimate(**job),  # type: ignore[arg-type]
            max_working_bytes=working,
            gc_grace_hours=grace,
        )
    except TypeError as error:
        raise ValueError("missing or unknown policy fields; catalogue caps are required") from error


def preflight(
    policy: CloudPolicy,
    usage: ProviderUsage | None,
    runner: RunnerCapacity,
    *,
    project_id: str | None,
    reclamation_verified: bool,
    now: datetime,
    deletion_only: bool = False,
) -> AdmissionDecision:
    decision = assess_admission(policy.capacity, usage, policy.job, runner, now=now)
    reasons = list(decision.reasons)
    resources = decision.resources
    if deletion_only:
        # GC creates no replacement dataset: storage pressure must not prevent retirement.
        reasons = [
            reason
            for reason in reasons
            if reason
            not in {
                "object_storage projected peak exceeds hard threshold",
                "sql_storage projected peak exceeds hard threshold",
                "Bronze quarantine cannot hold another failed extract",
            }
        ]
        resources = tuple(
            replace(item, peak=item.used, allowed=True)
            if item.resource in {"object_storage", "sql_storage"}
            else item
            for item in resources
        )
    if not reclamation_verified or project_id != policy.capacity.project_id:
        reasons.append(
            "object quota reclamation requires a matching project capability attestation"
        )
    return AdmissionDecision(not reasons, tuple(reasons), resources)


def require_capacity(
    policy: CloudPolicy,
    usage: ProviderUsage | None,
    runner: RunnerCapacity,
    *,
    project_id: str | None,
    reclamation_verified: bool,
    now: datetime,
    deletion_only: bool = False,
) -> None:
    decision = preflight(
        policy,
        usage,
        runner,
        project_id=project_id,
        reclamation_verified=reclamation_verified,
        now=now,
        deletion_only=deletion_only,
    )
    if not decision.allowed:
        raise ValueError("; ".join(decision.reasons))


def plan_frames(
    combined: "DataFrame",
    existing_keys: "DataFrame",
    policy: CloudPolicy,
    comparison: ComparisonPolicy,
    as_of: datetime,
) -> FramePlan:
    """Retain all valid history, and derive both catalogue inputs from latest data."""
    from zona_oportunidades.listings.retention import weekly_observations
    from zona_oportunidades.opportunities.catalogue import (
        CatalogueKey,
        HistoryFootprint,
        WeeklyKey,
        select_catalogue,
    )
    from zona_oportunidades.opportunities.transforms import current_listings

    selected = weekly_observations(combined, as_of)
    if selected.quarantine.limit(1).count() or selected.future.limit(1).count():
        raise ValueError(
            "invalid retention keys or future data; preserve input for manual quarantine"
        )
    retained = selected.retained
    latest = current_listings(retained, comparison, as_of)
    # Never re-admit an older valid price after a newer invalid observation.
    existing = latest.join(existing_keys, list(IDENTITY_COLUMNS), "left_semi")
    history = HistoryFootprint(
        frozenset(
            WeeklyKey(
                CatalogueKey(row.listing_id, row.operation, row.currency),
                utc_week_start(datetime.fromtimestamp(row.epoch, UTC)),
            )
            for row in retained.selectExpr(
                *IDENTITY_COLUMNS, "cast(observed_at as double) as epoch"
            ).collect()
        )
    )
    catalogue = select_catalogue(latest, existing, history, policy.catalogue, as_of)
    if not catalogue.current_count:
        raise ValueError("no eligible current data; keep stale Gold snapshot for operator review")
    return FramePlan(retained, catalogue.current)


def validate_frame(frame: "DataFrame", expected: "DataFrame", rows: int, as_of: datetime) -> None:
    from zona_oportunidades.listings.retention import weekly_observations

    if frame.schema.simpleString() != expected.schema.simpleString() or frame.count() != rows:
        raise ValueError("generation readback schema/count mismatch")
    selection = weekly_observations(frame, as_of)
    if (
        selection.quarantine.limit(1).count()
        or selection.future.limit(1).count()
        or selection.expired.limit(1).count()
        or selection.retained.count() != rows
    ):
        raise ValueError("generation readback violates five-week uniqueness/retention")


def publish_prepared(
    directory: Path,
    manifest: "DatasetManifest",
    opportunities: tuple[Opportunity, ...],
    *,
    conn: "Connection",
    registry: "DatasetRegistry",
    store: "S3Store",
    active: tuple["Generation", ...],
    dsn: str,
    minimum_count: int,
    as_of: datetime,
    bronze_ticket: "QuarantineTicket | None" = None,
) -> GoldRunResult:
    """Caller holds storage_lock through upload/readback and the atomic DB switch."""
    from zona_oportunidades.opportunities.repository import GoldRepository

    if not opportunities or manifest.rows <= 0:
        raise ValueError("empty publication cannot replace Gold")
    prefix = f"{manifest.kind}/{manifest.generation_id}"
    generation = registry.prepare(manifest, prefix)
    uploaded = store.upload(directory, manifest)
    if uploaded != prefix:
        raise ValueError("object upload returned an unexpected immutable prefix")
    with tempfile.TemporaryDirectory(prefix="cloud-verify-") as temporary:
        # The store verifies exact file inventory and hashes on download.
        store.download(generation, Path(temporary) / "readback")
    with conn.transaction():
        result = GoldRepository(dsn).replace(opportunities, minimum_count, as_of, connection=conn)
        registry.activate(
            generation, replaces=tuple(item.manifest.generation_id for item in active)
        )
        if bronze_ticket is not None:
            from zona_oportunidades.operations.quarantine import promote

            promote(conn, bronze_ticket)
    return result


def gc_generations(
    registry: "DatasetRegistry", store: "S3Store", *, before: datetime
) -> tuple[str, ...]:
    """Delete only registry-authorized superseded generations, never arbitrary prefixes.

    S3Store.delete verifies exact objects absent. Receipts remain; no estimated
    reclaimed bytes are subtracted from usage or claimed as provider quota recovery.
    """
    deleted: list[str] = []
    for generation in (*registry.retired(before), *registry.orphaned(before)):
        store.delete(generation)
        registry.mark_deleted(generation)
        deleted.append(generation.manifest.generation_id)
    return tuple(deleted)


def seed_sources(root: Path, as_of: datetime) -> tuple[Path, ...]:
    """Use verified immutable local run manifests; reject expired replay sources."""
    from zona_oportunidades.listings.storage import AtomicSilverPublisher

    start = (utc_week_start(as_of) - timedelta(weeks=4)).date()
    paths: list[Path] = []
    for path in sorted(root.glob("ingestion_date=*/run_id=*/_run_manifest.json")):
        value = _read_object(path)
        ingestion = date.fromisoformat(str(value.get("ingestion_date")))
        if not start <= ingestion <= as_of.astimezone(UTC).date():
            raise ValueError("seed replay source outside retained window")
        if (
            not (path.parent / "_SUCCESS").is_file()
            or value.get("run_id") != path.parent.name.removeprefix("run_id=")
            or path.parent.parent.name != f"ingestion_date={ingestion}"
            or value.get("parquet_inventory")
            != AtomicSilverPublisher._parquet_inventory(path.parent)
        ):
            raise ValueError("local seed inventory/manifest verification failed")
        paths.append(path.parent)
    if not paths:
        raise ValueError("seed requires verified local Silver run manifests")
    return tuple(paths)


def write_source_receipts(
    sources: tuple[Path, ...], output: Path, source_runs: tuple[str, ...]
) -> None:
    """Carry original input hashes/versions without rewriting immutable run manifests."""
    receipts: dict[str, SourceReceipt] = {}
    for directory in sources:
        original = directory / "_run_manifest.json"
        values: list[object]
        if original.is_file():
            values = [_read_object(original)]
        else:
            value = _read_object(directory / "_source_receipts.json")
            entries = value.get("sources")
            if not isinstance(entries, list):
                raise ValueError("missing original source receipts; publication refused")
            values = entries
        for item in values:
            if not isinstance(item, dict):
                raise ValueError("invalid source receipt")
            run, digest, version = (
                item.get("run_id"),
                item.get("input_sha256"),
                item.get("transformation_version"),
            )
            if (
                not isinstance(run, str)
                or not run
                or not isinstance(digest, str)
                or not re.fullmatch(r"[a-f0-9]{64}", digest)
                or not isinstance(version, str)
                or not version
            ):
                raise ValueError(
                    "source receipt requires run id, SHA256 and transformation version"
                )
            receipt = SourceReceipt(run, digest, version)
            if run in receipts and receipts[run] != receipt:
                raise ValueError("conflicting immutable source receipts")
            receipts[run] = receipt
    if set(source_runs) - receipts.keys():
        raise ValueError("retained rows have no immutable source receipt")
    (output / "_source_receipts.json").write_text(
        json.dumps({"sources": [asdict(receipts[run]) for run in source_runs]}, sort_keys=True),
        encoding="utf-8",
    )


def execute(
    mode: str,
    *,
    apply: bool,
    policy: CloudPolicy,
    usage: ProviderUsage | None,
    runner: RunnerCapacity,
    project_id: str | None,
    reclamation_verified: bool,
    dsn: str,
    bucket: str,
    endpoint_url: str | None,
    region: str | None,
    silver_root: Path | None = None,
    master: str | None = None,
    now: datetime | None = None,
    data_root: Path | None = None,
) -> CloudResult:
    now = now or datetime.now(UTC)
    if mode not in {"report", "seed", "ingest", "maintain", "gc"}:
        raise ValueError("unknown cloud mode")
    decision = preflight(
        policy,
        usage,
        runner,
        project_id=project_id,
        reclamation_verified=reclamation_verified,
        now=now,
        deletion_only=mode == "gc",
    )
    if not apply or mode == "report":
        return CloudResult(
            mode, False, decision.allowed, decision.reasons, resources=decision.resources
        )
    if not decision.allowed:
        raise ValueError("; ".join(decision.reasons))
    if not dsn or not bucket:
        raise ValueError("apply requires a writer DSN and private object bucket")
    if not endpoint_url or not endpoint_url.startswith("https://") or not region:
        raise ValueError("apply requires an explicit HTTPS storage endpoint and region")
    from zona_oportunidades.operations.storage import DatasetRegistry, S3Store, storage_lock

    store = S3Store(bucket, endpoint_url, region)
    if mode == "ingest":
        from zona_oportunidades.operations.ingestion import ingest

        return ingest(
            policy=policy,
            usage=usage,
            runner=runner,
            project_id=project_id,
            reclamation_verified=reclamation_verified,
            dsn=dsn,
            store=store,
            data_root=data_root,
            master=master,
            now=now,
        )
    with storage_lock(dsn) as conn:
        registry = DatasetRegistry(conn)
        # Recheck freshness after waiting for the shared lock; never reuse the initial time.
        require_capacity(
            policy,
            usage,
            runner,
            project_id=project_id,
            reclamation_verified=reclamation_verified,
            now=datetime.now(UTC),
            deletion_only=mode == "gc",
        )
        from zona_oportunidades.operations.budget import claim_job

        assert usage is not None
        claim_job(conn, policy.capacity, usage, policy.job, mode, now=now)
        if mode == "gc":
            from zona_oportunidades.operations.maintenance import prune_metadata
            from zona_oportunidades.operations.quarantine import cleanup_promoted

            cleanup_promoted(conn, store)
            deleted = gc_generations(
                registry, store, before=datetime.now(UTC) - timedelta(hours=policy.gc_grace_hours)
            )
            prune_metadata(conn, now=datetime.now(UTC))
            return CloudResult(mode, True, True, (), deleted_generations=deleted)
        return _rewrite(
            mode,
            registry=registry,
            conn=conn,
            store=store,
            policy=policy,
            usage=usage,
            runner=runner,
            project_id=project_id,
            reclamation_verified=reclamation_verified,
            dsn=dsn,
            silver_root=silver_root,
            master=master,
            now=now,
        )


def _rewrite(
    mode: str,
    *,
    registry: "DatasetRegistry",
    conn: "Connection",
    store: "S3Store",
    policy: CloudPolicy,
    usage: ProviderUsage | None,
    runner: RunnerCapacity,
    project_id: str | None,
    reclamation_verified: bool,
    dsn: str,
    silver_root: Path | None,
    master: str | None,
    now: datetime,
    extra_paths: tuple[Path, ...] = (),
    bronze_ticket: "QuarantineTicket | None" = None,
) -> CloudResult:
    from zona_oportunidades.listings.runner import create_local_spark
    from zona_oportunidades.operations.storage import build_manifest
    from zona_oportunidades.opportunities.transforms import comparable_statistics

    active = registry.active()
    if len(active) > 1:
        raise ValueError("only one bounded working generation is supported")
    if mode == "seed" and active:
        raise ValueError("seed requires an empty registry; use maintain for an active generation")
    if mode in {"maintain", "ingest"} and not active:
        raise ValueError("no active generation; seed verified Silver first")
    with tempfile.TemporaryDirectory(prefix="cloud-working-") as temporary:
        workspace = Path(temporary)
        paths: tuple[Path, ...]
        if mode == "seed":
            if silver_root is None:
                raise ValueError("seed requires --silver-root")
            paths = seed_sources(silver_root, now)
        else:
            if (
                sum(file.bytes for item in active for file in item.manifest.files)
                > policy.max_working_bytes
            ):
                raise ValueError("active generation exceeds working bound; download refused")
            paths = tuple(
                store.download(item, workspace / f"input-{index}")
                for index, item in enumerate(active)
            )
        paths += extra_paths
        if (
            sum(path.stat().st_size for root in paths for path in root.rglob("*") if path.is_file())
            > policy.max_working_bytes
        ):
            raise ValueError("input exceeds the bounded working generation; do not shorten history")
        spark = create_local_spark(master)
        try:
            combined = _read_silver(spark, paths, silver_root if mode == "seed" else None)
            records = conn.execute(
                "SELECT listing_id, operation FROM gold.opportunities"
            ).fetchall()
            keys = spark.createDataFrame(
                [(row[0], row[1], "USD") for row in records],
                "listing_id string, operation string, currency string",
            )
            comparison = ComparisonPolicy(max_age_days=policy.catalogue.max_age_days)
            plan = plan_frames(combined, keys, policy, comparison, now)
            retained, current = plan.retained, plan.current
            rows = retained.count()
            output = workspace / "output"
            retained.write.mode("errorifexists").parquet(str(output))
            validate_frame(spark.read.parquet(str(output)), retained, rows, now)
            source_runs = tuple(
                sorted(row.run_id for row in retained.select("run_id").distinct().collect())
            )
            write_source_receipts(paths, output, source_runs)
            manifest = build_manifest(
                output,
                generation_id=uuid.uuid4().hex,
                kind="silver",
                rows=rows,
                source_runs=source_runs,
            )
            actual_bytes = sum(file.bytes for file in manifest.files) + len(
                manifest.to_json().encode("utf-8")
            )
            if actual_bytes > min(policy.max_working_bytes, policy.job.compaction_output_bytes):
                raise ValueError(
                    "output exceeds reserved full-history capacity; publication stopped"
                )
            opportunities = tuple(
                Opportunity(**row.asDict())
                for row in comparable_statistics(current, comparison).toLocalIterator()
            )
            require_capacity(
                policy,
                usage,
                RunnerCapacity(
                    shutil.disk_usage(workspace).free,
                    runner.failed_extracts,
                    runner.failed_bronze_bytes,
                ),
                project_id=project_id,
                reclamation_verified=reclamation_verified,
                now=datetime.now(UTC),
            )
            result = publish_prepared(
                output,
                manifest,
                opportunities,
                conn=conn,
                registry=registry,
                store=store,
                active=active,
                dsn=dsn,
                minimum_count=comparison.minimum_count,
                as_of=now,
                bronze_ticket=bronze_ticket,
            )
            return CloudResult(mode, True, True, (), manifest.generation_id, rows, result.records)
        finally:
            spark.stop()


def _read_silver(spark: "SparkSession", paths: tuple[Path, ...], base: Path | None) -> "DataFrame":
    # Each source already stores its trusted lineage columns inside Parquet. Reading
    # run roots separately avoids mixing Hive partition paths with flat generations.
    frame = spark.read.parquet(str(paths[0]))
    for path in paths[1:]:
        frame = frame.unionByName(spark.read.parquet(str(path)))
    return frame


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("report", "seed", "ingest", "maintain", "gc"))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--usage", type=Path)
    parser.add_argument("--project-id")
    parser.add_argument("--reclamation-verified", action="store_true")
    parser.add_argument("--silver-root", type=Path)
    parser.add_argument("--bucket", default="")
    parser.add_argument("--endpoint-url")
    parser.add_argument("--region")
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    args = parser.parse_args(argv)
    if args.mode == "ingest" and args.apply:
        import signal

        from zona_oportunidades.pipeline.entrypoint import request_stop

        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
    try:
        from zona_oportunidades.shared.config import get_settings

        settings = get_settings()
        root = args.data_root
        if not root.is_dir():
            raise ValueError("data root must exist for disk/quarantine measurement")
        quarantine = tuple(path for path in (root / "quarantine").rglob("*") if path.is_file())
        runner = RunnerCapacity(
            shutil.disk_usage(root).free,
            sum(path.suffix == ".jsonl" for path in quarantine),
            sum(path.stat().st_size for path in quarantine),
        )
        result = execute(
            args.mode,
            apply=args.apply,
            policy=load_cloud_policy(args.policy),
            usage=load_usage(args.usage) if args.usage else None,
            runner=runner,
            project_id=args.project_id,
            reclamation_verified=args.reclamation_verified,
            dsn=settings.postgres_dsn,
            bucket=args.bucket,
            endpoint_url=args.endpoint_url,
            region=args.region,
            silver_root=args.silver_root,
            master=settings.spark_master,
            data_root=root,
        )
        print(json.dumps(asdict(result), allow_nan=False))
        return 0 if result.allowed else 1
    except (ValueError, OSError) as error:
        # Exceptions from providers/DSNs are deliberately not serialized to public CI logs.
        print(json.dumps({"applied": False, "error": str(error)}))
        return 1
    except Exception:
        print(
            json.dumps(
                {"applied": False, "error": "cloud operation failed; inspect private evidence"}
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
