# Pipeline and retention roadmap: a bounded working set

Status: implementation and release checklist, 27 October 2026. Bounded ingestion, weekly
retention, private quarantine, publication and GC are implemented locally. No cloud writes or
schedule have been enabled. Commands and remaining gates: [pipeline guide](pipeline/README.md).

## Objective: equilibrium after warm-up

Maintain a useful, bounded catalogue rather than accumulate every listing indefinitely.

`next catalogue = current catalogue + new admissions - retirements`

During warm-up, admissions can exceed retirements until the target is reached. At steady
state, admissions replace expired or explicitly retired entries. Refreshing an existing
listing updates its slot. A minimum/target/maximum band avoids changing the catalogue for
every tiny fluctuation; a hard byte budget always overrides the target count.

Choose the target after a representative pilot. A large global count is not sufficient:
Gold compares distinct current listings in matching operation, location and room groups,
with an area tolerance. Historical observations must not inflate the peer count. Track
the share of listings with at least the existing minimum of ten peers. Concentrating on
a few comparison groups may be more useful than sparse coverage across all regions.
The deployed 96-row snapshot currently has at most eight peers per listing and zero rows
meeting the ten-peer threshold. This makes comparison-group density a concrete pilot goal.

Do not infer that a listing disappeared because it was absent from one sampled search page.
Use a documented age/coverage policy. If the source fails, keep the last committed Gold and
report its age; do not claim that old observations are newly collected or fabricate coverage.

## Verified limits and measured baseline

Current published Neon Free allowances are 1 GB PostgreSQL storage, 5 GB object storage,
100 CU-hours/month and 5 GB/month public transfer. Compute or transfer exhaustion can suspend
the compute; storage exhaustion can reject operations that increase usage. Object-storage
egress shares the project's transfer allowance. Confirm this project's actual limits and
usage in its Console before unattended operation; use the lower verified limit if sources
or account settings disagree.

Sources: [Neon plans](https://neon.com/docs/introduction/plans),
[storage limits](https://neon.com/docs/storage/overview#limits).

The repository is public: standard GitHub-hosted runners are free for public repositories.
Artifacts/cache storage are separate concerns, so Silver and raw extracts stay in private
object storage, not public workflow artifacts.
[GitHub billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions).

Vercel Hobby also has separate limits, including 4 active CPU-hours/month and 100 GB fast data
transfer. It serves the map; Spark and maintenance run in Actions.
[Vercel Hobby](https://vercel.com/docs/plans/hobby).

Local measurements, not Neon's account usage:

| Measurement | Current size |
| --- | --- |
| Four active Silver runs | 120 rows, about 0.53 MB including local metadata |
| Each sampled 30-row Silver run | about 0.13–0.14 MB |
| Four Bronze extracts | about 1.18 MB total |
| Gold table plus indexes | 188,416 bytes for 96 rows |
| Local database including baseline/system storage | about 8 MB |
| Current map JSON response | about 89.5 KB |

These four small runs do not establish a production-size job. The configured collector has
eight search targets. With one page and roughly 30 results per target, a successful job
could collect roughly 240 observations; the page count is not a contractual row bound.

## Resource policy, sized from the job

Begin with explicit, conservative trial limits: one page per target, at most 250 collected
observations, 10 MB Bronze, 5 MB new Silver, and a 45-minute workflow timeout. These are proposed
application budgets, not provider limits. Exceeding one aborts publication and records the
reason rather than silently truncating a successful-looking run.

Measure records, serialized bytes, Parquet bytes, file counts, Gold/index size, peak runner
disk/RAM, duration, transferred bytes and provider usage before/after. Keep a short bounded
history of these measurements. Estimate the next job from the larger of the configured floor
and twice the largest recent bytes-per-row scaled to the configured row ceiling. Recalibrate
only after representative full-target runs; cap unexpected growth during collection as well.

For example, a 5 MB Silver allowance is well above extrapolating the present 30-row sample
to 240 rows. It remains a guard, not a prediction. A catalogue of 1,000 observation keys with
five weekly observations each would be roughly 23 MB at the current sample's bytes-per-row,
before churn, staging, metadata and retained old files. This is an illustration, not a chosen
catalogue target or guaranteed compression ratio.

Initial proposed thresholds:

| Resource | Maintenance priority / warning | Refuse additional work |
| --- | --- | --- |
| PostgreSQL storage | 60% of verified quota | projected publication peak above 80% |
| Object storage | 60% of verified quota | projected upload/compaction peak above 80% |
| Monthly compute and transfer | 60%: review forecast, reduce batch demand | 80%: stop discretionary ingestion and expensive maintenance |

The unused 20% is emergency/measurement headroom, not an extra allowance to consume normally.
Checks use actual account usage plus outstanding staged/obsolete data; row counts and live
S3 listings alone do not prove quota headroom. Unknown or stale capacity measurements block
new uploads and destructive maintenance until refreshed.
Reserve can support bounded, verified delete-only emergency maintenance; a large rewrite
must still pass its own space, compute and transfer checks.

`object peak = stored usage + next job allowance + compaction output allowance + failure reserve`

Stored usage already includes any old copies not yet reclaimed. Plan conservatively even when
uploads are serialized. Compact a bounded week/partition at a time; never assume room to rewrite
the entire archive. PostgreSQL checks reserve room for the replacement snapshot, indexes,
dead tuples and write/history overhead, not just its final row count. Account for runner disk
space for downloads, Spark temporary files, staging and images too.

Deletion is not instant capacity recovery. PostgreSQL VACUUM normally makes space reusable;
it need not shrink the file or the provider's usage immediately. Avoid automated VACUUM FULL,
which rewrites/locks tables and needs spare space. Confirm the provider's measured usage after
cleanup instead of counting requested deletions as reclaimed bytes.

## Two bounds: current catalogue and retained history

Gold holds only the bounded current serving catalogue. Refresh an existing slot first; admit
new identities against the catalogue and byte budgets. Once full, each new admission needs a
retirement under a deterministic freshness/coverage policy. Preserve representation in useful
comparison groups rather than simply keep the cheapest listings or blindly evict the oldest
row. Selection must not bias the comparable mean toward bargains.

Silver retains the latest observation per `(listing_id, operation, currency, UTC week)`, using
the current Monday-start UTC week and four previous weeks. Deterministic tie-breaking makes
compaction repeatable. This agreed rough-history policy is implemented in the manual cloud coordinator.

Also cap weekly distinct retained keys and bytes. Listing churn can grow historical identities
even if Gold's current row count is constant. Older retired keys still consume history until
they age out. Admission planning includes those keys, invalid/flagged observations and currency
variants; it must not silently shorten the agreed five-week policy to squeeze in more inputs.
If a valid retained window plus safe rewrite space cannot fit, reduce new admissions or pause.

Bronze becomes deletable only after the corresponding Silver publication is verified. Unpromoted
or failed extracts remain recoverable in a bounded private quarantine. Proposed reserve: two
maximum-sized failed extracts (20 MB initially). If it fills, stop collection for investigation;
do not silently delete unpromoted Bronze under the existing policy.

Bound ops logs, receipts, staging files and superseded generations too. Proposed audit retention
is 90 days. Reject replays older than the retained window so expiring old receipts cannot
resurrect discarded history. A pruned run is reported as pruned, not as a still-present output.

## Compaction and safe parallel work

Compaction has two jobs: discard observations outside the weekly policy and combine small
Parquet files. Keep the existing compression initially; benchmark a different codec only if
the measured byte savings justify its CPU cost. Gold still needs source metadata currently
extracted from raw_json, so do not drop that field without changing the data contract.

Publish compacted output to a new immutable prefix, read it back, verify schema, counts,
key uniqueness and checksums, then switch the active dataset reference. Original run manifests
are never edited to pretend rewritten bytes are their original output. Keep a small receipt
recording the source run/version/hash and compaction relationship. Only retire superseded files
after their replacement is verified and no coordinated reader needs the old prefix.

Neon currently documents object lifecycle configuration without lifecycle-rule enforcement,
and DeleteObject as a soft delete. Our code must perform and verify explicit cleanup. A small
upload/delete/reclamation test is a release gate: establish how provider quota accounting
changes and whether history/branches retain bytes before trusting the cleanup forecast.
[S3 compatibility](https://neon.com/docs/storage/s3-compatibility).

Ingestion and maintenance are separate Actions jobs and implementation tracks. Collection and
local Spark preparation can overlap maintenance. A shared PostgreSQL advisory lock coordinates
active-Silver reads, uploads, dataset switches, deletion and Gold publication. Hold that lock
through reading the selected immutable dataset and publishing the result, or wait; do not read
a prefix while another process is allowed to retire it. This deliberately serializes shared
state changes while allowing independent work to overlap. The website keeps reading the last
committed Gold snapshot throughout. The lock must be respected by local runs as well as CI.

Add a small active-dataset registry rather than recursively reading every Parquet file under
the root. Otherwise original and compacted generations could both be read. Orphan cleanup
uses publication status, age and the lock, never age alone. Per-run status updates must also
stop marking every other in-progress run failed, as the current entrypoint repository does.

## Compute and transfer are a separate equilibrium

Deleting rows cannot restore spent monthly CU-hours or transfer. Keep Spark in Actions, use
Neon's smallest suitable compute with scale-to-zero, and avoid frequent database health polls.
Cache the public map response at Vercel for about one hour initially; the weekly dataset can
tolerate that delay. Cover exposed equivalent paths so an alias does not bypass the cache.
Keep a last-good cached response usable on a temporary refresh failure where the platform
permits it. Measure cache hits and real usage, not assumed savings.

Proposed planning split: no more than 20 CU-hours/month for ingestion/maintenance, 60 for serving,
20 in reserve; no more than 1 GB/month transfer for batch work, 3 for serving, 1 in reserve.
These are project budgets inside the shared allowance, not isolated provider allocations.
Repeatedly downloading history for pointless daily compactions would waste that transfer.
Use a cheap daily manifest check; run Spark only when data/retention/size requires it.

The pipeline can be prevented from consuming serving headroom. Free-tier availability cannot
be guaranteed against unlimited public traffic or provider outages: caching, bounded work,
visible usage and refusing new ingestion are the controls, not a promise that limits disappear.

## Roadmap and release gates

| Phase | Ingestion track | Maintenance / capacity track | Gate before proceeding |
| --- | --- | --- | --- |
| 0. Baseline | Representative full-target pilot and explicit collection limits | Verify real account quotas, current usage and object-delete reclamation | Measured job cost and capacity report; choose catalogue/coverage target |
| 1. Shared budgets | Add preflight plus in-flight row/byte/time caps | Add usage inventory, forecast, bounded metadata and cache policy | Oversized/unknown-capacity jobs stop with Gold unchanged |
| 2. Build both commands | Finite CLI, real failure exit codes, restricted writer, one-time migrations | Weekly selector, dry-run compaction/deletion plan, replay/pruning rules | Deterministic retention tests and failure-path tests pass |
| 3. Safe persistence | Private bucket, immutable new-run upload/verification before Gold | New compacted prefixes, active registry, advisory lock and verified GC | Interruptions and overlapping jobs leave a readable valid dataset |
| 4. Manual trial | workflow_dispatch, bounded retries/timeouts, no schedule | Run maintenance dry-run then real cleanup alongside trial collection | Same job retried safely; pressure/failure tests preserve the live website |
| 5. Enable equilibrium | Weekly bounded collection/admission | Daily cheap check; compact/prune on change or pressure; post-run remeasure | A safe measured maintenance path ships before unattended ingestion |
| 6. Tune | Adjust target and collection scope using peer coverage and observed cost | Tune budgets from several runs, including staged/reclaimed bytes | Approve increased scope only while worst-case peaks still fit |

Important tests: five-week boundaries, same-week latest selection, currency/operation separation,
deterministic ties, no duplicate active generations, pruned replay, publication rollback,
nonempty serving snapshot preserved on failed collection, quota gates, quarantine full, crashes
between upload/switch/delete, and concurrent reader/deletion attempts. Use mutation tests on
thresholds, admission/eviction and retention filters. Routine tests use deterministic fixtures,
not the live source.

The desired outcome is bounded catalogue size, bounded retained history and bounded per-run
cost, with admitted and retired work balanced at steady state. No deletion or implementation
was performed while preparing this roadmap.
