# Riskstore Performance & Stress Test Harness — Specification and Build Plan

## Context

The Riskstore is a new on-prem ClickHouse 26.1 deployment (1 shard × 2 replicas, 3 Keeper nodes, RHEL 8, 32C/64vCPU, 500 GB RAM, 6 TB disk per replica) holding risk, valuations and PnL explains for four value streams (Rates, Credit, FX, Market Treasury). Data flows L0 (raw Kafka/JSON) → L1 (flattened columnar) → L2 (report-shaped), driven by `ingestion-service` and `risk-report-service`.

Before go-live we need defensible answers to: *what load can this cluster actually take, where does it break first, how does it behave when it breaks, and does it stay correct while breaking.* Today there is no repeatable way to answer any of that, and no baseline against which to detect regressions as schemas and query patterns evolve.

This plan delivers **`riskstore-perfbench`** — a standalone Java 21 + Muserver service, run on demand, that drives ClickHouse directly (bypassing the application services), measures it across every failure dimension that matters, and emits both an executive summary scorecard and a full detailed benchmark report.

**Decisions taken (confirmed with the user):**
- Stack: **Java 21 + Muserver** (no Spring), fat JAR.
- Test surface: **ClickHouse only** — the harness issues the SQL the services would issue; it does not go through Kafka or the services.
- Chaos/failover: **in scope, operator-triggered** — the harness defines, marks and measures faults; a human injects them. No root/SSH from the harness.
- Deliverable: **spec + working scaffolding** — buildable skeleton with one insert and one query scenario running end-to-end.

---

## Part 1 — Answering "what am I missing besides insert and query?"

Insert and query are two of roughly a dozen dimensions. The ones that actually take ClickHouse clusters down in production are mostly *not* those two. Ranked by risk to this specific topology:

| # | Dimension | Why it matters **here** | Primary signal |
|---|---|---|---|
| **D3** | **Merge & part lifecycle** | The #1 ClickHouse failure mode. 4 value streams × L0/L1/L2 × business-date partitions × small frequent batches = part explosion. Once merges fall behind inserts you hit `parts_to_delay_insert` (150) then `parts_to_throw_insert` (300) and ingestion stops. Insert benchmarks that run 10 minutes never surface this. | `asynchronous_metrics.MaxPartCountForPartition`, `system.merges` backlog, `ProfileEvents.DelayedInserts`/`RejectedInserts` |
| **D4** | **Replication & Keeper** | With only **3 Keeper nodes**, Keeper is the hard write ceiling — every insert block creates znodes. Keeper saturates long before CPU does. Also: **2 replicas means `insert_quorum=2` turns any single replica outage into a total write outage** — a design decision that must be tested, not assumed. | `system.zookeeper_info` (new in 26.1), `system.replicas.absolute_delay`/`queue_size`, `ZooKeeperWaitMicroseconds` |
| **D5** | **Mixed-workload concurrency & isolation** | Nothing runs alone. Rates ingest, Credit L2 report generation and ad-hoc queries collide. A p99 measured on an idle box is fiction. 26.1's `CREATE WORKLOAD` / `CREATE RESOURCE` is the control you'd reach for — test with and without it. | Per-stream latency delta under cross-load |
| **D6** | **Memory pressure & spill** | L1→L2 enrichment means joins, and joins are where ClickHouse OOMs. 500 GB looks generous until a grace-hash join on a full book fans out. | `MemoryTracking`, `peak_memory_usage`, MEMORY_LIMIT_EXCEEDED rate |
| **D7** | **Storage capacity, growth & IO** | **6 TB is the binding constraint.** 2 replicas of 1 shard = each node holds 100% of the data, and merges need free space ≥ the size of the parts being merged. Realistic usable ceiling is ~3–3.5 TB. You need measured bytes/row and compression ratio per table to know your true retention window. | Compression ratio, bytes/row, `DiskAvailable`, IO wait |
| **D8** | **Transform pipeline throughput** | L0→L1 and L1→L2 are read+write workloads with their own profile, plus any materialized-view cascade cost charged to the insert path. Freshness SLO (ingest → L2 readable) is a pipeline property, not an insert property. | End-to-end freshness latency |
| **D9** | **Resilience / chaos** | Replica loss, Keeper quorum loss (1 of 3 = degraded, 2 of 3 = **read-only**), disk full, restart with a large part count. Recovery time and catch-up rate are SLOs too. | Time-to-detect, time-to-recover, catch-up rate |
| **D10** | **Endurance / soak** | 24 h–7 d. Memory creep, unbounded part drift, system-log table growth, slow degradation. A 30-minute test cannot see any of it. | Trend slopes over the run |
| **D11** | **Correctness under stress** | A benchmark that silently loses or duplicates rows is a failed test that reports success. Reconcile rows in vs out, verify dedup on retry, and checksum both replicas for convergence. | Reconciliation checks |
| **D12** | **Breaking-point / capacity discovery** | "Does it pass at expected load?" is the wrong question. Ramp until SLO breach to find the knee and derive a headroom factor. | Rate at first SLO breach |
| **D13** | **Maintenance ops under load** | `ALTER ADD COLUMN`, mutations, lightweight deletes, `OPTIMIZE FINAL`, partition drops, TTL evictions — all while ingest runs. Mutations rewrite parts and compete with merges. | Mutation duration, impact on ingest |
| **D14** | **Backup / restore** | BACKUP's IO impact on the live workload, and RESTORE duration → your actual RTO. | Backup window degradation, restore wall-clock |
| **D15** | **Cold-start & cache effects** | Mark cache, uncompressed cache, OS page cache and query cache make the same query differ by 10–50×. If this isn't controlled, every number you publish is noise. | Cold vs warm, explicitly separated |
| **D16** | **Client & connection layer** | Connection pool exhaustion, `max_concurrent_queries` rejection behaviour, retry storms amplifying an incident. | Rejection rate, connection counts |
| **D17** | **Observability self-overhead** | `query_log`/`part_log`/`trace_log`/`metric_log` are real MergeTree tables with real write amplification and real disk cost at high QPS. | Bytes/day into `system.*_log` |

**The short version:** the three you most need and are most likely to skip are **D3 (parts/merges)**, **D4 (Keeper ceiling)** and **D11 (correctness under stress)**. D10 (soak) is the one that finds what all the others miss.

---

### Working baseline for the examples below

Every worked example in this section uses one consistent set of volume assumptions. **These are placeholders** — replace them with your measured figures and the arithmetic will re-run. They are stated explicitly so you can see which conclusions are sensitive to which input.

| Assumption | Value |
|---|---|
| Rates trades revalued per EOD snap | 2,000,000 |
| Measure/scenario combinations per trade | ~100 → **200M L0 pricing rows per business date** |
| Nested delta ladder per L0 row | 14 tenor points |
| L0 Atlas row, uncompressed JSON | ~1.5 KB |
| L0 Atlas row, compressed on disk | ~150 bytes |
| L1 sensitivity row, compressed on disk | ~25 bytes |
| EOD delivery window | ~2 hours, bursting 3–4× |
| Aggregate peak across four streams | **~120,000 rows/s** |
| `ingestion-service` batch size | 20,000 rows per INSERT |
| Retention (assumed) | 90 business dates |

Two derived numbers used repeatedly:

- 120,000 rows/s ÷ 20,000 rows per batch = **6 INSERTs/s**
- 120,000 rows/s × 150 bytes = **~18 MB/s** landing on disk before merge amplification

---

### D3 — Merge and part lifecycle

**Mechanism.** Every INSERT creates at least one new data part *per partition it touches*. Parts are immutable; a background pool merges them into progressively larger parts. MergeTree defends itself with three hard limits: at **`parts_to_delay_insert` = 150** active parts in a single partition it starts injecting artificial sleep into INSERTs; at **`parts_to_throw_insert` = 300** it rejects them outright with `TOO_MANY_PARTS`; and **`max_parts_in_total` = 100,000** across all partitions of a table is a backstop.

**Why it bites this cluster.** You have four value streams × three layers × a partition per business date. Nothing about that is unusual — what makes it dangerous is that ingestion does not degrade gracefully when merges fall behind. It runs fine, then it runs fine, then it stops.

**Worked example — the steady case.** At 6 INSERTs/s into a single business-date partition you create 6 parts/s. Merges must retire 6 parts/s to hold level. Merge write amplification is typically 3–6× (each row is rewritten once per level of the merge tree), so at 18 MB/s of ingest the background pool is moving roughly **90 MB/s of writes and ~180 MB/s of combined device IO** just to stand still. On NVMe that is nothing. On a shared SAN with other tenants it may well be your actual ceiling — which is why the disk *type* matters more than the disk *size* here.

**Worked example — the blowup.** Now a replay or a late-arriving feed puts five business dates in one batch. ClickHouse cannot merge across partitions, so that single INSERT creates **five** parts instead of one:

- 6 INSERTs/s × 5 partitions = **30 parts/s created**
- Each partition now merges independently, with fewer parts each, so merges are *less* efficient per unit of IO
- Net accumulation goes positive

**The number that actually matters is the net accumulation rate**, because it sets your time-to-failure — and therefore which test can possibly catch it:

| Net part accumulation | Time to hit 300 (throw) | Which test catches it |
|---|---|---|
| +1.0 part/s | 5 minutes | Any test |
| +0.05 part/s | ~100 minutes | `mixed-eod-peak`, not a 30-min run |
| +0.01 part/s | ~8.3 hours | **Only `soak-24h`** |

That bottom row is the whole argument for endurance testing. A cluster losing one part every hundred seconds passes every short benchmark you run and stops ingesting overnight.

**What failure looks like.** `DB::Exception: Too many parts (300). Merges are processing significantly slower than inserts` (code 252). Before that, silent slowdown as `DelayedInserts` climbs and INSERT latency creeps up for no visible reason.

**Measure.** `asynchronous_metrics.MaxPartCountForPartition` (the single most important number in the harness), `system.merges` backlog and `MergeMaxElapsedSec`, `ProfileEvents.DelayedInserts` / `RejectedInserts`, and `system.parts.files` (new in 26.1 — rising files-per-part means wide/sparse schemas driving merge cost).

**Read the slope, not the value.** A part count flat at 40 for twenty minutes then rising at 0.01/s is a failed run even though it never came near 150.

**Covered by.** `insert-baseline-rates`, `soak-24h`, `breaking-point`.

---

### D4 — Replication and Keeper

**Mechanism.** Every INSERT into a `ReplicatedMergeTree` is a distributed transaction against Keeper. Roughly: check-and-create a block-hash znode under `/blocks/` for deduplication, create a part znode under `/replicas/<replica>/parts/`, append an entry to `/log/`, and update replica pointers. The other replica watches `/log/`, fetches the part, and writes its own znodes. Call it **4–8 Keeper writes per INSERT block**, each requiring Raft quorum (2 of 3) plus fsync.

**Why it bites this cluster.** Three Keeper nodes tolerate exactly one failure. And because Keeper cost scales with the number of *blocks*, not the number of *rows*, your batch size is the dominant lever — which is a property of the ingestion service, not of ClickHouse.

**Worked example — batch size is the Keeper lever.**

| Batch size | INSERTs/s at 120k rows/s | Keeper writes/s (~8 per insert) | Verdict |
|---|---|---|---|
| 20,000 rows | 6 | ~50 | Trivial |
| 2,000 rows | 60 | ~500 | Comfortable |
| 200 rows | 600 | ~5,000 | **Keeper is now the ceiling** |

Same row throughput, 100× the Keeper load. If the ingestion service ever "optimises" for latency by shrinking batches, this is where it lands — and the symptom will look like a ClickHouse problem.

**Worked example — znode inventory.** Block hashes are retained per `replicated_deduplication_window` = **100 blocks per partition** and `replicated_deduplication_window_seconds` = **604,800 (7 days)**, whichever is tighter. With 4 streams × ~5 tables × 90 retained partitions = 1,800 partitions:

- block znodes: 1,800 × 100 ≈ **180,000**
- part znodes: 1,800 × ~50 active parts × 2 replicas ≈ **180,000**
- order of magnitude: **~400,000 znodes**, roughly 100–400 MB of Keeper heap

Keeper holds every znode in memory. That is fine on a properly sized host and not fine on the 4 GB VM that Keeper nodes often get provisioned as.

**The decision this dimension forces.** With **two** replicas there is no middle setting:

| `insert_quorum` | Behaviour | Failure mode |
|---|---|---|
| `0` / `1` | Ack from one replica, replicate async | Replica dies before replicating → that block is **lost** |
| `2` | Every insert waits for both | Replica down for OS patching → **all writes stop** |

There is no "quorum of 3 out of 5" compromise available to you. This is the single biggest availability decision in the architecture and it should be made from a measured run, not from a default. `chaos-replica-kill` is built to be run both ways.

**Latency reality check.** A Keeper write is network RTT + fsync on two nodes. Expect 1–5 ms with a dedicated NVMe log device. Expect 20–100 ms if the Keeper log shares a spindle with ClickHouse data — a classic and very expensive co-location mistake.

**Measure.** `system.zookeeper_info` (new in 26.1): `avg_latency`, `max_latency`, `outstanding_requests`, `znode_count`, `watch_count`, `approximate_data_size`, `synced_followers`, and the derived `KeeperCommitLag` (`leader_committed_log_idx − last_committed_idx`). Plus `system.replicas.absolute_delay` / `queue_size` and `ProfileEvents.ZooKeeperWaitMicroseconds`.

**Covered by.** `mixed-eod-peak`, `breaking-point`, `chaos-replica-kill`, chaos C3–C5.

---

### D5 — Mixed-workload concurrency and isolation

**Mechanism.** `max_threads` defaults to the core count, so a *single* large `GROUP BY` can claim all 64 vCPUs. Background merges draw from a separate pool (`background_pool_size`, default 16) but compete for the same CPU and the same disk.

**Why it bites this cluster.** Four value streams share one cluster with no natural boundary between them. Everything that makes a p99 look good in an isolated test — an idle box, a warm cache, an uncontended merge pool — is absent at 17:00 on the last business day of the quarter.

**Worked example — the cascade.** This is the shape of most real incidents, and note that no single dimension is "the" cause:

1. Credit's L2 report build starts: a three-way join, 32 threads, 40 GB resident.
2. Rates EOD ingest is at peak. Its **merges** are now starved of CPU by the Credit query.
3. Merge throughput drops below insert rate → part count climbs (**D3**).
4. Part count crosses 150 → `DelayedInserts` → INSERT latency rises.
5. `ingestion-service` back-pressures → Kafka consumer lag grows.
6. It gets escalated as a **Kafka** incident.

A D5 cause, presenting as a D3 symptom, reported as a messaging problem. This is exactly why the harness overlays server-side internals on the same time axis as client latency — the part-count chart is what makes step 3 visible.

**The control.** ClickHouse 26.1 workload scheduling:

```sql
CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD);
CREATE WORKLOAD all SETTINGS max_concurrent_threads = 64;
CREATE WORKLOAD ingestion IN all SETTINGS max_concurrent_threads = 32, priority = 0;
CREATE WORKLOAD reports   IN all SETTINGS max_concurrent_threads = 24, priority = 1;
CREATE WORKLOAD adhoc     IN all SETTINGS max_concurrent_threads = 8,  priority = 2;
```

Queries then carry `SETTINGS workload = 'reports'`. Note that declaring a CPU resource **disables** `concurrent_threads_soft_limit_num` — the workload setting takes over.

**How to measure isolation.** Run `insert-baseline-rates` alone and record p99. Run `mixed-eod-peak` and record the same workload's p99. The delta is your isolation cost. Then run it again with workloads declared. The difference between those two deltas is what workload scheduling is worth to you — and it is the only honest way to decide whether to adopt it.

**Covered by.** `mixed-eod-peak`, run with and without workload scheduling.

---

### D6 — Memory pressure and spill

**Mechanism.** `max_server_memory_usage_to_ram_ratio` defaults to 0.9, so ~450 GB of your 500 GB is available to ClickHouse. A hash join builds its hash table from the **right-hand** table entirely in memory. A `GROUP BY` holds one aggregate state per group key.

**Why it bites this cluster.** L1→L2 enrichment is join-shaped, and joins are ClickHouse's weakest area. 500 GB sounds like plenty right up until a group-by fans out.

**Worked example — the join.** `q_trade_join_valuation` builds on `l1_rates_trade`:

- One business date: 2M rows × ~150 bytes = **300 MB**. Comfortable.
- Ninety business dates: 180M rows × 150 bytes = **27 GB** per join.
- Five of those concurrently at quarter-end: **135 GB**. Now it is a real constraint.

**Worked example — the group-by that actually hurts.** `q_book_aggregate_wide` groups by `counterparty_id` × `book_id` × `currency` = 5,000 × 240 × 12 = up to **14.4M group keys**. A plain `sum()` state is 8 bytes, so that is only ~115 MB. But add `quantile()`, whose reservoir state runs to ~1 KB per group, and the same query needs **~14 GB on its own**.

That asymmetry is the finding: the aggregate *function* matters more than the row count. A report that adds a percentile column can multiply its memory by two orders of magnitude without changing a single `WHERE` clause.

**The two failure modes, and why the difference matters enormously.**

| Mode | What happens | Blast radius |
|---|---|---|
| `MEMORY_LIMIT_EXCEEDED` (code 241) | The query dies | One query. Graceful. |
| Linux OOM-killer | `clickhouse-server` is killed | Replica restarts → cold caches (**D15**), part re-attach (**D3**), replication catch-up (**D4**) |

The second turns a query problem into a cluster incident. **Setting `max_memory_usage` per workload is what keeps you in row one** — an unbounded per-query limit means the server-wide limit is the only thing between an ad-hoc query and an OOM kill.

**Spill controls to exercise.** `max_bytes_before_external_group_by` (set to roughly half of `max_memory_usage`), `max_bytes_before_external_sort`, and `join_algorithm` — compare `hash` against `grace_hash` on identical data. `grace_hash` spills to disk instead of dying; the run tells you what that safety costs in latency.

**Measure.** `peak_memory_usage` per `query_id` from `system.query_log`, `MemoryTracking`, `jemalloc.resident`, and the `MEMORY_LIMIT_EXCEEDED` count from `system.errors`.

**Covered by.** `mixed-eod-peak` (the `l2-report-build` workload pins `join_algorithm`), `breaking-point`.

---

### D7 — Storage capacity, growth and IO

**Mechanism.** One shard × two replicas means **each node holds 100% of the data** — replicas are copies, not slices. Separately, `max_bytes_to_merge_at_max_space_in_pool` defaults to **150 GB**, and ClickHouse refuses a merge when free space cannot accommodate it. Run the disk close to full and merges stop *before* writes do.

**Why it bites this cluster.** 6 TB is the binding constraint on the entire architecture, and the usable figure is a lot less than 6 TB.

**Worked example — retention, and this is the one to check first.** Using the baseline assumptions:

| Table | Rows per business date | Bytes/row | Per date |
|---|---|--:|--:|
| `l0_rates_atlas` | 200M | 150 | 30 GB |
| `l1_rates_valuation` | 200M | 60 | 12 GB |
| `l1_rates_sensitivity` | 200M × 14 = **2.8B** | 25 | **70 GB** |
| Rates subtotal | | | **~112 GB** |
| All four streams (Rates ≈ 40%) | | | **~280 GB/date** |

Against ~3.5 TB usable (6 TB less merge headroom and the free-space floor):

> **3.5 TB ÷ 280 GB = roughly 12 business dates of retention.**

Two and a half weeks. If your retention requirement is 90 days — or seven years for a regulatory archive — this number says the architecture needs a change *before* go-live, not after: more shards, tiered storage to object store, or aggregating at L2 and expiring L1 sensitivity aggressively.

Note where the mass is: **`l1_rates_sensitivity` is 62% of the footprint**, purely because the delta ladder turns one row into fourteen. That single fan-out decision dominates your storage bill.

I want to be clear that the 12-day figure rests entirely on the placeholder volumes above. It could be 40 days or it could be 4. **That is precisely why the harness measures `BytesPerRow` and `CompressionRatio` per table rather than asking you to estimate them** — swap in the measured numbers and this table becomes a real capacity plan.

**Also measure IO contention.** Merges and queries share one device. Merge throughput on NVMe runs 1–2 GB/s; on SAS SSD nearer 500 MB/s; on shared SAN, whatever the neighbours leave you. The D3 arithmetic (~180 MB/s of merge IO at peak) only works if the device can deliver it *while also* serving report queries.

**Measure.** `BytesPerRow` and `CompressionRatio` per table from `system.parts`, `DiskAvailableBytes`, `DiskFreeFraction`, and OS-level IO wait.

**Covered by.** The `capacity-growth` scenario (T10), plus `soak-24h` for the growth slope.

---

### D8 — Transform pipeline throughput

**Mechanism.** L0→L1 and L1→L2 are simultaneously read and write workloads against the same disk, competing with ingestion and with merges. The `l0_rates_atlas` → `l1_rates_sensitivity` transform does `ARRAY JOIN` over a JSON subcolumn, which is both a 14× row fan-out and a CPU-heavy parse.

**Why it bites this cluster.** The transform is very likely the **largest single workload in the system** — larger than ingestion itself. 200M rows in, 2.8B rows out, per business date, per stream. It is easy to size the cluster for ingestion and then be surprised by the layer that consumes it.

**Worked example — the freshness budget.** "Kafka publish → L2 readable" is a pipeline property, not an insert property. A plausible decomposition:

| Stage | Budget |
|---|---|
| Kafka → `ingestion-service` | ~100 ms |
| L0 INSERT ack | ~500 ms |
| L0→L1 transform trigger + run | 30–120 s |
| L1→L2 report build (joins) | 60–300 s |
| **End to end** | **~2–8 minutes** |

If the business expects "risk is visible within a minute of pricing", the transform cadence — not ClickHouse throughput — is what makes that false. That conversation is much cheaper before go-live.

**The design decision this dimension forces: materialized view vs scheduled `INSERT … SELECT`.**

| | Materialized view | Scheduled `INSERT … SELECT` |
|---|---|---|
| When it runs | Synchronously, on the insert path | Decoupled, on a cadence |
| Cost charged to | **The INSERT** — insert p99 includes MV cost | Its own workload |
| Freshness | Immediate | One cadence interval of lag |
| Complexity | Low | Needs watermarking / late-data handling |
| Failure coupling | MV error fails the INSERT | Transform can fail independently |

**Relevant to 26.1 specifically:** async-insert deduplication now extends end-to-end to dependent materialized views. Previously a retried async insert could be deduplicated at the source table while the MV still wrote duplicates downstream — which made `async_insert` genuinely unsafe in MV pipelines. That limitation is gone, which meaningfully reopens the MV option if you had ruled it out on an earlier version.

**Measure.** End-to-end freshness latency, transform duration, rows-in vs rows-out fan-out ratio, and insert p99 with and without MVs attached.

**Covered by.** The `l1-transform-*` and `l2-report-build` workloads in `mixed-eod-peak`; the `pipeline-freshness` scenario (T5).

---

### D9 — Resilience and chaos

**Mechanism.** Four distinct failure surfaces: a ClickHouse replica, a Keeper node, the network between them, and the disk. Each has its own detection time, its own degraded behaviour, and its own recovery cost.

**Why it bites this cluster.** The topology has two single-failure cliffs. Two replicas means losing one halves your query capacity *and* forces the `insert_quorum` decision from D4. Three Keeper nodes means losing two makes every replicated table read-only — a full write outage, not a degradation.

**Worked example — catch-up after a one-hour outage.** A replica is down for an hour of OS patching during a 24-hour ingest:

- Missed data: 18 MB/s × 3,600 s = **~65 GB** to fetch
- Over 10 GbE at a realistic 200–400 MB/s: **3–6 minutes** of catch-up
- But if `max_replicated_fetches_network_bandwidth` is capped at, say, 50 MB/s to protect the live workload: **~22 minutes**

And catch-up competes with the ongoing ingest it is trying to catch up *to*. If fetch throughput is below the ingest rate the replica never converges — it just falls further behind, which is why a flat-and-high `ReplicationQueueSize` means "stuck", not "busy".

**Worked example — restart with a high part count.** Startup attaches every active part. At roughly 1–5 ms per part, a table carrying 50,000 parts takes **50–250 seconds just to attach** before it serves a single query. Teams consistently quote a restart time measured on a quiet cluster and then discover the real one during an incident. Chaos C10 exists to put a number on it.

**Detection times to verify.** Keeper `session_timeout_ms` defaults to 30,000 — so a replica can take up to 30 s to notice it has lost its session, during which its behaviour is worth watching closely.

**Measure.** Time-to-detect (`ReplicaIsReadOnly` / `ReplicaSessionExpired` transitions — the harness raises these as automatic marks), time-to-recover, catch-up rate, and **zero data loss** as a non-negotiable.

**Covered by.** `chaos-replica-kill` plus chaos C1–C10 in the playbook.

---

### D10 — Endurance and soak

**Mechanism.** Nothing new — this is D3, D6, D7 and D17 observed over a long enough window for a slow slope to become a wall.

**Why it bites this cluster.** Every fast-moving failure has already been caught by the time you go live. What is left is the slow ones, and by construction they are invisible to short tests.

**Four things only a soak can see.**

1. **Part-count drift.** Covered in D3: +0.01 parts/s is 8.3 hours to failure. A 30-minute run sees a flat line.
2. **Memory creep.** `jemalloc.resident` trending up while workload is constant. Mark cache alone grows to `mark_cache_size` (default 5 GB) and stays there.
3. **System log growth.** See D17 — measured in GB/day, invisible in minutes.
4. **Keeper znode accumulation.** `replicated_deduplication_window_seconds` is **604,800 — seven days**. Block znodes accumulate for a full week before the retention window starts evicting them. **A 24-hour soak cannot see the steady state of Keeper memory.** If Keeper sizing matters to you — and with three nodes it does — you need a 7-day run.

That last point is worth planning around explicitly: `soak-24h` is the default, but a `soak-7d` before go-live is the only thing that establishes the true Keeper working set.

**Read the report differently.** For a soak, the percentiles are almost beside the point. **A flat p99 with a rising part count is a failed soak.** Look at slopes: part count, resident memory, disk used, znode count, replication lag. Anything that trends instead of oscillating is a finding.

**Covered by.** `soak-24h`, and a `soak-7d` variant for Keeper steady state.

---

### D11 — Correctness under stress

**Mechanism.** `ReplicatedMergeTree` deduplicates inserts by **block checksum** by default, retaining hashes per `replicated_deduplication_window` = 100 blocks per partition / 7 days. If an inserted block hashes identically to a recently seen one, it is silently discarded — no error, no warning, no row.

**Why it bites this cluster.** That default is exactly right for retry safety and exactly wrong for a benchmark, and the failure is silent in both directions.

**Worked example — the false positive.** `l0_rates_market_data` is deliberately low-cardinality: a few hundred curves, 23 tenors, 4 quote types. Two genuinely different snaps of an unchanged curve can serialise to **byte-identical blocks**. ClickHouse drops the second one as a retry. You lose real data and nothing anywhere reports an error. Reconciliation is the only thing that catches it.

**Worked example — the false negative.** A benchmark harness that reuses a pool of pre-generated batches inserts the same bytes repeatedly. ClickHouse deduplicates nearly all of them. The harness reports magnificent throughput because it is measuring how fast ClickHouse can *reject* data. This is a real and common way benchmark numbers end up meaningless — and it is why `BatchFactory` generates every batch fresh and stamps a unique `insert_deduplication_token` on each one.

**The three checks, in increasing order of severity.**

| Check | Question | What a gap means |
|---|---|---|
| Acknowledgement | Rows offered = rows ClickHouse said it wrote? | Inserts failed, or were deduplicated away |
| Persistence | Does a `count()` see those rows? | A part never committed |
| **Replica convergence** | Do both replicas hold identical counts? | **One replica quietly stopped replicating** — and your "fast" run was fast because it was only doing half the work |

Convergence is checked after a settling delay, since replication is asynchronous and an immediate comparison would report a false mismatch on a healthy cluster.

**The application-side implication.** `insert_deduplication_token` lets `ingestion-service` control block identity explicitly rather than relying on byte-identity. For a risk store where genuinely identical payloads are plausible, that is the correct design — and it needs deciding in the service, not the database.

**Measure.** `recon.rowsLost`, distinct-key counts vs inserted counts, and per-table `sum(rows)` from `system.parts` compared across replicas.

**Covered by.** The `reconciliation` block in every scenario; `chaos-replica-kill` for the fault case.

---

### D12 — Breaking-point and capacity discovery

**Mechanism.** Ramp load continuously until an SLO breaks. Record the rate at first breach and which SLO it was.

**Why it bites this cluster.** "Does it pass at expected peak?" is a yes/no that carries no information about margin. A cluster that passes at 100% of expected peak and breaks at 105% is in a completely different position from one that breaks at 400%, and both report "PASS".

**Worked example.** `breaking-point` ramps to 1M rows/s. Suppose the point-lookup canary (`q_valuation_point_lookup` p99 > 1 s) breaches first at 340k rows/s:

> headroom = 340,000 ÷ 120,000 = **2.8×**

**Why 3× is the floor for this specific topology**, not a round number:

| Claim on headroom | Factor |
|---|---|
| Losing one of two replicas halves query capacity | 2.0× |
| EOD burst above steady-state average | ~1.3× |
| Backfill / replay running alongside live ingest | ~1.2× |
| Volume growth before the next hardware cycle | ~1.2× |

Those compound past 3×. At 2.8× measured, you are already below the floor — and the useful part is that you know it now, with the specific first-breaking component named, rather than discovering it at quarter-end.

**The canary matters more than the headline.** Note that the first thing to break was a *query*, not an insert. A point lookup that should take milliseconds taking seconds is the cleanest possible saturation signal, because it has no legitimate reason to be slow. Insert throughput often keeps climbing for a while after the cluster has effectively stopped serving anyone.

**Expect this scenario to fail.** A `breaking-point` run that passes has not found the knee — it just means the ramp ceiling was too low.

**Covered by.** `breaking-point`.

---

### D13 — Maintenance operations under load

**Mechanism.** These operations differ enormously in cost, and the differences are not obvious from the SQL:

| Operation | Cost | Notes |
|---|---|---|
| `ALTER TABLE … ADD COLUMN` | Metadata only | Brief lock; effectively free |
| `ALTER TABLE … UPDATE/DELETE` (mutation) | **Rewrites every affected part** | Full read+write of matched data |
| Lightweight `DELETE` | Writes a `_row_exists` mask | Cheaper, but still a mutation |
| `OPTIMIZE … FINAL` | Rewrites the entire table | Never run this on a large table in production |
| `DROP PARTITION` | Effectively instant | **The right tool for retention** |
| TTL expiry | TTL merges | Consumes the same background pool as normal merges |

**Why it bites this cluster.** Mutations draw from the same `background_pool_size` as merges. A mutation and a merge backlog are the same resource contest, so a routine data fix is a part-count risk.

**Worked example — the 17:30 data fix.** Someone corrects a valuation model tag during EOD peak:

```sql
ALTER TABLE riskstore.l1_rates_valuation UPDATE valuation_model = 'LMM-1'
WHERE business_date = today();
```

On one business date of `l1_rates_valuation` (200M rows, 12 GB) that mutation rewrites **12 GB of parts**. Those rewrites compete with the merges that are currently just barely keeping up with 120k rows/s of ingest. Merges fall behind → part count climbs → D3. The mutation was not wrong; it was just issued at the one time of day when there was no slack.

This is the most common self-inflicted production incident in a ClickHouse risk store, and the mitigation is operational (a maintenance window, or a mutation-throttling policy) rather than technical.

**Note the retention implication.** Since `DROP PARTITION` is nearly free and `DELETE` is expensive, partitioning by business date is not just a query optimisation — it is what makes retention cheap. That is worth protecting when the schema evolves.

**Measure.** `system.mutations` `parts_to_do` and `is_done`, `MutationsPending`, and insert p99 during the mutation. A `MutationsPending` count that does not trend to zero means the mutation is stuck, not slow.

**Covered by.** The `maintenance-under-load` scenario (T9); chaos C9.

---

### D14 — Backup and restore

**Mechanism.** `BACKUP TABLE … TO Disk(…)` reads every active part. `RESTORE` writes them back. Both are large sequential IO workloads against the same device serving live traffic.

**Why it bites this cluster.** Your RTO is not a policy statement — it is a measured wall-clock number, and it is probably larger than anyone has assumed.

**Worked example.** At ~3.5 TB of data and a sustained 200 MB/s to the backup target:

> 3,500 GB ÷ 0.2 GB/s ≈ **4.9 hours**

That is your backup window, and during it the live workload is sharing the device. `RESTORE` is the same order of magnitude — so **your realistic RTO for a full rebuild is on the order of 5 hours**, before any validation. If the business has been told "we can recover in an hour", this is the number that corrects it.

**What to test.** Not just the duration, but the *degradation*: run `mixed-eod-peak` and start a backup mid-run. The interesting question is whether inserts still meet SLO while the backup streams. Incremental backups via `base_backup` reduce the window substantially and are worth measuring separately.

With two replicas both holding the full dataset, you can take the backup from either — but they share nothing, so backing up from one does not spare the other's disk any IO if queries are routed to both.

**Measure.** Backup wall-clock, restore wall-clock (your RTO), and insert/query p99 delta during the backup window.

**Covered by.** Not in the shipped scenario suite — flagged as an addition once the backup target and schedule are chosen.

---

### D15 — Cold-start and cache effects

**Mechanism.** Four caches stack up: the mark cache (`mark_cache_size`, default 5 GB), the uncompressed cache (off by default), the query cache (off by default), and — by far the largest — the **OS page cache**, which on this box is effectively the ~450 GB of RAM ClickHouse has not claimed.

**Why it bites this cluster.** With ~500 GB of RAM and ~3.5 TB of data, roughly 15% of the dataset is resident. But that 15% is not random: it is the recently-touched data, which means **the current business date very likely fits entirely in page cache**. A query against today's data can be 10–50× faster than the identical query against last month's.

**Worked example — the SLO that actually matters.** `q_pnl_explain_by_book` against a warm current business date might return in 400 ms. The same query as the *first* report of the morning, after an overnight restart, reads from disk and takes 12 seconds. Both are true. Only one of them is what your users experience at 07:00, and it is not the one that makes the benchmark look good.

**This is the dimension that silently invalidates everything else.** If cache state is not pinned, a "regression" between two runs is just as likely to be a warm run compared against a cold one. Any percentage comparison between differently-cached runs is noise dressed up as a finding.

**The discipline.** `SYSTEM DROP MARK CACHE / UNCOMPRESSED CACHE / QUERY CACHE / COMPILED EXPRESSION CACHE` clears what SQL can reach. It does **not** clear the OS page cache — that needs an operator running `sync && echo 3 > /proc/sys/vm/drop_caches` on every replica. The harness applies the SQL half, prints the manual step into the report, and records which policy was in force, so a cold run is auditable rather than assumed.

**Quote both numbers.** Cold and warm are both legitimate measurements. Quoting only one is how benchmark figures stop matching production.

**Measure.** Cold and warm variants of the same scenario, run and reported separately; `MarkCacheBytes`; `OSMemoryAvailable`.

**Covered by.** `query-baseline-cold`, with a warm variant to run alongside it.

---

### D16 — Client and connection layer

**Mechanism.** The boundary between `ingestion-service` and ClickHouse has its own limits, and failures there are invisible in ClickHouse's own metrics.

**Why it bites this cluster.** When ClickHouse rejects work, what the *client* does next determines whether you have an incident or an outage.

**Worked example — the retry storm.** Part count crosses 300 and ClickHouse starts throwing `TOO_MANY_PARTS`:

1. `ingestion-service` receives the error and retries immediately.
2. The retry is also rejected — the part count has not changed in 50 ms.
3. Retries now arrive faster than original traffic, adding connection and parsing load to a server already unable to merge fast enough.
4. The retry traffic makes it *harder* for merges to catch up, because they are competing for the same CPU.

A recoverable back-pressure signal becomes a self-sustaining outage. The fix is client-side — exponential backoff with jitter, and a circuit breaker on `TOO_MANY_PARTS` specifically — but you only find out it is missing by generating the condition.

**Worked example — invisible queueing.** If the service's connection pool is smaller than its concurrency, requests queue *inside the application*. ClickHouse sees healthy latency and low concurrency; the business sees slow risk. Nothing in `system.query_log` reveals it. This is a strong argument for eventually running the end-to-end variant of the harness as well as the DB-direct one — the DB-direct mode this harness implements is deliberately blind to it.

**Limits to exercise.** `max_concurrent_queries` (rejection arrives as code 202, `TOO_MANY_SIMULTANEOUS_QUERIES`), connection pool sizing, HTTP (8123) vs native (9000) protocol overhead, `keep_alive_timeout`, and TLS handshake cost if TLS is enabled.

**Measure.** Rejection rate by error code, `TCPConnection` / `HTTPConnection` counts, and client-side queue depth — which only the application can report.

**Covered by.** `breaking-point` reaches the rejection regime; the retry behaviour itself belongs to `ingestion-service` and should be tested there.

---

### D17 — Observability self-overhead

**Mechanism.** `system.query_log`, `part_log`, `metric_log`, `asynchronous_metric_log`, `trace_log` and `text_log` are not lightweight instrumentation — they are ordinary MergeTree tables with ordinary inserts, parts, merges and disk consumption. They compete with your data for the same resources.

**Why it bites this cluster.** Default TTLs on system log tables are inconsistent across versions and deployments — **modern builds commonly default to 30 days, but historically there was no TTL at all**. Verify it on your actual 26.1 build rather than assuming, because unbounded is a genuine possibility and the growth is invisible until it is not.

**Worked example — the daily budget.**

| Table | Volume | Size/day |
|---|---|--:|
| `query_log` | 2 rows/query; `ProfileEvents` map makes rows 2–5 KB. At 5 qps sustained = 432k queries/day | **~2.6 GB** |
| `part_log` | ~12 rows/s (6 parts created + ~6 merged) ≈ 1M rows/day | **~1 GB** |
| `metric_log` | 1 row/s, ~3,000 columns wide | **~0.3 GB** |
| `asynchronous_metric_log` | 1 row/s | **~0.1 GB** |
| **Subtotal, profiler off** | | **~4 GB/day** |
| `trace_log` **with query profiler enabled** | Samples per thread per millisecond | **10× everything else** |

At 4 GB/day with a 30-day TTL that is **120 GB — about 3.4% of your usable disk**. Acceptable, but it must be budgeted rather than discovered.

With `trace_log` left on in production, the same arithmetic gives **~40 GB/day → 1.2 TB**, which would consume roughly **a third of your usable capacity** to store telemetry about a database that is running out of room to store risk. The failure presents as D7.

**Recommendations to validate on your build.**

- Confirm the TTL actually set on each `system.*_log` table; set one explicitly if absent.
- Keep `trace_log` **off** by default; enable it deliberately when investigating.
- Consider widening `metric_log` `collect_interval_milliseconds` from its default.
- Include `system.*` tables in the D7 capacity projection, not as an afterthought.

**Note the harness's own contribution.** The system-table scraper polls every 2 seconds across both replicas. That is small against a saturated cluster but it is not zero, and the report states it rather than pretending otherwise.

**Measure.** `SystemLogBytes` per table (the `system_log_footprint` probe), and the growth slope across a soak run.

**Covered by.** `soak-24h`; the `system_log_footprint` probe in the metric catalogue.

---

## Part 2 — Harness design

### 2.1 Non-negotiable measurement principles

These are the difference between a benchmark and a number generator. Each is a hard requirement on the implementation:

1. **Open-loop load generation.** Arrivals are scheduled against a virtual clock at a fixed rate, independent of whether prior requests have completed. A closed-loop driver (N threads looping "send, wait, send") suffers *coordinated omission*: when the server stalls, the driver stops sending, so the stall never appears in the latency histogram. Latency is measured from **intended send time**, not actual send time.
2. **Harness runs off-box.** On a separate load-generator host. Running it on a replica means measuring your own CPU contention.
3. **Results DB is not the SUT.** Writing results into Riskstore would perturb the thing being measured. Results go to a separate ClickHouse instance/database (fallback: local JSONL/Parquet).
4. **Percentiles are merged, never averaged.** Latencies are stored as serialized **HdrHistogram** per (workload, 10 s window). HdrHistograms merge losslessly, so a true global p99.9 is recoverable. Averaging per-window percentiles is arithmetically meaningless.
5. **Steady-state windowing.** Ramp-up and cooldown are recorded but excluded from headline metrics.
6. **Deterministic seeded data.** Same seed → same data → runs are comparable.
7. **`query_id` correlation.** Every operation is stamped `{runId}:{workloadId}:{seq}` so client-side latency joins exactly to `system.query_log` server-side cost. This is what lets you say *"the p99 was 4 s and 3.6 s of it was server-side merge contention."*
8. **`SYSTEM FLUSH LOGS` before harvest.** System log tables flush asynchronously (~7 s); harvesting without a flush loses the tail of the run.
9. **Config fingerprinting.** Every run records SUT version, settings hash and schema hash. A run against different settings is not a comparable run.

### 2.2 Component architecture

Single fat JAR, one process:

```
                 ┌─────────────────────────────────────────────┐
  operator ──────▶  Muserver REST + self-hosted report UI      │
  (curl / CI)    │  /api/v1/runs, /scenarios, /marks, /report  │
                 ├─────────────────────────────────────────────┤
                 │  RunCoordinator ── PhaseScheduler            │
                 │       │                                     │
                 │       ├─ ArrivalScheduler (open-loop)        │
                 │       ├─ InsertDriver ──┐                    │
                 │       ├─ QueryDriver ───┤                    │
                 │       └─ DataGenerator ─┘                    │
                 ├─────────────────────────────────────────────┤
                 │  Collectors:  LatencyRecorder (HdrHistogram) │
                 │               SystemTableScraper (1–5 s)     │
                 │               QueryLog / PartLog harvester   │
                 │               OsMetricsScraper               │
                 ├─────────────────────────────────────────────┤
                 │  Verifier: Reconciler, SloEvaluator,         │
                 │            BaselineComparator                │
                 ├─────────────────────────────────────────────┤
                 │  ResultsWriter ──▶ separate ClickHouse       │
                 │  Reporter ──▶ JSON + self-contained HTML + MD│
                 └──────────────┬──────────────────────────────┘
                                │ native + HTTP
                    ┌───────────▼───────────┐
                    │  Riskstore SUT        │
                    │  2 replicas, 3 keeper │
                    └───────────────────────┘
```

### 2.3 REST API contract

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/health` | Liveness |
| `GET` | `/api/v1/preflight` | Cluster health, disk free, no competing load, config fingerprint vs baseline. **Blocks run start on failure.** |
| `GET` | `/api/v1/scenarios` | List loaded scenario definitions |
| `POST` | `/api/v1/runs` | Start a run: `{scenarioId, overrides{}, tags{}, baselineRunId?}` → `202 {runId}` |
| `GET` | `/api/v1/runs` | List runs, filterable by tag/status |
| `GET` | `/api/v1/runs/{id}` | Live status: phase, elapsed, current rate, rolling p99, error count |
| `POST` | `/api/v1/runs/{id}/marks` | **Chaos marker** — `{kind, target, note}` timestamps an operator-injected fault onto the run timeline |
| `POST` | `/api/v1/runs/{id}/abort` | Graceful stop, still produces a report |
| `GET` | `/api/v1/runs/{id}/report?format=json\|html\|md` | Final report |
| `GET` | `/api/v1/runs/{id}/compare/{baselineId}` | Regression delta |

Safety guard: the harness refuses to start unless the target cluster name appears in a configured allowlist **and** `allowDestructive: true` is set — prevents ever pointing this at production by accident.

### 2.4 Scenario definition (YAML)

```yaml
id: rates-peak-eod
description: Rates EOD burst with concurrent L2 report generation
seed: 42
phases:
  - { id: warmup, type: hold, rate: 20000, for: PT5M, excludeFromMetrics: true }
  - { id: ramp,   type: ramp, from: 20000, to: 120000, over: PT5M }
  - { id: steady, type: hold, rate: 120000, for: PT30M }
  - { id: spike,  type: hold, rate: 350000, for: PT5M }

workloads:
  - id: l0-rates-atlas
    kind: insert
    target: { table: l0_rates_atlas, format: JSONEachRow }
    share: 0.6                       # of the phase rate
    batch: { rows: 50000, maxDelay: PT1S }
    generator: { ref: rates-atlas-payload }
    settings:
      async_insert: 1
      wait_for_async_insert: 1
      insert_deduplication_token: "${runId}-${workloadId}-${seq}"

  - id: l1-transform
    kind: insertSelect
    sql: { ref: t_l0_to_l1_rates_valuation }
    triggerEvery: PT1M

  - id: l2-report
    kind: query
    arrivalRate: { unit: qps, value: 5 }
    queries:
      - { ref: q_pnl_explain_by_book, weight: 0.6 }
      - { ref: q_sensitivity_ladder,  weight: 0.4 }
    cachePolicy: cold                # cold | warm | mixed

slo:
  - { metric: "insert.l0-rates-atlas.p99",     op: lte, value: PT2S }
  - { metric: "query.q_pnl_explain_by_book.p95", op: lte, value: PT3S }
  - { metric: "sut.MaxPartCountForPartition.max", op: lte, value: 300 }
  - { metric: "sut.replication.absolute_delay.p99", op: lte, value: 30 }
  - { metric: "errors.rate",                    op: lte, value: 0.001 }
  - { metric: "recon.rowsLost",                 op: eq,  value: 0 }
```

### 2.5 Metric catalogue — exact SUT queries

Scraped per replica on a 1–5 s cadence and stored as timeseries. These go in `src/main/resources/sql/sut-metrics/`:

| Area | Query / source |
|---|---|
| **Parts (D3)** | `SELECT database, table, count() active_parts, sum(rows), sum(bytes_on_disk), sum(files) FROM system.parts WHERE active GROUP BY 1,2` — note `files` is new in 26.1 |
| **Part-count ceiling** | `SELECT value FROM system.asynchronous_metrics WHERE metric='MaxPartCountForPartition'` ← **the single most important early-warning signal** |
| **Merges (D3)** | `SELECT count(), sum(total_size_bytes_compressed), max(elapsed), max(progress), sum(memory_usage) FROM system.merges` |
| **Insert backpressure** | `ProfileEvents`: `DelayedInserts`, `RejectedInserts`, `DelayedInsertsMilliseconds` |
| **Mutations (D13)** | `SELECT count() FROM system.mutations WHERE NOT is_done` |
| **Replication (D4)** | `SELECT database, table, absolute_delay, queue_size, inserts_in_queue, merges_in_queue, is_readonly, is_session_expired, active_replicas, total_replicas FROM system.replicas` |
| **Replication queue** | `SELECT type, count(), max(num_tries), any(last_exception) FROM system.replication_queue GROUP BY type` |
| **Keeper (D4)** | `SELECT * FROM system.zookeeper_info` — **new in 26.1**: cluster size, latency, leadership, data volume. Plus `system.zookeeper_connection` and `ProfileEvents.ZooKeeperTransactions` / `ZooKeeperWaitMicroseconds` |
| **Memory (D6)** | `system.metrics`: `MemoryTracking`, `Query`, `Merge`, `BackgroundMergesAndMutationsPoolTask`; `system.asynchronous_metrics`: `jemalloc.resident`, `OSMemoryAvailable` |
| **Storage (D7)** | `system.asynchronous_metrics`: `DiskAvailable_default`, `DiskUsed_default`; compression ratio from `sum(data_uncompressed_bytes)/sum(data_compressed_bytes)` on `system.parts` |
| **Errors (D16)** | `SELECT name, value, last_error_message FROM system.errors WHERE value > 0` |
| **Per-op server cost** | `SELECT query_id, type, query_duration_ms, read_rows, read_bytes, written_rows, memory_usage, peak_memory_usage, ProfileEvents, exception_code FROM system.query_log WHERE query_id LIKE '{runId}%'` |
| **Part events** | `SELECT event_type, event_time, table, part_name, rows, size_in_bytes, duration_ms, merge_reason, peak_memory_usage, error FROM system.part_log WHERE ...` |
| **Index efficacy** | `mergeTreeAnalyzeIndexes()` — **new in 26.1**, shows exact row ranges scanned after primary + skip indexes. Used in the query-tuning section of the detailed report. |
| **OS** | node_exporter scrape if available, else a lightweight `/proc` reader agent |

### 2.6 Results schema (separate ClickHouse DB `perf_results`)

```
runs               (run_id, scenario_id, scenario_hash, started_at, ended_at, status,
                    sut_version, sut_settings_hash, schema_hash, harness_version, tags Map)
run_phases         (run_id, phase_id, started_at, ended_at, target_rate, excluded)
op_latency_hist    (run_id, workload_id, window_start, encoded_hdr String, count, min, max, errors)
op_throughput      (run_id, workload_id, ts, ops, rows, bytes, errors)
sut_metrics        (run_id, ts, node, metric, value)
sut_query_log      (run_id, query_id, ...harvested columns...)
sut_part_events    (run_id, ...harvested columns...)
os_metrics         (run_id, ts, node, metric, value)
chaos_events       (run_id, ts, kind, target, note, source)   -- operator marks + auto-detected
slo_results        (run_id, slo_id, metric, op, threshold, actual, passed)
reconciliation     (run_id, check_name, expected, actual, passed, detail)
```

### 2.7 Reporting

Three outputs from one run:

- **Summary (Markdown + top of HTML)** — a scorecard: PASS/FAIL per SLO, headline throughput and latency, the derived headroom factor, the first thing that broke, and a delta table vs the baseline run with regressions flagged. One screen. This is what goes in the go-live pack.
- **Detailed HTML** — per-workload latency tables (p50/p90/p95/p99/p99.9/max, from merged HdrHistograms), throughput and latency timeseries, ClickHouse internals overlaid on the same time axis (part count, merge backlog, replication lag, Keeper latency, memory), resource utilisation, error breakdown, top-N slowest queries with server-side cost from `query_log`, and **chaos event markers overlaid on every chart**.
- **JSON** — machine-readable, for a CI regression gate.

> **On-prem constraint:** the HTML report must be **fully self-contained** — inline CSS and inline SVG charts, no CDN or external asset references. Assume the environment is air-gapped.

### 2.8 Chaos playbook (operator-triggered)

The harness ships a playbook with exact commands, expected outcome, and pass criteria. The operator runs the fault and POSTs a mark; the harness correlates and scores automatically. It also **auto-detects** `is_readonly` / `is_session_expired` transitions and annotates them independently of the operator mark.

| ID | Fault | Expectation to verify |
|---|---|---|
| C1 | `SIGKILL` one CH replica under steady ingest | Writes continue iff `insert_quorum<2`; measure catch-up rate on restart; **zero data loss** |
| C2 | Graceful restart of one replica | Clean drain and rejoin; startup time with realistic part count |
| C3 | Kill 1 of 3 Keeper nodes | No write impact; measure the latency blip |
| C4 | Kill 2 of 3 Keeper nodes | **Quorum lost → tables go read-only.** Measure detection and recovery time |
| C5 | Kill the Keeper leader | Re-election time and its impact on the insert path |
| C6 | `tc netem` 50 ms replica↔replica and →Keeper | Replication lag growth and recovery |
| C7 | Fill disk to 95% | Merge failures, insert rejection, behaviour at the watermark |
| C8 | `stress-ng` CPU starvation on one replica | Query routing and degradation profile |
| C9 | Long `ALTER UPDATE` mutation during peak ingest | Mutation vs merge contention (D13) |
| C10 | Restart a replica holding a very high part count | Startup/attach time — often minutes, and often a surprise |

### 2.9 Scenario suite to ship

| Tier | Scenario | Duration | Dimensions |
|---|---|---|---|
| T1 | `smoke` — tiny load, all paths | 5 min | sanity, CI gate |
| T2 | `insert-baseline-{stream}` per value stream | 30 min | D1, D3 |
| T3 | `query-baseline-cold` / `-warm` | 30 min | D2, D15 |
| T4 | `mixed-eod-peak` — all 4 streams + L2 reports | 1 h | D5, D6, D8 |
| T5 | `pipeline-freshness` — L0→L1→L2 end-to-end latency | 1 h | D8 |
| T6 | `breaking-point` — ramp to SLO breach | 2 h | D12 |
| T7 | `soak-24h` / `soak-7d` | 24 h / 7 d | D10, D3, D17 |
| T8 | `chaos-*` — one per C1–C10 | 30–60 min | D9 |
| T9 | `maintenance-under-load` | 1 h | D13 |
| T10 | `capacity-growth` — measure bytes/row, project retention | 2 h | D7 |

---

## Part 3 — Build plan

### 3.1 Module layout

```
riskstore-perfbench/
  pom.xml                              Java 21, maven-shade fat JAR
  src/main/java/com/riskstore/perf/
    Main.java                          config load → Muserver boot
    api/                               RunsResource, ScenariosResource, PreflightResource,
                                       MarksResource, ReportResource, HealthResource
    config/                            HarnessConfig, ClusterConfig, SafetyGuard (Jackson-YAML)
    scenario/                          Scenario, Workload, Phase, SloSpec, ScenarioLoader, ScenarioValidator
    engine/                            RunCoordinator, RunContext, PhaseScheduler,
                                       ArrivalScheduler   ← open-loop, virtual clock
                                       InsertDriver, InsertSelectDriver, QueryDriver
    gen/                               Generator SPI, FieldSpec (cardinality/distribution),
                                       RatesAtlasGenerator, TradeGenerator, MarketDataGenerator, SeededRandom
    ch/                                ClickHouseClientFactory (client-v2), InsertSink,
                                       QueryExecutor, QueryIdFactory, CacheController
    metrics/                           LatencyRecorder (HdrHistogram), WindowedHistogramStore,
                                       ErrorCounter, SystemTableScraper, ZookeeperInfoScraper,
                                       QueryLogHarvester, PartLogHarvester, OsMetricsScraper
    verify/                            Reconciler, SloEvaluator, BaselineComparator
    results/                           ResultsWriter, ResultsSchemaBootstrap, RunRepository
    report/                            SummaryBuilder, HtmlReportRenderer, JsonReportRenderer,
                                       MarkdownRenderer, InlineSvgChart
  src/main/resources/
    scenarios/*.yaml
    sql/results-schema.sql
    sql/sut-metrics/*.sql
    sql/workloads/*.sql                query + transform library
    report/template.html, report.css
  src/test/java/...
  docs/CHAOS-PLAYBOOK.md
```

### 3.2 Dependencies (verified current)

| Dependency | Coordinate | Note |
|---|---|---|
| HTTP server | `io.muserver:mu-server:2.2.2` | `MuServerBuilder` + `RestHandlerBuilder`; Java 17+ |
| ClickHouse client | `com.clickhouse:client-v2:0.9.4` | v2 client — RowBinary/Native, lighter than JDBC |
| Latency | `org.hdrhistogram:HdrHistogram` | Lossless merge is why percentiles are trustworthy |
| YAML/JSON | `com.fasterxml.jackson.dataformat:jackson-dataformat-yaml` + `jackson-databind` | |
| Logging | `org.slf4j:slf4j-api` + `ch.qos.logback:logback-classic` | |
| Test | JUnit 5, AssertJ, Testcontainers (ClickHouse) | |

Java 21 **virtual threads** are the right fit for the open-loop driver: thousands of in-flight arrivals without pinning platform threads, so a stalled server doesn't throttle the arrival schedule.

### 3.3 Delivery phases

| Phase | Scope | Exit criteria |
|---|---|---|
| **P0 — Skeleton** | pom, `Main`, Muserver boot, config load, `/health`, `/preflight`, safety allowlist | `curl /api/v1/health` returns 200; preflight reports real cluster state |
| **P1 — Core engine** | Scenario model + loader, `ArrivalScheduler` (open-loop), `RunCoordinator`, `LatencyRecorder`, in-memory results | `smoke` scenario runs, prints p50/p99 |
| **P2 — Insert path** | `RatesAtlasGenerator`, `InsertSink` (JSONEachRow + RowBinary), dedup tokens, batching, `QueryIdFactory` | `insert-baseline-rates` runs end-to-end ← *scaffolding milestone (insert)* |
| **P3 — Query path** | SQL workload library, `QueryDriver`, weighted selection, `CacheController` (cold/warm) | `query-baseline-cold` runs end-to-end ← *scaffolding milestone (query)* |
| **P4 — SUT collectors** | `SystemTableScraper`, `ZookeeperInfoScraper`, query/part log harvesters, `SYSTEM FLUSH LOGS` | Full metric catalogue captured and joined by `query_id` |
| **P5 — Results + report** | `perf_results` schema bootstrap, `ResultsWriter`, JSON + self-contained HTML + Markdown | Report renders offline with inline SVG charts |
| **P6 — Verify + gate** | `Reconciler`, `SloEvaluator`, `BaselineComparator` | Scorecard PASS/FAIL; non-zero exit on regression for CI |
| **P7 — Scenario suite + chaos** | T2–T10 scenarios, `/marks`, auto-detection, `CHAOS-PLAYBOOK.md` | All chaos scenarios documented and scoreable |

**P0–P3 is the "working scaffolding" deliverable.** P4–P7 is the full harness.

### 3.4 Verification

1. **Local, no cluster needed** — Testcontainers single-node ClickHouse; run `smoke`; assert a report is produced and SLO evaluation fires on both pass and fail.
2. **Open-loop correctness** — unit-test `ArrivalScheduler` against an injected stalling sink: verify recorded latency reflects *intended* send time (i.e. the stall shows up). This is the test that proves you don't have coordinated omission.
3. **Histogram merge correctness** — merge known windowed HdrHistograms, assert the global p99.9 matches a brute-force computation over the raw sample set.
4. **Correlation** — run an insert + query scenario, assert every client-side op has a matching `system.query_log` row by `query_id`.
5. **On the real cluster** — run `preflight`, then `insert-baseline-rates`, then `query-baseline-cold`; sanity-check throughput and part counts against manual `clickhouse-client` observation.
6. **Reconciliation** — deliberately kill an insert mid-batch, restart, confirm the Reconciler reports the right expected/actual and that dedup tokens prevented duplicates.
7. **Report** — open the HTML with the network disabled; confirm it renders fully.

---

## Open items to confirm during build

These need your real numbers and don't block starting — but the SLO thresholds in §2.4 are **placeholders** until they're settled:

- Target ingest rates per value stream (rows/sec and bytes/sec, peak and EOD-burst).
- L2 report query latency SLOs and concurrency (how many reports fire simultaneously post-trigger).
- Freshness SLO: Kafka publish → L2 readable.
- Retention policy per layer — drives the D7 capacity projection against the 6 TB ceiling.
- `insert_quorum` decision: with only 2 replicas, `insert_quorum=2` means one replica outage stops all writes. Durability vs availability — test both.
- Whether 26.1 workload scheduling (`CREATE WORKLOAD`/`CREATE RESOURCE`) will be used for value-stream isolation, so D5 can be tested with and without it.

---

## Sources

- [ClickHouse Release 26.1](https://clickhouse.com/blog/clickhouse-release-26-01) — `system.zookeeper_info`, `system.parts.files`, `mergeTreeAnalyzeIndexes()`, async-insert dedup across MVs
- [Workload scheduling — ClickHouse Docs](https://clickhouse.com/docs/operations/workload-scheduling) — `CREATE RESOURCE` / `CREATE WORKLOAD`
- [io.muserver:mu-server](https://mvnrepository.com/artifact/io.muserver/mu-server/2.1.8) — 2.2.2
- [com.clickhouse:client-v2](https://central.sonatype.com/artifact/com.clickhouse/client-v2) — 0.9.4
- [Altinity KB — handy `system.query_log` queries](https://kb.altinity.com/altinity-kb-useful-queries/query_log/)
