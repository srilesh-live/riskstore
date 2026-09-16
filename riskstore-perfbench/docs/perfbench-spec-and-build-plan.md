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
