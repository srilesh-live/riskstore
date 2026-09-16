# riskstore-perfbench

On-demand performance and stress test harness for the Riskstore ClickHouse cluster (26.1, 1 shard × 2 replicas, 3 Keeper nodes).

Drives ClickHouse **directly** — it issues the SQL the ingestion and report services would issue, without going through Kafka or those services. That isolates database capacity: when something is slow, you can tell whether it's the DB or the application layer, because only one of them is in the picture.

Produces a one-screen executive summary and a detailed benchmark report from the same run.

---

## Quick start

```bash
mvn clean package
```

```bash
java -jar target/riskstore-perfbench.jar harness.yaml
```

Then:

```bash
curl -s localhost:8080/api/v1/preflight | head -40
```

Preflight refuses to start a run until `safety.allowDestructive: true` is set **and** the target cluster name is in `safety.allowedClusters`. Both are required, neither has a permissive default — runs write data and drop caches, so this is the only thing between an operator and accidentally load-testing production.

```bash
curl -XPOST localhost:8080/api/v1/runs -H 'Content-Type: application/json' -d '{"scenarioId":"smoke"}'
```

Open the report at `localhost:8080/api/v1/runs/RUN_ID/report?format=html`.

### First-time setup

Create the test tables from [`docs/riskstore-test-schema.sql`](docs/riskstore-test-schema.sql). It's a representative Rates subset across L0/L1/L2 — replace it with the real DDL once that's settled.

---

## What it measures

Insert and query throughput are two of roughly a dozen dimensions, and they are **not** the ones that take ClickHouse clusters down. Ranked by risk to this specific topology:

| Dimension | Why it matters here |
|---|---|
| **Merge & part lifecycle** | The #1 failure mode. Inserts are delayed at 150 parts per partition and rejected at 300. Ingestion doesn't slow gracefully when merges fall behind — it stops. `MaxPartCountForPartition` is the single most important signal in the whole harness. |
| **Replication & Keeper** | With only 3 Keeper nodes, Keeper is the hard write ceiling — it saturates long before CPU does. And 2 replicas means `insert_quorum=2` turns any single replica outage into a total write outage. |
| **Mixed-workload isolation** | Nothing runs alone. A p99 measured on an idle cluster is fiction. |
| **Memory & spill** | L1→L2 enrichment means joins, and joins are where ClickHouse OOMs. |
| **Storage & IO** | 6 TB is the binding constraint. Each replica holds 100% of the data, and merges need free space of the same order as the parts being merged — realistic usable ceiling is ~3–3.5 TB. |
| **Resilience** | Replica loss, Keeper quorum loss, disk full, restart with a large part count. Recovery time is an SLO too. |
| **Endurance** | Part drift, memory creep and log growth are invisible in a 30-minute run. |
| **Correctness under stress** | A benchmark that silently loses rows reports success while proving nothing. |
| **Capacity discovery** | "Does it pass at expected load?" tells you nothing about headroom. |

---

## Measurement principles

These are the difference between a benchmark and a number generator. Each is enforced in code and covered by a test.

**Open-loop load generation.** Arrival times come from a virtual clock, independent of completions. A closed-loop driver — N threads looping "send, wait, send" — stops sending when the server stalls, so the stall never enters the histogram. That's *coordinated omission*, and it routinely understates tail latency by an order of magnitude. Latency here is measured from each operation's **intended** arrival time. See [`ArrivalScheduler`](src/main/java/com/riskstore/perf/engine/ArrivalScheduler.java) and the test that proves it: [`ArrivalSchedulerTest`](src/test/java/com/riskstore/perf/engine/ArrivalSchedulerTest.java).

**Percentiles are merged, never averaged.** Latency is kept as per-window HdrHistograms, which merge losslessly. Averaging per-window percentiles is arithmetically meaningless and is the most common way a benchmark report ends up wrong.

**Back pressure without lying.** When in-flight work hits the cap, arrivals are *shed and counted*, not queued — blocking the pacer would silently reintroduce coordinated omission. Any run with non-zero drops is reported as **untrustworthy**, with the caveat printed above the numbers rather than buried below them.

**Harness limits are distinguished from database limits.** Overload drops with high in-flight means the *server* can't keep up. Large scheduler lag with low in-flight means the *harness* can't. Batch starvation means data generation couldn't keep up. All three are tracked and reported separately, because they have opposite remedies.

**Steady-state windowing.** Warmup and ramp phases are recorded but excluded from headline metrics.

**Correlation by `query_id`.** Every operation is stamped `{runId}:{workloadId}:{seq}`, so client-side latency joins exactly to `system.query_log`. This is what lets a report say *"p99 was 4s, of which 3.6s was server-side, in a query that read 900M rows because the primary index wasn't used."*

**Deterministic data.** Same seed, same data — so a latency delta between runs can't be blamed on different inputs.

**Config fingerprinting.** Version, settings and schema hashes are recorded per run. A run against different settings is flagged as *incomparable* rather than reported as a regression.

---

## Scenarios

| Scenario | Duration | Purpose |
|---|---|---|
| `smoke` | 5 min | Sanity check across every path. Don't quote its numbers. |
| `insert-baseline-rates` | 40 min | Ingest ceiling for one stream in isolation, with part tracking |
| `query-baseline-cold` | 30 min | Report latency, cold caches, quiet cluster |
| `mixed-eod-peak` | 1 hr | The realistic case: everything at once |
| `breaking-point` | ~2 hr | Ramp to SLO breach. **Expected to fail** — that's the point |
| `soak-24h` | 24 hr | Drift, creep and slow degradation |
| `chaos-replica-kill` | 50 min | Replica loss and recovery (operator-triggered) |

Scenarios are YAML. Drop a file into `./scenarios/` to add or override one by id — no rebuild. Same for `workloads.yaml` and `sut-metrics.yaml`, which override the bundled SQL entry by entry so a DBA can retune a query without forking the build.

> **The SLO thresholds in the bundled scenarios are placeholders.** They encode the shape of the assertion, not your real targets. Replace them with your actual ingest rates, report latency SLOs and freshness requirements before treating any pass as meaningful.

---

## Chaos testing

The harness defines and measures faults; **an operator injects them**. See [`docs/CHAOS-PLAYBOOK.md`](docs/CHAOS-PLAYBOOK.md) for ten scenarios with exact commands and pass criteria.

```bash
curl -XPOST localhost:8080/api/v1/runs/RUN_ID/marks -H 'Content-Type: application/json' -d '{"kind":"replica-kill","target":"ch-replica-2","note":"SIGKILL at T+15m"}'
```

Marks overlay as vertical lines on every chart. The scraper also auto-detects replicas going read-only or losing their Keeper session, so an *unplanned* fault can't pass unnoticed.

---

## API

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/health` | Liveness, loaded scenarios, active run |
| `GET` | `/api/v1/preflight` | Safety gate, cluster health, config fingerprint. 412 when blocked |
| `GET` | `/api/v1/scenarios` | Scenario catalogue with estimated durations |
| `POST` | `/api/v1/runs` | Start a run. 409 if one is already active |
| `GET` | `/api/v1/runs/{id}` | Live status: phase, rolling p99, in-flight, errors |
| `POST` | `/api/v1/runs/{id}/marks` | Record an operator-injected fault |
| `POST` | `/api/v1/runs/{id}/abort` | Stop load; still produces a report |
| `GET` | `/api/v1/runs/{id}/report?format=json\|html\|md` | Final report |
| `GET` | `/api/v1/runs/{id}/compare/{baselineId}` | Regression delta. 409 regressed, 422 incomparable |

Runs are serialised — two concurrent runs would contend for the resource each is trying to measure.

---

## Reports

Three outputs from one run, all rendering from the same model so they can't disagree:

- **`summary.md`** — the go-live pack artefact. Verdict, caveats, scorecard. One screen.
- **`report.html`** — detailed. Latency over time with ClickHouse internals on the same axis, per-statement breakdowns, fault timeline, environment fingerprint. **Fully self-contained** — inline CSS and SVG, no CDN, no web fonts. Assume the box is air-gapped.
- **`report.json`** — machine-readable, for the CI regression gate.

Written to `results/RUN_ID/` on disk **always**, and to a results ClickHouse when configured. That database is deliberately a *different* instance from the system under test: writing benchmark output into the cluster being benchmarked adds inserts, parts and merges to the thing being measured, in proportion to how much data the run produced.

---

## Deployment

Run on a host **separate from the ClickHouse replicas.** Co-locating the load generator with the database means the harness competes for the CPU it's trying to measure, and every resulting number is a blend of the two.

Sizing: batch generation is the harness's main cost. A 20,000-row Atlas batch is tens of megabytes, and `batch.queueDepth` multiplies that per workload. Budget 8+ cores and 16 GB heap for the mixed scenarios. If `harness.batchStarvations` or `harness.overloadDrops` is non-zero, the generator was the ceiling — not the database — and the run needs rerunning on bigger hardware before its numbers mean anything.

---

## Layout

```
src/main/java/com/riskstore/perf/
  engine/     ArrivalScheduler (open-loop core), RunCoordinator, drivers, BatchFactory, Preflight
  ch/         client-v2 access, InsertSink, QueryExecutor, QueryIdFactory, CacheController, SqlLibrary
  gen/        cardinality-modelled payload generators
  metrics/    LatencyRecorder (HdrHistogram), SystemTableScraper, ErrorCounter
  verify/     SloEvaluator, Reconciler, BaselineComparator, MetricResolver
  report/     SummaryBuilder, HTML/Markdown/JSON renderers, InlineSvgChart
  results/    ResultsWriter, schema bootstrap
  api/        ApiRoutes (Muserver)
src/main/resources/
  scenarios/*.yaml         the scenario suite
  sql/workloads.yaml       query and transform library
  sql/sut-metrics.yaml     server-side metric catalogue
  sql/results-schema.sql   results database DDL
docs/
  CHAOS-PLAYBOOK.md        ten fault scenarios with commands and pass criteria
  riskstore-test-schema.sql
```

Java 21 · Muserver 2.2.2 · clickhouse-java client-v2 0.9.4 · HdrHistogram
