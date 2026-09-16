-- ===========================================================================
-- Results schema for riskstore-perfbench.
--
-- Provisioned on a ClickHouse instance SEPARATE from the system under test.
-- Writing benchmark output into the cluster being benchmarked adds inserts,
-- parts and merges to the thing being measured, in proportion to how much data
-- the run produced. A long soak would end up measuring its own bookkeeping.
--
-- Statements are idempotent. Where the harness account has no CREATE rights,
-- run this by hand and set results.enabled = true; the bootstrap will log a
-- warning per statement and carry on writing rows.
--
-- Retention is 2 years by TTL. Benchmark history is small and its value is
-- almost entirely in year-over-year comparison, so keep it longer than feels
-- necessary.
-- ===========================================================================

CREATE DATABASE IF NOT EXISTS ${database};

-- One row per run. The scenario hash and settings fingerprint are what make a
-- later comparison honest: they distinguish "this regressed" from "somebody
-- changed the scenario or the server settings".
CREATE TABLE IF NOT EXISTS ${database}.runs
(
    run_id               String,
    scenario_id          LowCardinality(String),
    scenario_hash        String,
    state                LowCardinality(String),
    harness_version      LowCardinality(String),
    started_at_ms        Int64,
    ended_at_ms          Int64,
    duration_ms          Int64,
    measured_from_ms     Int64,
    measured_to_ms       Int64,
    passed               UInt8,
    trustworthy          UInt8,
    headline             String,
    sut_version          LowCardinality(String),
    settings_fingerprint String,
    inserted_at          DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (scenario_id, started_at_ms, run_id)
TTL inserted_at + INTERVAL 2 YEAR;

-- Latency as compressed HdrHistograms, one per workload per window.
--
-- encoded_hdr is a lossless base64 HdrHistogram. Because HdrHistograms merge
-- exactly, any consumer can recover a true p99.9 over an arbitrary time range
-- by decoding and merging the relevant windows. The p50/p90/p99 columns are
-- pre-computed for charting only - never average them to get a range
-- percentile, decode and merge instead.
CREATE TABLE IF NOT EXISTS ${database}.op_latency_hist
(
    run_id          String,
    workload_id     LowCardinality(String),
    window_start_ms Int64,
    window_end_ms   Int64,
    count           Int64,
    errors          Int64,
    min_us          Int64,
    max_us          Int64,
    p50_us          Int64,
    p90_us          Int64,
    p99_us          Int64,
    encoded_hdr     String,
    inserted_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (run_id, workload_id, window_start_ms)
TTL inserted_at + INTERVAL 2 YEAR;

-- Server-side samples: part counts, merge backlog, replication lag, Keeper
-- health, memory, disk. This is the table that answers "why", where
-- op_latency_hist only answers "what".
CREATE TABLE IF NOT EXISTS ${database}.sut_metrics
(
    run_id      String,
    ts_ms       Int64,
    node        LowCardinality(String),
    metric      LowCardinality(String),
    label       String,
    value       Float64,
    inserted_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (run_id, metric, node, ts_ms)
TTL inserted_at + INTERVAL 2 YEAR;

CREATE TABLE IF NOT EXISTS ${database}.slo_results
(
    run_id        String,
    metric        LowCardinality(String),
    op            LowCardinality(String),
    threshold_raw String,
    threshold     Float64,
    actual        Float64,
    unit          LowCardinality(String),
    resolved      UInt8,
    passed        UInt8,
    detail        String,
    inserted_at   DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (run_id, metric)
TTL inserted_at + INTERVAL 2 YEAR;

CREATE TABLE IF NOT EXISTS ${database}.reconciliation
(
    run_id      String,
    check_name  String,
    expected    Float64,
    actual      Float64,
    tolerance   Float64,
    passed      UInt8,
    detail      String,
    inserted_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (run_id, check_name)
TTL inserted_at + INTERVAL 2 YEAR;

-- Operator-injected faults and auto-detected state transitions, on the same
-- time axis as everything else.
CREATE TABLE IF NOT EXISTS ${database}.chaos_events
(
    run_id      String,
    ts_ms       Int64,
    kind        LowCardinality(String),
    target      String,
    note        String,
    source      LowCardinality(String),
    inserted_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (run_id, ts_ms)
TTL inserted_at + INTERVAL 2 YEAR;

-- Convenience view for trend analysis across runs of the same scenario.
CREATE VIEW IF NOT EXISTS ${database}.v_run_trend AS
SELECT
    r.scenario_id                              AS scenario_id,
    r.run_id                                   AS run_id,
    toDateTime(intDiv(r.started_at_ms, 1000))  AS started_at,
    r.passed                                   AS passed,
    r.trustworthy                              AS trustworthy,
    r.settings_fingerprint                     AS settings_fingerprint,
    h.workload_id                              AS workload_id,
    sum(h.count)                               AS ops,
    max(h.p99_us) / 1000.0                     AS worst_window_p99_ms
FROM ${database}.runs AS r
INNER JOIN ${database}.op_latency_hist AS h ON r.run_id = h.run_id
GROUP BY scenario_id, run_id, started_at, passed, trustworthy, settings_fingerprint, workload_id
ORDER BY scenario_id, started_at;
