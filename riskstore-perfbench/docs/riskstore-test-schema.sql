-- ===========================================================================
-- Reference test schema for riskstore-perfbench.
--
-- This is NOT the production Riskstore schema. It is a representative subset -
-- the Rates value stream across L0, L1 and L2 - shaped closely enough to the
-- real thing that its performance characteristics transfer: same cardinalities,
-- same partitioning, same join shapes, same nested-JSON flattening work.
--
-- Replace with the real DDL once it is settled. What must be preserved for the
-- results to remain meaningful:
--   * ReplicatedMergeTree, so replication and Keeper load are real
--   * PARTITION BY business date, which is what makes late-arriving data a
--     part-count risk
--   * ORDER BY leading with the columns queries actually filter on
--   * LowCardinality on genuinely low-cardinality columns, since that is a
--     large part of why ClickHouse compresses risk data as well as it does
--
-- Target: ClickHouse 26.1, 1 shard x 2 replicas, 3 Keeper nodes.
-- ===========================================================================

CREATE DATABASE IF NOT EXISTS riskstore;

-- ---------------------------------------------------------------------------
-- L0: raw pricing output, as it arrives from Kafka.
--
-- Top-level scalars are typed on the way in; the delta ladder stays as JSON,
-- because flattening it is precisely the work the L0 -> L1 transform has to do
-- and charging that cost to the transform rather than to ingestion is what
-- makes the two layers separately measurable.
--
-- The JSON type (native since 25.x, extended in 26.1) keeps the nested array
-- queryable through dynamic subcolumns. If a site prefers to keep the payload
-- opaque, use `risk_factors String` instead and set
-- input_format_json_read_arrays_as_strings = 1 on the insert.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS riskstore.l0_rates_atlas
(
    event_id         String,
    business_date    Date,
    snap_id          LowCardinality(String),
    value_stream     LowCardinality(String),
    source           LowCardinality(String),
    trade_id         String,
    book_id          LowCardinality(String),
    counterparty_id  String,
    instrument_type  LowCardinality(String),
    currency         LowCardinality(String),
    valuation_ccy    LowCardinality(String),
    pv               Float64,
    notional         Float64,
    measure          LowCardinality(String),
    risk_factors     JSON,
    valuation_model  LowCardinality(String),
    is_amended       Bool,
    as_of            DateTime64(3),
    ingest_ts        Int64,
    _received_at     DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/riskstore/l0_rates_atlas', '{replica}')
PARTITION BY business_date
ORDER BY (business_date, book_id, trade_id, event_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS riskstore.l0_rates_trade
(
    trade_id         String,
    version          UInt16,
    value_stream     LowCardinality(String),
    business_date    Date,
    book_id          LowCardinality(String),
    portfolio        LowCardinality(String),
    trader_id        LowCardinality(String),
    counterparty_id  String,
    status           LowCardinality(String),
    currency         LowCardinality(String),
    notional         Float64,
    trade_date       Date,
    maturity_date    Date,
    fixed_rate       Float64,
    is_cleared       Bool,
    ingest_ts        Int64,
    _received_at     DateTime DEFAULT now()
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/riskstore/l0_rates_trade', '{replica}', version)
PARTITION BY business_date
ORDER BY (business_date, trade_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS riskstore.l0_rates_market_data
(
    business_date    Date,
    snap_id          LowCardinality(String),
    curve_id         LowCardinality(String),
    currency         LowCardinality(String),
    curve_type       LowCardinality(String),
    tenor            LowCardinality(String),
    quote_type       LowCardinality(String),
    rate             Float64,
    discount_factor  Float64,
    source           LowCardinality(String),
    observation_ts   DateTime64(3),
    ingest_ts        Int64,
    _received_at     DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/riskstore/l0_rates_market_data', '{replica}')
PARTITION BY business_date
ORDER BY (business_date, snap_id, curve_id, tenor)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- L1: flattened columnar.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS riskstore.l1_rates_valuation
(
    business_date    Date,
    snap_id          LowCardinality(String),
    trade_id         String,
    book_id          LowCardinality(String),
    counterparty_id  String,
    instrument_type  LowCardinality(String),
    currency         LowCardinality(String),
    valuation_ccy    LowCardinality(String),
    pv               Float64,
    notional         Float64,
    valuation_model  LowCardinality(String),
    is_amended       Bool,
    as_of            DateTime64(3),
    transformed_at   DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/riskstore/l1_rates_valuation', '{replica}')
PARTITION BY business_date
ORDER BY (business_date, book_id, trade_id)
SETTINGS index_granularity = 8192;

-- One row per (trade, tenor): the exploded delta ladder. This is by far the
-- widest table by row count - roughly 14x the valuation table - and it is
-- normally the one that sets the storage ceiling.
CREATE TABLE IF NOT EXISTS riskstore.l1_rates_sensitivity
(
    business_date    Date,
    snap_id          LowCardinality(String),
    trade_id         String,
    book_id          LowCardinality(String),
    currency         LowCardinality(String),
    curve            LowCardinality(String),
    tenor            LowCardinality(String),
    delta            Float64,
    gamma            Float64,
    as_of            DateTime64(3),
    transformed_at   DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/riskstore/l1_rates_sensitivity', '{replica}')
PARTITION BY business_date
ORDER BY (business_date, book_id, tenor, trade_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS riskstore.l1_rates_trade
(
    business_date    Date,
    trade_id         String,
    book_id          LowCardinality(String),
    portfolio        LowCardinality(String),
    trader_id        LowCardinality(String),
    counterparty_id  String,
    status           LowCardinality(String),
    currency         LowCardinality(String),
    notional         Float64,
    trade_date       Date,
    maturity_date    Date,
    fixed_rate       Float64,
    is_cleared       Bool,
    transformed_at   DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/riskstore/l1_rates_trade', '{replica}')
PARTITION BY business_date
ORDER BY (business_date, book_id, trade_id)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- L2: report-shaped, built by the risk-report-service from L1 joins.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS riskstore.l2_rates_pnl_explain
(
    business_date    Date,
    snap_id          LowCardinality(String),
    book_id          LowCardinality(String),
    portfolio        LowCardinality(String),
    currency         LowCardinality(String),
    instrument_type  LowCardinality(String),
    trade_count      UInt64,
    total_pv         Float64,
    total_notional   Float64,
    total_delta      Float64,
    total_gamma      Float64,
    cleared_pv       Float64,
    uncleared_pv     Float64,
    generated_at     DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/riskstore/l2_rates_pnl_explain', '{replica}')
PARTITION BY business_date
ORDER BY (business_date, book_id, portfolio, currency)
SETTINGS index_granularity = 8192;

-- ===========================================================================
-- Single-node variant, for local development against one ClickHouse container.
--
-- Use this only for harness development. Never quote numbers from a
-- single-node run: it exercises neither replication nor Keeper, which are two
-- of the three dimensions most likely to be the real ceiling on the target
-- architecture.
--
--   Replace ReplicatedMergeTree('/path', '{replica}')  with MergeTree()
--   Replace ReplicatedReplacingMergeTree('/path', '{replica}', version)
--           with ReplacingMergeTree(version)
-- ===========================================================================
