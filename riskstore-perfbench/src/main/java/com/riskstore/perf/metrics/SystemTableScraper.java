package com.riskstore.perf.metrics;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import com.riskstore.perf.ch.ClickHouseClientFactory;
import com.riskstore.perf.ch.SqlLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Polls the SUT's own introspection tables on a fixed cadence.
 *
 * <p>This is where the dimensions that insert-and-query benchmarking misses actually get
 * measured: merge backlog and part counts, replication lag and Keeper health, memory and disk
 * headroom. Client-side latency tells you something got slow; these samples tell you why.
 *
 * <p>Every probe is defensive. A locked-down grant may forbid one system table, and a scraper
 * that dies on the first denied SELECT would take the run's whole diagnostic record with it, so
 * failures are counted and logged once rather than propagated.
 *
 * <p>Scraping is itself load. The default two-second cadence over a handful of cheap aggregate
 * queries is negligible against a saturated cluster, but it is not free and is recorded in the
 * report for honesty.
 */
public final class SystemTableScraper implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SystemTableScraper.class);

    /**
     * Probes run on every cycle. Each returns rows of (metric, label, value); the loader below
     * adapts the shapes. Ordered cheapest first so a slow cluster degrades the tail, not the head.
     */
    private static final List<String> CYCLE_PROBES = List.of(
            "async_metrics",
            "server_metrics",
            "parts_summary",
            "parts_files",
            "merges_summary",
            "replicas_summary",
            "mutations_pending",
            "keeper_info",
            "keeper_events",
            "disks",
            "errors_summary");

    private final ClickHouseClientFactory clients;
    private final SqlLibrary sql;
    private final long intervalMs;
    private final List<MetricSample> samples = Collections.synchronizedList(new ArrayList<>());
    private final Map<String, Integer> probeFailures = new LinkedHashMap<>();

    private ScheduledExecutorService scheduler;

    public SystemTableScraper(ClickHouseClientFactory clients, SqlLibrary sql, long intervalMs) {
        this.clients = clients;
        this.sql = sql;
        this.intervalMs = intervalMs;
    }

    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "sut-scraper");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::scrapeQuietly, 0, intervalMs, TimeUnit.MILLISECONDS);
        log.info("SUT scraper started at {}ms cadence across {} replica(s)", intervalMs, clients.nodeCount());
    }

    private void scrapeQuietly() {
        try {
            scrapeOnce();
        } catch (Throwable t) {
            log.debug("Scrape cycle failed: {}", t.toString());
        }
    }

    /** One full cycle across every replica. Also callable directly, which the tests rely on. */
    public void scrapeOnce() {
        long ts = System.currentTimeMillis();
        for (int i = 0; i < clients.nodeCount(); i++) {
            String node = clients.endpoint(i);
            Client client = clients.forNode(i);
            for (String probe : CYCLE_PROBES) {
                if (!sql.has(probe)) {
                    continue;
                }
                try {
                    collect(client, node, probe, ts);
                } catch (Exception e) {
                    noteFailure(probe, e);
                }
            }
        }
    }

    /**
     * Probe result contract: each row exposes {@code metric}, optional {@code label} and
     * {@code value}. Keeping every probe to that shape means new system tables can be added by
     * editing {@code sut-metrics.yaml} alone.
     */
    private void collect(Client client, String node, String probe, long ts) {
        List<GenericRecord> rows = client.queryAll(sql.get(probe));
        for (GenericRecord row : rows) {
            String metric = str(row, "metric");
            if (metric == null) {
                continue;
            }
            String label = str(row, "label");
            Double value = dbl(row);
            if (value == null) {
                continue;
            }
            samples.add(new MetricSample(ts, node, metric, label == null ? "" : label, value));
        }
    }

    private static String str(GenericRecord row, String column) {
        try {
            return row.getString(column);
        } catch (Exception e) {
            return null;
        }
    }

    private static Double dbl(GenericRecord row) {
        try {
            return row.getDouble("value");
        } catch (Exception e) {
            return null;
        }
    }

    private synchronized void noteFailure(String probe, Exception e) {
        int count = probeFailures.merge(probe, 1, Integer::sum);
        if (count == 1) {
            log.warn("Probe '{}' failed (will keep trying, logged once): {}", probe, e.toString());
        }
    }

    public List<MetricSample> samples() {
        synchronized (samples) {
            return List.copyOf(samples);
        }
    }

    /** Probes that never succeeded are surfaced in the report: a missing probe is a blind spot. */
    public synchronized Map<String, Integer> probeFailures() {
        return Map.copyOf(probeFailures);
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
