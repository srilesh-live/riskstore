package com.riskstore.perf.engine;

import com.riskstore.perf.ch.CacheController;
import com.riskstore.perf.ch.ClickHouseClientFactory;
import com.riskstore.perf.ch.InsertSink;
import com.riskstore.perf.ch.QueryExecutor;
import com.riskstore.perf.ch.QueryIdFactory;
import com.riskstore.perf.ch.SqlLibrary;
import com.riskstore.perf.config.HarnessConfig;
import com.riskstore.perf.gen.Generator;
import com.riskstore.perf.gen.GeneratorRegistry;
import com.riskstore.perf.metrics.LatencyRecorder;
import com.riskstore.perf.metrics.SystemTableScraper;
import com.riskstore.perf.scenario.Phase;
import com.riskstore.perf.scenario.Scenario;
import com.riskstore.perf.scenario.Workload;
import com.riskstore.perf.verify.Reconciler;
import com.riskstore.perf.verify.SloEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

/**
 * Executes one run end to end: preflight, phases, drain, harvest, verify.
 *
 * <p>The ordering here is not incidental. Histograms are force-rotated before anything reads
 * them, or the final partial window is missing from every percentile. Logs are flushed before
 * they are harvested, because ClickHouse writes its system log tables asynchronously and the
 * tail of the run would otherwise be absent. Reconciliation runs before SLO evaluation, so that
 * an SLO written against row loss has something to read.
 */
public final class RunCoordinator {

    private static final Logger log = LoggerFactory.getLogger(RunCoordinator.class);

    /** How often ramp phases recompute the arrival rate. Fine enough to look linear, coarse enough to be free. */
    private static final long RATE_STEP_MS = 100;
    private static final long DRAIN_TIMEOUT_MS = 120_000;

    private final HarnessConfig config;
    private final ClickHouseClientFactory clients;
    private final SqlLibrary workloadSql;
    private final SqlLibrary metricSql;
    private final GeneratorRegistry generators;

    public RunCoordinator(HarnessConfig config,
                          ClickHouseClientFactory clients,
                          SqlLibrary workloadSql,
                          SqlLibrary metricSql,
                          GeneratorRegistry generators) {
        this.config = config;
        this.clients = clients;
        this.workloadSql = workloadSql;
        this.metricSql = metricSql;
        this.generators = generators;
    }

    public void execute(RunContext ctx) {
        SystemTableScraper scraper = null;
        try {
            ctx.startedAtMs(System.currentTimeMillis());

            ctx.state(RunState.PREFLIGHT);
            Preflight.Report preflight = new Preflight(clients, config).run();
            ctx.environment(preflight.environment());
            if (!preflight.ok()) {
                fail(ctx, "preflight blocked the run - " + preflight.blocker());
                return;
            }

            QueryIdFactory queryIds = new QueryIdFactory(ctx.runId());
            build(ctx, queryIds);

            ctx.state(RunState.WARMING);
            new CacheController(clients).apply(ctx.scenario().cachePolicy, warmupQueries(ctx.scenario()));
            ctx.batchFactories().values().forEach(f -> f.awaitPrimed(30_000));

            scraper = new SystemTableScraper(clients, metricSql, config.collectors.sutScrapeIntervalMs);
            scraper.start();

            ctx.state(RunState.RUNNING);
            runPhases(ctx);

            ctx.state(RunState.DRAINING);
            drain(ctx);

            ctx.state(RunState.HARVESTING);
            scraper.scrapeOnce();
            ctx.sutSamples(scraper.samples());
            ctx.probeFailures(scraper.probeFailures());
            flushLogs();

            ctx.state(RunState.VERIFYING);
            ctx.reconResults(new Reconciler(clients).reconcile(ctx));
            ctx.sloResults(new SloEvaluator(ctx).evaluate(ctx.scenario().slo));

            ctx.endedAtMs(System.currentTimeMillis());
            ctx.state(RunState.COMPLETED);
            log.info("Run {} completed in {}ms - {}", ctx.runId(), ctx.durationMs(),
                    ctx.passed() ? "PASS" : "FAIL");

        } catch (Exception e) {
            log.error("Run {} failed", ctx.runId(), e);
            fail(ctx, e.toString());
        } finally {
            if (scraper != null) {
                scraper.close();
            }
            closeQuietly(ctx);
        }
    }

    private void build(RunContext ctx, QueryIdFactory queryIds) {
        Scenario scenario = ctx.scenario();
        InsertSink sink = new InsertSink(clients, queryIds);
        QueryExecutor executor = new QueryExecutor(clients, queryIds);
        long windowMs = config.collectors.latencyWindowMs;
        int maxInFlight = config.collectors.maxInFlightPerWorkload;

        for (Workload workload : scenario.workloads) {
            LatencyRecorder recorder = new LatencyRecorder(workload.id, windowMs);
            ctx.recorders().put(workload.id, recorder);

            ArrivalScheduler.Operation operation;
            if (workload.isInsert()) {
                Generator generator = generators.create(workload.generator.ref, workload.generator.params);
                BatchFactory factory = new BatchFactory(
                        ctx.runId(), workload, generator, scenario.seed, workload.batch.producers);
                factory.start();
                ctx.batchFactories().put(workload.id, factory);

                InsertDriver driver = new InsertDriver(workload, factory, sink, recorder, ctx.errors());
                ctx.insertDrivers().put(workload.id, driver);
                operation = driver;
            } else if (workload.isQuery()) {
                QueryDriver driver = new QueryDriver(
                        workload, workloadSql, executor, recorder, ctx.errors(), windowMs);
                ctx.queryDrivers().put(workload.id, driver);
                operation = driver;
            } else {
                // insertSelect: the L0 to L1 transform, paced by cadence rather than by rate.
                String sql = workloadSql.get(workload.sqlRef);
                operation = intended -> {
                    var result = executor.execute(workload.id, sql, workload.settings);
                    long done = System.nanoTime();
                    if (result.success()) {
                        recorder.recordSuccess(intended, done, result.rows(), result.bytes());
                    } else {
                        ctx.errors().record(result.error());
                        recorder.recordFailure(intended, done);
                    }
                };
            }

            ArrivalScheduler scheduler = new ArrivalScheduler(workload.id, operation, recorder, maxInFlight);
            scheduler.start();
            ctx.schedulers().put(workload.id, scheduler);
        }
        log.info("Run {} built {} workload(s)", ctx.runId(), scenario.workloads.size());
    }

    private void runPhases(RunContext ctx) {
        long measuredFrom = 0;
        long measuredTo = 0;

        for (Phase phase : ctx.scenario().phases) {
            if (ctx.state() == RunState.ABORTED) {
                return;
            }
            ctx.currentPhase(phase.id);
            long start = System.currentTimeMillis();
            long durationMs = phase.duration.toMillis();
            long end = start + durationMs;
            log.info("Run {} entering phase '{}' ({}ms, peak rate {})",
                    ctx.runId(), phase.id, durationMs, phase.peakRate());

            while (System.currentTimeMillis() < end && ctx.state() != RunState.ABORTED) {
                double progress = (System.currentTimeMillis() - start) / (double) durationMs;
                applyRates(ctx, phase.rateAt(progress));
                LockSupport.parkNanos(RATE_STEP_MS * 1_000_000L);
                rotateHistograms(ctx, false);
            }

            long actualEnd = System.currentTimeMillis();
            ctx.addPhaseRecord(new RunContext.PhaseRecord(
                    phase.id, start, actualEnd, phase.peakRate(), phase.excludeFromMetrics));

            // Steady state spans the measured phases only, so warmup and ramp cannot dilute
            // the headline percentiles.
            if (!phase.excludeFromMetrics) {
                if (measuredFrom == 0) {
                    measuredFrom = start;
                }
                measuredTo = actualEnd;
            }
        }

        ctx.measuredWindow(
                measuredFrom == 0 ? ctx.startedAtMs() : measuredFrom,
                measuredTo == 0 ? System.currentTimeMillis() : measuredTo);
    }

    /**
     * Distribute a phase rate across workloads.
     *
     * <p>Insert workloads express rate in rows per second and are converted to batches per
     * second. Query workloads with an explicit arrivalRate ignore the phase rate entirely, so a
     * report workload can hold a steady 5 QPS while ingestion ramps around it.
     */
    private void applyRates(RunContext ctx, double phaseRate) {
        for (Workload workload : ctx.scenario().workloads) {
            ArrivalScheduler scheduler = ctx.schedulers().get(workload.id);
            if (scheduler == null) {
                continue;
            }
            if (workload.arrivalRate != null) {
                scheduler.setRate(workload.arrivalRate.value);
            } else if (workload.isInsert()) {
                InsertDriver driver = ctx.insertDrivers().get(workload.id);
                scheduler.setRate(driver.opsPerSecondFor(phaseRate * workload.share));
            } else if (workload.isInsertSelect()) {
                scheduler.setRate(1000.0 / Math.max(1, workload.triggerEvery.toMillis()));
            } else {
                scheduler.setRate(phaseRate * workload.share);
            }
        }
    }

    private void rotateHistograms(RunContext ctx, boolean force) {
        long now = System.currentTimeMillis();
        ctx.recorders().values().forEach(r -> r.rotateIfDue(now, force));
        ctx.queryDrivers().values().forEach(d -> d.rotate(now, force));
    }

    private void drain(RunContext ctx) {
        ctx.schedulers().values().forEach(s -> s.setRate(0));
        ctx.schedulers().values().forEach(s -> s.drain(DRAIN_TIMEOUT_MS));
        // Force-rotate only after every operation has landed, so the final window is complete.
        rotateHistograms(ctx, true);
    }

    /**
     * ClickHouse flushes its system log tables roughly every 7 seconds. Harvesting without this
     * silently loses the last few seconds of the run - exactly the part that matters after a
     * chaos event.
     */
    private void flushLogs() {
        for (int i = 0; i < clients.nodeCount(); i++) {
            try {
                clients.forNode(i).execute("SYSTEM FLUSH LOGS").get();
            } catch (Exception e) {
                log.warn("SYSTEM FLUSH LOGS failed on {}: {}", clients.endpoint(i), e.toString());
            }
        }
    }

    private List<String> warmupQueries(Scenario scenario) {
        List<String> out = new ArrayList<>();
        for (Workload w : scenario.workloads) {
            if (w.isQuery()) {
                for (Workload.QueryRef q : w.queries) {
                    if (workloadSql.has(q.ref)) {
                        out.add(workloadSql.get(q.ref));
                    }
                }
            }
        }
        return out;
    }

    private void fail(RunContext ctx, String reason) {
        ctx.failureReason(reason);
        ctx.endedAtMs(System.currentTimeMillis());
        ctx.state(RunState.FAILED);
        log.error("Run {} failed: {}", ctx.runId(), reason);
    }

    private void closeQuietly(RunContext ctx) {
        ctx.schedulers().values().forEach(ArrivalScheduler::close);
        ctx.batchFactories().values().forEach(BatchFactory::close);
    }
}
