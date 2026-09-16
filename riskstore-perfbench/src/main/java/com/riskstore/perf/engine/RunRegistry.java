package com.riskstore.perf.engine;

import com.riskstore.perf.report.RunReport;
import com.riskstore.perf.report.SummaryBuilder;
import com.riskstore.perf.results.ResultsWriter;
import com.riskstore.perf.scenario.Scenario;
import com.riskstore.perf.scenario.ScenarioLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tracks runs and executes them one at a time.
 *
 * <p>Serialised deliberately. Two runs against the same cluster would contend for the very
 * resource each is trying to measure, and neither result would mean anything. A second request
 * while a run is active is refused with a clear reason rather than queued, because by the time a
 * queued run started the operator's assumptions about cluster state would be stale.
 */
public final class RunRegistry {

    private static final Logger log = LoggerFactory.getLogger(RunRegistry.class);
    private static final DateTimeFormatter RUN_ID_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC);

    private final ScenarioLoader scenarios;
    private final RunCoordinator coordinator;
    private final ResultsWriter results;

    private final Map<String, RunContext> runs = new ConcurrentHashMap<>();
    private final Map<String, RunReport> reports = new ConcurrentHashMap<>();
    private final AtomicReference<String> activeRunId = new AtomicReference<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "run-coordinator");
        t.setDaemon(false);
        return t;
    });

    public RunRegistry(ScenarioLoader scenarios, RunCoordinator coordinator, ResultsWriter results) {
        this.scenarios = scenarios;
        this.coordinator = coordinator;
        this.results = results;
    }

    /** Thrown when a run cannot be accepted. Carries a message meant for the operator, not a stack trace. */
    public static class RunRejected extends RuntimeException {
        public RunRejected(String message) {
            super(message);
        }
    }

    public String start(String scenarioId, Map<String, String> tags, String baselineRunId) {
        Scenario scenario = scenarios.get(scenarioId);
        if (scenario == null) {
            throw new RunRejected("Unknown scenario '" + scenarioId + "'. Known: " + scenarios.all().keySet());
        }

        String runId = "run-" + RUN_ID_FORMAT.format(ZonedDateTime.now(ZoneOffset.UTC)) + "-" + scenarioId;
        if (!activeRunId.compareAndSet(null, runId)) {
            throw new RunRejected("Run '" + activeRunId.get() + "' is already in progress. "
                    + "Concurrent runs would contend for the cluster being measured.");
        }

        RunContext ctx = new RunContext(runId, scenario, scenarios.hash(scenarioId), tags, baselineRunId);
        runs.put(runId, ctx);

        executor.submit(() -> {
            try {
                coordinator.execute(ctx);
            } finally {
                try {
                    RunReport report = new SummaryBuilder(ctx).build();
                    reports.put(runId, report);
                    results.write(report);
                } catch (Exception e) {
                    log.error("Could not build or persist the report for run {}", runId, e);
                } finally {
                    activeRunId.compareAndSet(runId, null);
                }
            }
        });

        log.info("Run {} accepted (scenario {})", runId, scenarioId);
        return runId;
    }

    public Optional<RunContext> context(String runId) {
        return Optional.ofNullable(runs.get(runId));
    }

    public Optional<RunReport> report(String runId) {
        RunReport finished = reports.get(runId);
        if (finished != null) {
            return Optional.of(finished);
        }
        // Mid-run: build a provisional report so the status endpoint has real numbers to show.
        return context(runId).map(ctx -> new SummaryBuilder(ctx).build());
    }

    public List<RunContext> list() {
        List<RunContext> out = new ArrayList<>(runs.values());
        out.sort((a, b) -> Long.compare(b.startedAtMs(), a.startedAtMs()));
        return out;
    }

    public Optional<String> activeRunId() {
        return Optional.ofNullable(activeRunId.get());
    }

    public boolean abort(String runId) {
        RunContext ctx = runs.get(runId);
        if (ctx == null || ctx.state().isTerminal()) {
            return false;
        }
        ctx.state(RunState.ABORTED);
        ctx.failureReason("aborted by operator");
        log.warn("Run {} aborted by operator", runId);
        return true;
    }

    /** Records an operator-injected fault against the active run. */
    public boolean mark(String runId, ChaosMark mark) {
        RunContext ctx = runs.get(runId);
        if (ctx == null) {
            return false;
        }
        ctx.addChaosMark(mark);
        log.info("Run {} marked: {} on {} ({})", runId, mark.kind(), mark.target(), mark.source());
        return true;
    }

    public void shutdown() {
        executor.shutdown();
    }
}
