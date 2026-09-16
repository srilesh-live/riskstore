package com.riskstore.perf.report;

import com.riskstore.perf.ch.CacheController;
import com.riskstore.perf.engine.ArrivalScheduler;
import com.riskstore.perf.engine.BatchFactory;
import com.riskstore.perf.engine.InsertDriver;
import com.riskstore.perf.engine.QueryDriver;
import com.riskstore.perf.engine.RunContext;
import com.riskstore.perf.metrics.LatencyRecorder;
import com.riskstore.perf.metrics.MetricSample;
import com.riskstore.perf.scenario.Workload;
import com.riskstore.perf.verify.ReconResult;
import com.riskstore.perf.verify.SloResult;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Reduces a finished {@link RunContext} to the {@link RunReport} every output format renders. */
public final class SummaryBuilder {

    public static final String HARNESS_VERSION = "0.1.0";

    /** Cap on points kept per series, so a seven-day soak still renders as a usable chart. */
    private static final int MAX_POINTS_PER_SERIES = 600;

    private final RunContext ctx;

    public SummaryBuilder(RunContext ctx) {
        this.ctx = ctx;
    }

    public RunReport build() {
        List<RunReport.WorkloadSummary> workloads = buildWorkloads();
        List<String> caveats = trustCaveats(workloads);

        return new RunReport(
                buildMeta(),
                buildVerdict(caveats),
                workloads,
                ctx.sloResults(),
                ctx.reconResults(),
                buildSutSeries(),
                ctx.chaosMarks(),
                ctx.errors().snapshot(),
                ctx.errors().samples(),
                ctx.environment(),
                buildNotes());
    }

    private RunReport.Meta buildMeta() {
        return new RunReport.Meta(
                ctx.runId(),
                ctx.scenario().id,
                ctx.scenario().description,
                ctx.scenarioHash(),
                ctx.state().name(),
                HARNESS_VERSION,
                ctx.startedAtMs(),
                ctx.endedAtMs(),
                ctx.durationMs(),
                ctx.measuredFromMs(),
                ctx.measuredToMs(),
                ctx.tags(),
                ctx.baselineRunId());
    }

    /**
     * Distinguishes "the cluster failed the test" from "the test could not be trusted".
     *
     * <p>Overload drops, batch starvation and large scheduler lag all mean the harness, not
     * ClickHouse, set the ceiling. Reporting a flattering throughput number in that situation
     * would be worse than reporting nothing, so the caveats are attached to the verdict itself
     * rather than buried in the detail.
     */
    private List<String> trustCaveats(List<RunReport.WorkloadSummary> workloads) {
        List<String> caveats = new ArrayList<>();
        for (RunReport.WorkloadSummary w : workloads) {
            if (w.overloadDrops() > 0) {
                caveats.add(w.id() + ": " + w.overloadDrops()
                        + " arrivals shed at the in-flight cap - the offered rate exceeded what completed, "
                        + "so throughput is a floor and latency is optimistic");
            }
            if (w.batchStarvations() > 0) {
                caveats.add(w.id() + ": " + w.batchStarvations()
                        + " batch starvations - data generation could not keep up, so the load generator "
                        + "limited this run rather than the database");
            }
            if (w.maxScheduleLagUs() > 250_000) {
                caveats.add(w.id() + ": scheduler fell up to "
                        + String.format("%.1fs", w.maxScheduleLagUs() / 1e6)
                        + " behind its arrival schedule - give the load generator more headroom before "
                        + "trusting these percentiles");
            }
        }
        if (!ctx.probeFailures().isEmpty()) {
            caveats.add("server-side probes failed and left blind spots: " + ctx.probeFailures().keySet());
        }
        return caveats;
    }

    private RunReport.Verdict buildVerdict(List<String> caveats) {
        List<SloResult> slos = ctx.sloResults();
        List<ReconResult> recon = ctx.reconResults();

        int sloPassed = (int) slos.stream().filter(SloResult::passed).count();
        int reconPassed = (int) recon.stream().filter(ReconResult::passed).count();

        // The worst breach by relative margin is the most useful thing to name first.
        String firstBreach = slos.stream()
                .filter(s -> !s.passed())
                .min(Comparator.comparingDouble(SloResult::headroomFraction))
                .map(s -> s.metric() + " = " + fmt(s.actual()) + " " + s.unit()
                        + " (limit " + s.thresholdRaw() + ")")
                .orElseGet(() -> recon.stream()
                        .filter(r -> !r.passed())
                        .findFirst()
                        .map(r -> "correctness check " + r.name() + ": expected " + fmt(r.expected())
                                + ", got " + fmt(r.actual()))
                        .orElse(null));

        double headroom = slos.stream()
                .filter(SloResult::resolved)
                .mapToDouble(SloResult::headroomFraction)
                .filter(d -> !Double.isNaN(d))
                .min()
                .orElse(Double.NaN);

        boolean passed = ctx.passed();
        String headline;
        if (!ctx.state().name().equals("COMPLETED")) {
            headline = "Run did not complete: " + (ctx.failureReason() == null ? ctx.state() : ctx.failureReason());
        } else if (passed && caveats.isEmpty()) {
            headline = String.format("PASS - all %d SLO(s) and %d correctness check(s) met, tightest margin %.0f%%.",
                    slos.size(), recon.size(), headroom * 100);
        } else if (passed) {
            headline = String.format("PASS with caveats - all %d SLO(s) met, but the measurement itself is "
                    + "not fully trustworthy (see caveats).", slos.size());
        } else {
            headline = String.format("FAIL - %d of %d SLO(s) and %d of %d correctness check(s) passed. First to give way: %s",
                    sloPassed, slos.size(), reconPassed, recon.size(), firstBreach);
        }

        return new RunReport.Verdict(passed, slos.size(), sloPassed, recon.size(), reconPassed,
                firstBreach, headline, headroom, caveats.isEmpty(), caveats);
    }

    private List<RunReport.WorkloadSummary> buildWorkloads() {
        List<RunReport.WorkloadSummary> out = new ArrayList<>();
        double seconds = measuredSeconds();

        for (Workload workload : ctx.scenario().workloads) {
            LatencyRecorder recorder = ctx.recorders().get(workload.id);
            if (recorder == null) {
                continue;
            }
            ArrivalScheduler scheduler = ctx.schedulers().get(workload.id);
            BatchFactory factory = ctx.batchFactories().get(workload.id);
            QueryDriver queryDriver = ctx.queryDrivers().get(workload.id);
            InsertDriver insertDriver = ctx.insertDrivers().get(workload.id);

            long rows = insertDriver != null ? insertDriver.rowsAcknowledged() : recorder.rows();
            long bytes = insertDriver != null ? insertDriver.bytesSent() : recorder.bytes();

            out.add(new RunReport.WorkloadSummary(
                    workload.id,
                    workload.kind,
                    recorder.ops(),
                    rows,
                    bytes,
                    recorder.errors(),
                    recorder.errorRate(),
                    recorder.overloadDrops(),
                    factory == null ? 0 : factory.starvations(),
                    scheduler == null ? 0 : scheduler.maxScheduleLagUs(),
                    seconds > 0 ? recorder.ops() / seconds : 0,
                    seconds > 0 ? rows / seconds : 0,
                    seconds > 0 ? bytes / seconds / (1024 * 1024) : 0,
                    latencyOf(recorder),
                    recorder.windows(),
                    queryDriver == null ? List.of() : breakdown(queryDriver)));
        }
        return out;
    }

    private List<RunReport.QueryBreakdown> breakdown(QueryDriver driver) {
        List<RunReport.QueryBreakdown> out = new ArrayList<>();
        Map<String, Long> rowsByRef = driver.perQueryRows();
        driver.perQueryRecorders().forEach((ref, recorder) -> out.add(new RunReport.QueryBreakdown(
                ref,
                recorder.ops(),
                rowsByRef.getOrDefault(ref, 0L),
                recorder.errors(),
                latencyOf(recorder))));
        out.sort(Comparator.comparingDouble((RunReport.QueryBreakdown q) -> q.latency().p99Ms()).reversed());
        return out;
    }

    private static RunReport.LatencyStats latencyOf(LatencyRecorder recorder) {
        var h = recorder.total();
        if (h.getTotalCount() == 0) {
            return new RunReport.LatencyStats(0, 0, 0, 0, 0, 0, 0);
        }
        return new RunReport.LatencyStats(
                h.getValueAtPercentile(50.0) / 1000.0,
                h.getValueAtPercentile(90.0) / 1000.0,
                h.getValueAtPercentile(95.0) / 1000.0,
                h.getValueAtPercentile(99.0) / 1000.0,
                h.getValueAtPercentile(99.9) / 1000.0,
                h.getMaxValue() / 1000.0,
                h.getMean() / 1000.0);
    }

    /** Groups raw samples into per-(metric, node, label) series and downsamples for charting. */
    private List<RunReport.SutSeries> buildSutSeries() {
        Map<String, List<MetricSample>> grouped = new TreeMap<>();
        for (MetricSample s : ctx.sutSamples()) {
            grouped.computeIfAbsent(s.metric() + " " + s.node() + " " + s.label(),
                    k -> new ArrayList<>()).add(s);
        }

        List<RunReport.SutSeries> out = new ArrayList<>();
        grouped.forEach((key, samples) -> {
            String[] parts = key.split(" ", -1);
            samples.sort(Comparator.comparingLong(MetricSample::tsMs));

            double min = samples.stream().mapToDouble(MetricSample::value).min().orElse(0);
            double max = samples.stream().mapToDouble(MetricSample::value).max().orElse(0);
            double avg = samples.stream().mapToDouble(MetricSample::value).average().orElse(0);
            double last = samples.get(samples.size() - 1).value();

            out.add(new RunReport.SutSeries(parts[0], parts[1], parts[2], min, max, avg, last,
                    downsample(samples)));
        });
        return out;
    }

    /** Keeps peaks rather than averaging them away: the spike is the signal in a part-count series. */
    private static List<RunReport.SutSeries.Point> downsample(List<MetricSample> samples) {
        if (samples.size() <= MAX_POINTS_PER_SERIES) {
            return samples.stream()
                    .map(s -> new RunReport.SutSeries.Point(s.tsMs(), s.value()))
                    .toList();
        }
        int bucketSize = (int) Math.ceil(samples.size() / (double) MAX_POINTS_PER_SERIES);
        List<RunReport.SutSeries.Point> out = new ArrayList<>();
        for (int i = 0; i < samples.size(); i += bucketSize) {
            List<MetricSample> bucket = samples.subList(i, Math.min(samples.size(), i + bucketSize));
            MetricSample peak = bucket.stream()
                    .max(Comparator.comparingDouble(MetricSample::value))
                    .orElse(bucket.get(0));
            out.add(new RunReport.SutSeries.Point(bucket.get(0).tsMs(), peak.value()));
        }
        return out;
    }

    private List<String> buildNotes() {
        List<String> notes = new ArrayList<>();
        notes.add("Load was generated open loop: latency is measured from each operation's intended "
                + "arrival time, not from when it was actually sent, so time lost to server stalls is "
                + "included rather than omitted.");
        notes.add("Percentiles are computed by merging per-window HdrHistograms, never by averaging "
                + "per-window percentiles.");
        if (!"none".equalsIgnoreCase(ctx.scenario().cachePolicy)) {
            notes.add("Cache policy '" + ctx.scenario().cachePolicy + "'. " + CacheController.describeColdProcedure());
        }
        notes.add("Server-side metrics were sampled from ClickHouse system tables during the run; this "
                + "sampling is itself a small load on the cluster.");
        if (ctx.measuredFromMs() > ctx.startedAtMs()) {
            notes.add("Headline figures cover the steady-state window only ("
                    + (ctx.measuredToMs() - ctx.measuredFromMs()) / 1000 + "s); warmup and ramp phases were excluded.");
        }
        return notes;
    }

    private double measuredSeconds() {
        long from = ctx.measuredFromMs();
        long to = ctx.measuredToMs();
        if (from == 0 || to <= from) {
            return Math.max(1, ctx.durationMs()) / 1000.0;
        }
        return (to - from) / 1000.0;
    }

    private static String fmt(double v) {
        if (Double.isNaN(v)) {
            return "n/a";
        }
        return Math.abs(v) >= 1000 ? String.format("%,.0f", v) : String.format("%.2f", v);
    }

    /** Convenience for the API layer, which wants a stable ordering of metric names. */
    public static Map<String, String> environmentOrdered(Map<String, String> env) {
        return new LinkedHashMap<>(new TreeMap<>(env));
    }
}
