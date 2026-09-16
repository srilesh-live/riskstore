package com.riskstore.perf.report;

import com.riskstore.perf.engine.ChaosMark;
import com.riskstore.perf.metrics.HistogramWindow;
import com.riskstore.perf.verify.ReconResult;
import com.riskstore.perf.verify.SloResult;

import java.util.List;
import java.util.Map;

/**
 * The rendered result of a run, in the shape every output format reads from.
 *
 * <p>Built once and then serialised to JSON, Markdown and HTML, so the executive summary and the
 * detailed report can never disagree about a number.
 */
public record RunReport(
        Meta meta,
        Verdict verdict,
        List<WorkloadSummary> workloads,
        List<SloResult> slos,
        List<ReconResult> reconciliation,
        List<SutSeries> sut,
        List<ChaosMark> chaos,
        Map<String, Long> errors,
        Map<String, String> errorSamples,
        Map<String, String> environment,
        List<String> notes) {

    public record Meta(
            String runId,
            String scenarioId,
            String scenarioDescription,
            String scenarioHash,
            String state,
            String harnessVersion,
            long startedAtMs,
            long endedAtMs,
            long durationMs,
            long measuredFromMs,
            long measuredToMs,
            Map<String, String> tags,
            String baselineRunId) {
    }

    /**
     * The one-screen answer.
     *
     * <p>{@code firstBreach} names what gave way first, which is almost always the useful
     * finding: a cluster does not degrade uniformly, it hits one limit and everything downstream
     * of that limit follows.
     */
    public record Verdict(
            boolean passed,
            int sloTotal,
            int sloPassed,
            int reconTotal,
            int reconPassed,
            String firstBreach,
            String headline,
            double headroomFraction,
            boolean measurementTrustworthy,
            List<String> trustCaveats) {
    }

    public record WorkloadSummary(
            String id,
            String kind,
            long ops,
            long rows,
            long bytes,
            long errors,
            double errorRate,
            long overloadDrops,
            long batchStarvations,
            long maxScheduleLagUs,
            double opsPerSec,
            double rowsPerSec,
            double mbPerSec,
            LatencyStats latency,
            List<HistogramWindow> windows,
            List<QueryBreakdown> perQuery) {
    }

    public record LatencyStats(
            double p50Ms,
            double p90Ms,
            double p95Ms,
            double p99Ms,
            double p999Ms,
            double maxMs,
            double meanMs) {
    }

    public record QueryBreakdown(
            String ref,
            long ops,
            long rows,
            long errors,
            LatencyStats latency) {
    }

    /** One server-side metric as a time series, already reduced for charting. */
    public record SutSeries(
            String metric,
            String node,
            String label,
            double min,
            double max,
            double avg,
            double last,
            List<Point> points) {

        public record Point(long tsMs, double value) {
        }
    }
}
