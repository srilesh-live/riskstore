package com.riskstore.perf.verify;

import com.riskstore.perf.report.RunReport;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Compares a run against a baseline and decides whether it regressed.
 *
 * <p>Latency is compared on p99 rather than the mean, because the mean hides exactly the
 * behaviour a risk store cares about: the report that took eleven seconds at end of day.
 *
 * <p>Two guards keep the gate from crying wolf. Runs whose environment fingerprint differs are
 * flagged as incomparable rather than reported as regressions, since a settings change explains
 * any delta. And an absolute floor stops trivially fast operations tripping the threshold - a
 * p99 moving from 2ms to 3ms is a 50% regression by arithmetic and noise by any other measure.
 */
public final class BaselineComparator {

    /** Relative worsening beyond which a metric is called a regression. */
    private static final double REGRESSION_THRESHOLD = 0.10;
    /** Latency deltas below this are ignored regardless of percentage. */
    private static final double NOISE_FLOOR_MS = 5.0;
    /** Throughput deltas below this fraction are ignored. */
    private static final double THROUGHPUT_NOISE_FLOOR = 0.02;

    public record Delta(
            String metric,
            double baseline,
            double current,
            double changeFraction,
            String unit,
            boolean regression,
            boolean improvement) {
    }

    public record Comparison(
            String baselineRunId,
            String currentRunId,
            boolean comparable,
            String incomparableReason,
            boolean regressed,
            List<Delta> deltas) {
    }

    private BaselineComparator() {
    }

    public static Comparison compare(RunReport baseline, RunReport current) {
        String reason = comparabilityProblem(baseline, current);
        List<Delta> deltas = new ArrayList<>();

        Map<String, RunReport.WorkloadSummary> baseByIdMap = index(baseline);
        for (RunReport.WorkloadSummary cur : current.workloads()) {
            RunReport.WorkloadSummary base = baseByIdMap.get(cur.id());
            if (base == null) {
                continue;
            }
            deltas.add(latencyDelta(cur.id() + ".p99", base.latency().p99Ms(), cur.latency().p99Ms()));
            deltas.add(latencyDelta(cur.id() + ".p999", base.latency().p999Ms(), cur.latency().p999Ms()));
            deltas.add(throughputDelta(cur.id() + ".rowsPerSec", base.rowsPerSec(), cur.rowsPerSec()));
        }

        boolean regressed = deltas.stream().anyMatch(Delta::regression);
        return new Comparison(
                baseline.meta().runId(),
                current.meta().runId(),
                reason == null,
                reason,
                reason == null && regressed,
                deltas);
    }

    /** Higher is worse. */
    private static Delta latencyDelta(String metric, double baseline, double current) {
        double change = baseline == 0 ? 0 : (current - baseline) / baseline;
        boolean regression = change > REGRESSION_THRESHOLD && (current - baseline) > NOISE_FLOOR_MS;
        boolean improvement = change < -REGRESSION_THRESHOLD && (baseline - current) > NOISE_FLOOR_MS;
        return new Delta(metric, baseline, current, change, "ms", regression, improvement);
    }

    /** Lower is worse. */
    private static Delta throughputDelta(String metric, double baseline, double current) {
        double change = baseline == 0 ? 0 : (current - baseline) / baseline;
        boolean regression = change < -REGRESSION_THRESHOLD && Math.abs(change) > THROUGHPUT_NOISE_FLOOR;
        boolean improvement = change > REGRESSION_THRESHOLD;
        return new Delta(metric, baseline, current, change, "rows/s", regression, improvement);
    }

    /**
     * Reasons two runs cannot be meaningfully compared. Returning null means they can.
     *
     * <p>An untrustworthy current run is treated as incomparable rather than as a pass: if the
     * harness shed load or starved for data, its p99 is not a measurement of the database.
     */
    private static String comparabilityProblem(RunReport baseline, RunReport current) {
        if (!baseline.meta().scenarioHash().equals(current.meta().scenarioHash())) {
            return "scenario definition changed between the two runs (" + baseline.meta().scenarioHash()
                    + " vs " + current.meta().scenarioHash() + ")";
        }
        String baseFingerprint = baseline.environment().get("settings.fingerprint");
        String curFingerprint = current.environment().get("settings.fingerprint");
        if (baseFingerprint != null && curFingerprint != null && !baseFingerprint.equals(curFingerprint)) {
            return "server settings differ between the two runs (" + baseFingerprint
                    + " vs " + curFingerprint + ")";
        }
        if (!current.verdict().measurementTrustworthy()) {
            return "the current run carries measurement caveats, so its latencies do not describe the database";
        }
        return null;
    }

    private static Map<String, RunReport.WorkloadSummary> index(RunReport report) {
        Map<String, RunReport.WorkloadSummary> out = new LinkedHashMap<>();
        report.workloads().forEach(w -> out.put(w.id(), w));
        return out;
    }
}
