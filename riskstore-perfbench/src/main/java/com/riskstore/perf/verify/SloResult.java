package com.riskstore.perf.verify;

/**
 * Outcome of one SLO assertion.
 *
 * <p>{@code resolved} is false when the metric could not be computed at all - usually a probe
 * that a restrictive grant blocked, or a workload that produced no successful operations. That
 * is reported as a failure rather than a pass: an SLO nobody could evaluate has not been met.
 */
public record SloResult(
        String sloId,
        String metric,
        String op,
        String thresholdRaw,
        double threshold,
        double actual,
        String unit,
        boolean resolved,
        boolean passed,
        String detail) {

    public static SloResult unresolved(String sloId, String metric, String op, String thresholdRaw, String why) {
        return new SloResult(sloId, metric, op, thresholdRaw, Double.NaN, Double.NaN, "", false, false, why);
    }

    /** Signed headroom against the threshold, as a fraction. Negative means a breach. */
    public double headroomFraction() {
        if (!resolved || threshold == 0 || Double.isNaN(actual)) {
            return Double.NaN;
        }
        return switch (op) {
            case "lte", "lt" -> (threshold - actual) / threshold;
            case "gte", "gt" -> (actual - threshold) / threshold;
            default -> 0.0;
        };
    }
}
