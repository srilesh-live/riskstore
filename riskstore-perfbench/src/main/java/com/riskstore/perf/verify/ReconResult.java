package com.riskstore.perf.verify;

/**
 * Outcome of one post-run correctness check.
 *
 * <p>These exist because a performance test that silently loses rows reports success while
 * having proved nothing. Throughput is only meaningful alongside evidence that the rows arrived,
 * arrived once, and are visible identically on both replicas.
 */
public record ReconResult(
        String name,
        double expected,
        double actual,
        double tolerance,
        boolean passed,
        String detail) {

    public static ReconResult of(String name, double expected, double actual, double tolerance, String detail) {
        double allowed = Math.abs(expected) * tolerance;
        boolean ok = Math.abs(expected - actual) <= allowed;
        return new ReconResult(name, expected, actual, tolerance, ok, detail);
    }

    public static ReconResult error(String name, String detail) {
        return new ReconResult(name, Double.NaN, Double.NaN, 0, false, detail);
    }

    public double delta() {
        return actual - expected;
    }

    public double deltaFraction() {
        return expected == 0 ? 0 : (actual - expected) / expected;
    }
}
