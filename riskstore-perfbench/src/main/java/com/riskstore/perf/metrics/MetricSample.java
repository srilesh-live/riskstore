package com.riskstore.perf.metrics;

/**
 * One scraped server-side observation.
 *
 * <p>{@code node} is carried on every sample because the facts that matter most here are
 * per-replica, not per-cluster: replication lag, part counts and disk headroom differ between
 * replicas, and an average across two replicas hides the one that is about to fall over.
 *
 * <p>{@code label} qualifies the metric within its family - typically {@code database.table} for
 * part and replication metrics, and empty for scalar server metrics.
 */
public record MetricSample(
        long tsMs,
        String node,
        String metric,
        String label,
        double value) {

    public static MetricSample of(long tsMs, String node, String metric, double value) {
        return new MetricSample(tsMs, node, metric, "", value);
    }

    public String key() {
        return label.isEmpty() ? metric : metric + "{" + label + "}";
    }
}
