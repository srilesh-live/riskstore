package com.riskstore.perf.scenario;

/**
 * A single pass/fail assertion evaluated against the run's metrics.
 *
 * <p>Metric names are dotted paths resolved by {@code verify/MetricResolver}, for example
 * {@code insert.l0-rates-atlas.p99}, {@code sut.MaxPartCountForPartition.max} or
 * {@code errors.rate}. Durations may be given as ISO-8601 ({@code PT2S}) or as a bare number
 * of milliseconds.
 */
public class SloSpec {

    public String metric;
    /** {@code lte}, {@code lt}, {@code gte}, {@code gt} or {@code eq}. */
    public String op = "lte";
    public String value;

    public String id() {
        return metric + " " + op + " " + value;
    }
}
