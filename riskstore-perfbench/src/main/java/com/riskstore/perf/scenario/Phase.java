package com.riskstore.perf.scenario;

import com.fasterxml.jackson.annotation.JsonAlias;

import java.time.Duration;

/**
 * One segment of the load profile. Phases run in declaration order.
 *
 * <p>{@code hold} drives a constant arrival rate; {@code ramp} interpolates linearly from
 * {@code from} to {@code to}. Ramp and warmup phases are normally excluded from headline
 * metrics so that steady-state numbers are not diluted by the approach to steady state.
 */
public class Phase {

    public String id;
    /** {@code hold} or {@code ramp}. */
    public String type = "hold";

    /** Aggregate arrival rate for a {@code hold} phase, in the workload's own rate unit. */
    public Double rate;
    /** Start rate for a {@code ramp} phase. */
    public Double from;
    /** End rate for a {@code ramp} phase. */
    public Double to;

    /** ISO-8601, e.g. {@code PT30M}. Accepted as either {@code for} or {@code over}. */
    @JsonAlias({"for", "over"})
    public Duration duration;

    public boolean excludeFromMetrics = false;

    public boolean isRamp() {
        return "ramp".equalsIgnoreCase(type);
    }

    /** Rate at a given fraction (0..1) of the way through this phase. */
    public double rateAt(double progress) {
        if (isRamp()) {
            double f = from == null ? 0 : from;
            double t = to == null ? f : to;
            return f + (t - f) * Math.max(0, Math.min(1, progress));
        }
        return rate == null ? 0 : rate;
    }

    public double peakRate() {
        return isRamp() ? Math.max(from == null ? 0 : from, to == null ? 0 : to) : (rate == null ? 0 : rate);
    }
}
