package com.riskstore.perf.verify;

import com.riskstore.perf.engine.RunContext;
import com.riskstore.perf.scenario.SloSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Turns the scenario's SLO list into a pass/fail scorecard.
 *
 * <p>Thresholds accept ISO-8601 durations ({@code PT2S}) as well as bare numbers, because a
 * latency SLO reads better as a duration and a part-count SLO reads better as an integer.
 * Durations normalise to milliseconds, matching {@link MetricResolver}'s latency unit.
 */
public final class SloEvaluator {

    private static final Logger log = LoggerFactory.getLogger(SloEvaluator.class);

    private final MetricResolver resolver;

    public SloEvaluator(RunContext ctx) {
        this.resolver = new MetricResolver(ctx);
    }

    public List<SloResult> evaluate(List<SloSpec> specs) {
        List<SloResult> results = new ArrayList<>();
        for (SloSpec spec : specs) {
            results.add(evaluateOne(spec));
        }
        return results;
    }

    private SloResult evaluateOne(SloSpec spec) {
        Double threshold = parseThreshold(spec.value);
        if (threshold == null) {
            return SloResult.unresolved(spec.id(), spec.metric, spec.op, spec.value,
                    "could not parse threshold: " + spec.value);
        }

        Optional<MetricResolver.Resolved> resolved = resolver.resolve(spec.metric);
        if (resolved.isEmpty()) {
            log.warn("SLO metric '{}' could not be resolved - counting as a failure", spec.metric);
            return SloResult.unresolved(spec.id(), spec.metric, spec.op, spec.value,
                    "metric unavailable (no data, or the probe that supplies it failed)");
        }

        double actual = resolved.get().value();
        boolean passed = compare(actual, spec.op, threshold);
        return new SloResult(spec.id(), spec.metric, spec.op, spec.value, threshold, actual,
                resolved.get().unit(), true, passed, passed ? "ok" : "breached");
    }

    private static boolean compare(double actual, String op, double threshold) {
        return switch (op) {
            case "lte" -> actual <= threshold;
            case "lt" -> actual < threshold;
            case "gte" -> actual >= threshold;
            case "gt" -> actual > threshold;
            case "eq" -> Math.abs(actual - threshold) < 1e-9;
            default -> false;
        };
    }

    /** ISO-8601 durations become milliseconds; everything else is taken as a plain number. */
    public static Double parseThreshold(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.startsWith("PT") || trimmed.startsWith("pt") || trimmed.startsWith("P")) {
            try {
                return (double) Duration.parse(trimmed.toUpperCase()).toMillis();
            } catch (Exception ignored) {
                // fall through to numeric parsing
            }
        }
        try {
            return Double.parseDouble(trimmed);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
