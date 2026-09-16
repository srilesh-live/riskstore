package com.riskstore.perf.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Groups failures by cause so the report can say what broke, not just how often.
 *
 * <p>ClickHouse exception text carries per-occurrence detail (part names, byte counts), so the
 * text is normalised down to a stable key before counting. Without that, a thousand
 * TOO_MANY_PARTS failures look like a thousand distinct problems.
 */
public final class ErrorCounter {

    private final Map<String, LongAdder> counts = new ConcurrentHashMap<>();
    private final Map<String, String> samples = new ConcurrentHashMap<>();

    public void record(Throwable t) {
        String key = classify(t);
        counts.computeIfAbsent(key, k -> new LongAdder()).increment();
        samples.putIfAbsent(key, rootMessage(t));
    }

    /**
     * Reduce a throwable to a stable bucket key.
     *
     * <p>ClickHouse server errors arrive as {@code Code: 252. DB::Exception: Too many parts ...};
     * the numeric code is the stable part and the prose is not.
     */
    static String classify(Throwable t) {
        String msg = rootMessage(t);
        int codeIdx = msg.indexOf("Code: ");
        if (codeIdx >= 0) {
            int dot = msg.indexOf('.', codeIdx + 6);
            if (dot > codeIdx) {
                String code = msg.substring(codeIdx + 6, dot).trim();
                return "clickhouse:" + code + name(msg);
            }
        }
        Throwable root = root(t);
        return root.getClass().getSimpleName();
    }

    /** Pull the DB::Exception symbolic name when present, e.g. {@code (TOO_MANY_PARTS)}. */
    private static String name(String msg) {
        int open = msg.indexOf('(', msg.indexOf("DB::Exception") + 1);
        if (open > 0) {
            int close = msg.indexOf(')', open);
            if (close > open && close - open < 64) {
                return ":" + msg.substring(open + 1, close);
            }
        }
        return "";
    }

    private static Throwable root(Throwable t) {
        Throwable cur = t;
        while (cur.getCause() != null && cur.getCause() != cur) {
            cur = cur.getCause();
        }
        return cur;
    }

    private static String rootMessage(Throwable t) {
        Throwable r = root(t);
        String m = r.getMessage();
        return m == null ? r.getClass().getName() : m;
    }

    /** Failure counts by cause, highest first. */
    public Map<String, Long> snapshot() {
        Map<String, Long> out = new LinkedHashMap<>();
        counts.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue().sum(), a.getValue().sum()))
                .forEach(e -> out.put(e.getKey(), e.getValue().sum()));
        return out;
    }

    /** One representative message per bucket, for the report. */
    public Map<String, String> samples() {
        return Map.copyOf(samples);
    }

    public long total() {
        return counts.values().stream().mapToLong(LongAdder::sum).sum();
    }
}
