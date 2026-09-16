package com.riskstore.perf.metrics;

/**
 * One rotated latency window, already reduced to a compressed HdrHistogram payload.
 *
 * <p>Windows are stored encoded rather than as live {@code Histogram} objects: a sparse
 * compressed histogram is a few hundred bytes, whereas a live one is tens of kilobytes. Over a
 * seven-day soak at a ten-second window that is the difference between megabytes and gigabytes.
 *
 * <p>{@link #encoded} is a lossless base64 HdrHistogram, so the full distribution can be
 * recovered and re-merged later by any consumer of the results database.
 */
public record HistogramWindow(
        long startMs,
        long endMs,
        long count,
        long errors,
        long minUs,
        long maxUs,
        long p50Us,
        long p90Us,
        long p99Us,
        String encoded) {
}
