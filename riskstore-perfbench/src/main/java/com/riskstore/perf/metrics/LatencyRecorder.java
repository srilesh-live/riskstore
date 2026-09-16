package com.riskstore.perf.metrics;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Latency and throughput accounting for a single workload.
 *
 * <p>Two properties matter here and both are deliberate:
 *
 * <ol>
 *   <li><b>Latency is recorded against intended send time.</b> The caller passes the arrival
 *       timestamp the scheduler planned, not the moment the request actually left. When the
 *       server stalls and the driver falls behind, that lost time lands in the histogram
 *       instead of vanishing. This is the coordinated-omission fix.
 *   <li><b>Percentiles are merged, never averaged.</b> A live accumulator histogram is kept
 *       alongside the per-window encodings, so the global p99.9 is computed from the union of
 *       all samples. Averaging per-window percentiles is arithmetically meaningless and is the
 *       single most common way benchmark reports end up wrong.
 * </ol>
 */
public final class LatencyRecorder {

    /** 600s ceiling at 2 significant digits: ample for a stalled cluster, cheap to keep live. */
    private static final long MAX_TRACKABLE_US = 600_000_000L;
    private static final int SIGNIFICANT_DIGITS = 2;

    private final String workloadId;
    private final long windowMs;

    private final Recorder recorder = new Recorder(MAX_TRACKABLE_US, SIGNIFICANT_DIGITS);
    private final Histogram accumulated = new Histogram(MAX_TRACKABLE_US, SIGNIFICANT_DIGITS);
    private final List<HistogramWindow> windows = Collections.synchronizedList(new ArrayList<>());

    private final LongAdder ops = new LongAdder();
    private final LongAdder rows = new LongAdder();
    private final LongAdder bytes = new LongAdder();
    private final LongAdder errors = new LongAdder();
    /** Arrivals shed because the in-flight cap was hit. Always surfaced: it invalidates the run. */
    private final LongAdder overloadDrops = new LongAdder();

    private final AtomicLong windowStartMs;
    private final AtomicLong windowErrorBase = new AtomicLong();

    private Histogram recycled;

    public LatencyRecorder(String workloadId, long windowMs) {
        this.workloadId = workloadId;
        this.windowMs = windowMs;
        this.windowStartMs = new AtomicLong(System.currentTimeMillis());
    }

    /**
     * Record one completed operation.
     *
     * @param intendedStartNanos the arrival time the scheduler planned, not the actual send time
     * @param completionNanos    {@code System.nanoTime()} at completion
     */
    public void recordSuccess(long intendedStartNanos, long completionNanos, long rowCount, long byteCount) {
        long latencyUs = Math.max(0, (completionNanos - intendedStartNanos) / 1_000L);
        recorder.recordValue(Math.min(latencyUs, MAX_TRACKABLE_US));
        ops.increment();
        rows.add(rowCount);
        bytes.add(byteCount);
    }

    /** A failed operation still costs wall-clock time, so its latency counts too. */
    public void recordFailure(long intendedStartNanos, long completionNanos) {
        long latencyUs = Math.max(0, (completionNanos - intendedStartNanos) / 1_000L);
        recorder.recordValue(Math.min(latencyUs, MAX_TRACKABLE_US));
        ops.increment();
        errors.increment();
    }

    public void recordOverloadDrop() {
        overloadDrops.increment();
    }

    /**
     * Close the current window if it is due. Called from the sampler thread only.
     *
     * @param force rotate regardless of elapsed time, used at end of run to capture the tail
     */
    public synchronized void rotateIfDue(long nowMs, boolean force) {
        long start = windowStartMs.get();
        if (!force && nowMs - start < windowMs) {
            return;
        }
        recycled = recorder.getIntervalHistogram(recycled);
        if (recycled.getTotalCount() > 0) {
            accumulated.add(recycled);
            long errsNow = errors.sum();
            long windowErrors = errsNow - windowErrorBase.getAndSet(errsNow);
            windows.add(new HistogramWindow(
                    start,
                    nowMs,
                    recycled.getTotalCount(),
                    windowErrors,
                    recycled.getMinValue(),
                    recycled.getMaxValue(),
                    recycled.getValueAtPercentile(50.0),
                    recycled.getValueAtPercentile(90.0),
                    recycled.getValueAtPercentile(99.0),
                    encode(recycled)));
        }
        windowStartMs.set(nowMs);
    }

    private static String encode(Histogram h) {
        ByteBuffer buf = ByteBuffer.allocate(h.getNeededByteBufferCapacity());
        int len = h.encodeIntoCompressedByteBuffer(buf);
        byte[] out = new byte[len];
        buf.rewind();
        buf.get(out, 0, len);
        return Base64.getEncoder().encodeToString(out);
    }

    /**
     * Merged distribution across every rotated window.
     *
     * <p>Call {@link #rotateIfDue(long, boolean)} with {@code force=true} first, or the final
     * partial window is missing from the result.
     */
    public synchronized Histogram total() {
        return accumulated.copy();
    }

    public synchronized long percentileUs(double percentile) {
        return accumulated.getValueAtPercentile(percentile);
    }

    public List<HistogramWindow> windows() {
        synchronized (windows) {
            return List.copyOf(windows);
        }
    }

    public String workloadId() {
        return workloadId;
    }

    public long ops() {
        return ops.sum();
    }

    public long rows() {
        return rows.sum();
    }

    public long bytes() {
        return bytes.sum();
    }

    public long errors() {
        return errors.sum();
    }

    public long overloadDrops() {
        return overloadDrops.sum();
    }

    public double errorRate() {
        long total = ops.sum();
        return total == 0 ? 0.0 : (double) errors.sum() / total;
    }
}
