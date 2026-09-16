package com.riskstore.perf.engine;

import com.riskstore.perf.metrics.LatencyRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Open-loop, constant-arrival-rate work scheduler. This is the heart of the harness.
 *
 * <h2>Why open loop</h2>
 *
 * <p>The obvious way to generate load is a pool of N threads each looping "send a request, wait
 * for the reply, send the next". That is a <em>closed-loop</em> driver and it systematically
 * lies about latency. When the server stalls for two seconds, a closed-loop driver stops
 * sending; the requests that should have been issued during the stall are never issued, so they
 * never appear in the histogram. The measured p99 reflects only the requests lucky enough to be
 * sent while the server was healthy. This is Gil Tene's <em>coordinated omission</em>, and it
 * routinely understates tail latency by an order of magnitude.
 *
 * <p>This scheduler instead computes arrival times from a virtual clock, independent of
 * completions. Arrival <i>k</i> is due at {@code start + k * (1e9 / rate)} nanoseconds whether
 * or not arrival <i>k-1</i> has finished. Each dispatched operation is handed its
 * <b>intended</b> start time, and {@link LatencyRecorder} measures from that instant rather
 * than from when the request actually left. Time lost to a stall therefore lands in the
 * histogram where it belongs.
 *
 * <h2>Back pressure without lying</h2>
 *
 * <p>If the server stops completing work, in-flight operations pile up. Blocking the pacer at
 * that point would silently reintroduce coordinated omission, so instead arrivals beyond
 * {@code maxInFlight} are <em>shed</em> and counted as overload drops. A run with a non-zero
 * drop count has exceeded what the setup can deliver, and the report says so rather than
 * quietly reporting a flattering number.
 *
 * <p>Two distinct failure modes are tracked separately, because they have opposite remedies:
 * <ul>
 *   <li>{@code overloadDrops} &gt; 0 with high in-flight - the <b>server</b> cannot keep up.
 *   <li>{@code maxScheduleLagUs} large with low in-flight - the <b>harness</b> cannot keep up,
 *       and the load-generator host needs more headroom before the numbers mean anything.
 * </ul>
 */
public final class ArrivalScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ArrivalScheduler.class);

    /** Cap on arrivals dispatched per wake-up, so catch-up cannot monopolise the pacer thread. */
    private static final int MAX_BURST = 4_096;
    /** Above this remaining time we park; below it we spin. Park granularity is ~1ms on most kernels. */
    private static final long SPIN_THRESHOLD_NANOS = 2_000_000L;

    /** One unit of work, handed the arrival time the scheduler planned for it. */
    @FunctionalInterface
    public interface Operation {
        void run(long intendedStartNanos);
    }

    private final String workloadId;
    private final Operation operation;
    private final LatencyRecorder recorder;
    private final ExecutorService workers;
    private final Semaphore inFlight;
    private final int maxInFlight;

    private final AtomicLong maxScheduleLagNanos = new AtomicLong();
    private final AtomicLong dispatched = new AtomicLong();

    private volatile double rate;
    private volatile boolean running;
    private Thread pacer;

    public ArrivalScheduler(String workloadId, Operation operation, LatencyRecorder recorder, int maxInFlight) {
        this.workloadId = workloadId;
        this.operation = operation;
        this.recorder = recorder;
        this.maxInFlight = maxInFlight;
        this.inFlight = new Semaphore(maxInFlight);
        this.workers = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("op-" + workloadId + "-", 0).factory());
    }

    public void start() {
        running = true;
        pacer = new Thread(this::pace, "pacer-" + workloadId);
        // A platform thread, not virtual: the pacer must not be descheduled by the very
        // virtual-thread scheduler whose work it is timing.
        pacer.setPriority(Thread.MAX_PRIORITY);
        pacer.setDaemon(true);
        pacer.start();
    }

    /** Set the target arrival rate in operations per second. Zero idles the scheduler. */
    public void setRate(double opsPerSecond) {
        this.rate = Math.max(0, opsPerSecond);
    }

    public double rate() {
        return rate;
    }

    private void pace() {
        long nextIntended = System.nanoTime();
        while (running) {
            double r = rate;
            if (r <= 0) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                // Rebase so an idle period is not later replayed as a burst of overdue arrivals.
                nextIntended = System.nanoTime();
                continue;
            }

            long gap = Math.max(1L, (long) (1_000_000_000.0 / r));
            if (nextIntended > System.nanoTime()) {
                sleepUntil(nextIntended);
            }

            long now = System.nanoTime();
            long lag = now - nextIntended;
            if (lag > 0) {
                maxScheduleLagNanos.accumulateAndGet(lag, Math::max);
            }

            int burst = 0;
            while (running && nextIntended <= now && burst < MAX_BURST) {
                dispatch(nextIntended);
                nextIntended += gap;
                burst++;
            }
            // If the burst cap was hit, nextIntended is deliberately left behind wall clock.
            // Keeping the schedule is what makes the lateness visible in the latency histogram
            // rather than silently forgiven.
        }
    }

    private void dispatch(long intendedStartNanos) {
        if (!inFlight.tryAcquire()) {
            recorder.recordOverloadDrop();
            return;
        }
        dispatched.incrementAndGet();
        try {
            workers.execute(() -> {
                try {
                    operation.run(intendedStartNanos);
                } catch (Throwable t) {
                    log.debug("Operation threw in workload {}: {}", workloadId, t.toString());
                } finally {
                    inFlight.release();
                }
            });
        } catch (RuntimeException e) {
            inFlight.release();
            recorder.recordOverloadDrop();
        }
    }

    /** Park for the bulk of the wait, spin for the last millisecond where park is too coarse. */
    private static void sleepUntil(long targetNanos) {
        long remaining = targetNanos - System.nanoTime();
        while (remaining > 0) {
            if (remaining > SPIN_THRESHOLD_NANOS) {
                LockSupport.parkNanos(remaining - 1_000_000L);
            } else {
                Thread.onSpinWait();
            }
            remaining = targetNanos - System.nanoTime();
        }
    }

    /** Stop generating arrivals and wait for outstanding operations to finish. */
    public void drain(long timeoutMs) {
        running = false;
        if (pacer != null) {
            LockSupport.unpark(pacer);
            try {
                pacer.join(1_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            boolean quiet = inFlight.tryAcquire(maxInFlight, timeoutMs, TimeUnit.MILLISECONDS);
            if (!quiet) {
                log.warn("Workload {} still had operations in flight after {}ms", workloadId, timeoutMs);
            } else {
                inFlight.release(maxInFlight);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public long dispatched() {
        return dispatched.get();
    }

    public long maxScheduleLagUs() {
        return maxScheduleLagNanos.get() / 1_000L;
    }

    public int inFlightNow() {
        return maxInFlight - inFlight.availablePermits();
    }

    public String workloadId() {
        return workloadId;
    }

    @Override
    public void close() {
        running = false;
        workers.shutdown();
        try {
            if (!workers.awaitTermination(30, TimeUnit.SECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
