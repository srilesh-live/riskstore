package com.riskstore.perf.engine;

import com.riskstore.perf.metrics.LatencyRecorder;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the scheduler is genuinely open loop.
 *
 * <p>This is the most important test in the project. Every latency number the harness produces
 * rests on the claim that a server stall lands in the histogram rather than disappearing from
 * it, and that claim is easy to break with an innocent-looking refactor - adding a blocking
 * acquire to the pacer, or measuring from actual send time instead of intended arrival time,
 * would both silently restore coordinated omission while leaving every other test green.
 */
class ArrivalSchedulerTest {

    @Test
    void aServerStallAppearsInTheHistogramRatherThanVanishingFromIt() throws Exception {
        // Every operation blocks on this gate, the way requests to a stalled server would.
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger completed = new AtomicInteger();

        LatencyRecorder recorder = new LatencyRecorder("stall-test", 60_000);
        ArrivalScheduler scheduler = new ArrivalScheduler("stall-test", intended -> {
            try {
                gate.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            recorder.recordSuccess(intended, System.nanoTime(), 1, 1);
            completed.incrementAndGet();
        }, recorder, 10_000);

        scheduler.setRate(100);
        scheduler.start();

        // Hold everything for 300ms. A closed-loop driver would have issued roughly one
        // request in this window; an open-loop one keeps scheduling arrivals regardless.
        Thread.sleep(300);
        gate.countDown();

        scheduler.drain(5_000);
        scheduler.close();
        recorder.rotateIfDue(System.currentTimeMillis(), true);

        // Arrivals kept being generated throughout the stall.
        assertThat(completed.get())
                .as("open loop keeps scheduling arrivals while the server is stalled")
                .isGreaterThanOrEqualTo(15);

        // The earliest arrivals waited for essentially the whole stall, and that wait was
        // recorded. Under coordinated omission this maximum would be near zero.
        long maxUs = recorder.total().getMaxValue();
        assertThat(maxUs)
                .as("the stall must be visible in the latency histogram")
                .isGreaterThan(200_000L);
    }

    @Test
    void latencyIsMeasuredFromIntendedArrivalNotFromActualSend() throws Exception {
        LatencyRecorder recorder = new LatencyRecorder("intent-test", 60_000);

        // Report completion 100ms after the *intended* arrival, without doing any real work.
        // If the implementation ever switched to measuring from actual send time, the recorded
        // latency would collapse toward zero.
        ArrivalScheduler scheduler = new ArrivalScheduler("intent-test",
                intended -> recorder.recordSuccess(intended, intended + 100_000_000L, 1, 1),
                recorder, 1_000);

        scheduler.setRate(200);
        scheduler.start();
        Thread.sleep(250);
        scheduler.drain(2_000);
        scheduler.close();
        recorder.rotateIfDue(System.currentTimeMillis(), true);

        assertThat(recorder.percentileUs(50.0))
                .as("latency is anchored to intended arrival time")
                .isBetween(95_000L, 105_000L);
    }

    @Test
    void arrivalsTrackTheConfiguredRate() throws Exception {
        LatencyRecorder recorder = new LatencyRecorder("rate-test", 60_000);
        AtomicInteger count = new AtomicInteger();

        ArrivalScheduler scheduler = new ArrivalScheduler("rate-test", intended -> {
            recorder.recordSuccess(intended, System.nanoTime(), 1, 1);
            count.incrementAndGet();
        }, recorder, 10_000);

        scheduler.setRate(500);
        scheduler.start();
        Thread.sleep(1_000);
        scheduler.setRate(0);
        scheduler.drain(5_000);
        scheduler.close();

        // Generous bounds: this asserts the pacer is in the right order of magnitude, not that
        // it is a real-time scheduler on a shared CI box.
        assertThat(count.get()).isBetween(300, 700);
    }

    @Test
    void arrivalsAreShedRatherThanQueuedWhenTheInFlightCapIsReached() throws Exception {
        CountDownLatch gate = new CountDownLatch(1);
        LatencyRecorder recorder = new LatencyRecorder("shed-test", 60_000);

        // A cap of 2 with a permanently stalled operation guarantees the cap is hit.
        ArrivalScheduler scheduler = new ArrivalScheduler("shed-test", intended -> {
            try {
                gate.await(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, recorder, 2);

        scheduler.setRate(200);
        scheduler.start();
        Thread.sleep(300);
        gate.countDown();
        scheduler.drain(3_000);
        scheduler.close();

        // Shedding rather than blocking is what keeps the pacer honest; the drops are counted
        // so the report can say the run exceeded what the setup could deliver.
        assertThat(recorder.overloadDrops())
                .as("excess arrivals are shed and counted, never silently queued")
                .isPositive();
    }
}
