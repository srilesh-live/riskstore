package com.riskstore.perf.engine;

import com.riskstore.perf.gen.Generator;
import com.riskstore.perf.gen.SeededRandom;
import com.riskstore.perf.scenario.Workload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Renders insert batches ahead of time on background threads.
 *
 * <p>Generating a batch inside the measured operation would fold harness CPU time into the
 * reported insert latency, which at six-figure row rates is not a rounding error. Producers
 * therefore keep a small bounded queue topped up and the driver takes a ready batch.
 *
 * <p>Every batch is freshly generated rather than drawn from a recycled pool. Recycling would be
 * cheaper but would give ClickHouse repeated identical blocks, inflating the compression ratio
 * and making the storage projection optimistic.
 *
 * <p>Each batch carries a unique deduplication token. Without one, ReplicatedMergeTree
 * deduplicates on block checksum, and any accidentally identical block would be silently
 * discarded - which reconciliation would then correctly, and confusingly, report as data loss.
 *
 * <p>Queue depth is deliberately small: a 20,000-row Atlas batch is tens of megabytes, so depth
 * trades directly against load-generator heap. {@link #starvations()} counts how often the
 * driver found the queue empty; a non-zero value means the harness, not the database, set the
 * ceiling for that run.
 */
public final class BatchFactory implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BatchFactory.class);

    /** One rendered batch, ready to go on the wire. */
    public record Batch(byte[] payload, long rows, String dedupToken, long sequence) {
    }

    private final String workloadId;
    private final Generator generator;
    private final int rowsPerBatch;
    private final long scenarioSeed;
    private final String runId;

    private final BlockingQueue<Batch> ready;
    private final List<Thread> producers = new ArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong sequence = new AtomicLong();
    private final AtomicLong starvations = new AtomicLong();
    private final AtomicLong produced = new AtomicLong();

    public BatchFactory(String runId, Workload workload, Generator generator, long scenarioSeed, int producerCount) {
        this.runId = runId;
        this.workloadId = workload.id;
        this.generator = generator;
        this.rowsPerBatch = workload.batch.rows;
        this.scenarioSeed = scenarioSeed;
        this.ready = new ArrayBlockingQueue<>(Math.max(2, workload.batch.queueDepth));
        for (int i = 0; i < Math.max(1, producerCount); i++) {
            Thread t = new Thread(this::produce, "batchgen-" + workloadId + "-" + i);
            t.setDaemon(true);
            producers.add(t);
        }
    }

    public void start() {
        producers.forEach(Thread::start);
    }

    private void produce() {
        StringBuilder sb = new StringBuilder(rowsPerBatch * 512);
        while (running.get()) {
            long seq = sequence.incrementAndGet();
            SeededRandom rnd = SeededRandom.forBatch(scenarioSeed, workloadId, seq);
            sb.setLength(0);
            long baseRow = seq * rowsPerBatch;
            for (int i = 0; i < rowsPerBatch; i++) {
                generator.appendRow(sb, rnd, baseRow + i);
                sb.append('\n');
            }
            byte[] payload = sb.toString().getBytes(StandardCharsets.UTF_8);
            Batch batch = new Batch(payload, rowsPerBatch, runId + ":" + workloadId + ":" + seq, seq);
            try {
                // Blocks when the queue is full, which is the normal steady state: producers
                // should idle, not race ahead and consume heap.
                if (ready.offer(batch, 1, TimeUnit.SECONDS)) {
                    produced.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Take the next ready batch.
     *
     * @return null if generation could not keep up within the timeout, which the caller must
     *         record rather than retry, so that harness starvation stays visible
     */
    public Batch take(long timeoutMs) {
        try {
            Batch b = ready.poll(timeoutMs, TimeUnit.MILLISECONDS);
            if (b == null) {
                starvations.incrementAndGet();
            }
            return b;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /** Block until the queue has at least one batch, so a run does not start into an empty pipe. */
    public void awaitPrimed(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (ready.isEmpty() && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        if (ready.isEmpty()) {
            log.warn("Batch factory for {} was not primed within {}ms", workloadId, timeoutMs);
        }
    }

    public long starvations() {
        return starvations.get();
    }

    public long produced() {
        return produced.get();
    }

    public long rowsPerBatch() {
        return rowsPerBatch;
    }

    @Override
    public void close() {
        running.set(false);
        producers.forEach(Thread::interrupt);
        ready.clear();
    }
}
