package com.riskstore.perf.engine;

import com.riskstore.perf.ch.InsertSink;
import com.riskstore.perf.ch.OpResult;
import com.riskstore.perf.metrics.ErrorCounter;
import com.riskstore.perf.metrics.LatencyRecorder;
import com.riskstore.perf.scenario.Workload;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Drives one insert workload: take a pre-rendered batch, write it, record the outcome.
 *
 * <p>Latency is measured from the scheduler's intended arrival time through to the server's
 * acknowledgement, so queueing delay inside the harness counts against the result exactly as it
 * would count against the real ingestion service.
 *
 * <p>{@link #rowsOffered()} is tracked separately from rows the server reports as written. The
 * gap between the two is the raw material for reconciliation: it is the difference between what
 * the harness believes it sent and what ClickHouse believes it stored.
 */
public final class InsertDriver implements ArrivalScheduler.Operation {

    private static final long BATCH_WAIT_MS = 5_000;

    private final Workload workload;
    private final BatchFactory batches;
    private final InsertSink sink;
    private final LatencyRecorder recorder;
    private final ErrorCounter errors;

    private final AtomicLong rowsOffered = new AtomicLong();
    private final AtomicLong rowsAcknowledged = new AtomicLong();
    private final AtomicLong bytesSent = new AtomicLong();

    public InsertDriver(Workload workload,
                        BatchFactory batches,
                        InsertSink sink,
                        LatencyRecorder recorder,
                        ErrorCounter errors) {
        this.workload = workload;
        this.batches = batches;
        this.sink = sink;
        this.recorder = recorder;
        this.errors = errors;
    }

    @Override
    public void run(long intendedStartNanos) {
        BatchFactory.Batch batch = batches.take(BATCH_WAIT_MS);
        if (batch == null) {
            // Generation could not keep up. Counted as an overload drop rather than a server
            // error, because attributing it to ClickHouse would be a lie.
            recorder.recordOverloadDrop();
            return;
        }

        rowsOffered.addAndGet(batch.rows());
        bytesSent.addAndGet(batch.payload().length);

        OpResult result = sink.insert(workload, batch.payload(), batch.rows(), batch.dedupToken());
        long done = System.nanoTime();

        if (result.success()) {
            rowsAcknowledged.addAndGet(result.rows());
            recorder.recordSuccess(intendedStartNanos, done, result.rows(), batch.payload().length);
        } else {
            errors.record(result.error());
            recorder.recordFailure(intendedStartNanos, done);
        }
    }

    /** Converts a row-per-second target into the batch-per-second rate the scheduler paces. */
    public double opsPerSecondFor(double rowsPerSecond) {
        return rowsPerSecond / Math.max(1, batches.rowsPerBatch());
    }

    public long rowsOffered() {
        return rowsOffered.get();
    }

    public long rowsAcknowledged() {
        return rowsAcknowledged.get();
    }

    public long bytesSent() {
        return bytesSent.get();
    }

    public String workloadId() {
        return workload.id;
    }
}
