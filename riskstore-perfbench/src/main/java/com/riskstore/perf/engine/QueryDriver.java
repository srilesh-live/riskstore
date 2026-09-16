package com.riskstore.perf.engine;

import com.riskstore.perf.ch.OpResult;
import com.riskstore.perf.ch.QueryExecutor;
import com.riskstore.perf.ch.SqlLibrary;
import com.riskstore.perf.metrics.ErrorCounter;
import com.riskstore.perf.metrics.LatencyRecorder;
import com.riskstore.perf.scenario.Workload;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

/**
 * Drives one query workload, selecting between statements by weight.
 *
 * <p>Per-statement latency is tracked in its own recorder as well as in the workload-level one.
 * A single blended p99 across a mixed report workload is close to useless: it tells you the
 * workload is slow without telling you which of the five reports is dragging it, and those are
 * the numbers an SLO is actually written against.
 */
public final class QueryDriver implements ArrivalScheduler.Operation {

    private final Workload workload;
    private final QueryExecutor executor;
    private final LatencyRecorder recorder;
    private final ErrorCounter errors;

    private final List<String> refs = new ArrayList<>();
    private final List<String> statements = new ArrayList<>();
    private final double[] cumulativeWeights;
    private final Map<String, LatencyRecorder> perQuery = new LinkedHashMap<>();
    private final Map<String, LongAdder> perQueryRows = new LinkedHashMap<>();

    public QueryDriver(Workload workload,
                       SqlLibrary sql,
                       QueryExecutor executor,
                       LatencyRecorder recorder,
                       ErrorCounter errors,
                       long latencyWindowMs) {
        this.workload = workload;
        this.executor = executor;
        this.recorder = recorder;
        this.errors = errors;

        double running = 0;
        List<Double> cumulative = new ArrayList<>();
        for (Workload.QueryRef q : workload.queries) {
            refs.add(q.ref);
            statements.add(sql.get(q.ref));
            running += Math.max(0, q.weight);
            cumulative.add(running);
            perQuery.put(q.ref, new LatencyRecorder(workload.id + "." + q.ref, latencyWindowMs));
            perQueryRows.put(q.ref, new LongAdder());
        }
        if (running <= 0) {
            throw new IllegalArgumentException("Query workload " + workload.id + " has zero total weight");
        }
        this.cumulativeWeights = new double[cumulative.size()];
        for (int i = 0; i < cumulative.size(); i++) {
            cumulativeWeights[i] = cumulative.get(i) / running;
        }
    }

    @Override
    public void run(long intendedStartNanos) {
        int idx = pick();
        String ref = refs.get(idx);

        OpResult result = executor.run(workload.id, statements.get(idx), workload.settings);
        long done = System.nanoTime();

        LatencyRecorder queryRecorder = perQuery.get(ref);
        if (result.success()) {
            recorder.recordSuccess(intendedStartNanos, done, result.rows(), result.bytes());
            queryRecorder.recordSuccess(intendedStartNanos, done, result.rows(), result.bytes());
            perQueryRows.get(ref).add(result.rows());
        } else {
            errors.record(result.error());
            recorder.recordFailure(intendedStartNanos, done);
            queryRecorder.recordFailure(intendedStartNanos, done);
        }
    }

    private int pick() {
        double r = ThreadLocalRandom.current().nextDouble();
        for (int i = 0; i < cumulativeWeights.length; i++) {
            if (r <= cumulativeWeights[i]) {
                return i;
            }
        }
        return cumulativeWeights.length - 1;
    }

    /** Rotates the per-statement histograms alongside the workload-level one. */
    public void rotate(long nowMs, boolean force) {
        perQuery.values().forEach(r -> r.rotateIfDue(nowMs, force));
    }

    public Map<String, LatencyRecorder> perQueryRecorders() {
        return Map.copyOf(perQuery);
    }

    public Map<String, Long> perQueryRows() {
        Map<String, Long> out = new LinkedHashMap<>();
        perQueryRows.forEach((k, v) -> out.put(k, v.sum()));
        return out;
    }

    public String workloadId() {
        return workload.id;
    }
}
