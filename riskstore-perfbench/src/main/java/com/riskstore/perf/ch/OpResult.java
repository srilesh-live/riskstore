package com.riskstore.perf.ch;

/**
 * Outcome of one driven operation.
 *
 * <p>{@code serverTimeMs} is ClickHouse's own view of how long it spent, taken from the response
 * summary. Comparing it with the harness-side latency separates server cost from queueing,
 * network and client cost, which is the first question anyone asks about a slow p99.
 */
public record OpResult(
        String queryId,
        boolean success,
        long rows,
        long bytes,
        long serverTimeMs,
        Throwable error) {

    public static OpResult ok(String queryId, long rows, long bytes, long serverTimeMs) {
        return new OpResult(queryId, true, rows, bytes, serverTimeMs, null);
    }

    public static OpResult failed(String queryId, Throwable error) {
        return new OpResult(queryId, false, 0, 0, 0, error);
    }
}
