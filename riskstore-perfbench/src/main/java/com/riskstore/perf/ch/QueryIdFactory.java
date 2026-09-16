package com.riskstore.perf.ch;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Mints deterministic query ids of the form {@code <runId>:<workloadId>:<seq>}.
 *
 * <p>The run id leads so the whole run can be harvested from {@code system.query_log} with a
 * single {@code query_id LIKE 'run-...%'} predicate against the primary key prefix.
 *
 * <p>This is what makes client-side and server-side measurements joinable. Without it you can
 * say "p99 was 4 seconds"; with it you can say "p99 was 4 seconds and 3.6 of them were server
 * side, in a query that read 900M rows because the primary index was not used".
 */
public final class QueryIdFactory {

    private final String runId;
    private final AtomicLong seq = new AtomicLong();

    public QueryIdFactory(String runId) {
        this.runId = runId;
    }

    public String next(String workloadId) {
        return runId + ":" + workloadId + ":" + seq.incrementAndGet();
    }

    /** Prefix for the {@code LIKE} predicate used by the query-log harvester. */
    public String runPrefix() {
        return runId + ":";
    }

    public String runId() {
        return runId;
    }

    public long issued() {
        return seq.get();
    }
}
