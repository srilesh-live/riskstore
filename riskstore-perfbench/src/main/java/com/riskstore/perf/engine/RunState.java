package com.riskstore.perf.engine;

/** Lifecycle of a single run, surfaced verbatim on the status endpoint. */
public enum RunState {
    PENDING,
    PREFLIGHT,
    WARMING,
    RUNNING,
    DRAINING,
    HARVESTING,
    VERIFYING,
    REPORTING,
    COMPLETED,
    FAILED,
    ABORTED;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == ABORTED;
    }
}
