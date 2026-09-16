package com.riskstore.perf.engine;

/**
 * A timestamped fault annotation on the run timeline.
 *
 * <p>The harness never injects faults itself. Granting a load generator the right to SIGKILL a
 * database replica or run {@code tc netem} on a production-like RHEL host is a security
 * conversation most sites will lose, and it is not needed: an operator runs the fault from the
 * playbook and posts a mark, and the harness correlates it against everything it is already
 * recording.
 *
 * <p>{@code source} distinguishes {@code operator} marks from {@code auto} ones. The scraper
 * raises its own marks when it sees a replica go read-only or lose its Keeper session, so the
 * timeline stays correct even when the operator forgets to post, and so an unplanned fault
 * during an otherwise clean run cannot pass unnoticed.
 */
public record ChaosMark(
        long tsMs,
        String kind,
        String target,
        String note,
        String source) {

    public static ChaosMark operator(String kind, String target, String note) {
        return new ChaosMark(System.currentTimeMillis(), kind, target, note == null ? "" : note, "operator");
    }

    public static ChaosMark auto(String kind, String target, String note) {
        return new ChaosMark(System.currentTimeMillis(), kind, target, note == null ? "" : note, "auto");
    }
}
