package com.riskstore.perf.ch;

import com.clickhouse.client.api.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Puts every replica into a known cache state before a run.
 *
 * <p>Mark cache, uncompressed cache, query cache and the OS page cache together move the same
 * query by one to two orders of magnitude. If the state is not pinned, a "regression" is just as
 * likely to be a warm run compared against a cold one. Cold and warm are both legitimate things
 * to measure; measuring an unknown mixture is not.
 *
 * <p>Note the limit of what a SQL client can do: {@code SYSTEM DROP ... CACHE} clears
 * ClickHouse's own caches, but the Linux page cache is outside its reach. A genuinely cold run
 * needs the operator to run {@code echo 3 > /proc/sys/vm/drop_caches} on each replica, which is
 * why {@link #describeColdProcedure()} exists and is printed into the report.
 */
public final class CacheController {

    private static final Logger log = LoggerFactory.getLogger(CacheController.class);

    private static final List<String> DROP_STATEMENTS = List.of(
            "SYSTEM DROP MARK CACHE",
            "SYSTEM DROP UNCOMPRESSED CACHE",
            "SYSTEM DROP QUERY CACHE",
            "SYSTEM DROP COMPILED EXPRESSION CACHE");

    private final ClickHouseClientFactory clients;

    public CacheController(ClickHouseClientFactory clients) {
        this.clients = clients;
    }

    /** Applies the scenario cache policy. Failures are logged, not fatal: some grants forbid SYSTEM. */
    public void apply(String policy, List<String> warmupQueries) {
        if (policy == null || "none".equalsIgnoreCase(policy)) {
            return;
        }
        if ("cold".equalsIgnoreCase(policy)) {
            dropAll();
        } else if ("warm".equalsIgnoreCase(policy)) {
            dropAll();
            warm(warmupQueries);
        }
    }

    public void dropAll() {
        for (int i = 0; i < clients.nodeCount(); i++) {
            Client c = clients.forNode(i);
            for (String stmt : DROP_STATEMENTS) {
                try {
                    c.execute(stmt).get();
                } catch (Exception e) {
                    log.warn("{} failed on {}: {}", stmt, clients.endpoint(i), e.toString());
                }
            }
        }
        log.info("Dropped ClickHouse caches on {} replica(s). OS page cache is NOT cleared - see report notes.",
                clients.nodeCount());
    }

    private void warm(List<String> warmupQueries) {
        if (warmupQueries == null || warmupQueries.isEmpty()) {
            return;
        }
        for (int i = 0; i < clients.nodeCount(); i++) {
            Client c = clients.forNode(i);
            for (String sql : warmupQueries) {
                try {
                    c.queryAll(sql);
                } catch (Exception e) {
                    log.warn("Warmup query failed on {}: {}", clients.endpoint(i), e.toString());
                }
            }
        }
        log.info("Warmed caches on {} replica(s) with {} query(ies)", clients.nodeCount(), warmupQueries.size());
    }

    /** Reproduced verbatim in the report so a cold run is auditable rather than assumed. */
    public static String describeColdProcedure() {
        return "ClickHouse caches dropped via SYSTEM DROP MARK/UNCOMPRESSED/QUERY/COMPILED EXPRESSION CACHE. "
                + "The Linux page cache is not reachable from SQL: for a fully cold run the operator must also "
                + "run 'sync && echo 3 > /proc/sys/vm/drop_caches' on every replica before starting.";
    }
}
