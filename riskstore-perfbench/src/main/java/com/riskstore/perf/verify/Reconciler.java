package com.riskstore.perf.verify;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import com.riskstore.perf.ch.ClickHouseClientFactory;
import com.riskstore.perf.engine.InsertDriver;
import com.riskstore.perf.engine.RunContext;
import com.riskstore.perf.scenario.Scenario;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Post-run correctness checks.
 *
 * <p>Three things are established, in increasing order of how badly their absence would
 * invalidate the run:
 *
 * <ol>
 *   <li><b>Acknowledgement</b> - the rows the harness offered match the rows ClickHouse said it
 *       wrote. A gap here means inserts silently failed or were deduplicated away.
 *   <li><b>Persistence</b> - a scenario-supplied count query sees those rows. A gap between
 *       acknowledged and visible points at deduplication or at a part that never committed.
 *   <li><b>Replica convergence</b> - both replicas report the same row count for the target
 *       tables. This is the check that catches a run which looked fast because one replica had
 *       quietly stopped replicating.
 * </ol>
 *
 * <p>Convergence is checked after a settling delay: replication is asynchronous, so an immediate
 * comparison would report a false mismatch on a healthy cluster.
 */
public final class Reconciler {

    private static final Logger log = LoggerFactory.getLogger(Reconciler.class);
    private static final long REPLICA_SETTLE_MS = 15_000;

    private final ClickHouseClientFactory clients;

    public Reconciler(ClickHouseClientFactory clients) {
        this.clients = clients;
    }

    public List<ReconResult> reconcile(RunContext ctx) {
        List<ReconResult> results = new ArrayList<>();

        results.add(acknowledgementCheck(ctx));
        results.addAll(scenarioChecks(ctx));
        results.addAll(convergenceChecks(ctx));

        return results;
    }

    /** Rows the harness believes it sent, against rows the server acknowledged. */
    private ReconResult acknowledgementCheck(RunContext ctx) {
        long offered = ctx.insertDrivers().values().stream().mapToLong(InsertDriver::rowsOffered).sum();
        long acked = ctx.insertDrivers().values().stream().mapToLong(InsertDriver::rowsAcknowledged).sum();
        if (offered == 0) {
            return new ReconResult("rows-acknowledged", 0, 0, 0, true, "no insert workloads in this scenario");
        }
        return ReconResult.of("rows-acknowledged", offered, acked, 0.0,
                "rows offered by the harness vs rows ClickHouse reported written");
    }

    /** Scenario-declared count queries, compared against acknowledged rows. */
    private List<ReconResult> scenarioChecks(RunContext ctx) {
        List<ReconResult> out = new ArrayList<>();
        long acked = ctx.insertDrivers().values().stream().mapToLong(InsertDriver::rowsAcknowledged).sum();

        for (Scenario.ReconCheck check : ctx.scenario().reconciliation) {
            try {
                double actual = scalar(clients.forNode(0), check.sql);
                double expected = "generated".equalsIgnoreCase(check.expect)
                        ? acked
                        : Double.parseDouble(check.expect);
                out.add(ReconResult.of(check.name, expected, actual, check.tolerance, check.sql));
            } catch (Exception e) {
                out.add(ReconResult.error(check.name, "check failed: " + e));
            }
        }
        return out;
    }

    /**
     * Compare row counts across replicas for every table this run wrote to.
     *
     * <p>Uses {@code system.parts} rather than {@code count()} so the comparison is cheap on a
     * table holding billions of rows, and so it reflects what each replica has actually
     * committed to disk.
     */
    private List<ReconResult> convergenceChecks(RunContext ctx) {
        List<ReconResult> out = new ArrayList<>();
        if (clients.nodeCount() < 2) {
            return out;
        }

        List<String> tables = ctx.scenario().workloads.stream()
                .filter(w -> w.isInsert() && w.target != null && w.target.table != null)
                .map(w -> w.target.table)
                .distinct()
                .toList();
        if (tables.isEmpty()) {
            return out;
        }

        log.info("Waiting {}ms for replication to settle before the convergence check", REPLICA_SETTLE_MS);
        try {
            Thread.sleep(REPLICA_SETTLE_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        for (String table : tables) {
            try {
                String sql = "SELECT sum(rows) AS value FROM system.parts WHERE active AND table = '"
                        + escape(table) + "'";
                double first = scalar(clients.forNode(0), sql);
                for (int node = 1; node < clients.nodeCount(); node++) {
                    double other = scalar(clients.forNode(node), sql);
                    out.add(ReconResult.of(
                            "replica-convergence:" + table + ":" + clients.endpoint(node),
                            first, other, 0.0,
                            "active rows on " + clients.endpoint(0) + " vs " + clients.endpoint(node)));
                }
            } catch (Exception e) {
                out.add(ReconResult.error("replica-convergence:" + table, "check failed: " + e));
            }
        }
        return out;
    }

    private static double scalar(Client client, String sql) {
        List<GenericRecord> rows = client.queryAll(sql);
        if (rows.isEmpty()) {
            return 0;
        }
        return rows.get(0).getDouble(1);
    }

    private static String escape(String s) {
        return s.replace("'", "''");
    }
}
