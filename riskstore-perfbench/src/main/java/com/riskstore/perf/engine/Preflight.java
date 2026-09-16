package com.riskstore.perf.engine;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import com.riskstore.perf.ch.ClickHouseClientFactory;
import com.riskstore.perf.config.HarnessConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Gate that every run passes through before any load is generated.
 *
 * <p>Two jobs. The first is safety: this harness writes data and drops caches, so it must be
 * impossible to point it at production by accident. That takes both an explicit cluster
 * allowlist and an explicit destructive flag, because either one alone is too easy to leave
 * switched on.
 *
 * <p>The second is comparability. A run against a cluster that is already busy, already lagging,
 * or configured differently from the baseline is not a comparable run, and finding that out
 * afterwards wastes however long the scenario took. The environment fingerprint captured here is
 * recorded on the run and shown in the report, so a surprising number can always be checked
 * against the conditions that produced it.
 */
public final class Preflight {

    private static final Logger log = LoggerFactory.getLogger(Preflight.class);

    /** Settings whose values materially change measured performance. Any drift breaks comparability. */
    private static final List<String> FINGERPRINT_SETTINGS = List.of(
            "max_threads",
            "max_insert_threads",
            "max_memory_usage",
            "max_server_memory_usage",
            "async_insert",
            "wait_for_async_insert",
            "max_concurrent_queries",
            "background_pool_size",
            "max_partitions_per_insert_block",
            "use_query_cache",
            "max_bytes_before_external_group_by",
            "join_algorithm");

    /** One named condition and whether it holds. {@code fatal} distinguishes a blocker from a warning. */
    public record Check(String name, boolean passed, boolean fatal, String detail) {
        public static Check ok(String name, String detail) {
            return new Check(name, true, false, detail);
        }

        public static Check fail(String name, String detail) {
            return new Check(name, false, true, detail);
        }

        public static Check warn(String name, String detail) {
            return new Check(name, false, false, detail);
        }
    }

    public record Report(boolean ok, List<Check> checks, Map<String, String> environment) {
        /** Human-readable reason the run cannot start, or null when it can. */
        public String blocker() {
            return checks.stream()
                    .filter(c -> !c.passed() && c.fatal())
                    .map(c -> c.name() + ": " + c.detail())
                    .findFirst()
                    .orElse(null);
        }
    }

    private final ClickHouseClientFactory clients;
    private final HarnessConfig config;

    public Preflight(ClickHouseClientFactory clients, HarnessConfig config) {
        this.clients = clients;
        this.config = config;
    }

    public Report run() {
        List<Check> checks = new ArrayList<>();
        Map<String, String> env = new LinkedHashMap<>();

        checks.add(safetyAllowlist());
        checks.add(destructiveFlag());
        checks.add(connectivity());

        for (int i = 0; i < clients.nodeCount(); i++) {
            String node = clients.endpoint(i);
            Client client = clients.forNode(i);
            checks.add(version(client, node, env));
            checks.add(diskHeadroom(client, node, env));
            checks.add(replicaHealth(client, node));
            checks.add(quiescence(client, node));
            fingerprintSettings(client, node, env);
        }

        env.put("harness.nodes", String.join(",", clients.endpoints()));
        env.put("harness.cluster", config.sut.clusterName);
        env.put("settings.fingerprint", fingerprint(env));

        boolean ok = checks.stream().noneMatch(c -> !c.passed() && c.fatal());
        return new Report(ok, checks, env);
    }

    private Check safetyAllowlist() {
        String cluster = config.sut.clusterName;
        if (config.safety.allowedClusters.contains(cluster)) {
            return Check.ok("safety.allowlist", "cluster " + cluster + " is allowlisted");
        }
        return Check.fail("safety.allowlist",
                "cluster '" + cluster + "' is not in safety.allowedClusters " + config.safety.allowedClusters);
    }

    private Check destructiveFlag() {
        if (config.safety.allowDestructive) {
            return Check.ok("safety.allowDestructive", "explicitly enabled");
        }
        return Check.fail("safety.allowDestructive",
                "set safety.allowDestructive=true to confirm this cluster may be written to and have its caches dropped");
    }

    private Check connectivity() {
        return clients.pingAll()
                ? Check.ok("connectivity", clients.nodeCount() + " replica(s) responding")
                : Check.fail("connectivity", "at least one replica did not respond to ping");
    }

    private Check version(Client client, String node, Map<String, String> env) {
        try {
            String v = scalarString(client, "SELECT version() AS value");
            env.put("version@" + node, v);
            return Check.ok("version@" + node, v);
        } catch (Exception e) {
            return Check.fail("version@" + node, e.toString());
        }
    }

    private Check diskHeadroom(Client client, String node, Map<String, String> env) {
        try {
            List<GenericRecord> rows = client.queryAll(
                    "SELECT name, free_space, total_space FROM system.disks");
            for (GenericRecord row : rows) {
                String name = row.getString("name");
                double free = row.getDouble("free_space");
                double total = row.getDouble("total_space");
                double fraction = total == 0 ? 0 : free / total;
                env.put("disk." + name + ".freeBytes@" + node, Long.toString((long) free));
                env.put("disk." + name + ".freeFraction@" + node, String.format("%.3f", fraction));
                if (fraction < config.safety.minFreeDiskFraction) {
                    // Merges need free space of the same order as the parts being merged, so a
                    // run started with thin headroom fails as a disk problem, not a perf result.
                    return Check.fail("disk." + name + "@" + node,
                            String.format("only %.1f%% free, need %.1f%% (merges need room to work)",
                                    fraction * 100, config.safety.minFreeDiskFraction * 100));
                }
            }
            return Check.ok("disk@" + node, "headroom above " + config.safety.minFreeDiskFraction);
        } catch (Exception e) {
            return Check.warn("disk@" + node, "could not read system.disks: " + e);
        }
    }

    private Check replicaHealth(Client client, String node) {
        try {
            List<GenericRecord> rows = client.queryAll(
                    "SELECT count() AS bad FROM system.replicas WHERE is_readonly OR is_session_expired");
            long bad = rows.isEmpty() ? 0 : rows.get(0).getLong("bad");
            if (bad > 0) {
                return Check.fail("replicas@" + node,
                        bad + " replicated table(s) are read-only or have an expired Keeper session");
            }
            return Check.ok("replicas@" + node, "all replicated tables writable");
        } catch (Exception e) {
            return Check.warn("replicas@" + node, "could not read system.replicas: " + e);
        }
    }

    /** Competing load makes a run incomparable, so it warns loudly rather than failing outright. */
    private Check quiescence(Client client, String node) {
        try {
            List<GenericRecord> rows = client.queryAll(
                    "SELECT count() AS running FROM system.processes WHERE query NOT LIKE '%system.processes%'");
            long running = rows.isEmpty() ? 0 : rows.get(0).getLong("running");
            if (running > 1) {
                return Check.warn("quiescence@" + node,
                        running + " queries already running - results will not be comparable to an idle baseline");
            }
            return Check.ok("quiescence@" + node, "cluster idle");
        } catch (Exception e) {
            return Check.warn("quiescence@" + node, "could not read system.processes: " + e);
        }
    }

    private void fingerprintSettings(Client client, String node, Map<String, String> env) {
        try {
            String inList = String.join("','", FINGERPRINT_SETTINGS);
            List<GenericRecord> rows = client.queryAll(
                    "SELECT name, value FROM system.settings WHERE name IN ('" + inList + "')");
            for (GenericRecord row : rows) {
                env.put("setting." + row.getString("name") + "@" + node, row.getString("value"));
            }
        } catch (Exception e) {
            log.warn("Could not fingerprint settings on {}: {}", node, e.toString());
        }
    }

    /** Stable hash over the captured settings, so two runs can be compared at a glance. */
    private static String fingerprint(Map<String, String> env) {
        StringBuilder sb = new StringBuilder();
        env.entrySet().stream()
                .filter(e -> e.getKey().startsWith("setting.") || e.getKey().startsWith("version@"))
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> sb.append(e.getKey()).append('=').append(e.getValue()).append(';'));
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(md.digest(sb.toString().getBytes())).substring(0, 16);
        } catch (Exception e) {
            return "unknown";
        }
    }

    private static String scalarString(Client client, String sql) {
        List<GenericRecord> rows = client.queryAll(sql);
        return rows.isEmpty() ? "" : rows.get(0).getString(1);
    }
}
