package com.riskstore.perf.ch;

import com.clickhouse.client.api.Client;
import com.riskstore.perf.config.HarnessConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Owns one {@link Client} per SUT replica.
 *
 * <p>Deliberately one client per endpoint rather than a single multi-endpoint client: the
 * scrapers must be able to address a named replica (replication lag and part counts are
 * per-replica facts), and the drivers round-robin explicitly so load distribution is a property
 * of the harness rather than of the driver's internal failover.
 */
public final class ClickHouseClientFactory implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseClientFactory.class);

    private final List<String> endpoints;
    private final List<Client> clients = new ArrayList<>();
    private final AtomicInteger cursor = new AtomicInteger();

    public ClickHouseClientFactory(HarnessConfig.SutConfig sut) {
        this.endpoints = List.copyOf(sut.nodes);
        for (String node : endpoints) {
            clients.add(build(node, sut.user, sut.password, sut.database, sut));
        }
        log.info("ClickHouse clients created for {}", endpoints);
    }

    /** Separate constructor for the results database, which has its own credentials and no pooling needs. */
    public static Client forResults(HarnessConfig.ResultsConfig cfg) {
        return new Client.Builder()
                .addEndpoint(cfg.url)
                .setUsername(cfg.user)
                .setPassword(cfg.password)
                .setDefaultDatabase(cfg.database)
                .setMaxConnections(8)
                .setConnectTimeout(10_000, ChronoUnit.MILLIS)
                .setSocketTimeout(120_000, ChronoUnit.MILLIS)
                .setClientName("riskstore-perfbench-results")
                .compressClientRequest(true)
                .build();
    }

    private static Client build(String endpoint, String user, String password, String db, HarnessConfig.SutConfig sut) {
        return new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(password)
                .setDefaultDatabase(db)
                .setMaxConnections(sut.maxConnections)
                .setConnectTimeout(sut.connectTimeoutMs, ChronoUnit.MILLIS)
                .setSocketTimeout(sut.socketTimeoutMs, ChronoUnit.MILLIS)
                .compressClientRequest(sut.compressClientRequest)
                .setClientName("riskstore-perfbench")
                .build();
    }

    /** Round-robin across replicas, so a workload spreads evenly rather than pinning one node. */
    public Client next() {
        return clients.get(Math.floorMod(cursor.getAndIncrement(), clients.size()));
    }

    public Client forNode(int index) {
        return clients.get(index);
    }

    public String endpoint(int index) {
        return endpoints.get(index);
    }

    public int nodeCount() {
        return clients.size();
    }

    public List<String> endpoints() {
        return endpoints;
    }

    /** True only if every replica answers. Used by preflight. */
    public boolean pingAll() {
        boolean ok = true;
        for (int i = 0; i < clients.size(); i++) {
            try {
                if (!clients.get(i).ping(5_000)) {
                    log.warn("Replica {} did not respond to ping", endpoints.get(i));
                    ok = false;
                }
            } catch (Exception e) {
                log.warn("Replica {} ping failed: {}", endpoints.get(i), e.toString());
                ok = false;
            }
        }
        return ok;
    }

    @Override
    public void close() {
        for (Client c : clients) {
            try {
                c.close();
            } catch (Exception e) {
                log.debug("Error closing client: {}", e.toString());
            }
        }
    }
}
