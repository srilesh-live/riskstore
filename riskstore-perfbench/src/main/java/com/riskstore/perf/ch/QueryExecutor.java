package com.riskstore.perf.ch;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;

import java.io.InputStream;
import java.util.Map;

/**
 * Runs a SELECT and drains the whole result.
 *
 * <p>Draining is the point. ClickHouse streams results, so a benchmark that measures only
 * time-to-first-byte reports a latency the user never experiences. Everything here is
 * time-to-last-byte.
 */
public final class QueryExecutor {

    private static final int DRAIN_BUFFER = 64 * 1024;

    private final ClickHouseClientFactory clients;
    private final QueryIdFactory queryIds;

    public QueryExecutor(ClickHouseClientFactory clients, QueryIdFactory queryIds) {
        this.clients = clients;
        this.queryIds = queryIds;
    }

    public OpResult run(String workloadId, String sql, Map<String, String> serverSettings) {
        String queryId = queryIds.next(workloadId);
        Client client = clients.next();

        QuerySettings settings = new QuerySettings().setQueryId(queryId);
        if (serverSettings != null) {
            for (Map.Entry<String, String> e : serverSettings.entrySet()) {
                settings.serverSetting(e.getKey(), e.getValue());
            }
        }

        try (QueryResponse response = client.query(sql, settings).get()) {
            long drained = drain(response.getInputStream());
            long rows = response.getResultRows();
            long bytes = response.getReadBytes();
            return OpResult.ok(queryId, rows, bytes > 0 ? bytes : drained, response.getServerTime());
        } catch (Exception e) {
            return OpResult.failed(queryId, e);
        }
    }

    /** Executes a statement with no result set (DDL, SYSTEM, INSERT ... SELECT). */
    public OpResult execute(String workloadId, String sql, Map<String, String> serverSettings) {
        String queryId = queryIds.next(workloadId);
        Client client = clients.next();

        QuerySettings settings = new QuerySettings().setQueryId(queryId);
        if (serverSettings != null) {
            for (Map.Entry<String, String> e : serverSettings.entrySet()) {
                settings.serverSetting(e.getKey(), e.getValue());
            }
        }

        try (QueryResponse response = client.query(sql, settings).get()) {
            drain(response.getInputStream());
            return OpResult.ok(queryId, response.getWrittenRows(), response.getWrittenBytes(), response.getServerTime());
        } catch (Exception e) {
            return OpResult.failed(queryId, e);
        }
    }

    private static long drain(InputStream in) throws Exception {
        if (in == null) {
            return 0;
        }
        byte[] buf = new byte[DRAIN_BUFFER];
        long total = 0;
        int read;
        while ((read = in.read(buf)) != -1) {
            total += read;
        }
        return total;
    }
}
