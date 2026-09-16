package com.riskstore.perf.ch;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;
import com.riskstore.perf.scenario.Workload;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Writes one pre-rendered batch to a target table. */
public final class InsertSink {

    private final ClickHouseClientFactory clients;
    private final QueryIdFactory queryIds;

    public InsertSink(ClickHouseClientFactory clients, QueryIdFactory queryIds) {
        this.clients = clients;
        this.queryIds = queryIds;
    }

    /**
     * Insert a rendered batch.
     *
     * <p>The deduplication token is derived from the batch identity rather than being random, so
     * that a retried batch after a chaos event is recognised by ClickHouse as the same batch.
     * That is what lets the reconciliation step distinguish real data loss from a benign retry.
     */
    public OpResult insert(Workload workload, byte[] payload, long rowCount, String dedupToken) {
        String queryId = queryIds.next(workload.id);
        Client client = clients.next();

        InsertSettings settings = new InsertSettings().setQueryId(queryId);
        if (dedupToken != null) {
            settings.setDeduplicationToken(dedupToken);
        }
        for (Map.Entry<String, String> e : workload.settings.entrySet()) {
            settings.serverSetting(e.getKey(), e.getValue());
        }

        ClickHouseFormat format = format(workload.target.format);
        try (InsertResponse response = client
                .insert(workload.target.table, new ByteArrayInputStream(payload), format, settings)
                .get()) {
            long written = response.getWrittenRows();
            return OpResult.ok(queryId, written > 0 ? written : rowCount, payload.length, response.getServerTime());
        } catch (Exception e) {
            return OpResult.failed(queryId, e);
        }
    }

    private static ClickHouseFormat format(String name) {
        if (name == null || name.isBlank()) {
            return ClickHouseFormat.JSONEachRow;
        }
        return ClickHouseFormat.valueOf(name);
    }

    /** Renders newline-delimited JSON rows, the wire shape the L0 tables take from Kafka. */
    public static byte[] renderJsonEachRow(Iterable<String> jsonRows) {
        StringBuilder sb = new StringBuilder(1 << 16);
        for (String row : jsonRows) {
            sb.append(row).append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
