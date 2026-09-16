package com.riskstore.perf.results;

import com.clickhouse.client.api.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Creates the results schema on first use.
 *
 * <p>The DDL lives in {@code sql/results-schema.sql} so a DBA can review and provision it by
 * hand on a site where the harness will not hold CREATE rights. Statements are idempotent, so
 * running this against an existing database is a no-op.
 */
public final class ResultsSchemaBootstrap {

    private static final Logger log = LoggerFactory.getLogger(ResultsSchemaBootstrap.class);

    private final Client client;
    private final String database;

    public ResultsSchemaBootstrap(Client client, String database) {
        this.client = client;
        this.database = database;
    }

    public void ensure() {
        for (String statement : statements()) {
            String sql = statement.trim();
            if (sql.isEmpty() || sql.startsWith("--")) {
                continue;
            }
            try {
                client.execute(sql.replace("${database}", database)).get();
            } catch (Exception e) {
                // A site that provisions the schema out of band will deny CREATE here. That is a
                // legitimate setup, so this warns rather than aborting the write.
                log.warn("Results schema statement failed (continuing): {}", firstLine(sql), e);
            }
        }
        log.debug("Results schema ensured in database {}", database);
    }

    private List<String> statements() {
        try (InputStream in = getClass().getResourceAsStream("/sql/results-schema.sql")) {
            if (in == null) {
                log.error("Bundled results-schema.sql is missing from the jar");
                return List.of();
            }
            String all = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return Arrays.asList(all.split(";\\s*\\n"));
        } catch (Exception e) {
            log.error("Could not read bundled results schema", e);
            return List.of();
        }
    }

    private static String firstLine(String sql) {
        int nl = sql.indexOf('\n');
        return nl < 0 ? sql : sql.substring(0, nl) + " ...";
    }
}
