package com.riskstore.perf.ch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Named SQL, kept out of Java so a DBA can review and tune it without a rebuild.
 *
 * <p>Held as YAML maps of {@code ref -> sql} rather than one file per statement, because the
 * harness ships as a fat JAR and enumerating a directory inside a JAR needs an index file
 * anyway. YAML block scalars keep the SQL readable in review.
 *
 * <p>A file of the same name in the on-disk scenario directory overrides the bundled copy, entry
 * by entry, so a site can retune one query without forking the build.
 */
public final class SqlLibrary {

    private static final Logger log = LoggerFactory.getLogger(SqlLibrary.class);

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
    private static final TypeReference<LinkedHashMap<String, String>> MAP_TYPE = new TypeReference<>() {
    };

    private final Map<String, String> statements = new LinkedHashMap<>();
    private final String resourceName;

    private SqlLibrary(String resourceName, Path overrideDir) {
        this.resourceName = resourceName;
        loadClasspath("/sql/" + resourceName);
        if (overrideDir != null) {
            loadFile(overrideDir.resolve(resourceName));
        }
    }

    /** The SELECT and INSERT...SELECT statements a scenario can reference. */
    public static SqlLibrary workloads(Path overrideDir) {
        return new SqlLibrary("workloads.yaml", overrideDir);
    }

    /** The system-table probes used by the metric collectors. */
    public static SqlLibrary sutMetrics(Path overrideDir) {
        return new SqlLibrary("sut-metrics.yaml", overrideDir);
    }

    private void loadClasspath(String path) {
        try (InputStream in = getClass().getResourceAsStream(path)) {
            if (in == null) {
                log.warn("No bundled SQL at {}", path);
                return;
            }
            merge(new String(in.readAllBytes(), StandardCharsets.UTF_8), path);
        } catch (Exception e) {
            log.warn("Could not load bundled SQL {}: {}", path, e.toString());
        }
    }

    private void loadFile(Path path) {
        if (path == null || !Files.isRegularFile(path)) {
            return;
        }
        try {
            merge(Files.readString(path, StandardCharsets.UTF_8), path.toString());
            log.info("Applied SQL overrides from {}", path);
        } catch (Exception e) {
            log.warn("Could not load SQL overrides {}: {}", path, e.toString());
        }
    }

    private void merge(String yaml, String source) throws Exception {
        Map<String, String> loaded = YAML.readValue(yaml, MAP_TYPE);
        if (loaded != null) {
            loaded.forEach((k, v) -> statements.put(k, v == null ? "" : v.trim()));
        }
        log.debug("Loaded {} statement(s) from {}", loaded == null ? 0 : loaded.size(), source);
    }

    public String get(String ref) {
        String sql = statements.get(ref);
        if (sql == null) {
            throw new IllegalArgumentException(
                    "Unknown SQL ref '" + ref + "' in " + resourceName + " (known: " + statements.keySet() + ")");
        }
        return sql;
    }

    public boolean has(String ref) {
        return statements.containsKey(ref);
    }

    public Set<String> refs() {
        return statements.keySet();
    }

    public Map<String, String> all() {
        return Map.copyOf(statements);
    }
}
