package com.riskstore.perf.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Loads {@link HarnessConfig} from YAML, expanding {@code ${ENV_VAR}} and {@code ${ENV_VAR:default}}. */
public final class ConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(ConfigLoader.class);
    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([A-Za-z0-9_]+)(?::([^}]*))?}");

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private ConfigLoader() {
    }

    public static HarnessConfig load(Path path) throws IOException {
        if (path == null || !Files.exists(path)) {
            log.warn("No config file at {} - using built-in defaults (localhost, results sink disabled)", path);
            return new HarnessConfig();
        }
        String raw = Files.readString(path, StandardCharsets.UTF_8);
        String expanded = expand(raw);
        HarnessConfig cfg = YAML.readValue(expanded, HarnessConfig.class);
        log.info("Loaded config from {} (cluster={}, nodes={})", path, cfg.sut.clusterName, cfg.sut.nodes);
        return cfg;
    }

    /** Package-visible so the expansion rules can be unit tested without touching the filesystem. */
    static String expand(String raw) {
        Matcher m = PLACEHOLDER.matcher(raw);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String name = m.group(1);
            String fallback = m.group(2) == null ? "" : m.group(2);
            String value = System.getenv(name);
            if (value == null) {
                value = System.getProperty(name, fallback);
            }
            m.appendReplacement(out, Matcher.quoteReplacement(value));
        }
        m.appendTail(out);
        return out.toString();
    }

    public static ObjectMapper yamlMapper() {
        return YAML;
    }
}
