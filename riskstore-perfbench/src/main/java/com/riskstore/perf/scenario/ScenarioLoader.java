package com.riskstore.perf.scenario;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * Loads scenarios from the configured directory, falling back to the bundled classpath set.
 *
 * <p>Each scenario is fingerprinted with a SHA-256 over its raw YAML. The fingerprint is recorded
 * on every run so a later comparison can tell "this regressed" apart from "somebody edited the
 * scenario".
 */
public final class ScenarioLoader {

    private static final Logger log = LoggerFactory.getLogger(ScenarioLoader.class);

    /** Bundled scenarios, so the harness is usable with an empty scenario directory. */
    private static final List<String> BUILT_IN = List.of(
            "smoke.yaml",
            "insert-baseline-rates.yaml",
            "query-baseline-cold.yaml",
            "mixed-eod-peak.yaml",
            "breaking-point.yaml",
            "soak-24h.yaml",
            "chaos-replica-kill.yaml");

    private final ObjectMapper yaml = new ObjectMapper(new YAMLFactory())
            .registerModule(new JavaTimeModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private final Map<String, Scenario> scenarios = new TreeMap<>();
    private final Map<String, String> hashes = new TreeMap<>();

    public ScenarioLoader(Path scenarioDir) {
        loadBuiltIns();
        if (scenarioDir != null && Files.isDirectory(scenarioDir)) {
            loadDirectory(scenarioDir);
        } else {
            log.info("Scenario directory {} not present - using {} built-in scenarios", scenarioDir, scenarios.size());
        }
    }

    private void loadBuiltIns() {
        for (String name : BUILT_IN) {
            try (InputStream in = getClass().getResourceAsStream("/scenarios/" + name)) {
                if (in == null) {
                    continue;
                }
                String raw = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                register(raw, URI.create("classpath:/scenarios/" + name));
            } catch (IOException e) {
                log.warn("Could not read bundled scenario {}: {}", name, e.toString());
            }
        }
    }

    private void loadDirectory(Path dir) {
        try (Stream<Path> files = Files.list(dir)) {
            files.filter(ScenarioLoader::isYaml).sorted().forEach(p -> {
                try {
                    register(Files.readString(p, StandardCharsets.UTF_8), p.toUri());
                } catch (IOException e) {
                    log.warn("Could not read scenario {}: {}", p, e.toString());
                }
            });
        } catch (IOException e) {
            log.warn("Could not list scenario directory {}: {}", dir, e.toString());
        }
    }

    private static boolean isYaml(Path p) {
        String n = p.getFileName().toString();
        return n.endsWith(".yaml") || n.endsWith(".yml");
    }

    /** On-disk scenarios override bundled ones with the same id, so operators can tune without a rebuild. */
    private void register(String rawYaml, URI source) {
        try {
            Scenario s = yaml.readValue(rawYaml, Scenario.class);
            if (s == null || s.id == null || s.id.isBlank()) {
                log.warn("Skipping scenario from {} - missing id", source);
                return;
            }
            List<String> problems = ScenarioValidator.validate(s);
            if (!problems.isEmpty()) {
                log.warn("Scenario {} from {} is invalid and was not loaded: {}", s.id, source, problems);
                return;
            }
            scenarios.put(s.id, s);
            hashes.put(s.id, sha256(rawYaml));
            log.debug("Loaded scenario {} from {}", s.id, source);
        } catch (Exception e) {
            log.warn("Could not parse scenario from {}: {}", source, e.toString());
        }
    }

    private static String sha256(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(md.digest(s.getBytes(StandardCharsets.UTF_8))).substring(0, 16);
        } catch (Exception e) {
            return "unknown";
        }
    }

    public Scenario get(String id) {
        return scenarios.get(id);
    }

    public String hash(String id) {
        return hashes.getOrDefault(id, "unknown");
    }

    public Map<String, Scenario> all() {
        return Map.copyOf(scenarios);
    }
}
