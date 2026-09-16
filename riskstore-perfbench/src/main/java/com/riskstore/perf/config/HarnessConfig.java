package com.riskstore.perf.config;

import java.util.ArrayList;
import java.util.List;

/**
 * Root harness configuration, loaded from harness.yaml.
 *
 * <p>Plain public fields keep Jackson binding trivial and keep the YAML and the Java shape
 * one-to-one, which matters when an operator is hand-editing config on a locked-down RHEL box.
 */
public class HarnessConfig {

    public ServerConfig server = new ServerConfig();
    public SutConfig sut = new SutConfig();
    public ResultsConfig results = new ResultsConfig();
    public SafetyConfig safety = new SafetyConfig();
    public PathsConfig paths = new PathsConfig();
    public CollectorConfig collectors = new CollectorConfig();

    public static class ServerConfig {
        public int port = 8080;
        public String host = "0.0.0.0";
    }

    /** The system under test. */
    public static class SutConfig {
        /** Logical cluster name; must appear in {@link SafetyConfig#allowedClusters}. */
        public String clusterName = "riskstore_local";
        /** One endpoint per replica. Scrapers poll every endpoint; drivers round-robin across them. */
        public List<String> nodes = new ArrayList<>(List.of("http://localhost:8123"));
        public String database = "riskstore";
        public String user = "default";
        public String password = "";
        public int maxConnections = 256;
        public int connectTimeoutMs = 10_000;
        public int socketTimeoutMs = 300_000;
        public boolean compressClientRequest = false;
    }

    /**
     * Results sink. Deliberately a different ClickHouse instance from the SUT: writing results
     * into the system under test perturbs the measurement.
     */
    public static class ResultsConfig {
        public boolean enabled = false;
        public String url = "http://localhost:8124";
        public String database = "perf_results";
        public String user = "default";
        public String password = "";
        /** Always written, whether or not the ClickHouse sink is enabled. */
        public String fallbackDir = "./results";
    }

    /** Guard rails that stop this ever being pointed at production by accident. */
    public static class SafetyConfig {
        public List<String> allowedClusters = new ArrayList<>(List.of("riskstore_local"));
        /**
         * Must be explicitly true before any run starts. Scenarios write data and may drop
         * caches, so every run is destructive to the target database.
         */
        public boolean allowDestructive = false;
        /** Preflight refuses to start a run below this much free disk on any replica. */
        public double minFreeDiskFraction = 0.25;
    }

    public static class PathsConfig {
        public String scenarioDir = "./scenarios";
        public String reportDir = "./reports";
    }

    public static class CollectorConfig {
        public int sutScrapeIntervalMs = 2_000;
        public int latencyWindowMs = 10_000;
        /** Cap on concurrent in-flight operations per workload before arrivals are shed. */
        public int maxInFlightPerWorkload = 2_048;
        public boolean harvestQueryLog = true;
        public boolean harvestPartLog = true;
    }
}
