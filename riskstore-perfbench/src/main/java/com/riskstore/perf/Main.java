package com.riskstore.perf;

import com.riskstore.perf.api.ApiRoutes;
import com.riskstore.perf.ch.ClickHouseClientFactory;
import com.riskstore.perf.ch.SqlLibrary;
import com.riskstore.perf.config.ConfigLoader;
import com.riskstore.perf.config.HarnessConfig;
import com.riskstore.perf.engine.RunCoordinator;
import com.riskstore.perf.engine.RunRegistry;
import com.riskstore.perf.gen.GeneratorRegistry;
import com.riskstore.perf.report.SummaryBuilder;
import com.riskstore.perf.results.ResultsWriter;
import com.riskstore.perf.scenario.ScenarioLoader;
import io.muserver.MuServer;
import io.muserver.MuServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 * Entry point.
 *
 * <p>Usage: {@code java -jar riskstore-perfbench.jar [config.yaml]}
 *
 * <p>Runs on a host separate from the ClickHouse replicas. Co-locating the load generator with
 * the database means the harness competes for the CPU it is trying to measure, and every
 * resulting number is a blend of the two.
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Path configPath = Path.of(args.length > 0 ? args[0] : "harness.yaml");
        HarnessConfig config = ConfigLoader.load(configPath);

        Path scenarioDir = Path.of(config.paths.scenarioDir);
        ScenarioLoader scenarios = new ScenarioLoader(scenarioDir);
        SqlLibrary workloadSql = SqlLibrary.workloads(scenarioDir);
        SqlLibrary metricSql = SqlLibrary.sutMetrics(scenarioDir);

        ClickHouseClientFactory clients = new ClickHouseClientFactory(config.sut);
        RunCoordinator coordinator = new RunCoordinator(
                config, clients, workloadSql, metricSql, new GeneratorRegistry());
        RunRegistry runs = new RunRegistry(scenarios, coordinator, new ResultsWriter(config));

        MuServer server = new ApiRoutes(config, scenarios, runs, clients)
                .register(MuServerBuilder.httpServer()
                        .withHttpPort(config.server.port)
                        .withInterface(config.server.host))
                .start();

        log.info("riskstore-perfbench {} listening on {}", SummaryBuilder.HARNESS_VERSION, server.uri());
        log.info("Target cluster '{}' across {} replica(s): {}",
                config.sut.clusterName, clients.nodeCount(), clients.endpoints());
        log.info("{} scenario(s) loaded: {}", scenarios.all().size(), scenarios.all().keySet());
        if (!config.safety.allowDestructive) {
            log.warn("safety.allowDestructive is false - preflight will refuse every run until it is set. "
                    + "This is deliberate: runs write data and drop caches on the target cluster.");
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down");
            runs.shutdown();
            server.stop();
            clients.close();
        }, "shutdown"));
    }
}
