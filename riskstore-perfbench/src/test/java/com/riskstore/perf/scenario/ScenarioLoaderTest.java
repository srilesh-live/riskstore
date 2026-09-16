package com.riskstore.perf.scenario;

import com.riskstore.perf.ch.SqlLibrary;
import com.riskstore.perf.gen.GeneratorRegistry;
import com.riskstore.perf.verify.SloEvaluator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that the bundled scenarios are internally consistent.
 *
 * <p>Every reference a scenario makes - to a SQL statement, to a generator, to an SLO metric
 * family - is resolved lazily at run time. Without these tests a typo in a scenario would only
 * surface twenty minutes into a soak, which is exactly when it costs the most.
 */
class ScenarioLoaderTest {

    private final ScenarioLoader loader = new ScenarioLoader(null);
    private final SqlLibrary workloadSql = SqlLibrary.workloads(null);
    private final SqlLibrary metricSql = SqlLibrary.sutMetrics(null);
    private final GeneratorRegistry generators = new GeneratorRegistry();

    @Test
    void everyBundledScenarioLoadsAndValidates() {
        assertThat(loader.all())
                .as("bundled scenarios must all survive validation at load time")
                .containsKeys("smoke", "insert-baseline-rates", "query-baseline-cold",
                        "mixed-eod-peak", "breaking-point", "soak-24h", "chaos-replica-kill");

        loader.all().forEach((id, scenario) ->
                assertThat(ScenarioValidator.validate(scenario))
                        .as("scenario %s", id)
                        .isEmpty());
    }

    @Test
    void everyScenarioIsFingerprinted() {
        loader.all().keySet().forEach(id ->
                assertThat(loader.hash(id))
                        .as("scenario %s must be fingerprinted so runs stay comparable", id)
                        .isNotEqualTo("unknown")
                        .hasSize(16));
    }

    @Test
    void everySqlReferenceResolves() {
        loader.all().forEach((id, scenario) -> scenario.workloads.forEach(workload -> {
            if (workload.isQuery()) {
                workload.queries.forEach(q -> assertThat(workloadSql.has(q.ref))
                        .as("scenario %s references unknown query %s", id, q.ref)
                        .isTrue());
            }
            if (workload.isInsertSelect()) {
                assertThat(workloadSql.has(workload.sqlRef))
                        .as("scenario %s references unknown transform %s", id, workload.sqlRef)
                        .isTrue();
            }
        }));
    }

    @Test
    void everyGeneratorReferenceResolvesAndAcceptsItsParameters() {
        loader.all().forEach((id, scenario) -> scenario.workloads.stream()
                .filter(Workload::isInsert)
                .forEach(workload -> {
                    var generator = generators.create(workload.generator.ref, workload.generator.params);
                    assertThat(generator.id()).isEqualTo(workload.generator.ref);
                    // describe() reflects the applied parameters and is reproduced in the
                    // report, so results stay interpretable.
                    assertThat(generator.describe()).as("scenario %s", id).isNotBlank();
                }));
    }

    @Test
    void everySloThresholdParses() {
        loader.all().forEach((id, scenario) -> scenario.slo.forEach(slo ->
                assertThat(SloEvaluator.parseThreshold(slo.value))
                        .as("scenario %s has an unparseable threshold on %s: %s", id, slo.metric, slo.value)
                        .isNotNull()));
    }

    @Test
    void allProbesUsedByTheScraperExist() {
        // Mirrors SystemTableScraper.CYCLE_PROBES. A probe that is referenced but missing is a
        // silent blind spot in exactly the dimensions insert-and-query benchmarking already misses.
        List<String> required = List.of(
                "async_metrics", "server_metrics", "parts_summary", "parts_files",
                "merges_summary", "replicas_summary", "mutations_pending",
                "keeper_info", "keeper_events", "disks", "errors_summary");

        required.forEach(probe -> assertThat(metricSql.has(probe))
                .as("sut-metrics.yaml is missing probe %s", probe)
                .isTrue());
    }

    @Test
    void phasesCarryUsableDurationsAndRates() {
        loader.all().forEach((id, scenario) -> scenario.phases.forEach(phase -> {
            assertThat(phase.duration)
                    .as("scenario %s phase %s", id, phase.id)
                    .isNotNull()
                    .isGreaterThan(Duration.ZERO);
            if (phase.isRamp()) {
                // A ramp must actually go somewhere, or the phase is a hold in disguise.
                assertThat(phase.rateAt(0.0)).isEqualTo(phase.from);
                assertThat(phase.rateAt(1.0)).isEqualTo(phase.to);
                assertThat(phase.rateAt(0.5)).isBetween(
                        Math.min(phase.from, phase.to), Math.max(phase.from, phase.to));
            }
        }));
    }

    @Test
    void insertWorkloadSharesDoNotExceedTheAvailableRate() {
        loader.all().forEach((id, scenario) -> {
            double totalShare = scenario.workloads.stream()
                    .filter(Workload::isInsert)
                    .mapToDouble(w -> w.share)
                    .sum();
            assertThat(totalShare)
                    .as("scenario %s over-allocates the phase rate across insert workloads", id)
                    .isLessThanOrEqualTo(1.0001);
        });
    }

    @Test
    void unknownGeneratorReferenceFailsLoudlyWithTheKnownSet() {
        assertThatThrownBy(() -> generators.create("no-such-generator", Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rates-atlas-payload");
    }

    private static org.assertj.core.api.AbstractThrowableAssert<?, ? extends Throwable> assertThatThrownBy(
            org.assertj.core.api.ThrowableAssert.ThrowingCallable callable) {
        return org.assertj.core.api.Assertions.assertThatThrownBy(callable);
    }
}
