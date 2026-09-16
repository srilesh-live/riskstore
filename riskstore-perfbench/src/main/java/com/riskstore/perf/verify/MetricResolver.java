package com.riskstore.perf.verify;

import com.riskstore.perf.engine.ArrivalScheduler;
import com.riskstore.perf.engine.InsertDriver;
import com.riskstore.perf.engine.QueryDriver;
import com.riskstore.perf.engine.RunContext;
import com.riskstore.perf.metrics.LatencyRecorder;
import com.riskstore.perf.metrics.MetricSample;

import java.util.List;
import java.util.Optional;

/**
 * Resolves the dotted metric names used in SLOs to a number and a unit.
 *
 * <p>Supported families:
 * <pre>
 *   insert.&lt;workload&gt;.{p50,p90,p95,p99,p999,max,mean}   latency, ms
 *   query.&lt;workload|queryRef&gt;.{p50,...}                  latency, ms
 *   throughput.&lt;workload&gt;.{rowsPerSec,opsPerSec}          rate
 *   errors.{rate,count}                                       fraction / count
 *   sut.&lt;metric&gt;.{max,min,avg,p99,last}                   server-side, native units
 *   recon.rowsLost                                            rows
 *   harness.{overloadDrops,maxScheduleLagUs,batchStarvations}  harness health
 * </pre>
 *
 * <p>Server-side metrics are windowed to the steady-state period, so a part count that spiked
 * during a deliberate warm-up burst does not fail an SLO written about steady state.
 */
public final class MetricResolver {

    /** A resolved metric plus the unit it is expressed in, for honest reporting. */
    public record Resolved(double value, String unit) {
    }

    private final RunContext ctx;

    public MetricResolver(RunContext ctx) {
        this.ctx = ctx;
    }

    public Optional<Resolved> resolve(String metric) {
        if (metric == null || metric.isBlank()) {
            return Optional.empty();
        }
        String[] parts = metric.split("\\.");
        String family = parts[0];

        return switch (family) {
            case "insert", "query" -> parts.length >= 3
                    ? latency(parts[1], parts[2])
                    : Optional.empty();
            case "throughput" -> parts.length >= 3 ? throughput(parts[1], parts[2]) : Optional.empty();
            case "errors" -> parts.length >= 2 ? errors(parts[1]) : Optional.empty();
            case "sut" -> parts.length >= 3 ? sut(parts[1], parts[2]) : Optional.empty();
            case "recon" -> parts.length >= 2 ? recon(parts[1]) : Optional.empty();
            case "harness" -> parts.length >= 2 ? harness(parts[1]) : Optional.empty();
            default -> Optional.empty();
        };
    }

    private Optional<Resolved> latency(String workloadOrRef, String stat) {
        LatencyRecorder recorder = ctx.recorders().get(workloadOrRef);
        if (recorder == null) {
            // Fall back to a per-statement recorder, so an SLO can name a single report query.
            for (QueryDriver driver : ctx.queryDrivers().values()) {
                LatencyRecorder perQuery = driver.perQueryRecorders().get(workloadOrRef);
                if (perQuery == null) {
                    perQuery = driver.perQueryRecorders().get(driver.workloadId() + "." + workloadOrRef);
                }
                if (perQuery != null) {
                    recorder = perQuery;
                    break;
                }
            }
        }
        if (recorder == null || recorder.ops() == 0) {
            return Optional.empty();
        }
        double us = switch (stat) {
            case "p50" -> recorder.percentileUs(50.0);
            case "p90" -> recorder.percentileUs(90.0);
            case "p95" -> recorder.percentileUs(95.0);
            case "p99" -> recorder.percentileUs(99.0);
            case "p999" -> recorder.percentileUs(99.9);
            case "max" -> recorder.total().getMaxValue();
            case "mean" -> recorder.total().getMean();
            default -> Double.NaN;
        };
        return Double.isNaN(us) ? Optional.empty() : Optional.of(new Resolved(us / 1_000.0, "ms"));
    }

    private Optional<Resolved> throughput(String workloadId, String stat) {
        LatencyRecorder recorder = ctx.recorders().get(workloadId);
        if (recorder == null) {
            return Optional.empty();
        }
        double seconds = measuredSeconds();
        if (seconds <= 0) {
            return Optional.empty();
        }
        return switch (stat) {
            case "rowsPerSec" -> Optional.of(new Resolved(recorder.rows() / seconds, "rows/s"));
            case "opsPerSec" -> Optional.of(new Resolved(recorder.ops() / seconds, "ops/s"));
            case "bytesPerSec" -> Optional.of(new Resolved(recorder.bytes() / seconds, "bytes/s"));
            default -> Optional.empty();
        };
    }

    private Optional<Resolved> errors(String stat) {
        long totalOps = ctx.recorders().values().stream().mapToLong(LatencyRecorder::ops).sum();
        long totalErrors = ctx.recorders().values().stream().mapToLong(LatencyRecorder::errors).sum();
        return switch (stat) {
            case "rate" -> Optional.of(new Resolved(totalOps == 0 ? 0 : (double) totalErrors / totalOps, "fraction"));
            case "count" -> Optional.of(new Resolved(totalErrors, "errors"));
            default -> Optional.empty();
        };
    }

    /** Server-side metric, aggregated across replicas within the steady-state window. */
    private Optional<Resolved> sut(String metric, String stat) {
        long from = ctx.measuredFromMs();
        long to = ctx.measuredToMs() > 0 ? ctx.measuredToMs() : Long.MAX_VALUE;
        List<Double> values = ctx.sutSamples().stream()
                .filter(s -> s.metric().equals(metric))
                .filter(s -> s.tsMs() >= from && s.tsMs() <= to)
                .map(MetricSample::value)
                .toList();
        if (values.isEmpty()) {
            return Optional.empty();
        }
        double result = switch (stat) {
            case "max" -> values.stream().mapToDouble(Double::doubleValue).max().orElse(Double.NaN);
            case "min" -> values.stream().mapToDouble(Double::doubleValue).min().orElse(Double.NaN);
            case "avg" -> values.stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN);
            case "last" -> values.get(values.size() - 1);
            case "p99" -> percentile(values, 99.0);
            case "p95" -> percentile(values, 95.0);
            default -> Double.NaN;
        };
        return Double.isNaN(result) ? Optional.empty() : Optional.of(new Resolved(result, ""));
    }

    private static double percentile(List<Double> values, double p) {
        List<Double> sorted = values.stream().sorted().toList();
        int idx = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(sorted.size() - 1, idx)));
    }

    private Optional<Resolved> recon(String stat) {
        if ("rowsLost".equals(stat)) {
            long offered = ctx.insertDrivers().values().stream().mapToLong(InsertDriver::rowsOffered).sum();
            long acked = ctx.insertDrivers().values().stream().mapToLong(InsertDriver::rowsAcknowledged).sum();
            return Optional.of(new Resolved(Math.max(0, offered - acked), "rows"));
        }
        return ctx.reconResults().stream()
                .filter(r -> r.name().equals(stat))
                .findFirst()
                .map(r -> new Resolved(r.delta(), "rows"));
    }

    private Optional<Resolved> harness(String stat) {
        return switch (stat) {
            case "overloadDrops" -> Optional.of(new Resolved(
                    ctx.recorders().values().stream().mapToLong(LatencyRecorder::overloadDrops).sum(), "drops"));
            case "maxScheduleLagUs" -> Optional.of(new Resolved(
                    ctx.schedulers().values().stream().mapToLong(ArrivalScheduler::maxScheduleLagUs).max().orElse(0),
                    "us"));
            case "batchStarvations" -> Optional.of(new Resolved(
                    ctx.batchFactories().values().stream().mapToLong(f -> f.starvations()).sum(), "starvations"));
            default -> Optional.empty();
        };
    }

    private double measuredSeconds() {
        long from = ctx.measuredFromMs();
        long to = ctx.measuredToMs();
        if (from == 0 || to <= from) {
            return ctx.durationMs() / 1000.0;
        }
        return (to - from) / 1000.0;
    }
}
