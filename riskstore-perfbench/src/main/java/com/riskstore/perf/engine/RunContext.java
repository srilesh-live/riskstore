package com.riskstore.perf.engine;

import com.riskstore.perf.metrics.ErrorCounter;
import com.riskstore.perf.metrics.LatencyRecorder;
import com.riskstore.perf.metrics.MetricSample;
import com.riskstore.perf.scenario.Scenario;
import com.riskstore.perf.verify.ReconResult;
import com.riskstore.perf.verify.SloResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Everything one run accumulates, from launch through to report.
 *
 * <p>Also the object the status endpoint reads while the run is in flight, so all mutable state
 * is either concurrent or guarded. Nothing here is derived at read time: the report renders from
 * this, and re-rendering must not produce different numbers.
 */
public final class RunContext {

    private final String runId;
    private final Scenario scenario;
    private final String scenarioHash;
    private final Map<String, String> tags;
    private final String baselineRunId;

    private volatile RunState state = RunState.PENDING;
    private volatile String currentPhase = "";
    private volatile String failureReason;
    private volatile long startedAtMs;
    private volatile long endedAtMs;

    /** Steady-state boundaries: phases flagged excludeFromMetrics sit outside these. */
    private volatile long measuredFromMs;
    private volatile long measuredToMs;

    private final Map<String, LatencyRecorder> recorders = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<String, ArrivalScheduler> schedulers = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<String, InsertDriver> insertDrivers = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<String, QueryDriver> queryDrivers = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<String, BatchFactory> batchFactories = Collections.synchronizedMap(new LinkedHashMap<>());

    private final ErrorCounter errors = new ErrorCounter();
    private final List<ChaosMark> chaosMarks = Collections.synchronizedList(new ArrayList<>());
    private final List<PhaseRecord> phaseRecords = Collections.synchronizedList(new ArrayList<>());

    private volatile List<MetricSample> sutSamples = List.of();
    private volatile List<SloResult> sloResults = List.of();
    private volatile List<ReconResult> reconResults = List.of();
    private volatile Map<String, String> environment = new LinkedHashMap<>();
    private volatile Map<String, Integer> probeFailures = Map.of();

    /** Wall-clock bounds of one executed phase, needed to window the metrics afterwards. */
    public record PhaseRecord(String phaseId, long startMs, long endMs, double targetRate, boolean excluded) {
    }

    public RunContext(String runId, Scenario scenario, String scenarioHash,
                      Map<String, String> tags, String baselineRunId) {
        this.runId = runId;
        this.scenario = scenario;
        this.scenarioHash = scenarioHash;
        this.tags = tags == null ? Map.of() : Map.copyOf(tags);
        this.baselineRunId = baselineRunId;
    }

    public String runId() {
        return runId;
    }

    public Scenario scenario() {
        return scenario;
    }

    public String scenarioHash() {
        return scenarioHash;
    }

    public Map<String, String> tags() {
        return tags;
    }

    public String baselineRunId() {
        return baselineRunId;
    }

    public RunState state() {
        return state;
    }

    public void state(RunState s) {
        this.state = s;
    }

    public String currentPhase() {
        return currentPhase;
    }

    public void currentPhase(String p) {
        this.currentPhase = p;
    }

    public String failureReason() {
        return failureReason;
    }

    public void failureReason(String r) {
        this.failureReason = r;
    }

    public long startedAtMs() {
        return startedAtMs;
    }

    public void startedAtMs(long v) {
        this.startedAtMs = v;
    }

    public long endedAtMs() {
        return endedAtMs;
    }

    public void endedAtMs(long v) {
        this.endedAtMs = v;
    }

    public long measuredFromMs() {
        return measuredFromMs;
    }

    public long measuredToMs() {
        return measuredToMs;
    }

    public void measuredWindow(long fromMs, long toMs) {
        this.measuredFromMs = fromMs;
        this.measuredToMs = toMs;
    }

    public Map<String, LatencyRecorder> recorders() {
        return recorders;
    }

    public Map<String, ArrivalScheduler> schedulers() {
        return schedulers;
    }

    public Map<String, InsertDriver> insertDrivers() {
        return insertDrivers;
    }

    public Map<String, QueryDriver> queryDrivers() {
        return queryDrivers;
    }

    public Map<String, BatchFactory> batchFactories() {
        return batchFactories;
    }

    public ErrorCounter errors() {
        return errors;
    }

    public void addChaosMark(ChaosMark mark) {
        chaosMarks.add(mark);
    }

    public List<ChaosMark> chaosMarks() {
        synchronized (chaosMarks) {
            return List.copyOf(chaosMarks);
        }
    }

    public void addPhaseRecord(PhaseRecord record) {
        phaseRecords.add(record);
    }

    public List<PhaseRecord> phaseRecords() {
        synchronized (phaseRecords) {
            return List.copyOf(phaseRecords);
        }
    }

    public List<MetricSample> sutSamples() {
        return sutSamples;
    }

    public void sutSamples(List<MetricSample> samples) {
        this.sutSamples = samples;
    }

    public List<SloResult> sloResults() {
        return sloResults;
    }

    public void sloResults(List<SloResult> results) {
        this.sloResults = results;
    }

    public List<ReconResult> reconResults() {
        return reconResults;
    }

    public void reconResults(List<ReconResult> results) {
        this.reconResults = results;
    }

    public Map<String, String> environment() {
        return environment;
    }

    public void environment(Map<String, String> env) {
        this.environment = env;
    }

    public Map<String, Integer> probeFailures() {
        return probeFailures;
    }

    public void probeFailures(Map<String, Integer> failures) {
        this.probeFailures = failures;
    }

    /** True only if every SLO and every reconciliation check passed. */
    public boolean passed() {
        return sloResults.stream().allMatch(SloResult::passed)
                && reconResults.stream().allMatch(ReconResult::passed)
                && state == RunState.COMPLETED;
    }

    public long durationMs() {
        long end = endedAtMs > 0 ? endedAtMs : System.currentTimeMillis();
        return startedAtMs == 0 ? 0 : end - startedAtMs;
    }
}
