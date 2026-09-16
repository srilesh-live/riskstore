package com.riskstore.perf.results;

import com.clickhouse.client.api.Client;
import com.riskstore.perf.ch.ClickHouseClientFactory;
import com.riskstore.perf.config.HarnessConfig;
import com.riskstore.perf.engine.ChaosMark;
import com.riskstore.perf.metrics.HistogramWindow;
import com.riskstore.perf.report.HtmlReportRenderer;
import com.riskstore.perf.report.JsonReportRenderer;
import com.riskstore.perf.report.MarkdownRenderer;
import com.riskstore.perf.report.RunReport;
import com.riskstore.perf.verify.ReconResult;
import com.riskstore.perf.verify.SloResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.clickhouse.data.ClickHouseFormat;

/**
 * Persists a finished run: always to disk, and to a results database when one is configured.
 *
 * <p>The results database is deliberately a <em>different</em> ClickHouse instance from the
 * system under test. Writing benchmark output into the cluster being benchmarked adds inserts,
 * parts and merges to the thing being measured, which is self-defeating - and worse, it does so
 * in proportion to how much data the run produced.
 *
 * <p>Disk output is written first and unconditionally. A results database that is down must
 * never cost you a soak run that took a day to produce.
 */
public final class ResultsWriter {

    private static final Logger log = LoggerFactory.getLogger(ResultsWriter.class);

    private final HarnessConfig config;

    public ResultsWriter(HarnessConfig config) {
        this.config = config;
    }

    /** @return the directory the run's artefacts were written to */
    public Path write(RunReport report) {
        Path dir = writeFiles(report);
        if (config.results.enabled) {
            try {
                writeDatabase(report);
            } catch (Exception e) {
                log.error("Could not write results for run {} to the results database - "
                        + "the on-disk copy in {} is unaffected", report.meta().runId(), dir, e);
            }
        }
        return dir;
    }

    private Path writeFiles(RunReport report) {
        Path dir = Path.of(config.results.fallbackDir, report.meta().runId());
        try {
            Files.createDirectories(dir);
            Files.writeString(dir.resolve("report.json"), JsonReportRenderer.render(report), StandardCharsets.UTF_8);
            Files.writeString(dir.resolve("summary.md"), MarkdownRenderer.render(report), StandardCharsets.UTF_8);
            Files.writeString(dir.resolve("report.html"), HtmlReportRenderer.render(report), StandardCharsets.UTF_8);
            log.info("Run {} artefacts written to {}", report.meta().runId(), dir.toAbsolutePath());
        } catch (Exception e) {
            log.error("Could not write run artefacts to {}", dir, e);
        }
        return dir;
    }

    private void writeDatabase(RunReport report) {
        try (Client client = ClickHouseClientFactory.forResults(config.results)) {
            new ResultsSchemaBootstrap(client, config.results.database).ensure();

            insert(client, "runs", List.of(runRow(report)));
            insert(client, "op_latency_hist", latencyRows(report));
            insert(client, "sut_metrics", sutRows(report));
            insert(client, "slo_results", sloRows(report));
            insert(client, "reconciliation", reconRows(report));
            insert(client, "chaos_events", chaosRows(report));

            log.info("Run {} results written to {}/{}", report.meta().runId(),
                    config.results.url, config.results.database);
        }
    }

    private void insert(Client client, String table, List<String> jsonRows) {
        if (jsonRows.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder(jsonRows.size() * 128);
        jsonRows.forEach(r -> sb.append(r).append('\n'));
        byte[] payload = sb.toString().getBytes(StandardCharsets.UTF_8);
        try {
            client.insert(table, new ByteArrayInputStream(payload), ClickHouseFormat.JSONEachRow).get();
        } catch (Exception e) {
            throw new IllegalStateException("Insert into results table " + table + " failed", e);
        }
    }

    private String runRow(RunReport report) {
        RunReport.Meta m = report.meta();
        StringBuilder sb = new StringBuilder();
        sb.append('{')
                .append(f("run_id", m.runId())).append(',')
                .append(f("scenario_id", m.scenarioId())).append(',')
                .append(f("scenario_hash", m.scenarioHash())).append(',')
                .append(f("state", m.state())).append(',')
                .append(f("harness_version", m.harnessVersion())).append(',')
                .append(n("started_at_ms", m.startedAtMs())).append(',')
                .append(n("ended_at_ms", m.endedAtMs())).append(',')
                .append(n("duration_ms", m.durationMs())).append(',')
                .append(n("measured_from_ms", m.measuredFromMs())).append(',')
                .append(n("measured_to_ms", m.measuredToMs())).append(',')
                .append(b("passed", report.verdict().passed())).append(',')
                .append(b("trustworthy", report.verdict().measurementTrustworthy())).append(',')
                .append(f("headline", report.verdict().headline())).append(',')
                .append(f("sut_version", report.environment().getOrDefault(
                        firstKeyStartingWith(report, "version@"), ""))).append(',')
                .append(f("settings_fingerprint", report.environment().getOrDefault("settings.fingerprint", "")))
                .append('}');
        return sb.toString();
    }

    private static String firstKeyStartingWith(RunReport report, String prefix) {
        return report.environment().keySet().stream()
                .filter(k -> k.startsWith(prefix))
                .findFirst()
                .orElse("");
    }

    private List<String> latencyRows(RunReport report) {
        List<String> rows = new ArrayList<>();
        for (RunReport.WorkloadSummary w : report.workloads()) {
            for (HistogramWindow win : w.windows()) {
                rows.add("{" + f("run_id", report.meta().runId()) + ","
                        + f("workload_id", w.id()) + ","
                        + n("window_start_ms", win.startMs()) + ","
                        + n("window_end_ms", win.endMs()) + ","
                        + n("count", win.count()) + ","
                        + n("errors", win.errors()) + ","
                        + n("min_us", win.minUs()) + ","
                        + n("max_us", win.maxUs()) + ","
                        + n("p50_us", win.p50Us()) + ","
                        + n("p90_us", win.p90Us()) + ","
                        + n("p99_us", win.p99Us()) + ","
                        + f("encoded_hdr", win.encoded()) + "}");
            }
        }
        return rows;
    }

    private List<String> sutRows(RunReport report) {
        List<String> rows = new ArrayList<>();
        for (RunReport.SutSeries s : report.sut()) {
            for (RunReport.SutSeries.Point p : s.points()) {
                rows.add("{" + f("run_id", report.meta().runId()) + ","
                        + n("ts_ms", p.tsMs()) + ","
                        + f("node", s.node()) + ","
                        + f("metric", s.metric()) + ","
                        + f("label", s.label()) + ","
                        + d("value", p.value()) + "}");
            }
        }
        return rows;
    }

    private List<String> sloRows(RunReport report) {
        List<String> rows = new ArrayList<>();
        for (SloResult s : report.slos()) {
            rows.add("{" + f("run_id", report.meta().runId()) + ","
                    + f("metric", s.metric()) + ","
                    + f("op", s.op()) + ","
                    + f("threshold_raw", s.thresholdRaw()) + ","
                    + d("threshold", s.threshold()) + ","
                    + d("actual", s.actual()) + ","
                    + f("unit", s.unit()) + ","
                    + b("resolved", s.resolved()) + ","
                    + b("passed", s.passed()) + ","
                    + f("detail", s.detail()) + "}");
        }
        return rows;
    }

    private List<String> reconRows(RunReport report) {
        List<String> rows = new ArrayList<>();
        for (ReconResult r : report.reconciliation()) {
            rows.add("{" + f("run_id", report.meta().runId()) + ","
                    + f("check_name", r.name()) + ","
                    + d("expected", r.expected()) + ","
                    + d("actual", r.actual()) + ","
                    + d("tolerance", r.tolerance()) + ","
                    + b("passed", r.passed()) + ","
                    + f("detail", r.detail()) + "}");
        }
        return rows;
    }

    private List<String> chaosRows(RunReport report) {
        List<String> rows = new ArrayList<>();
        for (ChaosMark m : report.chaos()) {
            rows.add("{" + f("run_id", report.meta().runId()) + ","
                    + n("ts_ms", m.tsMs()) + ","
                    + f("kind", m.kind()) + ","
                    + f("target", m.target()) + ","
                    + f("note", m.note()) + ","
                    + f("source", m.source()) + "}");
        }
        return rows;
    }

    private static String f(String key, String value) {
        return "\"" + key + "\":\"" + escape(value) + "\"";
    }

    private static String n(String key, long value) {
        return "\"" + key + "\":" + value;
    }

    private static String d(String key, double value) {
        return "\"" + key + "\":" + (Double.isNaN(value) || Double.isInfinite(value) ? 0 : value);
    }

    private static String b(String key, boolean value) {
        return "\"" + key + "\":" + (value ? 1 : 0);
    }

    private static String escape(String s) {
        if (s == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder(s.length() + 16);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }
}
