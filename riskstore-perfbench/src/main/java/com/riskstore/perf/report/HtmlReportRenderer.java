package com.riskstore.perf.report;

import com.riskstore.perf.engine.ChaosMark;
import com.riskstore.perf.metrics.HistogramWindow;
import com.riskstore.perf.verify.ReconResult;
import com.riskstore.perf.verify.SloResult;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The detailed report: a single self-contained HTML file.
 *
 * <p>No CDN, no external stylesheet, no remote font. On-prem risk infrastructure is normally
 * air-gapped, and a report that renders as unstyled text on the machine where it matters is not
 * a report. Everything - CSS, charts, data - is inlined.
 *
 * <p>The layout puts server-side internals on the same time axis as client-side latency, because
 * that juxtaposition is the whole point: a latency chart alone says something got slow, and the
 * part-count and replication-lag charts beside it say why.
 */
public final class HtmlReportRenderer {

    /** Server-side series worth charting by default, in the order an operator would read them. */
    private static final List<String> HEADLINE_METRICS = List.of(
            "MaxPartCountForPartition",
            "ActiveParts",
            "MergesRunning",
            "DelayedInserts",
            "ReplicationAbsoluteDelay",
            "ReplicationQueueSize",
            "KeeperAvgLatencyMs",
            "KeeperOutstandingRequests",
            "KeeperCommitLag",
            "MemoryTracking",
            "DiskAvailableBytes");

    private HtmlReportRenderer() {
    }

    public static String render(RunReport report) {
        StringBuilder html = new StringBuilder(64 * 1024);
        RunReport.Meta meta = report.meta();
        RunReport.Verdict verdict = report.verdict();

        html.append("<title>Perf run ").append(esc(meta.scenarioId())).append("</title>");
        html.append("<style>").append(css()).append("</style>");

        html.append("<header class=\"masthead ").append(verdict.passed() ? "pass" : "fail").append("\">");
        html.append("<div class=\"badge\">").append(verdict.passed() ? "PASS" : "FAIL").append("</div>");
        html.append("<div><h1>").append(esc(meta.scenarioId())).append("</h1>");
        html.append("<p class=\"headline\">").append(esc(verdict.headline())).append("</p>");
        if (meta.scenarioDescription() != null && !meta.scenarioDescription().isBlank()) {
            html.append("<p class=\"desc\">").append(esc(meta.scenarioDescription())).append("</p>");
        }
        html.append("</div></header>");

        renderCaveats(html, verdict);
        renderMeta(html, meta, verdict);
        renderSlos(html, report);
        renderRecon(html, report);
        renderWorkloads(html, report);
        renderLatencyCharts(html, report);
        renderSutCharts(html, report);
        renderChaos(html, report);
        renderErrors(html, report);
        renderEnvironment(html, report);
        renderNotes(html, report);

        html.append("<footer><p>riskstore-perfbench ").append(esc(meta.harnessVersion()))
                .append(" &middot; run <code>").append(esc(meta.runId())).append("</code></p></footer>");
        return html.toString();
    }

    private static void renderCaveats(StringBuilder html, RunReport.Verdict verdict) {
        if (verdict.measurementTrustworthy()) {
            return;
        }
        html.append("<section class=\"caveats\"><h2>Measurement caveats</h2>");
        html.append("<p>These affect whether the numbers below can be trusted at all. Read them first.</p><ul>");
        verdict.trustCaveats().forEach(c -> html.append("<li>").append(esc(c)).append("</li>"));
        html.append("</ul></section>");
    }

    private static void renderMeta(StringBuilder html, RunReport.Meta meta, RunReport.Verdict verdict) {
        html.append("<section><h2>Run</h2><div class=\"tiles\">");
        tile(html, "Duration", MarkdownRenderer.formatDuration(meta.durationMs()), "wall clock");
        tile(html, "Steady state",
                MarkdownRenderer.formatDuration(Math.max(0, meta.measuredToMs() - meta.measuredFromMs())),
                "measured window");
        tile(html, "SLOs", verdict.sloPassed() + " / " + verdict.sloTotal(), "passed");
        tile(html, "Correctness", verdict.reconPassed() + " / " + verdict.reconTotal(), "checks passed");
        if (!Double.isNaN(verdict.headroomFraction())) {
            tile(html, "Tightest margin", String.format("%.0f%%", verdict.headroomFraction() * 100),
                    "against nearest SLO");
        }
        html.append("</div>");
        html.append("<p class=\"meta\">Started ").append(Instant.ofEpochMilli(meta.startedAtMs()))
                .append(" &middot; scenario <code>").append(esc(meta.scenarioHash()))
                .append("</code> &middot; state ").append(esc(meta.state()));
        if (meta.baselineRunId() != null) {
            html.append(" &middot; baseline <code>").append(esc(meta.baselineRunId())).append("</code>");
        }
        html.append("</p></section>");
    }

    private static void tile(StringBuilder html, String label, String value, String sub) {
        html.append("<div class=\"tile\"><div class=\"tile-label\">").append(esc(label))
                .append("</div><div class=\"tile-value\">").append(esc(value))
                .append("</div><div class=\"tile-sub\">").append(esc(sub)).append("</div></div>");
    }

    private static void renderSlos(StringBuilder html, RunReport report) {
        if (report.slos().isEmpty()) {
            return;
        }
        html.append("<section><h2>SLO scorecard</h2><div class=\"scroll\"><table>");
        html.append("<thead><tr><th></th><th>Metric</th><th>Limit</th><th class=\"n\">Actual</th>")
                .append("<th class=\"n\">Margin</th><th>Notes</th></tr></thead><tbody>");
        for (SloResult s : report.slos()) {
            html.append("<tr class=\"").append(s.passed() ? "ok" : "bad").append("\">");
            html.append("<td><span class=\"pill ").append(s.passed() ? "pill-ok" : "pill-bad").append("\">")
                    .append(s.passed() ? "PASS" : "FAIL").append("</span></td>");
            html.append("<td><code>").append(esc(s.metric())).append("</code> ").append(esc(s.op())).append("</td>");
            html.append("<td>").append(esc(s.thresholdRaw())).append("</td>");
            html.append("<td class=\"n\">").append(s.resolved()
                    ? esc(fmt(s.actual()) + " " + s.unit()) : "n/a").append("</td>");
            html.append("<td class=\"n\">").append(s.resolved() && !Double.isNaN(s.headroomFraction())
                    ? String.format("%.0f%%", s.headroomFraction() * 100) : "&mdash;").append("</td>");
            html.append("<td class=\"muted\">").append(esc(s.detail())).append("</td>");
            html.append("</tr>");
        }
        html.append("</tbody></table></div></section>");
    }

    private static void renderRecon(StringBuilder html, RunReport report) {
        if (report.reconciliation().isEmpty()) {
            return;
        }
        html.append("<section><h2>Correctness under load</h2>");
        html.append("<p class=\"muted\">A run that loses or duplicates rows has not passed, however fast it was.</p>");
        html.append("<div class=\"scroll\"><table><thead><tr><th></th><th>Check</th><th class=\"n\">Expected</th>")
                .append("<th class=\"n\">Actual</th><th class=\"n\">Delta</th><th>Detail</th></tr></thead><tbody>");
        for (ReconResult r : report.reconciliation()) {
            html.append("<tr class=\"").append(r.passed() ? "ok" : "bad").append("\">");
            html.append("<td><span class=\"pill ").append(r.passed() ? "pill-ok" : "pill-bad").append("\">")
                    .append(r.passed() ? "PASS" : "FAIL").append("</span></td>");
            html.append("<td>").append(esc(r.name())).append("</td>");
            html.append("<td class=\"n\">").append(fmt(r.expected())).append("</td>");
            html.append("<td class=\"n\">").append(fmt(r.actual())).append("</td>");
            html.append("<td class=\"n\">").append(fmt(r.delta())).append("</td>");
            html.append("<td class=\"muted\">").append(esc(r.detail())).append("</td></tr>");
        }
        html.append("</tbody></table></div></section>");
    }

    private static void renderWorkloads(StringBuilder html, RunReport report) {
        html.append("<section><h2>Workloads</h2><div class=\"scroll\"><table>");
        html.append("<thead><tr><th>Workload</th><th>Kind</th><th class=\"n\">Ops</th><th class=\"n\">Rows</th>")
                .append("<th class=\"n\">Rows/s</th><th class=\"n\">MB/s</th><th class=\"n\">p50</th>")
                .append("<th class=\"n\">p95</th><th class=\"n\">p99</th><th class=\"n\">p99.9</th>")
                .append("<th class=\"n\">max</th><th class=\"n\">Errors</th><th class=\"n\">Shed</th>")
                .append("</tr></thead><tbody>");
        for (RunReport.WorkloadSummary w : report.workloads()) {
            RunReport.LatencyStats l = w.latency();
            html.append("<tr><td><strong>").append(esc(w.id())).append("</strong></td>");
            html.append("<td>").append(esc(w.kind())).append("</td>");
            html.append("<td class=\"n\">").append(num(w.ops())).append("</td>");
            html.append("<td class=\"n\">").append(num(w.rows())).append("</td>");
            html.append("<td class=\"n\">").append(String.format("%,.0f", w.rowsPerSec())).append("</td>");
            html.append("<td class=\"n\">").append(String.format("%.1f", w.mbPerSec())).append("</td>");
            html.append("<td class=\"n\">").append(ms(l.p50Ms())).append("</td>");
            html.append("<td class=\"n\">").append(ms(l.p95Ms())).append("</td>");
            html.append("<td class=\"n\">").append(ms(l.p99Ms())).append("</td>");
            html.append("<td class=\"n\">").append(ms(l.p999Ms())).append("</td>");
            html.append("<td class=\"n\">").append(ms(l.maxMs())).append("</td>");
            html.append("<td class=\"n ").append(w.errors() > 0 ? "bad-text" : "").append("\">")
                    .append(num(w.errors())).append("</td>");
            html.append("<td class=\"n ").append(w.overloadDrops() > 0 ? "bad-text" : "").append("\">")
                    .append(num(w.overloadDrops())).append("</td></tr>");
        }
        html.append("</tbody></table></div>");

        for (RunReport.WorkloadSummary w : report.workloads()) {
            if (w.perQuery().isEmpty()) {
                continue;
            }
            html.append("<h3>").append(esc(w.id())).append(" &mdash; per statement</h3>");
            html.append("<p class=\"muted\">Ordered by p99. A blended percentile across a mixed report "
                    + "workload hides which statement is actually slow.</p>");
            html.append("<div class=\"scroll\"><table><thead><tr><th>Query</th><th class=\"n\">Ops</th>")
                    .append("<th class=\"n\">Rows</th><th class=\"n\">p50</th><th class=\"n\">p95</th>")
                    .append("<th class=\"n\">p99</th><th class=\"n\">max</th><th class=\"n\">Errors</th>")
                    .append("</tr></thead><tbody>");
            for (RunReport.QueryBreakdown q : w.perQuery()) {
                html.append("<tr><td><code>").append(esc(q.ref())).append("</code></td>");
                html.append("<td class=\"n\">").append(num(q.ops())).append("</td>");
                html.append("<td class=\"n\">").append(num(q.rows())).append("</td>");
                html.append("<td class=\"n\">").append(ms(q.latency().p50Ms())).append("</td>");
                html.append("<td class=\"n\">").append(ms(q.latency().p95Ms())).append("</td>");
                html.append("<td class=\"n\">").append(ms(q.latency().p99Ms())).append("</td>");
                html.append("<td class=\"n\">").append(ms(q.latency().maxMs())).append("</td>");
                html.append("<td class=\"n\">").append(num(q.errors())).append("</td></tr>");
            }
            html.append("</tbody></table></div>");
        }
        html.append("</section>");
    }

    private static void renderLatencyCharts(StringBuilder html, RunReport report) {
        List<Long> faults = report.chaos().stream().map(ChaosMark::tsMs).toList();
        html.append("<section><h2>Latency over time</h2>");
        html.append("<p class=\"muted\">p99 per 10s window. Vertical lines mark injected faults.</p>");
        for (RunReport.WorkloadSummary w : report.workloads()) {
            if (w.windows().isEmpty()) {
                continue;
            }
            List<InlineSvgChart.XY> points = w.windows().stream()
                    .map(win -> new InlineSvgChart.XY(win.startMs(), win.p99Us() / 1000.0))
                    .toList();
            html.append(InlineSvgChart.lineChart(w.id() + " p99", "ms", points, faults));
        }
        html.append("</section>");
    }

    /**
     * Server-side internals, on the same time axis as latency above.
     *
     * <p>Headline metrics are charted; everything else collapses into a summary table so a long
     * soak run does not produce a hundred charts nobody reads.
     */
    private static void renderSutCharts(StringBuilder html, RunReport report) {
        if (report.sut().isEmpty()) {
            return;
        }
        List<Long> faults = report.chaos().stream().map(ChaosMark::tsMs).toList();
        Set<String> headline = Set.copyOf(HEADLINE_METRICS);

        html.append("<section><h2>ClickHouse internals</h2>");
        html.append("<p class=\"muted\">Sampled from system tables during the run. Part count and merge "
                + "backlog are the early-warning signals: inserts are delayed at 150 parts per partition "
                + "and rejected at 300.</p>");

        for (String metric : HEADLINE_METRICS) {
            report.sut().stream()
                    .filter(s -> s.metric().equals(metric))
                    .forEach(series -> {
                        List<InlineSvgChart.XY> points = series.points().stream()
                                .map(p -> new InlineSvgChart.XY(p.tsMs(), p.value()))
                                .toList();
                        String title = series.metric()
                                + (series.label().isEmpty() ? "" : " " + series.label())
                                + " @ " + series.node();
                        html.append(InlineSvgChart.lineChart(title, "", points, faults));
                    });
        }

        html.append("<h3>All sampled metrics</h3><div class=\"scroll\"><table>");
        html.append("<thead><tr><th>Metric</th><th>Label</th><th>Node</th><th class=\"n\">min</th>")
                .append("<th class=\"n\">avg</th><th class=\"n\">max</th><th class=\"n\">last</th>")
                .append("</tr></thead><tbody>");
        report.sut().stream()
                .sorted(Comparator.comparing(RunReport.SutSeries::metric)
                        .thenComparing(RunReport.SutSeries::label))
                .forEach(s -> {
                    html.append("<tr").append(headline.contains(s.metric()) ? " class=\"ok\"" : "").append(">");
                    html.append("<td><code>").append(esc(s.metric())).append("</code></td>");
                    html.append("<td class=\"muted\">").append(esc(s.label())).append("</td>");
                    html.append("<td class=\"muted\">").append(esc(s.node())).append("</td>");
                    html.append("<td class=\"n\">").append(InlineSvgChart.shortNumber(s.min())).append("</td>");
                    html.append("<td class=\"n\">").append(InlineSvgChart.shortNumber(s.avg())).append("</td>");
                    html.append("<td class=\"n\">").append(InlineSvgChart.shortNumber(s.max())).append("</td>");
                    html.append("<td class=\"n\">").append(InlineSvgChart.shortNumber(s.last())).append("</td></tr>");
                });
        html.append("</tbody></table></div></section>");
    }

    private static void renderChaos(StringBuilder html, RunReport report) {
        if (report.chaos().isEmpty()) {
            return;
        }
        html.append("<section><h2>Fault timeline</h2><div class=\"scroll\"><table>");
        html.append("<thead><tr><th>Time</th><th>Kind</th><th>Target</th><th>Source</th><th>Note</th></tr></thead><tbody>");
        for (ChaosMark m : report.chaos()) {
            long offset = Math.max(0, m.tsMs() - report.meta().startedAtMs()) / 1000;
            html.append("<tr><td>T+").append(offset).append("s</td>");
            html.append("<td><code>").append(esc(m.kind())).append("</code></td>");
            html.append("<td>").append(esc(m.target())).append("</td>");
            html.append("<td>").append(esc(m.source())).append("</td>");
            html.append("<td class=\"muted\">").append(esc(m.note())).append("</td></tr>");
        }
        html.append("</tbody></table></div></section>");
    }

    private static void renderErrors(StringBuilder html, RunReport report) {
        if (report.errors().isEmpty()) {
            return;
        }
        html.append("<section><h2>Errors</h2><div class=\"scroll\"><table>");
        html.append("<thead><tr><th>Cause</th><th class=\"n\">Count</th><th>Example</th></tr></thead><tbody>");
        for (Map.Entry<String, Long> e : report.errors().entrySet()) {
            html.append("<tr><td><code>").append(esc(e.getKey())).append("</code></td>");
            html.append("<td class=\"n\">").append(num(e.getValue())).append("</td>");
            html.append("<td class=\"muted small\">")
                    .append(esc(truncate(report.errorSamples().getOrDefault(e.getKey(), ""), 400)))
                    .append("</td></tr>");
        }
        html.append("</tbody></table></div></section>");
    }

    private static void renderEnvironment(StringBuilder html, RunReport report) {
        if (report.environment().isEmpty()) {
            return;
        }
        html.append("<section><h2>Environment fingerprint</h2>");
        html.append("<p class=\"muted\">A run against different settings is not comparable to this one.</p>");
        html.append("<div class=\"scroll\"><table><thead><tr><th>Key</th><th>Value</th></tr></thead><tbody>");
        report.environment().forEach((k, v) -> html.append("<tr><td><code>").append(esc(k))
                .append("</code></td><td>").append(esc(v)).append("</td></tr>"));
        html.append("</tbody></table></div></section>");
    }

    private static void renderNotes(StringBuilder html, RunReport report) {
        if (report.notes().isEmpty()) {
            return;
        }
        html.append("<section><h2>Method</h2><ul class=\"notes\">");
        report.notes().forEach(n -> html.append("<li>").append(esc(n)).append("</li>"));
        html.append("</ul></section>");
    }

    private static String css() {
        try (InputStream in = HtmlReportRenderer.class.getResourceAsStream("/report/report.css")) {
            if (in != null) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (Exception ignored) {
            // Fall through to the built-in minimal stylesheet.
        }
        return "body{font-family:system-ui,sans-serif;margin:2rem;line-height:1.5}"
                + "table{border-collapse:collapse;width:100%}td,th{border:1px solid #ccc;padding:.35rem .5rem}";
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }

    private static String esc(String s) {
        return InlineSvgChart.escape(s);
    }

    private static String num(long v) {
        return String.format("%,d", v);
    }

    private static String ms(double v) {
        return v >= 1000 ? String.format("%.2fs", v / 1000) : String.format("%.1fms", v);
    }

    private static String fmt(double v) {
        if (Double.isNaN(v)) {
            return "n/a";
        }
        return Math.abs(v) >= 1000 ? String.format("%,.0f", v) : String.format("%.2f", v);
    }

    /** Exposed for the histogram-detail view the API can render on demand. */
    public static String histogramTable(List<HistogramWindow> windows) {
        StringBuilder sb = new StringBuilder();
        sb.append("<table><thead><tr><th>Window</th><th class=\"n\">Count</th><th class=\"n\">p50</th>")
                .append("<th class=\"n\">p90</th><th class=\"n\">p99</th><th class=\"n\">max</th></tr></thead><tbody>");
        for (HistogramWindow w : windows) {
            sb.append("<tr><td>").append(Instant.ofEpochMilli(w.startMs())).append("</td>");
            sb.append("<td class=\"n\">").append(w.count()).append("</td>");
            sb.append("<td class=\"n\">").append(ms(w.p50Us() / 1000.0)).append("</td>");
            sb.append("<td class=\"n\">").append(ms(w.p90Us() / 1000.0)).append("</td>");
            sb.append("<td class=\"n\">").append(ms(w.p99Us() / 1000.0)).append("</td>");
            sb.append("<td class=\"n\">").append(ms(w.maxUs() / 1000.0)).append("</td></tr>");
        }
        return sb.append("</tbody></table>").toString();
    }
}
