package com.riskstore.perf.report;

import com.riskstore.perf.verify.ReconResult;
import com.riskstore.perf.verify.SloResult;

import java.time.Instant;
import java.util.Map;

/**
 * The executive summary: one screen, no charts.
 *
 * <p>This is the artefact that goes into a go-live pack, so it leads with the verdict and the
 * caveats rather than with methodology. Anything that undermines confidence in the numbers
 * appears above the numbers, not in an appendix.
 */
public final class MarkdownRenderer {

    private MarkdownRenderer() {
    }

    public static String render(RunReport report) {
        StringBuilder md = new StringBuilder(4096);
        RunReport.Meta meta = report.meta();
        RunReport.Verdict verdict = report.verdict();

        md.append("# Riskstore performance run: ").append(meta.scenarioId()).append("\n\n");
        md.append("**").append(verdict.passed() ? "PASS" : "FAIL").append("** — ")
                .append(verdict.headline()).append("\n\n");

        if (!verdict.measurementTrustworthy()) {
            md.append("> **Measurement caveats — read before quoting these numbers**\n>\n");
            for (String caveat : verdict.trustCaveats()) {
                md.append("> - ").append(caveat).append('\n');
            }
            md.append('\n');
        }

        md.append("| | |\n|---|---|\n");
        md.append("| Run id | `").append(meta.runId()).append("` |\n");
        md.append("| Scenario | ").append(meta.scenarioId()).append(" (`")
                .append(meta.scenarioHash()).append("`) |\n");
        md.append("| Started | ").append(Instant.ofEpochMilli(meta.startedAtMs())).append(" |\n");
        md.append("| Duration | ").append(formatDuration(meta.durationMs())).append(" |\n");
        md.append("| Steady-state window | ")
                .append(formatDuration(Math.max(0, meta.measuredToMs() - meta.measuredFromMs()))).append(" |\n");
        md.append("| SLOs passed | ").append(verdict.sloPassed()).append(" / ").append(verdict.sloTotal()).append(" |\n");
        md.append("| Correctness checks passed | ").append(verdict.reconPassed()).append(" / ")
                .append(verdict.reconTotal()).append(" |\n");
        if (!Double.isNaN(verdict.headroomFraction())) {
            md.append("| Tightest SLO margin | ").append(String.format("%.0f%%", verdict.headroomFraction() * 100))
                    .append(" |\n");
        }
        md.append('\n');

        renderWorkloads(md, report);
        renderSlos(md, report);
        renderRecon(md, report);
        renderErrors(md, report);
        renderNotes(md, report);

        return md.toString();
    }

    private static void renderWorkloads(StringBuilder md, RunReport report) {
        md.append("## Workloads\n\n");
        md.append("| Workload | Kind | Ops | Rows | Rows/s | MB/s | p50 | p95 | p99 | p99.9 | max | Errors |\n");
        md.append("|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|\n");
        for (RunReport.WorkloadSummary w : report.workloads()) {
            RunReport.LatencyStats l = w.latency();
            md.append("| ").append(w.id())
                    .append(" | ").append(w.kind())
                    .append(" | ").append(num(w.ops()))
                    .append(" | ").append(num(w.rows()))
                    .append(" | ").append(String.format("%,.0f", w.rowsPerSec()))
                    .append(" | ").append(String.format("%.1f", w.mbPerSec()))
                    .append(" | ").append(ms(l.p50Ms()))
                    .append(" | ").append(ms(l.p95Ms()))
                    .append(" | ").append(ms(l.p99Ms()))
                    .append(" | ").append(ms(l.p999Ms()))
                    .append(" | ").append(ms(l.maxMs()))
                    .append(" | ").append(w.errors())
                    .append(" |\n");
        }
        md.append('\n');

        for (RunReport.WorkloadSummary w : report.workloads()) {
            if (w.perQuery().isEmpty()) {
                continue;
            }
            md.append("### ").append(w.id()).append(" — per statement\n\n");
            md.append("| Query | Ops | Rows | p50 | p95 | p99 | max | Errors |\n");
            md.append("|---|--:|--:|--:|--:|--:|--:|--:|\n");
            for (RunReport.QueryBreakdown q : w.perQuery()) {
                md.append("| `").append(q.ref()).append("` | ").append(num(q.ops()))
                        .append(" | ").append(num(q.rows()))
                        .append(" | ").append(ms(q.latency().p50Ms()))
                        .append(" | ").append(ms(q.latency().p95Ms()))
                        .append(" | ").append(ms(q.latency().p99Ms()))
                        .append(" | ").append(ms(q.latency().maxMs()))
                        .append(" | ").append(q.errors()).append(" |\n");
            }
            md.append('\n');
        }
    }

    private static void renderSlos(StringBuilder md, RunReport report) {
        if (report.slos().isEmpty()) {
            return;
        }
        md.append("## SLO scorecard\n\n");
        md.append("| | Metric | Limit | Actual | Margin |\n|---|---|---|--:|--:|\n");
        for (SloResult s : report.slos()) {
            md.append("| ").append(s.passed() ? "PASS" : "FAIL")
                    .append(" | `").append(s.metric()).append("` ").append(s.op())
                    .append(" | ").append(s.thresholdRaw())
                    .append(" | ").append(s.resolved() ? fmt(s.actual()) + " " + s.unit() : "n/a")
                    .append(" | ").append(s.resolved() && !Double.isNaN(s.headroomFraction())
                            ? String.format("%.0f%%", s.headroomFraction() * 100) : "—")
                    .append(" |\n");
            if (!s.resolved()) {
                md.append("| | ").append(s.detail()).append(" | | | |\n");
            }
        }
        md.append('\n');
    }

    private static void renderRecon(StringBuilder md, RunReport report) {
        if (report.reconciliation().isEmpty()) {
            return;
        }
        md.append("## Correctness under load\n\n");
        md.append("| | Check | Expected | Actual | Delta |\n|---|---|--:|--:|--:|\n");
        for (ReconResult r : report.reconciliation()) {
            md.append("| ").append(r.passed() ? "PASS" : "FAIL")
                    .append(" | ").append(r.name())
                    .append(" | ").append(fmt(r.expected()))
                    .append(" | ").append(fmt(r.actual()))
                    .append(" | ").append(fmt(r.delta())).append(" |\n");
        }
        md.append('\n');
    }

    private static void renderErrors(StringBuilder md, RunReport report) {
        if (report.errors().isEmpty()) {
            return;
        }
        md.append("## Errors\n\n| Cause | Count |\n|---|--:|\n");
        for (Map.Entry<String, Long> e : report.errors().entrySet()) {
            md.append("| `").append(e.getKey()).append("` | ").append(e.getValue()).append(" |\n");
        }
        md.append('\n');
    }

    private static void renderNotes(StringBuilder md, RunReport report) {
        if (report.notes().isEmpty()) {
            return;
        }
        md.append("## Method notes\n\n");
        report.notes().forEach(n -> md.append("- ").append(n).append('\n'));
        md.append('\n');
    }

    public static String formatDuration(long ms) {
        long seconds = ms / 1000;
        if (seconds < 60) {
            return seconds + "s";
        }
        if (seconds < 3600) {
            return (seconds / 60) + "m " + (seconds % 60) + "s";
        }
        return (seconds / 3600) + "h " + ((seconds % 3600) / 60) + "m";
    }

    private static String num(long v) {
        return String.format("%,d", v);
    }

    private static String ms(double v) {
        if (v >= 1000) {
            return String.format("%.2fs", v / 1000);
        }
        return String.format("%.1fms", v);
    }

    private static String fmt(double v) {
        if (Double.isNaN(v)) {
            return "n/a";
        }
        return Math.abs(v) >= 1000 ? String.format("%,.0f", v) : String.format("%.2f", v);
    }
}
