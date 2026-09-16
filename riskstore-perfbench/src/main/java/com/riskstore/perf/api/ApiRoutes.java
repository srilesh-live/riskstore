package com.riskstore.perf.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.riskstore.perf.ch.ClickHouseClientFactory;
import com.riskstore.perf.config.HarnessConfig;
import com.riskstore.perf.engine.ChaosMark;
import com.riskstore.perf.engine.Preflight;
import com.riskstore.perf.engine.RunContext;
import com.riskstore.perf.engine.RunRegistry;
import com.riskstore.perf.report.HtmlReportRenderer;
import com.riskstore.perf.report.JsonReportRenderer;
import com.riskstore.perf.report.MarkdownRenderer;
import com.riskstore.perf.report.RunReport;
import com.riskstore.perf.report.SummaryBuilder;
import com.riskstore.perf.scenario.Scenario;
import com.riskstore.perf.scenario.ScenarioLoader;
import com.riskstore.perf.verify.BaselineComparator;
import io.muserver.Method;
import io.muserver.MuServerBuilder;
import io.muserver.MuRequest;
import io.muserver.MuResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The on-demand control surface.
 *
 * <p>Plain route handlers rather than a JAX-RS layer: the API is small, and keeping it explicit
 * avoids dragging an annotation-processing stack into a tool whose whole job is to not perturb
 * what it measures.
 */
public final class ApiRoutes {

    private static final Logger log = LoggerFactory.getLogger(ApiRoutes.class);
    private static final ObjectMapper JSON = JsonReportRenderer.mapper();

    private final HarnessConfig config;
    private final ScenarioLoader scenarios;
    private final RunRegistry runs;
    private final ClickHouseClientFactory clients;

    public ApiRoutes(HarnessConfig config,
                     ScenarioLoader scenarios,
                     RunRegistry runs,
                     ClickHouseClientFactory clients) {
        this.config = config;
        this.scenarios = scenarios;
        this.runs = runs;
        this.clients = clients;
    }

    public MuServerBuilder register(MuServerBuilder builder) {
        return builder
                .addHandler(Method.GET, "/api/v1/health", this::health)
                .addHandler(Method.GET, "/api/v1/preflight", this::preflight)
                .addHandler(Method.GET, "/api/v1/scenarios", this::listScenarios)
                .addHandler(Method.POST, "/api/v1/runs", this::startRun)
                .addHandler(Method.GET, "/api/v1/runs", this::listRuns)
                .addHandler(Method.GET, "/api/v1/runs/{id}", this::runStatus)
                .addHandler(Method.POST, "/api/v1/runs/{id}/marks", this::addMark)
                .addHandler(Method.POST, "/api/v1/runs/{id}/abort", this::abortRun)
                .addHandler(Method.GET, "/api/v1/runs/{id}/report", this::runReport)
                .addHandler(Method.GET, "/api/v1/runs/{id}/compare/{baselineId}", this::compare)
                .addHandler(Method.GET, "/", this::index);
    }

    private void health(MuRequest req, MuResponse resp, Map<String, String> params) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "up");
        body.put("harnessVersion", SummaryBuilder.HARNESS_VERSION);
        body.put("cluster", config.sut.clusterName);
        body.put("nodes", clients.endpoints());
        body.put("scenarios", scenarios.all().keySet());
        body.put("activeRun", runs.activeRunId().orElse(null));
        json(resp, 200, body);
    }

    private void preflight(MuRequest req, MuResponse resp, Map<String, String> params) {
        Preflight.Report report = new Preflight(clients, config).run();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ok", report.ok());
        body.put("blocker", report.blocker());
        body.put("checks", report.checks());
        body.put("environment", report.environment());
        json(resp, report.ok() ? 200 : 412, body);
    }

    private void listScenarios(MuRequest req, MuResponse resp, Map<String, String> params) {
        Map<String, Object> body = new LinkedHashMap<>();
        scenarios.all().forEach((id, s) -> {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("description", s.description);
            entry.put("hash", scenarios.hash(id));
            entry.put("phases", s.phases.stream().map(p -> p.id + " (" + p.duration + ")").toList());
            entry.put("workloads", s.workloads.stream().map(w -> w.id + ":" + w.kind).toList());
            entry.put("sloCount", s.slo.size());
            entry.put("estimatedDuration", totalDuration(s));
            body.put(id, entry);
        });
        json(resp, 200, body);
    }

    private static String totalDuration(Scenario s) {
        long ms = s.phases.stream()
                .filter(p -> p.duration != null)
                .mapToLong(p -> p.duration.toMillis())
                .sum();
        return MarkdownRenderer.formatDuration(ms);
    }

    private void startRun(MuRequest req, MuResponse resp, Map<String, String> params) throws Exception {
        JsonNode body = readJson(req);
        String scenarioId = text(body, "scenarioId");
        if (scenarioId == null) {
            json(resp, 400, Map.of("error", "scenarioId is required"));
            return;
        }

        Map<String, String> tags = new LinkedHashMap<>();
        JsonNode tagNode = body.get("tags");
        if (tagNode != null && tagNode.isObject()) {
            tagNode.fields().forEachRemaining(e -> tags.put(e.getKey(), e.getValue().asText()));
        }

        try {
            String runId = runs.start(scenarioId, tags, text(body, "baselineRunId"));
            json(resp, 202, Map.of(
                    "runId", runId,
                    "status", "/api/v1/runs/" + runId,
                    "report", "/api/v1/runs/" + runId + "/report?format=html"));
        } catch (RunRegistry.RunRejected e) {
            json(resp, 409, Map.of("error", e.getMessage()));
        }
    }

    private void listRuns(MuRequest req, MuResponse resp, Map<String, String> params) {
        var body = runs.list().stream().map(ApiRoutes::statusOf).toList();
        json(resp, 200, body);
    }

    private void runStatus(MuRequest req, MuResponse resp, Map<String, String> params) {
        Optional<RunContext> ctx = runs.context(params.get("id"));
        if (ctx.isEmpty()) {
            json(resp, 404, Map.of("error", "unknown run " + params.get("id")));
            return;
        }
        json(resp, 200, statusOf(ctx.get()));
    }

    /** Live status: enough for an operator to watch a soak run without opening the full report. */
    private static Map<String, Object> statusOf(RunContext ctx) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("runId", ctx.runId());
        body.put("scenarioId", ctx.scenario().id);
        body.put("state", ctx.state().name());
        body.put("phase", ctx.currentPhase());
        body.put("startedAtMs", ctx.startedAtMs());
        body.put("durationMs", ctx.durationMs());
        body.put("failureReason", ctx.failureReason());

        Map<String, Object> live = new LinkedHashMap<>();
        ctx.recorders().forEach((id, recorder) -> live.put(id, Map.of(
                "ops", recorder.ops(),
                "rows", recorder.rows(),
                "errors", recorder.errors(),
                "overloadDrops", recorder.overloadDrops(),
                "p99Ms", recorder.percentileUs(99.0) / 1000.0,
                "targetRate", Optional.ofNullable(ctx.schedulers().get(id))
                        .map(s -> s.rate()).orElse(0.0),
                "inFlight", Optional.ofNullable(ctx.schedulers().get(id))
                        .map(s -> s.inFlightNow()).orElse(0))));
        body.put("workloads", live);
        body.put("chaosMarks", ctx.chaosMarks().size());
        return body;
    }

    /**
     * Records an operator-injected fault. The harness never injects faults itself; see
     * {@link ChaosMark} for why.
     */
    private void addMark(MuRequest req, MuResponse resp, Map<String, String> params) throws Exception {
        JsonNode body = readJson(req);
        String kind = text(body, "kind");
        if (kind == null) {
            json(resp, 400, Map.of("error", "kind is required, e.g. replica-kill or keeper-quorum-loss"));
            return;
        }
        ChaosMark mark = ChaosMark.operator(kind, orEmpty(text(body, "target")), text(body, "note"));
        if (!runs.mark(params.get("id"), mark)) {
            json(resp, 404, Map.of("error", "unknown run " + params.get("id")));
            return;
        }
        json(resp, 201, Map.of("marked", mark));
    }

    private void abortRun(MuRequest req, MuResponse resp, Map<String, String> params) {
        boolean aborted = runs.abort(params.get("id"));
        json(resp, aborted ? 200 : 404, Map.of(
                "aborted", aborted,
                "note", aborted
                        ? "Load generation stops; the report is still produced from what was collected."
                        : "Run not found or already finished."));
    }

    private void runReport(MuRequest req, MuResponse resp, Map<String, String> params) {
        Optional<RunReport> report = runs.report(params.get("id"));
        if (report.isEmpty()) {
            json(resp, 404, Map.of("error", "unknown run " + params.get("id")));
            return;
        }
        String format = Optional.ofNullable(req.query().get("format")).orElse("json");
        switch (format) {
            case "html" -> {
                resp.status(200);
                resp.contentType("text/html; charset=utf-8");
                resp.write("<!doctype html><html><head><meta charset=\"utf-8\">"
                        + "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
                        + HtmlReportRenderer.render(report.get()) + "</body></html>");
            }
            case "md" -> {
                resp.status(200);
                resp.contentType("text/markdown; charset=utf-8");
                resp.write(MarkdownRenderer.render(report.get()));
            }
            default -> {
                resp.status(200);
                resp.contentType("application/json; charset=utf-8");
                resp.write(JsonReportRenderer.render(report.get()));
            }
        }
    }

    private void compare(MuRequest req, MuResponse resp, Map<String, String> params) {
        Optional<RunReport> current = runs.report(params.get("id"));
        Optional<RunReport> baseline = runs.report(params.get("baselineId"));
        if (current.isEmpty() || baseline.isEmpty()) {
            json(resp, 404, Map.of("error", "both runs must exist in this harness instance"));
            return;
        }
        var comparison = BaselineComparator.compare(baseline.get(), current.get());
        // 422 when incomparable, so a CI gate cannot mistake "we could not tell" for "no regression".
        json(resp, comparison.comparable() ? (comparison.regressed() ? 409 : 200) : 422, comparison);
    }

    private void index(MuRequest req, MuResponse resp, Map<String, String> params) {
        resp.status(200);
        resp.contentType("text/html; charset=utf-8");
        StringBuilder sb = new StringBuilder();
        sb.append("<!doctype html><html><head><meta charset=\"utf-8\"><title>riskstore-perfbench</title>")
                .append("<style>body{font-family:system-ui,sans-serif;margin:2rem;max-width:60rem;line-height:1.6}")
                .append("code{background:#eee;padding:.1rem .3rem;border-radius:3px}</style></head><body>");
        sb.append("<h1>riskstore-perfbench</h1><p>Cluster <code>")
                .append(config.sut.clusterName).append("</code>, ")
                .append(clients.nodeCount()).append(" replica(s).</p>");
        sb.append("<h2>Scenarios</h2><ul>");
        scenarios.all().forEach((id, s) -> sb.append("<li><code>").append(id).append("</code> &mdash; ")
                .append(s.description).append(" (").append(totalDuration(s)).append(")</li>"));
        sb.append("</ul><h2>Runs</h2><ul>");
        runs.list().forEach(r -> sb.append("<li><a href=\"/api/v1/runs/").append(r.runId())
                .append("/report?format=html\">").append(r.runId()).append("</a> &mdash; ")
                .append(r.state()).append("</li>"));
        sb.append("</ul></body></html>");
        resp.write(sb.toString());
    }

    private static JsonNode readJson(MuRequest req) throws Exception {
        String body = req.readBodyAsString();
        if (body == null || body.isBlank()) {
            return JSON.createObjectNode();
        }
        return JSON.readTree(body);
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }

    private static String orEmpty(String s) {
        return s == null ? "" : s;
    }

    private static void json(MuResponse resp, int status, Object body) {
        try {
            resp.status(status);
            resp.contentType("application/json; charset=utf-8");
            resp.write(JSON.writeValueAsString(body));
        } catch (Exception e) {
            log.error("Could not write JSON response", e);
            resp.status(500);
            resp.write("{\"error\":\"serialisation failed\"}");
        }
    }
}
