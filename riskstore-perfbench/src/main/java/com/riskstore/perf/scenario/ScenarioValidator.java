package com.riskstore.perf.scenario;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Structural validation, applied at load time rather than at run time.
 *
 * <p>A scenario that fails here never becomes loadable, so an operator finds out at startup
 * rather than twenty minutes into a soak test.
 */
public final class ScenarioValidator {

    private static final Set<String> VALID_OPS = Set.of("lte", "lt", "gte", "gt", "eq");

    private ScenarioValidator() {
    }

    public static List<String> validate(Scenario s) {
        List<String> problems = new ArrayList<>();

        if (s.id == null || s.id.isBlank()) {
            problems.add("scenario id is required");
        }
        if (s.phases.isEmpty()) {
            problems.add("at least one phase is required");
        }
        if (s.workloads.isEmpty()) {
            problems.add("at least one workload is required");
        }

        validatePhases(s, problems);
        validateWorkloads(s, problems);
        validateSlos(s, problems);

        return problems;
    }

    private static void validatePhases(Scenario s, List<String> problems) {
        Set<String> seen = new HashSet<>();
        for (Phase p : s.phases) {
            if (p.id == null || p.id.isBlank()) {
                problems.add("every phase needs an id");
                continue;
            }
            if (!seen.add(p.id)) {
                problems.add("duplicate phase id: " + p.id);
            }
            if (p.duration == null || p.duration.isZero() || p.duration.isNegative()) {
                problems.add("phase " + p.id + " needs a positive duration (for/over)");
            }
            if (p.isRamp()) {
                if (p.from == null || p.to == null) {
                    problems.add("ramp phase " + p.id + " needs both from and to");
                }
            } else if (p.rate == null) {
                problems.add("hold phase " + p.id + " needs a rate");
            }
        }
    }

    private static void validateWorkloads(Scenario s, List<String> problems) {
        Set<String> seen = new HashSet<>();
        for (Workload w : s.workloads) {
            if (w.id == null || w.id.isBlank()) {
                problems.add("every workload needs an id");
                continue;
            }
            if (!seen.add(w.id)) {
                problems.add("duplicate workload id: " + w.id);
            }
            if (w.isInsert()) {
                if (w.target == null || w.target.table == null || w.target.table.isBlank()) {
                    problems.add("insert workload " + w.id + " needs target.table");
                }
                if (w.generator == null || w.generator.ref == null || w.generator.ref.isBlank()) {
                    problems.add("insert workload " + w.id + " needs generator.ref");
                }
                if (w.batch == null || w.batch.rows <= 0) {
                    problems.add("insert workload " + w.id + " needs a positive batch.rows");
                }
            } else if (w.isQuery()) {
                if (w.queries == null || w.queries.isEmpty()) {
                    problems.add("query workload " + w.id + " needs at least one entry under queries");
                }
            } else if (w.isInsertSelect()) {
                if (w.sqlRef == null || w.sqlRef.isBlank()) {
                    problems.add("insertSelect workload " + w.id + " needs sqlRef");
                }
                if (w.triggerEvery == null || w.triggerEvery.isZero()) {
                    problems.add("insertSelect workload " + w.id + " needs a positive triggerEvery");
                }
            } else {
                problems.add("workload " + w.id + " has unknown kind: " + w.kind);
            }
        }
    }

    private static void validateSlos(Scenario s, List<String> problems) {
        for (SloSpec slo : s.slo) {
            if (slo.metric == null || slo.metric.isBlank()) {
                problems.add("every slo needs a metric");
                continue;
            }
            if (slo.value == null) {
                problems.add("slo " + slo.metric + " needs a value");
            }
            if (!VALID_OPS.contains(slo.op)) {
                problems.add("slo " + slo.metric + " has unknown op: " + slo.op);
            }
        }
    }
}
