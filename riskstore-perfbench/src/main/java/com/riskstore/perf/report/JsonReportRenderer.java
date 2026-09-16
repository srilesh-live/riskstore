package com.riskstore.perf.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Machine-readable output, and the input to the CI regression gate.
 *
 * <p>Pretty-printed on purpose: these files get committed to a results repository and diffed
 * between runs, and a single-line JSON blob makes that useless.
 */
public final class JsonReportRenderer {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .enable(SerializationFeature.INDENT_OUTPUT);

    private JsonReportRenderer() {
    }

    public static String render(RunReport report) {
        try {
            return MAPPER.writeValueAsString(report);
        } catch (Exception e) {
            throw new IllegalStateException("Could not serialise report for run "
                    + report.meta().runId(), e);
        }
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }
}
