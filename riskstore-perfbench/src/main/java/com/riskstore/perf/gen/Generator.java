package com.riskstore.perf.gen;

import java.util.Map;

/**
 * Renders synthetic L0 payloads.
 *
 * <p>Implementations append directly to a {@link StringBuilder} rather than returning objects or
 * going through a JSON library. At six-figure row rates, per-row Jackson serialisation becomes
 * the bottleneck and you end up benchmarking the harness instead of the database.
 *
 * <p>Cardinality is the property that matters most. ClickHouse compression ratios, primary-index
 * effectiveness and part sizes all follow from how many distinct values each column holds, so
 * every generator states its cardinalities explicitly rather than emitting uniform noise.
 */
public interface Generator {

    /** Name referenced from a scenario as {@code generator.ref}. */
    String id();

    /** Append exactly one JSON object (no trailing newline) for the given row. */
    void appendRow(StringBuilder out, SeededRandom rnd, long rowSequence);

    /** Applies scenario-supplied overrides, e.g. book count or business date. */
    default void configure(Map<String, String> params) {
    }

    /** Human-readable cardinality summary, reproduced in the report so results are interpretable. */
    default String describe() {
        return id();
    }
}
