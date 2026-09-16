package com.riskstore.perf.gen;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Resolves a scenario's {@code generator.ref} to a fresh, configured {@link Generator}.
 *
 * <p>A new instance is handed to each workload rather than sharing one, because
 * {@link Generator#configure(Map)} mutates cardinality settings and two workloads in the same
 * scenario legitimately want different ones - for example a Rates stream at 2M trades running
 * beside a Credit stream at 400k.
 */
public final class GeneratorRegistry {

    private final Map<String, Supplier<Generator>> factories = new LinkedHashMap<>();

    public GeneratorRegistry() {
        register("rates-atlas-payload", RatesAtlasGenerator::new);
        register("trade-payload", TradeGenerator::new);
        register("market-data-payload", MarketDataGenerator::new);
    }

    public void register(String ref, Supplier<Generator> factory) {
        factories.put(ref, factory);
    }

    public Generator create(String ref, Map<String, String> params) {
        Supplier<Generator> factory = factories.get(ref);
        if (factory == null) {
            throw new IllegalArgumentException(
                    "Unknown generator ref: " + ref + " (known: " + factories.keySet() + ")");
        }
        Generator g = factory.get();
        g.configure(params);
        return g;
    }

    public java.util.Set<String> known() {
        return factories.keySet();
    }
}
