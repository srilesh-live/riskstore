package com.riskstore.perf.scenario;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** One concurrent stream of work within a scenario. */
public class Workload {

    public String id;
    /** {@code insert}, {@code query} or {@code insertSelect}. */
    public String kind = "insert";

    /** Fraction of the phase rate this workload takes. Used by {@code insert} workloads. */
    public double share = 1.0;

    /** Fixed arrival rate, overriding the phase rate. Used by {@code query} workloads. */
    public ArrivalRate arrivalRate;

    // --- insert ---
    public Target target = new Target();
    public Batch batch = new Batch();
    public GeneratorRef generator = new GeneratorRef();

    // --- query ---
    public List<QueryRef> queries = new ArrayList<>();

    // --- insertSelect ---
    /** Named SQL from {@code sql/workloads/}, run on a fixed cadence rather than a rate. */
    public String sqlRef;
    public Duration triggerEvery;

    /** Server settings applied per operation, e.g. {@code async_insert}, {@code max_threads}. */
    public Map<String, String> settings = new LinkedHashMap<>();

    public boolean isInsert() {
        return "insert".equalsIgnoreCase(kind);
    }

    public boolean isQuery() {
        return "query".equalsIgnoreCase(kind);
    }

    public boolean isInsertSelect() {
        return "insertSelect".equalsIgnoreCase(kind);
    }

    public static class Target {
        public String table;
        /** Currently {@code JSONEachRow}; the L0 tables take raw JSON payloads. */
        public String format = "JSONEachRow";
    }

    public static class Batch {
        public int rows = 10_000;
        public Duration maxDelay = Duration.ofSeconds(1);
        /**
         * How many rendered batches to keep queued ahead of the driver. Trades directly against
         * load-generator heap: an Atlas batch of 20,000 rows is tens of megabytes.
         */
        public int queueDepth = 4;
        /** Threads rendering batches for this workload. */
        public int producers = 2;
    }

    public static class GeneratorRef {
        public String ref;
        public Map<String, String> params = new LinkedHashMap<>();
    }

    public static class ArrivalRate {
        /** {@code qps}, {@code rows_per_sec} or {@code ops_per_sec}. */
        public String unit = "qps";
        public double value = 1.0;
    }

    public static class QueryRef {
        public String ref;
        public double weight = 1.0;
    }
}
