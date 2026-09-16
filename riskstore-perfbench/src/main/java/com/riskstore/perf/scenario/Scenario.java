package com.riskstore.perf.scenario;

import java.util.ArrayList;
import java.util.List;

/** A declarative test definition, loaded from {@code scenarios/*.yaml}. */
public class Scenario {

    public String id;
    public String description = "";
    /** Fixes the data generator so two runs of the same scenario produce identical data. */
    public long seed = 42L;

    public List<Phase> phases = new ArrayList<>();
    public List<Workload> workloads = new ArrayList<>();
    public List<SloSpec> slo = new ArrayList<>();

    /** Reconciliation checks run after the load phases complete. */
    public List<ReconCheck> reconciliation = new ArrayList<>();

    /** Cache handling applied once before the run: {@code none}, {@code cold} or {@code warm}. */
    public String cachePolicy = "none";

    public Phase phase(String phaseId) {
        return phases.stream().filter(p -> p.id.equals(phaseId)).findFirst().orElse(null);
    }

    public Workload workload(String workloadId) {
        return workloads.stream().filter(w -> w.id.equals(workloadId)).findFirst().orElse(null);
    }

    /** A named post-run correctness assertion; see {@code verify/Reconciler}. */
    public static class ReconCheck {
        public String name;
        /** SQL returning a single numeric column. */
        public String sql;
        /** {@code generated} compares against rows the harness believes it sent. */
        public String expect = "generated";
        public double tolerance = 0.0;
    }
}
