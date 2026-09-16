package com.riskstore.perf.gen;

import java.util.SplittableRandom;

/**
 * Deterministic per-batch randomness.
 *
 * <p>Each batch derives its own stream from (scenario seed, workload id, batch sequence), so the
 * generator is thread-safe without contention and two runs of the same scenario produce byte-
 * identical data. That second property is what makes a comparison against a baseline run mean
 * anything: without it, a latency delta could always be blamed on different data.
 */
public final class SeededRandom {

    private final SplittableRandom rnd;

    private SeededRandom(long seed) {
        this.rnd = new SplittableRandom(seed);
    }

    public static SeededRandom forBatch(long scenarioSeed, String workloadId, long batchSequence) {
        long mixed = scenarioSeed * 0x9E3779B97F4A7C15L
                + workloadId.hashCode() * 0xBF58476D1CE4E5B9L
                + batchSequence * 0x94D049BB133111EBL;
        return new SeededRandom(mixed);
    }

    public int nextInt(int bound) {
        return rnd.nextInt(bound);
    }

    public long nextLong(long bound) {
        return rnd.nextLong(bound);
    }

    public double nextDouble() {
        return rnd.nextDouble();
    }

    /** Roughly normal via the sum of three uniforms; cheap enough to call millions of times. */
    public double nextGaussianish(double mean, double stdDev) {
        double u = rnd.nextDouble() + rnd.nextDouble() + rnd.nextDouble() - 1.5;
        return mean + u * stdDev * 1.4142;
    }

    public <T> T pick(T[] values) {
        return values[rnd.nextInt(values.length)];
    }

    public boolean chance(double probability) {
        return rnd.nextDouble() < probability;
    }
}
