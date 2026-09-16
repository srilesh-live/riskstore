package com.riskstore.perf.metrics;

import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the histogram discipline.
 *
 * <p>Averaging per-window percentiles is the most common way a benchmark report ends up
 * arithmetically wrong, and it is an easy mistake to reintroduce because the result looks
 * plausible. These tests pin the merged-histogram behaviour that prevents it.
 */
class LatencyRecorderTest {

    @Test
    void globalPercentilesComeFromMergingWindowsNotAveragingThem() {
        LatencyRecorder recorder = new LatencyRecorder("merge-test", 10);
        List<Long> allSamples = new ArrayList<>();

        // Five windows with deliberately different distributions. If percentiles were averaged
        // across windows, the slow window would be diluted and the true p99 lost.
        long[] windowMeans = {1_000, 2_000, 3_000, 500_000, 1_500};
        long now = System.currentTimeMillis();

        for (int w = 0; w < windowMeans.length; w++) {
            for (int i = 0; i < 1_000; i++) {
                long micros = windowMeans[w] + i;
                allSamples.add(micros);
                recorder.recordSuccess(0, micros * 1_000L, 1, 1);
            }
            now += 20;
            recorder.rotateIfDue(now, true);
        }

        Histogram bruteForce = new Histogram(600_000_000L, 2);
        allSamples.forEach(bruteForce::recordValue);

        assertThat(recorder.windows()).hasSize(windowMeans.length);

        // 2 significant digits means up to 1% quantisation, so compare within tolerance.
        assertPercentileClose(recorder, bruteForce, 50.0);
        assertPercentileClose(recorder, bruteForce, 99.0);
        assertPercentileClose(recorder, bruteForce, 99.9);

        assertThat(recorder.total().getTotalCount()).isEqualTo(allSamples.size());
    }

    private static void assertPercentileClose(LatencyRecorder recorder, Histogram expected, double percentile) {
        long actual = recorder.percentileUs(percentile);
        long want = expected.getValueAtPercentile(percentile);
        assertThat((double) actual)
                .as("p%s recovered from merged windows", percentile)
                .isCloseTo(want, org.assertj.core.data.Offset.offset(want * 0.02 + 1));
    }

    @Test
    void windowEncodingIsLosslessAndReDecodable() throws Exception {
        LatencyRecorder recorder = new LatencyRecorder("encode-test", 10);
        for (int i = 1; i <= 500; i++) {
            recorder.recordSuccess(0, i * 1_000_000L, 1, 1);
        }
        recorder.rotateIfDue(System.currentTimeMillis() + 100, true);

        List<HistogramWindow> windows = recorder.windows();
        assertThat(windows).hasSize(1);

        // The encoded payload is what lands in the results database. Any consumer must be able
        // to decode it and merge it with other windows to get a true range percentile.
        HistogramWindow window = windows.get(0);
        byte[] decoded = Base64.getDecoder().decode(window.encoded());
        Histogram restored = Histogram.decodeFromCompressedByteBuffer(ByteBuffer.wrap(decoded), 0);

        assertThat(restored.getTotalCount()).isEqualTo(window.count()).isEqualTo(500);
        assertThat(restored.getValueAtPercentile(99.0)).isEqualTo(window.p99Us());
        assertThat(restored.getMaxValue()).isEqualTo(window.maxUs());
    }

    @Test
    void failedOperationsCountTowardLatencyAndTheErrorRate() {
        LatencyRecorder recorder = new LatencyRecorder("error-test", 10_000);

        for (int i = 0; i < 90; i++) {
            recorder.recordSuccess(0, 1_000_000L, 1, 1);
        }
        // A failure still consumed wall-clock time, so it belongs in the distribution.
        for (int i = 0; i < 10; i++) {
            recorder.recordFailure(0, 50_000_000L);
        }
        recorder.rotateIfDue(System.currentTimeMillis() + 100, true);

        assertThat(recorder.ops()).isEqualTo(100);
        assertThat(recorder.errors()).isEqualTo(10);
        assertThat(recorder.errorRate()).isEqualTo(0.10);
        assertThat(recorder.total().getTotalCount()).isEqualTo(100);
        assertThat(recorder.total().getMaxValue()).isGreaterThan(40_000L);
    }

    @Test
    void overloadDropsAreTrackedSeparatelyFromErrors() {
        LatencyRecorder recorder = new LatencyRecorder("drop-test", 10_000);
        recorder.recordSuccess(0, 1_000_000L, 1, 1);
        recorder.recordOverloadDrop();
        recorder.recordOverloadDrop();

        // A shed arrival is not a server error - conflating them would blame ClickHouse for a
        // load-generator limit.
        assertThat(recorder.errors()).isZero();
        assertThat(recorder.overloadDrops()).isEqualTo(2);
        assertThat(recorder.ops()).isEqualTo(1);
    }
}
