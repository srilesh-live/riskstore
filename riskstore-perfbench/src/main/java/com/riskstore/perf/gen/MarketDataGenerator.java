package com.riskstore.perf.gen;

import java.time.LocalDate;
import java.util.Map;

/**
 * Synthetic market data curve points for {@code l0_<stream>_market_data}.
 *
 * <p>The distinguishing feature is very low column cardinality and very high row count: the same
 * few hundred curves observed across many tenors and snaps. That combination compresses
 * extremely well, so this generator is the one that tells you the honest bytes-per-row figure
 * for the storage projection against the 6 TB per-replica ceiling.
 */
public final class MarketDataGenerator implements Generator {

    private static final String[] CURVE_TYPES = {"OIS", "LIBOR", "SOFR", "ESTR", "SONIA", "TONA", "BASIS", "CREDIT"};
    private static final String[] CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "SEK"};
    private static final String[] TENORS =
            {"1D", "1W", "2W", "1M", "2M", "3M", "6M", "9M", "1Y", "18M", "2Y", "3Y", "4Y", "5Y",
             "7Y", "10Y", "12Y", "15Y", "20Y", "25Y", "30Y", "40Y", "50Y"};
    private static final String[] QUOTE_TYPES = {"MID", "BID", "ASK", "CLOSE"};

    private int snapCount = 24;
    private LocalDate baseDate = LocalDate.now();

    @Override
    public String id() {
        return "market-data-payload";
    }

    @Override
    public void configure(Map<String, String> params) {
        if (params == null) {
            return;
        }
        if (params.containsKey("snaps")) {
            snapCount = Integer.parseInt(params.get("snaps").trim());
        }
        if (params.containsKey("baseDate")) {
            baseDate = LocalDate.parse(params.get("baseDate"));
        }
    }

    @Override
    public void appendRow(StringBuilder out, SeededRandom rnd, long rowSequence) {
        String ccy = rnd.pick(CURRENCIES);
        String curveType = rnd.pick(CURVE_TYPES);
        String tenor = rnd.pick(TENORS);
        int snap = rnd.nextInt(snapCount);

        out.append("{\"business_date\":\"").append(baseDate)
                .append("\",\"snap_id\":\"S").append(pad(snap, 2))
                .append("\",\"curve_id\":\"").append(ccy).append('-').append(curveType)
                .append("\",\"currency\":\"").append(ccy)
                .append("\",\"curve_type\":\"").append(curveType)
                .append("\",\"tenor\":\"").append(tenor)
                .append("\",\"quote_type\":\"").append(rnd.pick(QUOTE_TYPES))
                .append("\",\"rate\":").append(round6(rnd.nextGaussianish(0.038, 0.012)))
                .append(",\"discount_factor\":").append(round6(0.5 + rnd.nextDouble() * 0.5))
                .append(",\"source\":\"MDS\"")
                .append(",\"observation_ts\":\"").append(baseDate).append("T")
                .append(pad(rnd.nextInt(24), 2)).append(':').append(pad(rnd.nextInt(60), 2)).append(":00.000Z\"")
                .append(",\"ingest_ts\":").append(System.currentTimeMillis())
                .append('}');
    }

    private static String pad(int value, int width) {
        String s = Integer.toString(value);
        int missing = width - s.length();
        return missing <= 0 ? s : "0".repeat(missing) + s;
    }

    private static double round6(double v) {
        return Math.round(v * 1_000_000.0) / 1_000_000.0;
    }

    @Override
    public String describe() {
        return "market-data-payload: curves=" + (CURRENCIES.length * CURVE_TYPES.length)
                + ", tenors=" + TENORS.length + ", snaps=" + snapCount
                + " (low cardinality, high row count - use this for the bytes-per-row projection)";
    }
}
