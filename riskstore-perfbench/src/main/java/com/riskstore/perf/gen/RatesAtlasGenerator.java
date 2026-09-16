package com.riskstore.perf.gen;

import java.time.LocalDate;
import java.util.Map;

/**
 * Synthetic Atlas pricing output for the Rates value stream, shaped for {@code l0_rates_atlas}.
 *
 * <p>Models a realistic EOD risk payload: a trade-level PV plus a delta ladder across tenor
 * buckets, which is the shape that makes L0 rows wide and JSON parsing expensive. The nested
 * {@code risk_factors} array is deliberately present because flattening it is exactly the work
 * the L0 to L1 transform has to do, and its width drives that cost.
 *
 * <p>Default cardinalities are sized for a mid-size rates book and are the numbers reproduced in
 * the report:
 * <ul>
 *   <li>trades: 2,000,000 - dominates part size and primary-key cardinality
 *   <li>books: 240, counterparties: 5,000, currencies: 12, instrument types: 8
 *   <li>tenor buckets per row: 14 - the delta ladder
 *   <li>business dates: 1 (a single EOD snap); raise it to model late-arriving data, which
 *       spreads writes across partitions and is a common cause of part-count blowups
 * </ul>
 */
public final class RatesAtlasGenerator implements Generator {

    private static final String[] CURRENCIES =
            {"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "SEK", "NOK", "NZD", "SGD", "HKD"};
    private static final String[] INSTRUMENTS =
            {"IRS", "OIS", "FRA", "SWAPTION", "CAP", "FLOOR", "XCCY", "BASIS"};
    private static final String[] TENORS =
            {"1D", "1W", "1M", "3M", "6M", "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "15Y", "20Y", "30Y"};
    private static final String[] MEASURES = {"PV", "DELTA", "GAMMA", "VEGA", "THETA"};
    private static final String[] SNAPS = {"EOD", "INTRADAY", "SOD"};

    private int tradeCount = 2_000_000;
    private int bookCount = 240;
    private int counterpartyCount = 5_000;
    private int businessDateCount = 1;
    private LocalDate baseDate = LocalDate.now();
    private String snapId = "EOD";

    @Override
    public String id() {
        return "rates-atlas-payload";
    }

    @Override
    public void configure(Map<String, String> params) {
        if (params == null) {
            return;
        }
        tradeCount = intParam(params, "trades", tradeCount);
        bookCount = intParam(params, "books", bookCount);
        counterpartyCount = intParam(params, "counterparties", counterpartyCount);
        businessDateCount = intParam(params, "businessDates", businessDateCount);
        if (params.containsKey("baseDate")) {
            baseDate = LocalDate.parse(params.get("baseDate"));
        }
        if (params.containsKey("snapId")) {
            snapId = params.get("snapId");
        }
    }

    private static int intParam(Map<String, String> p, String key, int fallback) {
        String v = p.get(key);
        return v == null ? fallback : Integer.parseInt(v.trim());
    }

    @Override
    public void appendRow(StringBuilder out, SeededRandom rnd, long rowSequence) {
        int trade = rnd.nextInt(tradeCount);
        int book = rnd.nextInt(bookCount);
        int cp = rnd.nextInt(counterpartyCount);
        String ccy = rnd.pick(CURRENCIES);
        String instrument = rnd.pick(INSTRUMENTS);
        LocalDate businessDate = baseDate.minusDays(businessDateCount <= 1 ? 0 : rnd.nextInt(businessDateCount));
        double pv = rnd.nextGaussianish(0, 2_500_000);

        out.append("{\"event_id\":\"evt-").append(rowSequence)
                .append("\",\"business_date\":\"").append(businessDate)
                .append("\",\"snap_id\":\"").append(snapId.equals("MIXED") ? rnd.pick(SNAPS) : snapId)
                .append("\",\"value_stream\":\"rates\",\"source\":\"atlas\"")
                .append(",\"trade_id\":\"TRD-").append(pad(trade, 9))
                .append("\",\"book_id\":\"RATES-").append(pad(book, 4))
                .append("\",\"counterparty_id\":\"CP-").append(pad(cp, 6))
                .append("\",\"instrument_type\":\"").append(instrument)
                .append("\",\"currency\":\"").append(ccy)
                .append("\",\"valuation_ccy\":\"USD\"")
                .append(",\"pv\":").append(round2(pv))
                .append(",\"notional\":").append(round2(Math.abs(rnd.nextGaussianish(50_000_000, 20_000_000))))
                .append(",\"measure\":\"").append(rnd.pick(MEASURES)).append('"');

        // The delta ladder. Flattening this is the L0 to L1 transform's main job.
        out.append(",\"risk_factors\":[");
        for (int i = 0; i < TENORS.length; i++) {
            if (i > 0) {
                out.append(',');
            }
            out.append("{\"tenor\":\"").append(TENORS[i])
                    .append("\",\"curve\":\"").append(ccy).append("-OIS")
                    .append("\",\"delta\":").append(round2(rnd.nextGaussianish(0, 15_000)))
                    .append(",\"gamma\":").append(round4(rnd.nextGaussianish(0, 120)))
                    .append('}');
        }
        out.append(']');

        out.append(",\"valuation_model\":\"LMM-").append(rnd.nextInt(4) + 1).append('"')
                .append(",\"is_amended\":").append(rnd.chance(0.02))
                .append(",\"as_of\":\"").append(businessDate).append("T17:0")
                .append(rnd.nextInt(6)).append(':').append(pad(rnd.nextInt(60), 2)).append(".000Z\"")
                .append(",\"ingest_ts\":").append(System.currentTimeMillis())
                .append('}');
    }

    private static String pad(int value, int width) {
        String s = Integer.toString(value);
        int missing = width - s.length();
        if (missing <= 0) {
            return s;
        }
        return "0".repeat(missing) + s;
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    private static double round4(double v) {
        return Math.round(v * 10_000.0) / 10_000.0;
    }

    @Override
    public String describe() {
        return "rates-atlas-payload: trades=" + tradeCount + ", books=" + bookCount
                + ", counterparties=" + counterpartyCount + ", currencies=" + CURRENCIES.length
                + ", instruments=" + INSTRUMENTS.length + ", tenorsPerRow=" + TENORS.length
                + ", businessDates=" + businessDateCount + ", snap=" + snapId;
    }
}
