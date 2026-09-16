package com.riskstore.perf.gen;

import java.time.LocalDate;
import java.util.Map;

/**
 * Synthetic trade records for {@code l0_<stream>_trade}.
 *
 * <p>Narrow and high cardinality, which is the opposite profile to the pricing payload: the
 * interesting cost here is primary-key cardinality and the join behaviour when L1 trade data is
 * joined against valuations to build L2. Sized by default at 2,000,000 trades so it matches the
 * trade universe used by {@link RatesAtlasGenerator}.
 */
public final class TradeGenerator implements Generator {

    private static final String[] STATUS = {"LIVE", "MATURED", "TERMINATED", "AMENDED", "PENDING"};
    private static final String[] PORTFOLIOS = {"CORE", "HEDGE", "PROP", "CLIENT", "XVA"};
    private static final String[] CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD"};
    private static final String[] STREAMS = {"rates", "credit", "fx", "mkt_treasury"};

    private int tradeCount = 2_000_000;
    private int bookCount = 240;
    private int traderCount = 400;
    private String valueStream = "rates";
    private LocalDate baseDate = LocalDate.now();

    @Override
    public String id() {
        return "trade-payload";
    }

    @Override
    public void configure(Map<String, String> params) {
        if (params == null) {
            return;
        }
        if (params.containsKey("trades")) {
            tradeCount = Integer.parseInt(params.get("trades").trim());
        }
        if (params.containsKey("books")) {
            bookCount = Integer.parseInt(params.get("books").trim());
        }
        if (params.containsKey("traders")) {
            traderCount = Integer.parseInt(params.get("traders").trim());
        }
        if (params.containsKey("valueStream")) {
            valueStream = params.get("valueStream").trim();
        }
        if (params.containsKey("baseDate")) {
            baseDate = LocalDate.parse(params.get("baseDate"));
        }
    }

    @Override
    public void appendRow(StringBuilder out, SeededRandom rnd, long rowSequence) {
        int trade = rnd.nextInt(tradeCount);
        LocalDate tradeDate = baseDate.minusDays(rnd.nextInt(3650));
        LocalDate maturity = tradeDate.plusDays(180 + rnd.nextInt(10_950));
        String stream = "ALL".equals(valueStream) ? rnd.pick(STREAMS) : valueStream;

        out.append("{\"trade_id\":\"TRD-").append(pad(trade, 9))
                .append("\",\"version\":").append(1 + rnd.nextInt(5))
                .append(",\"value_stream\":\"").append(stream)
                .append("\",\"business_date\":\"").append(baseDate)
                .append("\",\"book_id\":\"RATES-").append(pad(rnd.nextInt(bookCount), 4))
                .append("\",\"portfolio\":\"").append(rnd.pick(PORTFOLIOS))
                .append("\",\"trader_id\":\"TR-").append(pad(rnd.nextInt(traderCount), 5))
                .append("\",\"counterparty_id\":\"CP-").append(pad(rnd.nextInt(5_000), 6))
                .append("\",\"status\":\"").append(rnd.pick(STATUS))
                .append("\",\"currency\":\"").append(rnd.pick(CURRENCIES))
                .append("\",\"notional\":").append(round2(Math.abs(rnd.nextGaussianish(50_000_000, 25_000_000))))
                .append(",\"trade_date\":\"").append(tradeDate)
                .append("\",\"maturity_date\":\"").append(maturity)
                .append("\",\"fixed_rate\":").append(round4(rnd.nextGaussianish(0.035, 0.015)))
                .append(",\"is_cleared\":").append(rnd.chance(0.72))
                .append(",\"ingest_ts\":").append(System.currentTimeMillis())
                .append('}');
    }

    private static String pad(int value, int width) {
        String s = Integer.toString(value);
        int missing = width - s.length();
        return missing <= 0 ? s : "0".repeat(missing) + s;
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    private static double round4(double v) {
        return Math.round(v * 10_000.0) / 10_000.0;
    }

    @Override
    public String describe() {
        return "trade-payload: trades=" + tradeCount + ", books=" + bookCount
                + ", traders=" + traderCount + ", stream=" + valueStream;
    }
}
