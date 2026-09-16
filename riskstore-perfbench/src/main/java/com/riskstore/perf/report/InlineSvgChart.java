package com.riskstore.perf.report;

import java.util.List;

/**
 * Minimal inline-SVG line charts.
 *
 * <p>Hand-rolled rather than pulled from a charting library because the report has to open on an
 * air-gapped RHEL box with no network: every byte must be in the file. SVG also stays crisp when
 * the report is printed into a go-live pack, which is where these usually end up.
 *
 * <p>Colours come from CSS custom properties so the same markup works in light and dark.
 */
public final class InlineSvgChart {

    private static final int WIDTH = 960;
    private static final int HEIGHT = 220;
    private static final int PAD_LEFT = 64;
    private static final int PAD_RIGHT = 16;
    private static final int PAD_TOP = 16;
    private static final int PAD_BOTTOM = 28;

    private InlineSvgChart() {
    }

    /** One point of a series in chart space. */
    public record XY(long x, double y) {
    }

    /**
     * Render a time series with optional vertical fault markers.
     *
     * @param chaosAtMs epoch timestamps to mark, so a latency spike can be read against the
     *                  fault that caused it rather than guessed at
     */
    public static String lineChart(String title, String unit, List<XY> points, List<Long> chaosAtMs) {
        if (points == null || points.isEmpty()) {
            return "<p class=\"empty\">No data for " + escape(title) + "</p>";
        }

        long minX = points.stream().mapToLong(XY::x).min().orElse(0);
        long maxX = points.stream().mapToLong(XY::x).max().orElse(1);
        double maxY = points.stream().mapToDouble(XY::y).max().orElse(1);
        double minY = Math.min(0, points.stream().mapToDouble(XY::y).min().orElse(0));
        if (maxY <= minY) {
            maxY = minY + 1;
        }
        long spanX = Math.max(1, maxX - minX);

        StringBuilder svg = new StringBuilder(4096);
        svg.append("<figure class=\"chart\"><figcaption>").append(escape(title));
        if (unit != null && !unit.isBlank()) {
            svg.append(" <span class=\"unit\">(").append(escape(unit)).append(")</span>");
        }
        svg.append("</figcaption>");
        svg.append("<svg viewBox=\"0 0 ").append(WIDTH).append(' ').append(HEIGHT)
                .append("\" preserveAspectRatio=\"xMidYMid meet\" role=\"img\" aria-label=\"")
                .append(escape(title)).append("\">");

        // Horizontal gridlines with value labels.
        for (int i = 0; i <= 4; i++) {
            double frac = i / 4.0;
            double value = minY + (maxY - minY) * (1 - frac);
            int y = (int) (PAD_TOP + frac * (HEIGHT - PAD_TOP - PAD_BOTTOM));
            svg.append("<line x1=\"").append(PAD_LEFT).append("\" y1=\"").append(y)
                    .append("\" x2=\"").append(WIDTH - PAD_RIGHT).append("\" y2=\"").append(y)
                    .append("\" class=\"grid\"/>");
            svg.append("<text x=\"").append(PAD_LEFT - 8).append("\" y=\"").append(y + 4)
                    .append("\" class=\"axis\" text-anchor=\"end\">").append(shortNumber(value)).append("</text>");
        }

        StringBuilder path = new StringBuilder(points.size() * 16);
        for (int i = 0; i < points.size(); i++) {
            XY p = points.get(i);
            int x = plotX(p.x(), minX, spanX);
            int y = plotY(p.y(), minY, maxY);
            path.append(i == 0 ? 'M' : 'L').append(x).append(' ').append(y);
        }
        svg.append("<path d=\"").append(path).append("\" class=\"series\" fill=\"none\"/>");

        if (chaosAtMs != null) {
            for (Long ts : chaosAtMs) {
                if (ts == null || ts < minX || ts > maxX) {
                    continue;
                }
                int x = plotX(ts, minX, spanX);
                svg.append("<line x1=\"").append(x).append("\" y1=\"").append(PAD_TOP)
                        .append("\" x2=\"").append(x).append("\" y2=\"").append(HEIGHT - PAD_BOTTOM)
                        .append("\" class=\"fault\"/>");
            }
        }

        svg.append("<text x=\"").append(PAD_LEFT).append("\" y=\"").append(HEIGHT - 8)
                .append("\" class=\"axis\">0s</text>");
        svg.append("<text x=\"").append(WIDTH - PAD_RIGHT).append("\" y=\"").append(HEIGHT - 8)
                .append("\" class=\"axis\" text-anchor=\"end\">").append(spanX / 1000).append("s</text>");
        svg.append("</svg></figure>");
        return svg.toString();
    }

    private static int plotX(long x, long minX, long spanX) {
        return (int) (PAD_LEFT + (x - minX) / (double) spanX * (WIDTH - PAD_LEFT - PAD_RIGHT));
    }

    private static int plotY(double y, double minY, double maxY) {
        double frac = (y - minY) / (maxY - minY);
        return (int) (HEIGHT - PAD_BOTTOM - frac * (HEIGHT - PAD_TOP - PAD_BOTTOM));
    }

    static String shortNumber(double v) {
        double abs = Math.abs(v);
        if (abs >= 1_000_000_000) {
            return String.format("%.1fB", v / 1_000_000_000);
        }
        if (abs >= 1_000_000) {
            return String.format("%.1fM", v / 1_000_000);
        }
        if (abs >= 1_000) {
            return String.format("%.1fk", v / 1_000);
        }
        if (abs >= 10) {
            return String.format("%.0f", v);
        }
        return String.format("%.2f", v);
    }

    static String escape(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }
}
