/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.askamerica;

import org.apache.calcite.adapter.askamerica.ChartScene.Anchor;
import org.apache.calcite.adapter.askamerica.ChartScene.Dot;
import org.apache.calcite.adapter.askamerica.ChartScene.Group;
import org.apache.calcite.adapter.askamerica.ChartScene.Label;
import org.apache.calcite.adapter.askamerica.ChartScene.Line;
import org.apache.calcite.adapter.askamerica.ChartScene.Path;
import org.apache.calcite.adapter.askamerica.ChartScene.Rect;

import java.awt.Color;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Turns chart data into a laid-out {@link ChartScene}.
 *
 * <p>All geometry lives here, computed once and handed to both output backends. Two decisions
 * are deliberate departures from what the previous renderer did, both because a run of the
 * comparative eval showed them costing the reader:
 *
 * <p><b>No category label is ever dropped.</b> The old renderer thinned labels when they
 * collided, so an eight-bar chart printed four names and left the other four bars anonymous —
 * including, in the observed run, the one bar the caller had specifically annotated as not
 * being a state. A chart whose bars cannot be identified has not communicated its data. Labels
 * are rotated, and shortened only as a last resort, but every category keeps one.
 *
 * <p><b>No scientific notation on an axis.</b> Money in the hundreds of thousands rendered as
 * {@code 1E5}, which is unreadable in the one place a chart has to be exact. Ticks are grouped
 * integers up to a million and compact SI beyond it.
 */
final class ChartLayout {

    private ChartLayout() {}

    /** Categorical palette: distinguishable in sequence and at small sizes. */
    private static final Color[] PALETTE = {
        new Color(0x25, 0x63, 0xeb), new Color(0xea, 0x58, 0x0c),
        new Color(0x05, 0x96, 0x69), new Color(0x7c, 0x3a, 0xed),
        new Color(0xdc, 0x26, 0x26), new Color(0x08, 0x91, 0xb2),
        new Color(0xca, 0x8a, 0x04), new Color(0xdb, 0x27, 0x77),
    };

    private static final Color BACKGROUND = Color.WHITE;
    private static final Color INK = new Color(0x1f, 0x23, 0x28);
    private static final Color AXIS = new Color(0x9c, 0xa3, 0xaf);
    private static final Color GRID = new Color(0xe5, 0xe7, 0xeb);

    /**
     * Empty space kept between the title and the plot, and at the very bottom.
     *
     * <p>Reserved rather than reclaimed. The SVG hands a caller ids and classes and invites
     * annotations, and the first agent to accept the invitation put a callout straight through
     * the top gridline label and a footnote off the right edge — not carelessly, but because
     * the scaffold packed every pixel and then asked for more. Giving up ~34px of plot height
     * buys a place to put the sentence that qualifies the chart, which on a chart worth
     * annotating is the better trade.
     */
    private static final int ANNOTATION_BAND = 20;
    private static final int FOOTNOTE_BAND = 16;

    private static final int TITLE_SIZE = 15;
    private static final int TICK_SIZE = 11;
    private static final int AXIS_TITLE_SIZE = 12;

    static Color color(int index) {
        return PALETTE[Math.floorMod(index, PALETTE.length)];
    }

    /** Layout of a line or bar chart over a shared category axis. */
    static ChartScene categoryChart(String type, String title, String xLabel, String yLabel,
            List<String> categories, List<ChartRenderer.SeriesSpec> series, int width,
            int height) {
        ChartScene scene = new ChartScene(width, height, BACKGROUND);

        double min = 0;
        double max = Double.NEGATIVE_INFINITY;
        for (ChartRenderer.SeriesSpec s : series) {
            for (Double v : s.values) {
                if (v != null) {
                    max = Math.max(max, v);
                    min = Math.min(min, v);
                }
            }
        }
        if (max == Double.NEGATIVE_INFINITY) {
            max = 1;
        }
        Ticks ticks = niceTicks(min, max);

        int legendHeight = series.size() > 1 ? 24 : 0;
        // Rotate rather than drop: measure first, then reserve the depth the choice needs.
        int widest = 0;
        for (String c : categories) {
            widest = Math.max(widest, ChartScene.textWidth(c, TICK_SIZE, false));
        }
        int tickLabelWidth = 0;
        for (String t : ticks.labels) {
            tickLabelWidth = Math.max(tickLabelWidth, ChartScene.textWidth(t, TICK_SIZE, false));
        }
        double left = 18 + (yLabel == null || yLabel.isEmpty() ? 0 : 18) + tickLabelWidth + 10;
        double right = 22;
        double titleBottom = title == null || title.isEmpty() ? 12 : 36;
        double top = titleBottom + ANNOTATION_BAND;
        double slotWidth = (width - left - right) / Math.max(1, categories.size());
        boolean rotate = widest > slotWidth - 6;
        double bottom = (rotate ? Math.min(96, widest * 0.72 + 22) : 30)
            + (xLabel == null || xLabel.isEmpty() ? 8 : 22) + legendHeight + FOOTNOTE_BAND;

        double plotW = width - left - right;
        double plotH = height - top - bottom;

        addFrame(scene, title, xLabel, yLabel, left, top, plotW, plotH, width, height, ticks,
            legendHeight);
        scene.bounds(left, top, plotW, plotH, titleBottom + 4, top - 4, height - 4);

        // Category ticks. Every category gets a label, rotated when they would collide.
        Group xTicks = new Group().at("x-axis-labels");
        for (int i = 0; i < categories.size(); i++) {
            double cx = left + slotWidth * (i + 0.5);
            String text = categories.get(i);
            String shown = rotate ? text : fitTo(text, (int) slotWidth - 6);
            Label lab = rotate
                ? new Label(cx, top + plotH + 14, shown, INK, TICK_SIZE, Anchor.END, -45, false)
                : new Label(cx, top + plotH + 18, shown, INK, TICK_SIZE, Anchor.MIDDLE, 0, false);
            lab.styled("tick").at("xtick-" + ChartScene.slug(text));
            xTicks.add(lab);
        }
        scene.add(xTicks);

        boolean bar = "bar".equals(type);
        for (int si = 0; si < series.size(); si++) {
            ChartRenderer.SeriesSpec s = series.get(si);
            Color c = color(si);
            Group g = new Group().at("series-" + ChartScene.slug(s.name))
                .styled("series");
            if (bar) {
                double groupPad = slotWidth * 0.18;
                double barW = (slotWidth - groupPad * 2) / series.size();
                for (int i = 0; i < categories.size(); i++) {
                    Double v = i < s.values.size() ? s.values.get(i) : null;
                    if (v == null) {
                        continue;
                    }
                    double y = valueToY(v, ticks, top, plotH);
                    double zero = valueToY(0, ticks, top, plotH);
                    double x = left + slotWidth * i + groupPad + barW * si;
                    g.add(new Rect(x, Math.min(y, zero), barW - 1, Math.abs(zero - y), c)
                        .at("mark-" + ChartScene.slug(s.name) + "-"
                            + ChartScene.slug(categories.get(i)))
                        .styled("bar"));
                }
            } else {
                Path p = new Path(c, null, 2);
                for (int i = 0; i < categories.size(); i++) {
                    Double v = i < s.values.size() ? s.values.get(i) : null;
                    if (v == null) {
                        continue;
                    }
                    p.to(left + slotWidth * (i + 0.5), valueToY(v, ticks, top, plotH));
                }
                g.add(p.at("line-" + ChartScene.slug(s.name)).styled("line"));
                for (int i = 0; i < categories.size(); i++) {
                    Double v = i < s.values.size() ? s.values.get(i) : null;
                    if (v == null) {
                        continue;
                    }
                    g.add(new Dot(left + slotWidth * (i + 0.5), valueToY(v, ticks, top, plotH),
                        3, c, 1.0)
                        .at("mark-" + ChartScene.slug(s.name) + "-"
                            + ChartScene.slug(categories.get(i)))
                        .styled("point"));
                }
            }
            scene.add(g);
        }

        if (series.size() > 1) {
            addLegend(scene, seriesNames(series), height - 10 - FOOTNOTE_BAND, width);
        }
        return scene;
    }

    /** Layout of a pie chart: one series, one slice per category. */
    static ChartScene pieChart(String title, List<String> categories,
            List<Double> values, int width, int height) {
        ChartScene scene = new ChartScene(width, height, BACKGROUND);
        double top = title == null || title.isEmpty() ? 16 : 44;
        if (title != null && !title.isEmpty()) {
            scene.add(new Label(width / 2.0, 26, title, INK, TITLE_SIZE, Anchor.MIDDLE, 0, true)
                .at("chart-title").styled("title"));
        }
        double total = 0;
        for (Double v : values) {
            total += v == null ? 0 : Math.max(0, v);
        }
        if (total <= 0) {
            total = 1;
        }
        scene.bounds(0, top, width, height - top - FOOTNOTE_BAND,
            title == null || title.isEmpty() ? 8 : 32, top - 4, height - 4);
        double cx = width / 2.0;
        double cy = top + (height - top - 20) / 2.0;
        double r = Math.min(width, height - top) * 0.32;

        double angle = -Math.PI / 2;
        Group slices = new Group().at("slices");
        Group labels = new Group().at("slice-labels");
        for (int i = 0; i < categories.size(); i++) {
            double v = i < values.size() && values.get(i) != null
                ? Math.max(0, values.get(i)) : 0;
            double sweep = v / total * Math.PI * 2;
            Path wedge = new Path(null, color(i), 0);
            wedge.to(cx, cy);
            int steps = Math.max(2, (int) (sweep / 0.08));
            for (int k = 0; k <= steps; k++) {
                double a = angle + sweep * k / steps;
                wedge.to(cx + Math.cos(a) * r, cy + Math.sin(a) * r);
            }
            slices.add(wedge.at("mark-" + ChartScene.slug(categories.get(i))).styled("slice"));

            double mid = angle + sweep / 2;
            double lx = cx + Math.cos(mid) * (r + 14);
            double ly = cy + Math.sin(mid) * (r + 14) + 4;
            String pct = String.format(Locale.ROOT, "%s %.0f%%", categories.get(i),
                v / total * 100);
            labels.add(new Label(lx, ly, pct, INK, TICK_SIZE,
                Math.cos(mid) < -0.1 ? Anchor.END : Math.cos(mid) > 0.1
                    ? Anchor.START : Anchor.MIDDLE, 0, false)
                .at("slice-label-" + ChartScene.slug(categories.get(i)))
                .styled("value-label"));
            angle += sweep;
        }
        scene.add(slices);
        scene.add(labels);
        return scene;
    }

    /** Layout of a scatter or bubble chart over two numeric axes. */
    static ChartScene pointChart(boolean bubble, String title, String xLabel, String yLabel,
            List<ChartRenderer.PointSeriesSpec> series, int width, int height) {
        ChartScene scene = new ChartScene(width, height, BACKGROUND);
        double xmin = Double.POSITIVE_INFINITY;
        double xmax = Double.NEGATIVE_INFINITY;
        double ymin = Double.POSITIVE_INFINITY;
        double ymax = Double.NEGATIVE_INFINITY;
        double smax = 0;
        for (ChartRenderer.PointSeriesSpec s : series) {
            for (Double v : s.x) {
                xmin = Math.min(xmin, v);
                xmax = Math.max(xmax, v);
            }
            for (Double v : s.y) {
                ymin = Math.min(ymin, v);
                ymax = Math.max(ymax, v);
            }
            if (bubble && s.size != null) {
                for (Double v : s.size) {
                    smax = Math.max(smax, Math.abs(v));
                }
            }
        }
        if (xmin > xmax) {
            xmin = 0;
            xmax = 1;
        }
        if (ymin > ymax) {
            ymin = 0;
            ymax = 1;
        }
        Ticks yt = niceTicks(ymin, ymax);
        Ticks xt = niceTicks(xmin, xmax);

        int legendHeight = series.size() > 1 ? 24 : 0;
        int tickLabelWidth = 0;
        for (String t : yt.labels) {
            tickLabelWidth = Math.max(tickLabelWidth, ChartScene.textWidth(t, TICK_SIZE, false));
        }
        double left = 18 + (yLabel == null || yLabel.isEmpty() ? 0 : 18) + tickLabelWidth + 10;
        double right = 26;
        double titleBottom = title == null || title.isEmpty() ? 12 : 36;
        double top = titleBottom + ANNOTATION_BAND;
        double bottom = 34 + (xLabel == null || xLabel.isEmpty() ? 8 : 22) + legendHeight
            + FOOTNOTE_BAND;
        double plotW = width - left - right;
        double plotH = height - top - bottom;

        addFrame(scene, title, xLabel, yLabel, left, top, plotW, plotH, width, height, yt,
            legendHeight);
        scene.bounds(left, top, plotW, plotH, titleBottom + 4, top - 4, height - 4);

        Group xTicks = new Group().at("x-axis-labels");
        for (int i = 0; i < xt.values.size(); i++) {
            double v = xt.values.get(i);
            double x = left + (v - xt.min) / (xt.max - xt.min) * plotW;
            xTicks.add(new Label(x, top + plotH + 18, xt.labels.get(i), INK, TICK_SIZE,
                Anchor.MIDDLE, 0, false).styled("tick").at("xtick-" + i));
        }
        scene.add(xTicks);

        for (int si = 0; si < series.size(); si++) {
            ChartRenderer.PointSeriesSpec s = series.get(si);
            Color c = color(si);
            Group g = new Group().at("series-" + ChartScene.slug(s.name))
                .styled("series");
            for (int i = 0; i < s.x.size(); i++) {
                double px = left + (s.x.get(i) - xt.min) / (xt.max - xt.min) * plotW;
                double py = valueToY(s.y.get(i), yt, top, plotH);
                double r = 4;
                if (bubble && s.size != null && smax > 0) {
                    r = 4 + Math.sqrt(Math.abs(s.size.get(i)) / smax) * 18;
                }
                g.add(new Dot(px, py, r, c, bubble ? 0.55 : 0.85)
                    .at("mark-" + ChartScene.slug(s.name) + "-" + i).styled("point"));
            }
            scene.add(g);
        }

        if (series.size() > 1) {
            addLegend(scene, pointSeriesNames(series), height - 10 - FOOTNOTE_BAND, width);
        }
        return scene;
    }

    // ── shared pieces ────────────────────────────────────────────────────────

    private static void addFrame(ChartScene scene, String title, String xLabel, String yLabel,
            double left, double top, double plotW, double plotH, int width, int height,
            Ticks ticks, int legendHeight) {
        if (title != null && !title.isEmpty()) {
            scene.add(new Label(width / 2.0, 26, title, INK, TITLE_SIZE, Anchor.MIDDLE, 0, true)
                .at("chart-title").styled("title"));
        }
        Group grid = new Group().at("gridlines");
        for (int i = 0; i < ticks.values.size(); i++) {
            double y = valueToY(ticks.values.get(i), ticks, top, plotH);
            grid.add(new Line(left, y, left + plotW, y, GRID, 1, true).styled("grid"));
            grid.add(new Label(left - 8, y + 4, ticks.labels.get(i), INK, TICK_SIZE,
                Anchor.END, 0, false).styled("tick").at("ytick-" + i));
        }
        scene.add(grid);
        scene.add(new Group().at("axes")
            .add(new Line(left, top, left, top + plotH, AXIS, 1, false).styled("axis"))
            .add(new Line(left, top + plotH, left + plotW, top + plotH, AXIS, 1, false)
                .styled("axis")));
        if (xLabel != null && !xLabel.isEmpty()) {
            // Above the legend when there is one, or it prints straight through the swatches.
            scene.add(new Label(left + plotW / 2, height - 8 - legendHeight - FOOTNOTE_BAND,
                xLabel, INK, AXIS_TITLE_SIZE, Anchor.MIDDLE, 0, false)
                .at("x-axis-title").styled("axis-title"));
        }
        if (yLabel != null && !yLabel.isEmpty()) {
            scene.add(new Label(14, top + plotH / 2, yLabel, INK, AXIS_TITLE_SIZE,
                Anchor.MIDDLE, -90, false).at("y-axis-title").styled("axis-title"));
        }
    }

    private static void addLegend(ChartScene scene, List<String> names, double y, int width) {
        int gap = 16;
        int total = 0;
        for (String n : names) {
            total += 12 + 4 + ChartScene.textWidth(n, TICK_SIZE, false) + gap;
        }
        double x = Math.max(8, (width - (total - gap)) / 2.0);
        Group legend = new Group().at("legend");
        for (int i = 0; i < names.size(); i++) {
            legend.add(new Rect(x, y - 8, 11, 11, color(i))
                .at("legend-swatch-" + ChartScene.slug(names.get(i))));
            legend.add(new Label(x + 16, y + 1, names.get(i), INK, TICK_SIZE, Anchor.START, 0,
                false).styled("legend-label")
                .at("legend-label-" + ChartScene.slug(names.get(i))));
            x += 12 + 4 + ChartScene.textWidth(names.get(i), TICK_SIZE, false) + gap;
        }
        scene.add(legend);
    }

    private static List<String> seriesNames(List<ChartRenderer.SeriesSpec> series) {
        List<String> out = new ArrayList<>();
        for (ChartRenderer.SeriesSpec s : series) {
            out.add(s.name);
        }
        return out;
    }

    private static List<String> pointSeriesNames(List<ChartRenderer.PointSeriesSpec> series) {
        List<String> out = new ArrayList<>();
        for (ChartRenderer.PointSeriesSpec s : series) {
            out.add(s.name);
        }
        return out;
    }

    private static double valueToY(double v, Ticks t, double top, double plotH) {
        return top + plotH - (v - t.min) / (t.max - t.min) * plotH;
    }

    /** Shortens a label only when rotation is not in play and it still cannot fit. */
    private static String fitTo(String text, int maxWidth) {
        if (maxWidth <= 8 || ChartScene.textWidth(text, TICK_SIZE, false) <= maxWidth) {
            return text;
        }
        String s = text;
        while (s.length() > 1
            && ChartScene.textWidth(s + "…", TICK_SIZE, false) > maxWidth) {
            s = s.substring(0, s.length() - 1);
        }
        return s + "…";
    }

    /** An axis range rounded to human numbers, with its rendered tick labels. */
    static final class Ticks {
        final double min;
        final double max;
        final List<Double> values = new ArrayList<>();
        final List<String> labels = new ArrayList<>();

        Ticks(double min, double max) {
            this.min = min;
            this.max = max;
        }
    }

    /** Axis bounds and ticks on 1/2/5×10ⁿ steps, the spacing people read without thinking. */
    static Ticks niceTicks(double min, double max) {
        if (min == max) {
            max = min + (min == 0 ? 1 : Math.abs(min) * 0.1);
        }
        // Step from the raw range. Rounding the range up first and then dividing compounds
        // two roundings: a 105,382 maximum became a 200,000 span, a 50,000 step and a 150,000
        // axis, leaving a third of the plot empty and the data squashed into the bottom.
        double step = niceNum((max - min) / 5, true);
        double lo = Math.floor(min / step) * step;
        double hi = Math.ceil(max / step) * step;
        Ticks t = new Ticks(lo, hi);
        for (double v = lo; v <= hi + step * 0.5; v += step) {
            double rounded = Math.abs(v) < step * 1e-9 ? 0 : v;
            t.values.add(rounded);
            t.labels.add(formatTick(rounded, step));
        }
        return t;
    }

    private static double niceNum(double range, boolean round) {
        double exp = Math.floor(Math.log10(range));
        double f = range / Math.pow(10, exp);
        double nf;
        if (round) {
            nf = f < 1.5 ? 1 : f < 3 ? 2 : f < 7 ? 5 : 10;
        } else {
            nf = f <= 1 ? 1 : f <= 2 ? 2 : f <= 5 ? 5 : 10;
        }
        return nf * Math.pow(10, exp);
    }

    /**
     * A tick as a reader would write it: grouped digits, then compact SI past a million.
     *
     * <p>Never scientific notation. Household income near $100,000 rendered as {@code 1E5} on
     * the previous renderer, which is precisely the wrong place to make a reader decode an
     * exponent.
     */
    static String formatTick(double v, double step) {
        double abs = Math.abs(v);
        if (abs >= 1e9) {
            return trim(v / 1e9) + "B";
        }
        if (abs >= 1e6) {
            return trim(v / 1e6) + "M";
        }
        if (step >= 1 && v == Math.rint(v)) {
            return String.format(Locale.ROOT, "%,d", (long) v);
        }
        int decimals = step >= 1 ? 0 : Math.min(4, (int) Math.ceil(-Math.log10(step)));
        return String.format(Locale.ROOT, "%,." + decimals + "f", v);
    }

    private static String trim(double v) {
        String s = String.format(Locale.ROOT, "%.1f", v);
        return s.endsWith(".0") ? s.substring(0, s.length() - 2) : s;
    }
}
