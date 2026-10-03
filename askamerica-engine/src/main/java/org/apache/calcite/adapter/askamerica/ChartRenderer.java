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

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Validates chart data and lays it out, for the {@code render_chart} MCP tool.
 *
 * <p>Produces a {@link ChartScene} — one set of coordinates — which the caller writes out as
 * both SVG and PNG. Rendering the same data twice through separate renderers would make their
 * agreement a hope; rendering one scene twice makes it arithmetic, which is what lets the tool
 * hand back markup a caller can trust to be the picture the reader saw.
 *
 * <p>Runs headless: the caller (McpServer) sets {@code java.awt.headless=true} before any chart
 * is built, since this is a stdio server process with no display and text measurement still
 * needs the font machinery.
 */
final class ChartRenderer {

    private ChartRenderer() {}

    /** One named series of values, one per category — for line/bar/pie. */
    static final class SeriesSpec {
        final String name;
        final List<Double> values;
        /** Optional hover text per value, same length as values; null entries take the default. */
        final List<String> tooltips;

        SeriesSpec(String name, List<Double> values) {
            this(name, values, null);
        }

        SeriesSpec(String name, List<Double> values, List<String> tooltips) {
            this.name = name;
            this.values = values;
            this.tooltips = tooltips;
        }
    }

    /**
     * One named series of (x, y[, size]) points — for scatter and bubble. Unlike
     * {@link SeriesSpec}, a point has no category axis to anchor a gap to, so a missing
     * coordinate is a caller error (omit the point) rather than a renderable gap.
     */
    static final class PointSeriesSpec {
        final String name;
        final List<Double> x;
        final List<Double> y;
        final List<Double> size;
        /** Optional hover text per point, same length as x; null entries take the default. */
        final List<String> tooltips;
        /** One short text per point, or null when the series carries no labels. */
        final List<String> labels;
        /** One of {@link #LABEL_MODES}. */
        final String labelMode;
        /** How many points {@code extremes} labels. */
        final int labelCount;
        /** Named groups of point labels, in legend order; null when nothing is highlighted. */
        final Map<String, List<String>> highlight;

        static final List<String> LABEL_MODES =
            Arrays.asList("all", "none", "highlighted", "extremes");
        static final int DEFAULT_LABEL_COUNT = 5;

        PointSeriesSpec(String name, List<Double> x, List<Double> y, List<Double> size) {
            this(name, x, y, size, null, null, "none", DEFAULT_LABEL_COUNT, null);
        }

        PointSeriesSpec(String name, List<Double> x, List<Double> y, List<Double> size,
                List<String> tooltips) {
            this(name, x, y, size, tooltips, null, "none", DEFAULT_LABEL_COUNT, null);
        }

        PointSeriesSpec(String name, List<Double> x, List<Double> y, List<Double> size,
                List<String> tooltips, List<String> labels, String labelMode, int labelCount,
                Map<String, List<String>> highlight) {
            this.name = name;
            this.x = x;
            this.y = y;
            this.size = size;
            this.tooltips = tooltips;
            this.labels = labels;
            this.labelMode = labelMode;
            this.labelCount = labelCount;
            this.highlight = highlight;
        }
    }


    /**
     * Open, high, low and close per category, and optionally the volume traded — for
     * candlestick. A category with no trade has all four prices null and is drawn as a gap.
     */
    static final class Candles {
        final String name;
        final List<Double> open;
        final List<Double> high;
        final List<Double> low;
        final List<Double> close;
        /** One volume per category, or null when the caller gave none. */
        final List<Double> volume;

        Candles(String name, List<Double> open, List<Double> high, List<Double> low,
                List<Double> close, List<Double> volume) {
            this.name = name;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = volume;
        }
    }

    /** A shaded interval per category — for fan. Null on both sides where it does not apply. */
    static final class Band {
        final String name;
        final List<Double> low;
        final List<Double> high;

        Band(String name, List<Double> low, List<Double> high) {
            this.name = name;
            this.low = low;
            this.high = high;
        }
    }

    /** A marker line: at a value on the y axis, or at a category on the x axis. */
    static final class RefLine {
        final Double value;
        final String category;
        final String label;

        RefLine(Double value, String category, String label) {
            this.value = value;
            this.category = category;
            this.label = label;
        }
    }

    /** Chart types drawn over an ordered axis, whose labels may be thinned. */
    static final List<String> ORDERED_TYPES = Arrays.asList("candlestick", "fan");

    /**
     * How a bar chart is oriented, ordered and annotated. Meaningless for every other chart
     * type, which {@link #layout} rejects rather than ignoring so a caller is never told a
     * line chart was sorted.
     */
    static final class BarOptions {
        static final BarOptions DEFAULT = new BarOptions("auto", "none", false, null, false);

        final String orientation;
        final String sort;
        final boolean valueLabels;
        final ValueFormat valueFormat;
        /** Whether a horizontal chart may grow taller than requested to fit every row. */
        final boolean growHeight;

        private BarOptions(String orientation, String sort, boolean valueLabels,
                ValueFormat valueFormat, boolean growHeight) {
            this.orientation = orientation;
            this.sort = sort;
            this.valueLabels = valueLabels;
            this.valueFormat = valueFormat;
            this.growHeight = growHeight;
        }

        /** Validates the caller's arguments; null means the argument was not supplied. */
        static BarOptions parse(String orientation, String sort, Boolean valueLabels,
                String valueFormat, boolean growHeight) {
            String o = orientation == null ? "auto" : orientation.toLowerCase(Locale.ROOT);
            if (!"auto".equals(o) && !"vertical".equals(o) && !"horizontal".equals(o)) {
                throw new IllegalArgumentException("orientation '" + orientation
                    + "' is not recognized — use auto, vertical, or horizontal.");
            }
            String so = sort == null ? "none" : sort.toLowerCase(Locale.ROOT);
            if (!"none".equals(so) && !"asc".equals(so) && !"desc".equals(so)) {
                throw new IllegalArgumentException(
                    "sort '" + sort + "' is not recognized — use none, asc, or desc.");
            }
            ValueFormat fmt = ValueFormat.parse(valueFormat);
            return new BarOptions(o, so, valueLabels != null && valueLabels, fmt, growHeight);
        }

        boolean isDefault() {
            return "auto".equals(orientation) && "none".equals(sort) && !valueLabels;
        }

        /** A copy that may grow a horizontal chart's height to fit its rows. */
        BarOptions growing() {
            return new BarOptions(orientation, sort, valueLabels, valueFormat, true);
        }

        String format(double v) {
            return ValueFormat.render(valueFormat, v);
        }
    }

    /** Lays out a line, bar, or pie chart over a shared category axis. */
    static ChartScene layout(String chartType, String title, String xLabel, String yLabel,
            List<String> categories, List<SeriesSpec> series, int width, int height) {
        return layout(chartType, title, xLabel, yLabel, categories, series, width, height,
            BarOptions.DEFAULT);
    }

    /** As above, with bar orientation, ordering and value labels, and the value format. */
    static ChartScene layout(String chartType, String title, String xLabel, String yLabel,
            List<String> categories, List<SeriesSpec> series, int width, int height,
            BarOptions bar) {
        return layout(chartType, title, xLabel, yLabel, categories, series, width, height, bar,
            Collections.<RefLine>emptyList());
    }

    /** As above, with reference lines, which a line chart alone takes here. */
    static ChartScene layout(String chartType, String title, String xLabel, String yLabel,
            List<String> categories, List<SeriesSpec> series, int width, int height,
            BarOptions bar, List<RefLine> refLines) {
        if (categories.isEmpty()) {
            throw new IllegalArgumentException("categories must not be empty");
        }
        if (series.isEmpty()) {
            throw new IllegalArgumentException("series must not be empty");
        }
        for (SeriesSpec s : series) {
            if (s.values.size() != categories.size()) {
                throw new IllegalArgumentException(
                    "series '" + s.name + "' has " + s.values.size()
                    + " values but there are " + categories.size() + " categories");
            }
            checkTooltips("series '" + s.name + "'", s.tooltips, s.values.size());
        }
        ValueFormat fmt = bar.valueFormat;

        String type = normalizeType(chartType);
        if (!"bar".equals(type) && !bar.isDefault()) {
            throw new IllegalArgumentException(
                "orientation, sort and value_labels apply to chart_type 'bar' only, "
                + "not '" + type + "'.");
        }
        if (!refLines.isEmpty() && !"line".equals(type)) {
            throw new IllegalArgumentException(
                "reference_lines apply to chart_type 'line', 'fan' and 'candlestick' only, "
                + "not '" + type + "'.");
        }
        checkRefLines(refLines, categories);
        if ("pie".equals(type)) {
            return ChartLayout.pieChart(title, categories, series.get(0).values,
                series.get(0).tooltips, fmt, width, height);
        }
        if (!"bar".equals(type) && !"line".equals(type)) {
            throw new IllegalArgumentException(
                "Unknown chart_type: " + type + " — use line, bar, pie, scatter, bubble, "
                + "candlestick, or fan.");
        }
        return ChartLayout.categoryChart(type, title, xLabel, yLabel, categories, series,
            width, height, null, bar, refLines);
    }

    /**
     * Lays out a candlestick or fan chart over an ordered category axis.
     *
     * <p>A candlestick takes {@code candles} and no bands; a fan takes bands and at least one
     * line. Either may carry further lines over the same categories, and reference lines.
     *
     * @param forcedDomain a y-axis domain to include, or null
     */
    static ChartScene layoutOrdered(String chartType, String title, String xLabel,
            String yLabel, List<String> categories, List<SeriesSpec> series, Candles candles,
            List<Band> bands, List<RefLine> refLines, int width, int height,
            ValueFormat valueFormat, double[] forcedDomain) {
        String type = normalizeType(chartType);
        if (!ORDERED_TYPES.contains(type)) {
            throw new IllegalArgumentException(
                "chart_type '" + type + "' is not candlestick or fan.");
        }
        if (categories.isEmpty()) {
            throw new IllegalArgumentException("categories must not be empty");
        }
        int n = categories.size();
        for (SeriesSpec s : series) {
            if (s.values.size() != n) {
                throw new IllegalArgumentException(
                    "series '" + s.name + "' has " + s.values.size()
                    + " values but there are " + n + " categories");
            }
            checkTooltips("series '" + s.name + "'", s.tooltips, s.values.size());
        }
        if ("candlestick".equals(type)) {
            if (candles == null) {
                throw new IllegalArgumentException("chart_type 'candlestick' needs candles: "
                    + "{open, high, low, close} arrays, one value per category.");
            }
            if (!bands.isEmpty()) {
                throw new IllegalArgumentException(
                    "bands apply to chart_type 'fan' only, not 'candlestick'.");
            }
            checkCandles(candles, categories);
        } else {
            if (candles != null) {
                throw new IllegalArgumentException(
                    "candles apply to chart_type 'candlestick' only, not 'fan'.");
            }
            if (bands.isEmpty()) {
                throw new IllegalArgumentException("chart_type 'fan' needs bands: "
                    + "[{name, low, high}], one low and one high per category.");
            }
            if (series.isEmpty()) {
                throw new IllegalArgumentException("chart_type 'fan' needs at least one "
                    + "series: the history, the central forecast, or both.");
            }
            for (Band b : bands) {
                checkBand(b, categories);
            }
        }
        checkRefLines(refLines, categories);
        return ChartLayout.orderedChart(title, xLabel, yLabel, categories, series, candles,
            bands, refLines, width, height, valueFormat, forcedDomain);
    }

    private static void checkCandles(Candles c, List<String> categories) {
        int n = categories.size();
        if (c.open.size() != n || c.high.size() != n || c.low.size() != n
                || c.close.size() != n) {
            throw new IllegalArgumentException("candles has " + c.open.size() + " open, "
                + c.high.size() + " high, " + c.low.size() + " low and " + c.close.size()
                + " close values but there are " + n + " categories");
        }
        if (c.volume != null && c.volume.size() != n) {
            throw new IllegalArgumentException("candles has " + c.volume.size()
                + " volume values but there are " + n + " categories");
        }
        boolean any = false;
        for (int i = 0; i < n; i++) {
            Double o = c.open.get(i);
            Double h = c.high.get(i);
            Double l = c.low.get(i);
            Double cl = c.close.get(i);
            int given = (o == null ? 0 : 1) + (h == null ? 0 : 1) + (l == null ? 0 : 1)
                + (cl == null ? 0 : 1);
            if (given == 0) {
                continue;
            }
            if (given != 4) {
                throw new IllegalArgumentException("candle '" + categories.get(i)
                    + "' has some of open, high, low and close but not all — pass all four, "
                    + "or null for all four where nothing traded.");
            }
            if (h < Math.max(o, cl) || l > Math.min(o, cl)) {
                throw new IllegalArgumentException("candle '" + categories.get(i)
                    + "' has open " + o + ", high " + h + ", low " + l + ", close " + cl
                    + " — the high must be the largest and the low the smallest.");
            }
            if (c.volume != null && c.volume.get(i) != null && c.volume.get(i) < 0) {
                throw new IllegalArgumentException("candle '" + categories.get(i)
                    + "' has a negative volume");
            }
            any = true;
        }
        if (!any) {
            throw new IllegalArgumentException("candles has no category with prices");
        }
    }

    private static void checkBand(Band b, List<String> categories) {
        int n = categories.size();
        if (b.low.size() != n || b.high.size() != n) {
            throw new IllegalArgumentException("band '" + b.name + "' has " + b.low.size()
                + " low and " + b.high.size() + " high values but there are " + n
                + " categories");
        }
        boolean any = false;
        for (int i = 0; i < n; i++) {
            Double lo = b.low.get(i);
            Double hi = b.high.get(i);
            if (lo == null && hi == null) {
                continue;
            }
            if (lo == null || hi == null) {
                throw new IllegalArgumentException("band '" + b.name + "' at '"
                    + categories.get(i) + "' has one of low and high but not the other");
            }
            if (lo > hi) {
                throw new IllegalArgumentException("band '" + b.name + "' at '"
                    + categories.get(i) + "' has low " + lo + " above high " + hi);
            }
            any = true;
        }
        if (!any) {
            throw new IllegalArgumentException("band '" + b.name + "' has no values");
        }
    }

    private static void checkRefLines(List<RefLine> refLines, List<String> categories) {
        for (RefLine r : refLines) {
            if ((r.value == null) == (r.category == null)) {
                throw new IllegalArgumentException("a reference line needs exactly one of "
                    + "value (a y-axis value) and category (an x-axis category).");
            }
            if (r.category != null && !categories.contains(r.category)) {
                throw new IllegalArgumentException("reference line category '" + r.category
                    + "' is not in categories");
            }
        }
    }

    /** Lays out a true numeric-axis scatter or bubble chart from (x, y[, size]) points. */
    static ChartScene layoutPoints(String chartType, String title, String xLabel, String yLabel,
            List<PointSeriesSpec> series, int width, int height) {
        return layoutPoints(chartType, title, xLabel, yLabel, series, width, height, null);
    }

    /** As above, with an optional {@code value_format} for the y ticks and default tooltips. */
    static ChartScene layoutPoints(String chartType, String title, String xLabel, String yLabel,
            List<PointSeriesSpec> series, int width, int height, ValueFormat valueFormat) {
        if (series.isEmpty()) {
            throw new IllegalArgumentException("points must not be empty");
        }
        String type = normalizeType(chartType);
        boolean bubble = "bubble".equals(type);
        if (!bubble && !"scatter".equals(type)) {
            throw new IllegalArgumentException(
                "chart_type '" + type + "' does not take points — use categories/series "
                + "instead, or use scatter/bubble with points.");
        }
        for (PointSeriesSpec s : series) {
            if (s.x.size() != s.y.size()) {
                throw new IllegalArgumentException(
                    "points series '" + s.name + "' has " + s.x.size() + " x values but "
                    + s.y.size() + " y values");
            }
            if (s.x.contains(null) || s.y.contains(null)) {
                throw new IllegalArgumentException(
                    "points series '" + s.name + "' has a null x or y — a scatter/bubble "
                    + "point has no category axis to anchor a gap to, so omit the point "
                    + "instead of passing null");
            }
            checkTooltips("points series '" + s.name + "'", s.tooltips, s.x.size());
            validateLabels(s, series.size());
            if ("highlighted".equals(s.labelMode) && s.highlight == null) {
                throw new IllegalArgumentException("points series '" + s.name
                    + "' has label_mode 'highlighted' but no highlight groups to label.");
            }
            if (bubble) {
                if (s.size == null || s.size.size() != s.x.size()) {
                    throw new IllegalArgumentException(
                        "bubble series '" + s.name + "' needs one size value per (x, y) point");
                }
                if (s.size.contains(null)) {
                    throw new IllegalArgumentException(
                        "bubble series '" + s.name + "' has a null size — omit the point "
                        + "instead of passing null");
                }
            }
        }
        return ChartLayout.pointChart(bubble, title, xLabel, yLabel, series, width, height,
            valueFormat);
    }

    private static void validateLabels(PointSeriesSpec s, int seriesCount) {
        if (!PointSeriesSpec.LABEL_MODES.contains(s.labelMode)) {
            throw new IllegalArgumentException("points series '" + s.name + "' has label_mode '"
                + s.labelMode + "' — use all, none, highlighted, or extremes.");
        }
        if (s.labelCount < 1) {
            throw new IllegalArgumentException(
                "points series '" + s.name + "' has label_count " + s.labelCount
                + " — it must be at least 1.");
        }
        boolean needsLabels = !"none".equals(s.labelMode) || s.highlight != null;
        if (s.labels == null) {
            if (needsLabels) {
                throw new IllegalArgumentException("points series '" + s.name + "' asks for "
                    + "labels (label_mode '" + s.labelMode + "'"
                    + (s.highlight != null ? " / highlight" : "")
                    + ") but has no labels array — pass one short text per point.");
            }
            return;
        }
        if (s.labels.size() != s.x.size()) {
            throw new IllegalArgumentException("points series '" + s.name + "' has "
                + s.x.size() + " x values but " + s.labels.size() + " labels");
        }
        if (s.highlight == null) {
            return;
        }
        if (seriesCount != 1) {
            throw new IllegalArgumentException("highlight groups colour the points of one series, "
                + "so they need exactly one points series, not " + seriesCount
                + " — use separate series for separate colours.");
        }
        for (Map.Entry<String, List<String>> g : s.highlight.entrySet()) {
            for (String l : g.getValue()) {
                if (!s.labels.contains(l)) {
                    throw new IllegalArgumentException("highlight group '" + g.getKey()
                        + "' names '" + l + "', which is not in the labels array");
                }
            }
        }
    }

    /** Retained for callers that only want the raster. */
    static byte[] renderPng(String chartType, String title, String xLabel, String yLabel,
            List<String> categories, List<SeriesSpec> series, int width, int height)
            throws IOException {
        return layout(chartType, title, xLabel, yLabel, categories, series, width, height)
            .toPng();
    }

    /** Retained for callers that only want the raster. */
    static byte[] renderPointsPng(String chartType, String title, String xLabel, String yLabel,
            List<PointSeriesSpec> series, int width, int height) throws IOException {
        return layoutPoints(chartType, title, xLabel, yLabel, series, width, height).toPng();
    }

    private static void checkTooltips(String what, List<String> tooltips, int marks) {
        if (tooltips != null && tooltips.size() != marks) {
            throw new IllegalArgumentException(
                what + " has " + tooltips.size() + " tooltips but " + marks
                + " values — tooltips must be the same length as the values.");
        }
    }

    private static String normalizeType(String chartType) {
        return (chartType == null || chartType.isEmpty())
            ? "line" : chartType.toLowerCase(Locale.ROOT);
    }
}
