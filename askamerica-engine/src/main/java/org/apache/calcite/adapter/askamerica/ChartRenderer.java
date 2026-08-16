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

import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.BubbleChart;
import org.knowm.xchart.BubbleChartBuilder;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.CategorySeries.CategorySeriesRenderStyle;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.XYSeries.XYSeriesRenderStyle;
import org.knowm.xchart.internal.chartpart.IChart;
import org.knowm.xchart.style.CategoryStyler;
import org.knowm.xchart.style.XYStyler;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Locale;

import javax.imageio.ImageIO;

/**
 * Renders chart data to a PNG image via XChart, for the {@code render_chart} MCP tool. Runs
 * headless — the caller (McpServer) sets {@code java.awt.headless=true} before any chart is
 * rendered, since this is a stdio server process with no display.
 */
final class ChartRenderer {

    private ChartRenderer() {}

    /** One named series of values, one per category — for line/bar/pie. */
    static final class SeriesSpec {
        final String name;
        final List<Double> values;

        SeriesSpec(String name, List<Double> values) {
            this.name = name;
            this.values = values;
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

        PointSeriesSpec(String name, List<Double> x, List<Double> y, List<Double> size) {
            this.name = name;
            this.x = x;
            this.y = y;
            this.size = size;
        }
    }

    static byte[] renderPng(String chartType, String title, String xLabel, String yLabel,
            List<String> categories, List<SeriesSpec> series, int width, int height)
            throws IOException {
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
        }

        String type = normalizeType(chartType);
        IChart chart = "pie".equals(type)
            ? buildPieChart(title, categories, series, width, height)
            : buildCategoryChart(type, title, xLabel, yLabel, categories, series, width, height);
        return toPngBytes(chart);
    }

    /** Renders a true numeric-axis scatter or bubble chart from (x, y[, size]) points. */
    static byte[] renderPointsPng(String chartType, String title, String xLabel, String yLabel,
            List<PointSeriesSpec> series, int width, int height) throws IOException {
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

        IChart chart = bubble
            ? buildBubbleChart(title, xLabel, yLabel, series, width, height)
            : buildXYChart(title, xLabel, yLabel, series, width, height);
        return toPngBytes(chart);
    }

    private static String normalizeType(String chartType) {
        return (chartType == null || chartType.isEmpty())
            ? "line" : chartType.toLowerCase(Locale.ROOT);
    }

    private static CategoryChart buildCategoryChart(String type, String title, String xLabel,
            String yLabel, List<String> categories, List<SeriesSpec> series, int width,
            int height) {
        CategorySeriesRenderStyle renderStyle;
        switch (type) {
            case "bar":
                renderStyle = CategorySeriesRenderStyle.Bar;
                break;
            case "line":
                renderStyle = CategorySeriesRenderStyle.Line;
                break;
            default:
                throw new IllegalArgumentException(
                    "Unknown chart_type: " + type + " — use line, bar, pie, scatter, or "
                    + "bubble.");
        }

        CategoryChart chart = new CategoryChartBuilder()
            .width(width).height(height)
            .title(title == null ? "" : title)
            .xAxisTitle(xLabel == null ? "" : xLabel)
            .yAxisTitle(yLabel == null ? "" : yLabel)
            .build();
        CategoryStyler styler = chart.getStyler();
        styler.setDefaultSeriesRenderStyle(renderStyle);
        styler.setLegendVisible(series.size() > 1);
        for (SeriesSpec s : series) {
            chart.addSeries(s.name, categories, s.values);
        }
        return chart;
    }

    private static PieChart buildPieChart(String title, List<String> categories,
            List<SeriesSpec> series, int width, int height) {
        PieChart chart = new PieChartBuilder()
            .width(width).height(height)
            .title(title == null ? "" : title)
            .build();
        List<Double> values = series.get(0).values;
        for (int i = 0; i < categories.size(); i++) {
            chart.addSeries(categories.get(i), values.get(i));
        }
        return chart;
    }

    private static XYChart buildXYChart(String title, String xLabel, String yLabel,
            List<PointSeriesSpec> series, int width, int height) {
        XYChart chart = new XYChartBuilder()
            .width(width).height(height)
            .title(title == null ? "" : title)
            .xAxisTitle(xLabel == null ? "" : xLabel)
            .yAxisTitle(yLabel == null ? "" : yLabel)
            .build();
        XYStyler styler = chart.getStyler();
        styler.setDefaultSeriesRenderStyle(XYSeriesRenderStyle.Scatter);
        styler.setLegendVisible(series.size() > 1);
        for (PointSeriesSpec s : series) {
            chart.addSeries(s.name, s.x, s.y);
        }
        return chart;
    }

    private static BubbleChart buildBubbleChart(String title, String xLabel, String yLabel,
            List<PointSeriesSpec> series, int width, int height) {
        BubbleChart chart = new BubbleChartBuilder()
            .width(width).height(height)
            .title(title == null ? "" : title)
            .xAxisTitle(xLabel == null ? "" : xLabel)
            .yAxisTitle(yLabel == null ? "" : yLabel)
            .build();
        chart.getStyler().setLegendVisible(series.size() > 1);
        for (PointSeriesSpec s : series) {
            chart.addSeries(s.name, s.x, s.y, s.size);
        }
        return chart;
    }

    private static byte[] toPngBytes(IChart chart) throws IOException {
        BufferedImage image = BitmapEncoder.getBufferedImage(chart);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ImageIO.write(image, "png", out);
        return out.toByteArray();
    }
}
