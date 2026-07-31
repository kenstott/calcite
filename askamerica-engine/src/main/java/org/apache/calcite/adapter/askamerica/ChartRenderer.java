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
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.CategorySeries.CategorySeriesRenderStyle;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.internal.chartpart.IChart;
import org.knowm.xchart.style.CategoryStyler;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Locale;

import javax.imageio.ImageIO;

/**
 * Renders categories + one or more numeric series to a PNG chart image via XChart, for the
 * {@code render_chart} MCP tool. Runs headless — the caller (McpServer) sets
 * {@code java.awt.headless=true} before any chart is rendered, since this is a stdio server
 * process with no display.
 */
final class ChartRenderer {

    private ChartRenderer() {}

    /** One named series of values, one per category. */
    static final class SeriesSpec {
        final String name;
        final List<Double> values;

        SeriesSpec(String name, List<Double> values) {
            this.name = name;
            this.values = values;
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

        String type = (chartType == null || chartType.isEmpty())
            ? "line" : chartType.toLowerCase(Locale.ROOT);
        IChart chart = "pie".equals(type)
            ? buildPieChart(title, categories, series, width, height)
            : buildCategoryChart(type, title, xLabel, yLabel, categories, series, width, height);
        return toPngBytes(chart);
    }

    private static CategoryChart buildCategoryChart(String type, String title, String xLabel,
            String yLabel, List<String> categories, List<SeriesSpec> series, int width,
            int height) {
        CategorySeriesRenderStyle renderStyle;
        switch (type) {
            case "bar":
                renderStyle = CategorySeriesRenderStyle.Bar;
                break;
            case "scatter":
                renderStyle = CategorySeriesRenderStyle.Scatter;
                break;
            case "line":
                renderStyle = CategorySeriesRenderStyle.Line;
                break;
            default:
                throw new IllegalArgumentException(
                    "Unknown chart_type: " + type + " — use line, bar, scatter, or pie.");
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

    private static byte[] toPngBytes(IChart chart) throws IOException {
        BufferedImage image = BitmapEncoder.getBufferedImage(chart);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ImageIO.write(image, "png", out);
        return out.toByteArray();
    }
}
