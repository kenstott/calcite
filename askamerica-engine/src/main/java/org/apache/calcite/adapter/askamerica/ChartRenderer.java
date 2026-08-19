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
import java.util.List;
import java.util.Locale;

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

    /** Lays out a line, bar, or pie chart over a shared category axis. */
    static ChartScene layout(String chartType, String title, String xLabel, String yLabel,
            List<String> categories, List<SeriesSpec> series, int width, int height) {
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
        if ("pie".equals(type)) {
            return ChartLayout.pieChart(title, categories, series.get(0).values, width, height);
        }
        if (!"bar".equals(type) && !"line".equals(type)) {
            throw new IllegalArgumentException(
                "Unknown chart_type: " + type + " — use line, bar, pie, scatter, or bubble.");
        }
        return ChartLayout.categoryChart(type, title, xLabel, yLabel, categories, series,
            width, height);
    }

    /** Lays out a true numeric-axis scatter or bubble chart from (x, y[, size]) points. */
    static ChartScene layoutPoints(String chartType, String title, String xLabel, String yLabel,
            List<PointSeriesSpec> series, int width, int height) {
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
        return ChartLayout.pointChart(bubble, title, xLabel, yLabel, series, width, height);
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

    private static String normalizeType(String chartType) {
        return (chartType == null || chartType.isEmpty())
            ? "line" : chartType.toLowerCase(Locale.ROOT);
    }
}
