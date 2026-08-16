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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A {@code null} in a series' values marks a missing data point (e.g. an unpublished ACS 1-year
 * vintage) and must render as a gap, not silently become a zero — see the render_chart MCP tool.
 */
@Tag("unit")
class ChartRendererTest {

  @Test void nullValueRendersGapNotException() {
    List<String> categories = Arrays.asList("2018", "2019", "2020", "2021");
    List<Double> values = Arrays.asList(74991.0, 77933.0, null, 88465.0);
    ChartRenderer.SeriesSpec series = new ChartRenderer.SeriesSpec("NH", values);

    byte[] png = assertDoesNotThrow(() ->
        ChartRenderer.renderPng("line", "test", "Year", "Income",
            categories, Collections.singletonList(series), 400, 300));

    // PNG magic number: 89 50 4E 47 0D 0A 1A 0A
    assertTrue(png.length > 8);
    assertTrue((png[0] & 0xFF) == 0x89 && png[1] == 'P' && png[2] == 'N' && png[3] == 'G');
  }

  @Test void mismatchedLengthsStillRejected() {
    List<String> categories = Arrays.asList("2018", "2019", "2020");
    List<Double> values = Arrays.asList(74991.0, 77933.0);
    ChartRenderer.SeriesSpec series = new ChartRenderer.SeriesSpec("NH", values);

    assertThrows(IllegalArgumentException.class, () ->
        ChartRenderer.renderPng("line", "test", "Year", "Income",
            categories, Collections.singletonList(series), 400, 300));
  }

  @Test void scatterRendersTrueXyPoints() {
    List<Double> x = Arrays.asList(1.0, 2.0, 3.0, 4.0);
    List<Double> y = Arrays.asList(10.0, 12.0, 9.0, 15.0);
    ChartRenderer.PointSeriesSpec points = new ChartRenderer.PointSeriesSpec("states", x, y, null);

    byte[] png = assertDoesNotThrow(() ->
        ChartRenderer.renderPointsPng("scatter", "test", "X", "Y",
            Collections.singletonList(points), 400, 300));

    assertTrue(png.length > 8);
    assertTrue((png[0] & 0xFF) == 0x89 && png[1] == 'P' && png[2] == 'N' && png[3] == 'G');
  }

  @Test void bubbleRendersXyzPoints() {
    List<Double> x = Arrays.asList(1.0, 2.0, 3.0);
    List<Double> y = Arrays.asList(10.0, 12.0, 9.0);
    List<Double> size = Arrays.asList(5.0, 20.0, 8.0);
    ChartRenderer.PointSeriesSpec points =
        new ChartRenderer.PointSeriesSpec("counties", x, y, size);

    byte[] png = assertDoesNotThrow(() ->
        ChartRenderer.renderPointsPng("bubble", "test", "X", "Y",
            Collections.singletonList(points), 400, 300));

    assertTrue(png.length > 8);
    assertTrue((png[0] & 0xFF) == 0x89 && png[1] == 'P' && png[2] == 'N' && png[3] == 'G');
  }

  @Test void bubbleWithoutSizeRejected() {
    List<Double> x = Arrays.asList(1.0, 2.0);
    List<Double> y = Arrays.asList(10.0, 12.0);
    ChartRenderer.PointSeriesSpec points = new ChartRenderer.PointSeriesSpec("counties", x, y, null);

    assertThrows(IllegalArgumentException.class, () ->
        ChartRenderer.renderPointsPng("bubble", "test", "X", "Y",
            Collections.singletonList(points), 400, 300));
  }

  @Test void nullXInPointsRejectedNotSilentlyDropped() {
    List<Double> x = Arrays.asList(1.0, null, 3.0);
    List<Double> y = Arrays.asList(10.0, 12.0, 9.0);
    ChartRenderer.PointSeriesSpec points = new ChartRenderer.PointSeriesSpec("states", x, y, null);

    assertThrows(IllegalArgumentException.class, () ->
        ChartRenderer.renderPointsPng("scatter", "test", "X", "Y",
            Collections.singletonList(points), 400, 300));
  }
}
