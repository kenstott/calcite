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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Candlestick and fan charts, and the reference lines a line chart shares with them.
 *
 * <p>These plot periods in order, so two rules differ from the category charts: the y axis is
 * fitted to the data rather than anchored at zero, and period labels are thinned to fit.
 */
@Tag("unit")
class ChartOrderedTest {

  private static final List<String> DAYS =
      Arrays.asList("09-01", "09-02", "09-03", "09-04");
  private static final List<ChartRenderer.SeriesSpec> NO_SERIES = Collections.emptyList();
  private static final List<ChartRenderer.Band> NO_BANDS = Collections.emptyList();
  private static final List<ChartRenderer.RefLine> NO_REFS = Collections.emptyList();

  private static List<Double> nums(Double... v) {
    return Arrays.asList(v);
  }

  private static ChartRenderer.Candles candles(List<Double> high) {
    return new ChartRenderer.Candles("yes price",
        nums(0.40, 0.46, null, 0.44), high, nums(0.38, 0.41, null, 0.40),
        nums(0.46, 0.42, null, 0.44), nums(120.0, 300.0, null, 0.0));
  }

  private static ChartScene candlestick(ChartRenderer.Candles c,
      List<ChartRenderer.RefLine> refs) {
    return ChartRenderer.layoutOrdered("candlestick", "Price", "Day", "Price", DAYS, NO_SERIES,
        c, NO_BANDS, refs, 600, 400, null, null);
  }

  private static int count(String text, String needle) {
    int n = 0;
    for (int at = text.indexOf(needle); at >= 0; at = text.indexOf(needle, at + 1)) {
      n++;
    }
    return n;
  }

  @Test void aCandleIsGreenWhenItClosedUpAndRedWhenDown() throws Exception {
    ChartScene scene = candlestick(candles(nums(0.47, 0.48, null, 0.45)), NO_REFS);
    String svg = scene.toSvg();
    int up = svg.indexOf("id=\"mark-yes-price-09-01\"");
    int down = svg.indexOf("id=\"mark-yes-price-09-02\"");
    assertTrue(up > 0 && down > up, svg);
    assertTrue(svg.substring(up, down).contains("#059669"));
    assertTrue(svg.substring(down).contains("#dc2626"));
    assertFalse(svg.substring(up, down).contains("#dc2626"));
    // The untraded day is a gap, and a day with no volume has no volume bar.
    assertFalse(svg.contains("mark-yes-price-09-03"));
    assertTrue(svg.contains("id=\"volume-09-02\""));
    assertFalse(svg.contains("id=\"volume-09-04\""));
    assertTrue(svg.contains("open 0.4, high 0.47, low 0.38, close 0.46, volume 120"), svg);
    byte[] png = scene.toPng();
    assertTrue((png[0] & 0xFF) == 0x89 && png[1] == 'P');
  }

  @Test void aCandleWhoseHighIsNotTheHighestIsRejected() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> candlestick(candles(nums(0.45, 0.48, null, 0.45)), NO_REFS));
    assertTrue(e.getMessage().contains("09-01"), e.getMessage());
  }

  @Test void aCandleWithSomePricesMissingIsRejected() {
    ChartRenderer.Candles c = new ChartRenderer.Candles("p", nums(0.4, 0.4, 0.4, 0.4),
        nums(0.5, null, 0.5, 0.5), nums(0.3, 0.3, 0.3, 0.3), nums(0.4, 0.4, 0.4, 0.4), null);
    assertThrows(IllegalArgumentException.class, () -> candlestick(c, NO_REFS));
  }

  @Test void aCandlestickNeedsCandlesAndTakesNoBands() {
    assertThrows(IllegalArgumentException.class, () -> candlestick(null, NO_REFS));
    ChartRenderer.Band band = new ChartRenderer.Band("b", nums(1.0, 1.0, 1.0, 1.0),
        nums(2.0, 2.0, 2.0, 2.0));
    assertThrows(IllegalArgumentException.class,
        () -> ChartRenderer.layoutOrdered("candlestick", null, null, null, DAYS, NO_SERIES,
            candles(nums(0.47, 0.48, null, 0.45)), Collections.singletonList(band), NO_REFS,
            600, 400, null, null));
  }

  private static ChartScene fan(List<ChartRenderer.Band> bands) {
    List<String> months = Arrays.asList("Jun", "Jul", "Aug", "Sep", "Oct");
    return ChartRenderer.layoutOrdered("fan", "CPI index", "Month", "Index", months,
        Arrays.asList(
            new ChartRenderer.SeriesSpec("history", nums(321.5, 322.1, 322.9, null, null)),
            new ChartRenderer.SeriesSpec("median", nums(null, null, 322.9, 323.6, 324.2))),
        null, bands, NO_REFS, 600, 400, null, null);
  }

  private static ChartRenderer.Band band(String name, double half) {
    return new ChartRenderer.Band(name,
        nums(null, null, 322.9, 323.6 - half, 324.2 - 2 * half),
        nums(null, null, 322.9, 323.6 + half, 324.2 + 2 * half));
  }

  @Test void aFanDrawsItsWidestBandFirstWhateverTheOrderGiven() {
    String svg = fan(Arrays.asList(band("50%", 0.2), band("95%", 0.9), band("80%", 0.5)))
        .toSvg();
    int b95 = svg.indexOf("id=\"band-95\"");
    int b80 = svg.indexOf("id=\"band-80\"");
    int b50 = svg.indexOf("id=\"band-50\"");
    assertTrue(b95 > 0 && b95 < b80 && b80 < b50, svg);
    // Lines are drawn over the bands, and every band and line is in the legend.
    assertTrue(svg.indexOf("id=\"line-median\"") > b50);
    assertTrue(svg.contains("legend-label-95") && svg.contains("legend-label-history"));
  }

  @Test void aFanFitsItsAxisToTheDataRatherThanZero() {
    String svg = fan(Collections.singletonList(band("80%", 0.5))).toSvg();
    assertFalse(svg.contains(">0</text>"), "the axis starts at zero: " + svg);
    assertTrue(svg.contains(">322</text>") || svg.contains(">321</text>"), svg);
  }

  @Test void aFanNeedsBandsAndALine() {
    assertThrows(IllegalArgumentException.class, () -> fan(NO_BANDS));
    assertThrows(IllegalArgumentException.class,
        () -> ChartRenderer.layoutOrdered("fan", null, null, null, DAYS, NO_SERIES, null,
            Collections.singletonList(new ChartRenderer.Band("b", nums(1.0, 1.0, 1.0, 1.0),
                nums(2.0, 2.0, 2.0, 2.0))), NO_REFS, 600, 400, null, null));
    // A band whose low is above its high, and one given on only one side.
    assertThrows(IllegalArgumentException.class,
        () -> fan(Collections.singletonList(new ChartRenderer.Band("b",
            nums(null, null, 3.0, 3.0, 3.0), nums(null, null, 2.0, 4.0, 4.0)))));
    assertThrows(IllegalArgumentException.class,
        () -> fan(Collections.singletonList(new ChartRenderer.Band("b",
            nums(null, null, 3.0, null, 3.0), nums(null, null, 4.0, 4.0, 4.0)))));
  }

  @Test void periodLabelsAreThinnedToWhatFits() {
    List<String> days = new ArrayList<>();
    List<Double> o = new ArrayList<>();
    List<Double> h = new ArrayList<>();
    List<Double> l = new ArrayList<>();
    for (int i = 0; i < 120; i++) {
      days.add(String.format("2026-%02d-%02d", 6 + i / 30, 1 + i % 30));
      o.add(0.5);
      h.add(0.6);
      l.add(0.4);
    }
    String svg = ChartRenderer.layoutOrdered("candlestick", null, null, null, days, NO_SERIES,
        new ChartRenderer.Candles("p", o, h, l, o, null), NO_BANDS, NO_REFS, 600, 400, null,
        null).toSvg();
    int labels = count(svg, "id=\"xtick-");
    assertTrue(labels >= 2 && labels <= 8, "labels drawn: " + labels);
    assertEquals(120, count(svg, "class=\"candle\""));
  }

  @Test void aReferenceLineWidensTheAxisAndCarriesItsLabel() {
    List<ChartRenderer.RefLine> refs = Arrays.asList(
        new ChartRenderer.RefLine(0.9, null, "fair value"),
        new ChartRenderer.RefLine(null, "09-02", "CPI release"));
    String svg = candlestick(candles(nums(0.47, 0.48, null, 0.45)), refs).toSvg();
    assertTrue(svg.contains(">fair value</text>") && svg.contains(">CPI release</text>"), svg);
    assertTrue(svg.contains(">0.9</text>") || svg.contains(">1</text>"), svg);
    assertEquals(2, count(svg, "class=\"reference\""));

    // A line chart takes them too; an unlabelled value line is labelled with the value.
    String line = ChartRenderer.layout("line", "Payoff", "Settles at", "Profit",
        Arrays.asList("2.9", "3.0", "3.1"),
        Collections.singletonList(new ChartRenderer.SeriesSpec("basket", nums(1.0, 2.0, 5.0))),
        600, 400, ChartRenderer.BarOptions.DEFAULT,
        Collections.singletonList(new ChartRenderer.RefLine(-20.0, null, null))).toSvg();
    assertTrue(line.contains(">-20</text>"), line);
    assertEquals(1, count(line, "class=\"reference\""));
  }

  @Test void aReferenceLineIsRejectedWhereItCannotBeDrawn() {
    List<ChartRenderer.SeriesSpec> one =
        Collections.singletonList(new ChartRenderer.SeriesSpec("s", nums(1.0, 2.0, 3.0, 4.0)));
    ChartRenderer.RefLine value = new ChartRenderer.RefLine(2.0, null, null);
    assertThrows(IllegalArgumentException.class,
        () -> ChartRenderer.layout("bar", null, null, null, DAYS, one, 600, 400,
            ChartRenderer.BarOptions.DEFAULT, Collections.singletonList(value)));
    assertThrows(IllegalArgumentException.class,
        () -> ChartRenderer.layout("pie", null, null, null, DAYS, one, 600, 400,
            ChartRenderer.BarOptions.DEFAULT, Collections.singletonList(value)));
    // Neither a value nor a category, both, and a category the chart does not have.
    for (ChartRenderer.RefLine bad : Arrays.asList(
        new ChartRenderer.RefLine(null, null, "x"),
        new ChartRenderer.RefLine(2.0, "09-01", "x"),
        new ChartRenderer.RefLine(null, "10-01", "x"))) {
      assertThrows(IllegalArgumentException.class,
          () -> ChartRenderer.layout("line", null, null, null, DAYS, one, 600, 400,
              ChartRenderer.BarOptions.DEFAULT, Collections.singletonList(bad)));
    }
  }

  @Test void aDashboardPanelTakesACandlestickAndAFan() {
    DashboardLayout.Panel c = new DashboardLayout.Panel();
    c.chartType = "candlestick";
    c.title = "Price";
    c.categories = DAYS;
    c.series = new ArrayList<>();
    c.candles = candles(nums(0.47, 0.48, null, 0.45));
    c.refLines = Collections.singletonList(new ChartRenderer.RefLine(0.5, null, "fair"));
    DashboardLayout.Panel f = new DashboardLayout.Panel();
    f.chartType = "fan";
    f.title = "Forecast";
    f.categories = Arrays.asList("Jun", "Jul", "Aug", "Sep", "Oct");
    f.series = Collections.singletonList(
        new ChartRenderer.SeriesSpec("median", nums(321.5, 322.1, 322.9, 323.6, 324.2)));
    f.bands = Collections.singletonList(band("80%", 0.5));
    String svg = DashboardLayout.compose("Board", null, null, Arrays.asList(c, f), 2, 1000,
        500).toSvg();
    assertTrue(svg.contains("class=\"candle\"") && svg.contains("class=\"band\""), svg);
    assertTrue(svg.contains(">fair</text>"));
  }
}
