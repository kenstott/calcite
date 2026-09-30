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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Horizontal bars, sorting, value labels and label wrapping for bar charts. */
@Tag("unit")
public class ChartBarOrientationTest {

    private static final List<String> MODEL_FIT = Arrays.asList(
        "2014 data, in-sample", "2015 data, in-sample", "2016 data, out-of-sample",
        "2017 data, out-of-sample");

    private static ChartRenderer.SeriesSpec series(String name, double... values) {
        List<Double> vs = new ArrayList<>();
        for (double v : values) {
            vs.add(v);
        }
        return new ChartRenderer.SeriesSpec(name, vs);
    }

    private static ChartRenderer.BarOptions opts(String orientation, String sort,
            Boolean labels, String format, boolean grow) {
        return ChartRenderer.BarOptions.parse(orientation, sort, labels, format, grow);
    }

    private static String svg(List<String> cats, ChartRenderer.SeriesSpec s, int w, int h,
            ChartRenderer.BarOptions o) {
        return ChartRenderer.layout("bar", "t", "x", "y", cats, Arrays.asList(s), w, h, o)
            .toSvg();
    }

    @Test void modelFitLabelsShowInFullInANarrowDashboardPanel() {
        String svg = ChartLayout.categoryChart("bar", "Fit", null, null, MODEL_FIT,
            Arrays.asList(series("r2", 0.9, 0.8, 0.7, 0.6)), 412, 300, null,
            ChartRenderer.BarOptions.DEFAULT).toSvg();
        for (String c : MODEL_FIT) {
            assertTrue(svg.contains(">" + c + "<"), c);
        }
        assertFalse(svg.contains("…"));
    }

    @Test void modelFitLabelsShowInFullOnASingleChart() {
        String svg = svg(MODEL_FIT, series("r2", 0.9, 0.8, 0.7, 0.6), 800, 500,
            ChartRenderer.BarOptions.DEFAULT);
        for (String c : MODEL_FIT) {
            assertTrue(svg.contains(">" + c + "<"), c);
        }
    }

    @Test void fiftyTwoCategoriesGoHorizontalWithEveryLabelReadable() {
        List<String> cats = new ArrayList<>();
        double[] vals = new double[52];
        for (int i = 0; i < 52; i++) {
            cats.add("State number " + i);
            vals[i] = i + 1;
        }
        ChartScene scene = ChartRenderer.layout("bar", "t", null, null, cats,
            Arrays.asList(series("v", vals)), 800, 500,
            ChartRenderer.BarOptions.DEFAULT.growing());
        String svg = scene.toSvg();
        assertTrue(svg.contains("category-axis-labels"));
        for (String c : cats) {
            assertTrue(svg.contains(">" + c + "<"), c);
        }
        assertFalse(svg.contains("…"));
        assertTrue(svg.contains("viewBox=\"0 0 800 9") || svg.contains("viewBox=\"0 0 800 1"));
    }

    @Test void verticalOrientationIsHonouredEvenWithManyCategories() {
        List<String> cats = new ArrayList<>();
        double[] vals = new double[12];
        for (int i = 0; i < 12; i++) {
            cats.add("C" + i);
            vals[i] = i;
        }
        String svg = svg(cats, series("v", vals), 800, 400,
            opts("vertical", null, null, null, false));
        assertFalse(svg.contains("category-axis-labels"));
    }

    @Test void negativeValueDrawsLeftOfZeroWithItsLabel() {
        String svg = svg(Arrays.asList("A", "B"), series("v", 100, -302), 800, 300,
            opts("horizontal", null, true, "$#,##0", false));
        assertTrue(svg.contains("-$302"), svg);
        assertTrue(svg.contains("$100"));
        double zero = attr(svg, "class=\"axis\"", "x1");
        double negX = attr(svg, "id=\"mark-v-b\"", "x");
        double negW = attr(svg, "id=\"mark-v-b\"", "width");
        double posX = attr(svg, "id=\"mark-v-a\"", "x");
        assertEquals(zero, negX + negW, 0.6);
        assertEquals(zero, posX, 0.6);
        assertTrue(negX < zero);
    }

    @Test void sortDescPutsLargestFirstAndCarriesEverySeries() {
        List<String> cats = Arrays.asList("low", "high", "mid");
        ChartRenderer.SeriesSpec a = series("a", 1, 9, 5);
        ChartRenderer.SeriesSpec b = series("b", 10, 20, 30);
        String svg = ChartRenderer.layout("bar", "t", null, null, cats, Arrays.asList(a, b),
            800, 300, opts("horizontal", "desc", null, null, false)).toSvg();
        assertTrue(svg.indexOf("xtick-high") < svg.indexOf("xtick-mid"));
        assertTrue(svg.indexOf("xtick-mid") < svg.indexOf("xtick-low"));
        // series b's value for "high" is 20 and must have moved with its category
        assertTrue(attr(svg, "id=\"mark-b-high\"", "width")
            > attr(svg, "id=\"mark-b-low\"", "width") * 1.5);
    }

    @Test void longLabelWrapsToTwoLinesBeforeShortening() {
        List<String> lines = ChartLayout.wrapLabel(
            "Professional, scientific and technical services", 170, 2);
        assertEquals(2, lines.size());
        assertFalse(lines.get(0).endsWith("…"));
        assertFalse(lines.get(1).endsWith("…"));
        List<String> cut = ChartLayout.wrapLabel(
            "Professional, scientific and technical services and more besides", 100, 2);
        assertTrue(cut.get(cut.size() - 1).endsWith("…"));
    }

    @Test void barOnlyOptionsAreRejectedOnOtherChartTypes() {
        assertThrows(IllegalArgumentException.class, () -> ChartRenderer.layout("line", "t",
            null, null, Arrays.asList("a"), Arrays.asList(series("v", 1)), 400, 300,
            opts("horizontal", null, null, null, false)));
        assertThrows(IllegalArgumentException.class,
            () -> opts("sideways", null, null, null, false));
        assertThrows(IllegalArgumentException.class,
            () -> opts(null, "up", null, null, false));
        assertThrows(IllegalArgumentException.class,
            () -> opts(null, null, null, "#,##0'", false));
    }

    private static double attr(String svg, String marker, String name) {
        int at = svg.indexOf(marker);
        assertTrue(at >= 0, marker);
        int start = svg.lastIndexOf('<', at);
        int end = svg.indexOf('>', at);
        String tag = svg.substring(start, end);
        int a = tag.indexOf(" " + name + "=\"");
        assertTrue(a >= 0, name + " in " + tag);
        int q = tag.indexOf('"', a) + 1;
        return Double.parseDouble(tag.substring(q, tag.indexOf('"', q)));
    }
}
