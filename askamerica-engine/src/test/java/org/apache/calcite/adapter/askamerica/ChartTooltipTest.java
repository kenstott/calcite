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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Hover tooltips are native SVG {@code <title>} elements: no script, announced by readers. */
@Tag("unit")
public class ChartTooltipTest {

    private static final List<String> CATS = Arrays.asList("California", "Utah");

    private static ChartRenderer.SeriesSpec series(List<String> tips) {
        return new ChartRenderer.SeriesSpec("Real change",
            Arrays.asList(18130.0, 9000.0), tips);
    }

    private static int count(String hay, String needle) {
        int n = 0;
        for (int i = hay.indexOf(needle); i >= 0; i = hay.indexOf(needle, i + 1)) {
            n++;
        }
        return n;
    }

    @Test void callerTooltipsBecomeTitlesAndEveryMarkGetsAHitTarget() {
        for (String type : Arrays.asList("bar", "line")) {
            String svg = ChartRenderer.layout(type, "t", "x", "y", CATS,
                Arrays.asList(series(Arrays.asList("CA: +$18,130", "UT: +$9,000"))), 600, 400)
                .toSvg();
            assertTrue(svg.contains("<title>CA: +$18,130</title>"), type);
            assertTrue(svg.contains("<title>UT: +$9,000</title>"), type);
            assertEquals(2, count(svg, "class=\"hit\""), type);
            assertFalse(svg.contains("<script"), type);
        }
    }

    @Test void defaultTooltipNamesCategorySeriesAndFormattedValue() {
        String svg = ChartRenderer.layout("bar", "t", "x", "y", CATS,
            Arrays.asList(series(null)), 600, 400,
            ChartRenderer.BarOptions.parse(null, null, null, "$,.0f", false)).toSvg();
        assertTrue(svg.contains("<title>California — Real change: $18,130</title>"), svg);
        assertTrue(svg.contains(">$20,000<"), "axis ticks use value_format");
    }

    @Test void horizontalBarsCarryTooltipsToo() {
        String svg = ChartRenderer.layout("bar", "t", "x", "y", CATS,
            Arrays.asList(series(null)), 600, 400,
            ChartRenderer.BarOptions.parse("horizontal", null, null, null, false)).toSvg();
        assertTrue(svg.contains("<title>Utah — Real change: 9,000</title>"), svg);
        assertEquals(2, count(svg, "class=\"hit\""));
    }

    @Test void pieSlicesAndScatterPointsCarryTooltips() {
        String pie = ChartRenderer.layout("pie", "t", "x", "y", CATS,
            Arrays.asList(new ChartRenderer.SeriesSpec("s", Arrays.asList(1.0, 3.0))), 600, 400)
            .toSvg();
        assertTrue(pie.contains("<title>Utah: 3 (75.0%)</title>"), pie);

        String pts = ChartRenderer.layoutPoints("scatter", "t", "x", "y",
            Arrays.asList(new ChartRenderer.PointSeriesSpec("p", Arrays.asList(1.0),
                Arrays.asList(0.25), null, Arrays.asList("custom"))), 600, 400, ValueFormat.parse("+.1%")).toSvg();
        assertTrue(pts.contains("<title>custom</title>"), pts);
    }

    @Test void tooltipTitleTextIsEscaped() {
        String svg = ChartRenderer.layout("bar", "t", "x", "y", CATS,
            Arrays.asList(series(Arrays.asList("<b>&", "x"))), 600, 400).toSvg();
        assertTrue(svg.contains("<title>&lt;b&gt;&amp;</title>"), svg);
    }

    @Test void pngIsUnaffectedAndMismatchedTooltipsAreRejected() throws Exception {
        ChartScene scene = ChartRenderer.layout("bar", "t", "x", "y", CATS,
            Arrays.asList(series(Arrays.asList("a", "b"))), 600, 400);
        assertTrue(scene.toPng().length > 100);
        assertThrows(IllegalArgumentException.class, () -> ChartRenderer.layout("bar", "t", "x",
            "y", CATS, Arrays.asList(series(Arrays.asList("only one"))), 600, 400));
    }

    @Test void valueFormatSubset() {
        assertEquals("$1,234", ValueFormat.parse("$,.0f").format(1234.4));
        assertEquals("+12.3%", ValueFormat.parse("+.1%").format(0.1234));
        assertEquals("-$5", ValueFormat.parse("$,.0f").format(-5));
        assertEquals("0", ValueFormat.parse(",.0f").format(-0.2));
        assertEquals("$1,234", ValueFormat.parse("$#,##0").format(1234.4));
        assertThrows(IllegalArgumentException.class, () -> ValueFormat.parse("0.0.0"));
    }
}
