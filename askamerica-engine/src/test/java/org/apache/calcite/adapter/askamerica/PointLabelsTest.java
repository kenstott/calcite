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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Optional per-point labels, label modes and highlight groups on scatter/bubble charts. */
@Tag("unit")
public class PointLabelsTest {

    private static final List<String> STATES =
        Arrays.asList("AL", "AK", "AZ", "CA", "LA", "ND", "WA", "WY");

    private static List<Double> nums(double... v) {
        List<Double> out = new ArrayList<>();
        for (double d : v) {
            out.add(d);
        }
        return out;
    }

    private static ChartRenderer.PointSeriesSpec spec(List<String> labels, String mode,
            Map<String, List<String>> highlight) {
        return new ChartRenderer.PointSeriesSpec("States",
            nums(1, 2, 3, 4, 5, 6, 7, 8), nums(2, 1, 4, 3, 6, 5, 8, 20), null, null, labels, mode, 2,
            highlight);
    }

    private static String svg(ChartRenderer.PointSeriesSpec s) {
        return ChartRenderer.layoutPoints("scatter", "t", "x", "y", Arrays.asList(s), 700, 450)
            .toSvg();
    }

    private static int count(String svg, String regex) {
        Matcher m = Pattern.compile(regex).matcher(svg);
        int n = 0;
        while (m.find()) {
            n++;
        }
        return n;
    }

    @Test
    void noLabelsByDefault() {
        String svg = svg(new ChartRenderer.PointSeriesSpec("S", nums(1, 2), nums(1, 2), null));
        assertFalse(svg.contains("id=\"label-"));
    }

    @Test
    void allModeLabelsEveryPointWithIds() {
        String svg = svg(spec(STATES, "all", null));
        assertEquals(8, count(svg, "id=\"label-\\d+\""));
        assertTrue(svg.contains("id=\"label-3\""));
        assertTrue(svg.contains(">CA</text>"));
    }

    @Test
    void extremesLabelsTheFurthestFromTheFittedLine() {
        String svg = svg(spec(STATES, "extremes", null));
        assertEquals(2, count(svg, "id=\"label-\\d+\""));
        assertTrue(svg.contains(">WY</text>"), "the outlier must be labelled");
    }

    @Test
    void highlightGroupsGetLegendEntriesAndMutedRest() {
        Map<String, List<String>> h = new LinkedHashMap<>();
        h.put("Oil", Arrays.asList("LA", "ND"));
        String svg = svg(spec(STATES, "highlighted", h));
        assertTrue(svg.contains("legend-label-oil"), "one group still needs a legend entry");
        assertEquals(2, count(svg, "id=\"label-\\d+\""));
        assertEquals(6, count(svg, "fill=\"#9ca3af\""), "non-members are muted");
    }

    @Test
    void labelsDoNotOverlapWhenPointsCollide() {
        List<String> labels = Arrays.asList("A", "B", "C", "D");
        ChartRenderer.PointSeriesSpec s = new ChartRenderer.PointSeriesSpec("S",
            nums(1, 1, 1, 5), nums(1, 1, 1, 5), null, null, labels, "all", 5, null);
        String svg = svg(s);
        List<Double> ys = new ArrayList<>();
        Matcher m = Pattern.compile("id=\"label-[0-2]\"[^>]* y=\"([0-9.]+)\"").matcher(svg);
        while (m.find()) {
            ys.add(Double.parseDouble(m.group(1)));
        }
        java.util.Collections.sort(ys);
        assertEquals(3, ys.size());
        assertTrue(ys.get(1) - ys.get(0) >= 12 && ys.get(2) - ys.get(1) >= 12, ys.toString());
    }

    @Test
    void mismatchedOrMissingLabelsAreRejected() {
        assertThrows(IllegalArgumentException.class,
            () -> svg(spec(STATES.subList(0, 3), "all", null)));
        assertThrows(IllegalArgumentException.class, () -> svg(spec(null, "all", null)));
        assertThrows(IllegalArgumentException.class, () -> svg(spec(STATES, "some", null)));
        Map<String, List<String>> h = new LinkedHashMap<>();
        h.put("Nope", Arrays.asList("ZZ"));
        assertThrows(IllegalArgumentException.class, () -> svg(spec(STATES, "highlighted", h)));
    }
}
