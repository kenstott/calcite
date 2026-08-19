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

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilderFactory;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A dashboard is one artifact assembled from several panels, and two things have to hold for it
 * to be trustworthy.
 *
 * <p><b>Panels must not be silently comparable when they are not.</b> Two charts side by side
 * with independently fitted axes invite a comparison the picture does not support — the taller
 * bar can be the smaller number — and a reader gets no cue that anything is wrong. Panels naming
 * one {@code scale_group} must therefore end up on one domain, and panels that name none must
 * keep their own.
 *
 * <p><b>Panels must not collide.</b> Every chart carries ids like {@code mark-california} so it
 * can be edited; two panels of the same data would emit that id twice, which makes the document
 * invalid and the editing contract meaningless.
 */
@Tag("unit")
public class DashboardTest {

    private static final List<String> YEARS = Arrays.asList("2014", "2019", "2024");

    private static ChartRenderer.SeriesSpec series(String name, double... values) {
        List<Double> vs = new ArrayList<>();
        for (double v : values) {
            vs.add(v);
        }
        return new ChartRenderer.SeriesSpec(name, vs);
    }

    private static DashboardLayout.Panel chart(String title, String scaleGroup, double... vals) {
        DashboardLayout.Panel p = new DashboardLayout.Panel();
        p.kind = "chart";
        p.chartType = "line";
        p.title = title;
        p.yLabel = "USD";
        p.categories = YEARS;
        p.series = Arrays.asList(series("v", vals));
        p.scaleGroup = scaleGroup;
        return p;
    }

    private static DashboardLayout.Panel stat(String label, String value) {
        DashboardLayout.Panel p = new DashboardLayout.Panel();
        p.kind = "stat";
        p.label = label;
        p.value = value;
        p.delta = "+22.0%";
        p.deltaDirection = "up";
        return p;
    }

    /** The largest y-axis tick inside a given nested panel, as the reader would read it. */
    private static int axisMax(String svg, int panelIndex) {
        List<String> panels = new ArrayList<>();
        Matcher m = Pattern.compile("<svg x=.*?</svg>", Pattern.DOTALL).matcher(svg);
        while (m.find()) {
            panels.add(m.group());
        }
        // Only the y-axis ticks. The x-axis category labels share class="tick", and on a
        // year axis they parse as numbers — reading those back gave 2024 as the "domain".
        Matcher t = Pattern.compile("id=\"[^\"]*ytick-\\d+\"[^>]*>([\\d,]+)</text>")
            .matcher(panels.get(panelIndex));
        int max = -1;
        while (t.find()) {
            max = Math.max(max, Integer.parseInt(t.group(1).replace(",", "")));
        }
        return max;
    }

    @Test void panelsSharingAScaleGroupGetOneDomain() {
        // Two series an order of magnitude apart. Fitted independently they would look alike.
        String svg = DashboardLayout.compose("t", null, null,
            Arrays.asList(chart("small", "money", 10, 20, 30),
                chart("large", "money", 100, 200, 300)),
            2, 900, 500).toSvg();

        assertEquals(axisMax(svg, 0), axisMax(svg, 1),
            "panels in one scale_group must share a y-domain, or the picture invites a "
                + "comparison it does not support");
        assertTrue(axisMax(svg, 0) >= 300, "the shared domain must cover the larger panel");
    }

    @Test void panelsWithoutAScaleGroupKeepTheirOwnDomain() {
        String svg = DashboardLayout.compose("t", null, null,
            Arrays.asList(chart("small", null, 10, 20, 30),
                chart("large", null, 100, 200, 300)),
            2, 900, 500).toSvg();

        assertNotEquals(axisMax(svg, 0), axisMax(svg, 1),
            "without a scale_group each panel fits its own axis — forcing a shared one would "
                + "waste the small panel's whole range");
    }

    @Test void independentScaleGroupsDoNotBleedIntoEachOther() {
        String svg = DashboardLayout.compose("t", null, null,
            Arrays.asList(chart("a", "money", 10, 20, 30),
                chart("b", "money", 20, 25, 30),
                chart("c", "rates", 100, 200, 300)),
            3, 1200, 500).toSvg();

        assertEquals(axisMax(svg, 0), axisMax(svg, 1), "same group shares");
        assertNotEquals(axisMax(svg, 0), axisMax(svg, 2), "a different group is separate");
    }

    @Test void panelIdsAreNamespacedSoNothingCollides() {
        String svg = DashboardLayout.compose("t", null, null,
            Arrays.asList(chart("one", null, 1, 2, 3), chart("two", null, 1, 2, 3)),
            2, 900, 500).toSvg();

        List<String> ids = new ArrayList<>();
        Matcher m = Pattern.compile("id=\"([^\"]+)\"").matcher(svg);
        while (m.find()) {
            ids.add(m.group(1));
        }
        assertEquals(ids.size(), new java.util.HashSet<>(ids).size(),
            "duplicate ids make the document invalid and per-panel editing ambiguous");
        assertTrue(svg.contains("id=\"p1-"), "panel 1 namespace");
        assertTrue(svg.contains("id=\"p2-"), "panel 2 namespace");
    }

    @Test void identicalPanelsStillProduceDistinctIds() {
        // The exact case that would collide: the same chart twice.
        String svg = DashboardLayout.compose("t", null, null,
            Arrays.asList(chart("same", null, 1, 2, 3), chart("same", null, 1, 2, 3)),
            2, 900, 500).toSvg();
        assertTrue(svg.contains("id=\"p1-series-v\""), svg.substring(0, 400));
        assertTrue(svg.contains("id=\"p2-series-v\""));
    }

    @Test void aStatRowIsShorterThanAChartRow() {
        // Giving every row equal height made three tiles take as much of the board as the
        // chart explaining them, which inverts what the reader should look at first.
        String svg = DashboardLayout.compose("t", null, null,
            Arrays.asList(stat("a", "1"), stat("b", "2"), chart("c", null, 1, 2, 3)),
            2, 900, 600).toSvg();

        Matcher m = Pattern.compile("<svg x=\"[\\d.]+\" y=\"[\\d.]+\" width=\"[\\d.]+\" "
            + "height=\"([\\d.]+)\"").matcher(svg);
        assertTrue(m.find(), "the chart panel should be nested");
        assertTrue(Double.parseDouble(m.group(1)) > 200,
            "the chart row should get the space the stat row gave up");
    }

    @Test void producesWellFormedSelfContainedXml() {
        String svg = DashboardLayout.compose("Title", "Subtitle", "Footnote",
            Arrays.asList(stat("Real rise", "+$19,029"), chart("trend", null, 1, 2, 3)),
            2, 900, 500).toSvg();

        assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder()
            .parse(new ByteArrayInputStream(svg.getBytes(StandardCharsets.UTF_8))));
        assertFalse(svg.contains("<script"), "no scripts");
        assertFalse(svg.contains("xlink:href") || svg.contains("<image"), "no external assets");
        assertTrue(svg.contains("prefers-color-scheme: dark"), "survives a dark page");
        assertTrue(svg.contains("Title") && svg.contains("Subtitle")
            && svg.contains("Footnote"), "chrome is rendered");
    }

    @Test void rastersTheSameCompositionSoImageAndMarkupAgree() {
        DashboardLayout.Dashboard d = DashboardLayout.compose("t", null, null,
            Arrays.asList(stat("a", "1"), chart("c", null, 1, 2, 3)), 2, 900, 500);
        byte[] png = assertDoesNotThrow(d::toPng);
        assertEquals((byte) 0x89, png[0], "PNG magic byte");
        assertTrue(png.length > 500);
        assertTrue(d.toSvg().contains("p2-series-v"), "same composition backs both outputs");
    }

    @Test void carriesTheEditingContractForwardToPanels() {
        String svg = DashboardLayout.compose("t", null, null,
            Arrays.asList(chart("one", null, 1, 2, 3)), 1, 700, 400).toSvg();
        assertTrue(svg.contains("Do NOT move a panel's internal geometry"),
            "the composed document must restate the contract, since the per-panel headers "
                + "are not carried into it");
        assertTrue(svg.contains("<g id=\"p1-annotations\">"), "per-panel annotation group");
        assertTrue(svg.lastIndexOf("<g id=\"annotations\">") > svg.lastIndexOf("<svg x="),
            "the dashboard-level annotation group paints last");
    }

    @Test void refusesAnEmptyBoardRatherThanEmittingABlankOne() {
        assertThrows(IllegalArgumentException.class, () ->
            DashboardLayout.compose("t", null, null, new ArrayList<>(), 2, 900, 500));
    }

    @Test void aSpanningPanelTakesTheColumnsItAsksFor() {
        DashboardLayout.Panel wide = chart("wide", null, 1, 2, 3);
        wide.span = 2;
        String svg = DashboardLayout.compose("t", null, null,
            Arrays.asList(wide, chart("narrow", null, 1, 2, 3)), 2, 1000, 600).toSvg();

        Matcher m = Pattern.compile("<svg x=\"[\\d.]+\" y=\"[\\d.]+\" width=\"([\\d.]+)\"")
            .matcher(svg);
        assertTrue(m.find());
        double first = Double.parseDouble(m.group(1));
        assertTrue(m.find());
        double second = Double.parseDouble(m.group(1));
        assertTrue(first > second * 1.5,
            "a span=2 panel should be about twice as wide: " + first + " vs " + second);
    }
}
