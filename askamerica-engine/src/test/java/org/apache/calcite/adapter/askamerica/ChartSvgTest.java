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

import javax.xml.parsers.DocumentBuilderFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * render_chart returns a picture and its source, and both have to hold up.
 *
 * <p>Two defects from the 2026-08-19 comparative eval are pinned here. An eight-bar chart
 * printed four category labels and left the other four bars anonymous — including the one the
 * caller had annotated as not being a state — so the reader could not tell which bar was which.
 * And a y-axis of household income rendered as {@code 1E5}, an exponent in the one place a
 * chart has to be exact.
 *
 * <p>The rest covers the contract the SVG makes: it must parse, it must carry the ids and
 * classes the tool description tells a caller to target, and it must be self-contained.
 */
@Tag("unit")
public class ChartSvgTest {

    private static final List<String> EIGHT = Arrays.asList(
        "California", "D.C.*", "Washington", "Colorado",
        "Utah", "Massachusetts", "Oregon", "Idaho");

    private static ChartRenderer.SeriesSpec series(String name, double... values) {
        List<Double> vs = new ArrayList<>();
        for (double v : values) {
            vs.add(v);
        }
        return new ChartRenderer.SeriesSpec(name, vs);
    }

    private static String barSvg() {
        return ChartRenderer.layout("bar",
            "Largest 10-year rise in median household income",
            "State", "Rise (US$)", EIGHT,
            Arrays.asList(series("Nominal rise",
                38216, 38059, 38023, 35810, 35736, 35668, 34145, 33305)),
            800, 500).toSvg();
    }

    @Test void labelsEveryCategoryOnACrowdedAxis() {
        String svg = barSvg();
        for (String category : EIGHT) {
            assertTrue(svg.contains(">" + category.replace("&", "&amp;") + "<"),
                "every bar must be identifiable; '" + category + "' had no label");
        }
    }

    @Test void keepsCrowdedLabelsHorizontalWhileTheyStillFit() {
        // Eight state names in 800px do fit side by side, so nothing should be rotated or cut.
        String svg = barSvg();
        assertFalse(svg.contains("rotate(-45"), "no need to rotate when the names fit");
        assertFalse(svg.contains("…"), "no need to truncate when the names fit");
    }

    @Test void aBoldTitleThatAwtMeasuresAsFittingButBrowsersRenderWiderShrinksFurther() {
        // Observed live 2026-09-04 on askamerica.ai: this exact 62-char bold title in a 412px
        // panel measured under Java AWT's SansSerif font as fitting at font-size 11 (drawn
        // untruncated), but Chrome renders the SVG's declared font-family (system-ui/
        // -apple-system) measurably wider at bold weight, so the shipped title overflowed the
        // panel and was clipped at BOTH ends (title text is centred) -- "Empirical check..."
        // arrived as "npirical check...listi". ChartScene.textWidth's bold safety margin makes
        // fittedTitleSize's own re-check (against that same margined measurement) shrink the
        // font further -- to 10px here -- so the full title still fits without needing
        // to fall back to ellipsis truncation at all.
        String title = "Empirical check: days from CVE publication to CISA KEV listing";
        String svg = ChartRenderer.layout("bar", title, "Days", "Share", Arrays.asList(
            "<=0 (same day/before)", "1-7", "8-30", "31+"),
            Arrays.asList(series("Share", 29, 26, 20, 25)), 412, 268).toSvg();

        int at = svg.indexOf(title);
        assertTrue(at > 0, "shrinking further must keep the title readable in full: " + svg);
        String elem = svg.substring(svg.lastIndexOf("<text", at), at);
        assertTrue(elem.contains("font-size=\"10\""),
            "expected the title to shrink to 10px once the bold safety margin no longer lets "
            + "it fit at a larger size, got: " + elem);
    }

    @Test void rotatesRatherThanDroppingWhenLabelsGenuinelyCollide() {
        // The same eight names in half the width. The old renderer answered this by printing
        // every other label; every bar must still be identifiable.
        String svg = ChartRenderer.layout("bar", "t", "State", "US$", EIGHT,
            Arrays.asList(series("Nominal rise",
                38216, 38059, 38023, 35810, 35736, 35668, 34145, 33305)),
            400, 500).toSvg();

        assertTrue(svg.contains("rotate(-45"),
            "eight names in 400px cannot sit horizontally — rotate them, do not drop them");
        for (String category : EIGHT) {
            assertTrue(svg.contains(">" + category + "<"),
                "'" + category + "' lost its label when the axis got tight");
        }
    }

    @Test void neverPrintsScientificNotationOnAnAxis() {
        String svg = ChartRenderer.layout("line", "California median household income",
            "Year", "US$", Arrays.asList("2014", "2024"),
            Arrays.asList(series("Nominal", 61933, 100149)), 800, 500).toSvg();

        assertFalse(svg.contains("E5") || svg.contains("E4") || svg.contains("1.0E"),
            "an income axis must read as money, not as an exponent: " + svg);
        assertTrue(svg.contains("100,000") || svg.contains("120,000"),
            "ticks should be grouped digits: " + svg);
    }

    @Test void formatsLargeMagnitudesCompactlyRatherThanAsExponents() {
        assertEquals("1,000", ChartLayout.formatTick(1000, 100));
        assertEquals("100,000", ChartLayout.formatTick(100000, 20000));
        assertEquals("2.5M", ChartLayout.formatTick(2500000, 500000));
        assertEquals("3B", ChartLayout.formatTick(3000000000.0, 1000000000.0));
    }

    @Test void producesWellFormedXml() {
        assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder()
            .parse(new ByteArrayInputStream(barSvg().getBytes(StandardCharsets.UTF_8))));
    }

    @Test void carriesTheIdsTheToolTellsCallersToTarget() {
        String svg = barSvg();
        assertTrue(svg.contains("id=\"series-nominal-rise\""), "series group id");
        assertTrue(svg.contains("id=\"mark-nominal-rise-california\""), "per-mark id");
        assertTrue(svg.contains("id=\"chart-title\""), "title id");
        assertTrue(svg.contains("class=\"tick\""), "tick class");
        assertTrue(svg.contains("id=\"y-axis-title\""), "axis title id");
    }

    @Test void statesTheEditingContractUpFront() {
        String svg = barSvg();
        assertTrue(svg.contains("Safe and expected to edit"),
            "a caller handed anonymous markup replaces it instead of adjusting it");
        assertTrue(svg.contains("Do NOT move plotted geometry"),
            "the one unsafe edit has to be named, or the chart can silently stop matching "
                + "the data it came from");
    }

    @Test void tellsTheCallerWhereThereIsRoomToAnnotate() {
        // Observed 2026-08-19b: the first agent to use this scaffold put its callout through
        // the top gridline label and its footnote off the right edge. It followed the header
        // exactly — the header just never said where the free space was.
        String svg = barSvg();
        assertTrue(svg.contains("WHERE THERE IS ROOM"), "the header must name the free bands");
        assertTrue(svg.contains("annotation band"), "band above the plot");
        assertTrue(svg.contains("footnote line"), "band at the bottom");
        assertTrue(svg.contains("The plot rectangle itself is x "),
            "and the plot rectangle, so 'inside' is a deliberate choice not a guess");
    }

    @Test void reservesTheBandsItAdvertisesSoNothingIsAlreadyThere() {
        String svg = barSvg();
        java.util.regex.Matcher band = java.util.regex.Pattern
            .compile("annotation band\\s+y ([0-9.]+)\\.\\.([0-9.]+)").matcher(svg);
        assertTrue(band.find(), "band coordinates must be stated: " + svg.substring(0, 900));
        double top = Double.parseDouble(band.group(1));
        double bottom = Double.parseDouble(band.group(2));
        assertTrue(bottom > top, "the band must have height");

        // Nothing may already be drawn inside the band it calls empty.
        java.util.regex.Matcher ys = java.util.regex.Pattern
            .compile("<(?:text|rect|line|circle)[^>]*?\\sy(?:1)?=\"([0-9.]+)\"")
            .matcher(svg);
        while (ys.find()) {
            double y = Double.parseDouble(ys.group(1));
            assertFalse(y > top && y < bottom,
                "element at y=" + y + " sits inside the supposedly free band "
                    + top + ".." + bottom);
        }
    }

    @Test void offersAnAnnotationsGroupThatPaintsLast() {
        String svg = barSvg();
        assertTrue(svg.contains("<g id=\"annotations\">"), "the group must exist");
        // lastIndexOf on both: the header comment mentions each id by name, so indexOf would
        // match the documentation rather than the element it documents.
        assertTrue(svg.lastIndexOf("<g id=\"annotations\">")
                > svg.lastIndexOf("<g id=\"series-"),
            "annotations must come after the series, or additions paint underneath the data");
    }

    @Test void isSelfContainedWithNoExternalReferencesOrScripts() {
        String svg = barSvg();
        assertFalse(svg.contains("<script"), "no scripts");
        // The SVG namespace is the one permitted http:// occurrence; nothing is ever fetched.
        assertEquals(1, svg.split("http", -1).length - 1,
            "the namespace declaration should be the only http reference");
        assertFalse(svg.contains("xlink:href") || svg.contains("<image"), "no external assets");
        assertTrue(svg.contains("xmlns=\"http://www.w3.org/2000/svg\""), "namespaced");
    }

    @Test void readsInDarkModeAsWellAsLight() {
        assertTrue(barSvg().contains("prefers-color-scheme: dark"),
            "a chart pasted into a dark page should not become black text on black");
    }

    @Test void escapesMarkupInCallerSuppliedText() {
        String svg = ChartRenderer.layout("bar", "Profit <&> Loss", "x", "y",
            Arrays.asList("A & B"), Arrays.asList(series("s", 1)), 400, 300).toSvg();

        assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder()
            .parse(new ByteArrayInputStream(svg.getBytes(StandardCharsets.UTF_8))),
            "a title with angle brackets must not break the document");
        assertTrue(svg.contains("Profit &lt;&amp;&gt; Loss"));
    }

    @Test void rendersTheSameSceneAsAPngSoThePictureCannotDisagreeWithTheMarkup() {
        ChartScene scene = ChartRenderer.layout("bar", "t", "x", "y", EIGHT,
            Arrays.asList(series("s", 1, 2, 3, 4, 5, 6, 7, 8)), 800, 500);
        byte[] png = assertDoesNotThrow(scene::toPng);
        assertTrue(png.length > 100, "PNG should have content");
        assertEquals((byte) 0x89, png[0], "PNG magic byte");
        // Same object, so the SVG cannot have been laid out from different numbers.
        assertTrue(scene.toSvg().contains("id=\"mark-s-idaho\""));
    }

    @Test void handlesEveryChartTypeItAdvertises() {
        List<ChartRenderer.PointSeriesSpec> pts = Arrays.asList(
            new ChartRenderer.PointSeriesSpec("p",
                Arrays.asList(1.0, 2.0), Arrays.asList(3.0, 4.0), Arrays.asList(5.0, 6.0)));
        for (String type : Arrays.asList("line", "bar", "pie")) {
            assertTrue(ChartRenderer.layout(type, "t", "x", "y", Arrays.asList("a", "b"),
                Arrays.asList(series("s", 1, 2)), 600, 400).toSvg().contains("<svg"), type);
        }
        assertTrue(ChartRenderer.layoutPoints("scatter", "t", "x", "y", pts, 600, 400)
            .toSvg().contains("<svg"), "scatter");
        assertTrue(ChartRenderer.layoutPoints("bubble", "t", "x", "y", pts, 600, 400)
            .toSvg().contains("<svg"), "bubble");
    }

    @Test void survivesAGapInASeriesRatherThanPlottingItAsZero() {
        List<Double> withGap = Arrays.asList(1.0, null, 3.0);
        String svg = ChartRenderer.layout("line", "t", "x", "y",
            Arrays.asList("2019", "2020", "2021"),
            Arrays.asList(new ChartRenderer.SeriesSpec("s", withGap)), 600, 400).toSvg();

        assertTrue(svg.contains("id=\"mark-s-2019\""), "2019 plotted");
        assertTrue(svg.contains("id=\"mark-s-2021\""), "2021 plotted");
        assertFalse(svg.contains("id=\"mark-s-2020\""),
            "a missing year is a gap, not a zero — the 2020 ACS was never published");
    }

    // ---- axis titles must fit the axis they label ---------------------------------

    /** The exact y-axis title that reached a published board clipped to "…income chang". */
    private static final String LONG_Y = "10-yr real median HH income change (%)";

    private static String svgWithLongAxisTitles(int width, int height) {
        return ChartRenderer.layout("bar", "Party share vs income",
            "Republican share of House+Senate seats (%)", LONG_Y, EIGHT,
            Arrays.asList(series("Change",
                38216, 38059, 38023, 35810, 35736, 35668, 34145, 33305)),
            width, height).toSvg();
    }

    /** The font-size actually emitted on the element carrying the given id. */
    private static int emittedSize(String svg, String id) {
        int at = svg.indexOf("id=\"" + id + "\"");
        assertTrue(at > 0, "no element with id " + id);
        int end = svg.indexOf('>', at);
        String tag = svg.substring(at, end);
        int fs = tag.indexOf("font-size=\"");
        assertTrue(fs > 0, "no font-size on " + id + ": " + tag);
        int from = fs + "font-size=\"".length();
        return Integer.parseInt(tag.substring(from, tag.indexOf('"', from)));
    }

    @Test void aRotatedYTitleShrinksToThePlotHeight() {
        // Rotated, the title is bounded by the plot HEIGHT. On a short panel the nominal 12px
        // does not fit, so the layout must reduce it — asserting on the EMITTED size is what
        // distinguishes a real fix from an overflowing label, since SVG markup holds the whole
        // string either way and only clips when rasterised.
        int shortPanel = emittedSize(svgWithLongAxisTitles(900, 240), "y-axis-title");
        int tallPanel = emittedSize(svgWithLongAxisTitles(900, 620), "y-axis-title");
        assertTrue(shortPanel < tallPanel,
            "a short panel must shrink the rotated title; got " + shortPanel
            + "px vs " + tallPanel + "px");
        assertTrue(shortPanel >= 8, "never shrink below the legibility floor, got " + shortPanel);
    }

    @Test void aRoomyPanelKeepsTheWholeAxisTitleAtFullSize() {
        String svg = svgWithLongAxisTitles(900, 620);
        assertTrue(svg.contains(">" + LONG_Y + "<"),
            "with height to spare the title must not be shortened at all");
        assertEquals(12, emittedSize(svg, "y-axis-title"),
            "and it must stay at the nominal size");
    }

    @Test void theStylesheetDoesNotOverrideAComputedSize() {
        // A CSS rule outranks a presentation attribute, so a blanket `.axis-title { font-size }`
        // reinstates the size the layout just reduced — the SVG then overruns while the PNG,
        // built from the same numbers, fits. Any size rule here would silently undo the fitting.
        String svg = svgWithLongAxisTitles(900, 240);
        String style = svg.substring(svg.indexOf("<style>"), svg.indexOf("</style>"));
        assertFalse(style.contains("font-size"),
            "the default stylesheet must not set font-size; it would override the fitted "
            + "sizes the layout computes. Found: " + style);
    }

    // ---- a legend must never eat the panel it labels --------------------------------

    private static List<ChartRenderer.PointSeriesSpec> onePointEach(int n) {
        List<ChartRenderer.PointSeriesSpec> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            out.add(new ChartRenderer.PointSeriesSpec("State " + i,
                Arrays.asList((double) i), Arrays.asList((double) (i % 7)), null));
        }
        return out;
    }

    @Test void aFiftyOnePointScatterDrawsNoLegend() {
        // The real board: one mark per state. Fifty-one hues are not distinguishable, so the
        // key returned nothing while costing the plot its space.
        String svg = ChartRenderer.layoutPoints("scatter", "R seat share vs income change",
            "R share (%)", "10-yr real change (%)", onePointEach(51), 430, 300).toSvg();
        // Assert on the legend GROUP, not the word: `.legend-label` lives in the stylesheet
        // whether or not a legend is drawn, so a substring test on "legend" always passes.
        assertFalse(svg.contains("id=\"legend\""),
            "an identity scatter must not draw a colour key it cannot make legible");
        assertFalse(svg.contains("legend-swatch-"),
            "and must emit no swatches");
    }

    @Test void identityScatterKeepsEveryMarkAddressable() {
        // Dropping the legend must not drop the names: annotation depends on these ids.
        String svg = ChartRenderer.layoutPoints("scatter", "t", "x", "y", onePointEach(51),
            430, 300).toSvg();
        assertTrue(svg.contains("mark-state-0-0"), "each mark keeps its id, got: " + svg);
        assertTrue(svg.contains("mark-state-50-0"), "including the last one");
    }

    @Test void aFewGroupsStillGetTheirLegend() {
        // The cap must not punish the ordinary case.
        List<ChartRenderer.PointSeriesSpec> few = new ArrayList<>();
        few.add(new ChartRenderer.PointSeriesSpec("Republican",
            Arrays.asList(1.0, 2.0), Arrays.asList(3.0, 4.0), null));
        few.add(new ChartRenderer.PointSeriesSpec("Democratic",
            Arrays.asList(2.0, 3.0), Arrays.asList(1.0, 5.0), null));
        String svg = ChartRenderer.layoutPoints("scatter", "t", "x", "y", few, 430, 300).toSvg();
        assertTrue(svg.contains("legend-label-republican") && svg.contains("legend-label-democratic"),
            "two real series must still be keyed, got: " + svg);
    }

    @Test void anOverlongBarLegendSaysWhatItOmitted() {
        // Category charts share the cap. Truncating in silence would read as a complete key.
        List<ChartRenderer.SeriesSpec> many = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            many.add(series("Series number " + i, 1, 2, 3, 4, 5, 6, 7, 8));
        }
        String svg = ChartRenderer.layout("bar", "t", "x", "y", EIGHT, many, 500, 400).toSvg();
        assertTrue(svg.contains("more<") || svg.contains("more &"),
            "a capped legend must name the count it left out, got: " + svg);
    }

    // ---- a short panel wraps the y title instead of gutting it ----------------------

    @Test void aShortPanelWrapsTheYTitleRatherThanEllipsisingIt() {
        // The live board ellipsised its y title to "% change in profita...", which names
        // nothing. Use a title that cannot fit a short panel on one line at any legible size.
        String label = "% change in profitability, pre- vs post-election";
        String svg = svgAt(label, 430, 240);
        assertTrue(svg.contains("y-axis-title-2"),
            "a second line should carry the remainder, got a single line only");
        // Both halves survive: no word is dropped, which ellipsising would have done.
        assertTrue(svg.contains(">% change in profitability,<")
                || svg.contains(">% change in<") || svg.contains(">% change in profitability<"),
            "first line missing");
        assertTrue(svg.contains("post-election<"), "second line missing");
    }

    @Test void aTallPanelStillUsesOneLine() {
        String svg = svgAt("% change in profitability, pre- vs post-election", 430, 620);
        assertFalse(svg.contains("y-axis-title-2"),
            "with room to spare a second line is wasted margin");
        assertTrue(svg.contains(">% change in profitability, pre- vs post-election<"),
            "and the title stays whole");
    }

    @Test void aSingleWordTitleIsNotWrapped() {
        // Nothing to split on; ellipsis remains the honest fallback.
        String svg = svgAt("Dollarsandmorecharacterstoforceoverflowhere", 430, 240);
        assertFalse(svg.contains("y-axis-title-2"), "a one-word title cannot wrap");
    }

    private static String svgAt(String yLabel, int w, int h) {
        return ChartRenderer.layout("bar", "Profitability change", "Industry", yLabel,
            Arrays.asList("Pharma", "Tech", "Oil", "Ag", "Banking"),
            Arrays.asList(series("delta", -5.2, -3.2, -1.2, 35.0, 10.6)), w, h).toSvg();
    }

    // ---- numeric x-tick labels must not run into each other ------------------------

    private static List<ChartRenderer.PointSeriesSpec> moneyPoints() {
        List<Double> x = new ArrayList<>();
        List<Double> y = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            x.add(35000.0 + i * 1000);
            y.add(500.0 + (i % 9) * 50);
        }
        List<ChartRenderer.PointSeriesSpec> out = new ArrayList<>();
        out.add(new ChartRenderer.PointSeriesSpec("states", x, y, null));
        return out;
    }

    /** Every x tick label drawn, in document order, with its x coordinate. */
    private static List<double[]> xTickPositions(String svg) {
        List<double[]> out = new ArrayList<>();
        // class="tick" sits between id and x, so the two attributes are not adjacent.
        java.util.regex.Matcher m = java.util.regex.Pattern
            .compile("id=\"xtick-(\\d+)\"[^>]*?x=\"([0-9.]+)\"").matcher(svg);
        while (m.find()) {
            out.add(new double[] {Double.parseDouble(m.group(2))});
        }
        return out;
    }

    @Test void wideXTickLabelsAreThinnedRatherThanOverprinted() {
        // A live board rendered "35,00040,00045,00050,000" — the labels are centred on their
        // ticks and never rotated, so on a narrow panel they simply collide.
        String svg = ChartRenderer.layoutPoints("scatter", "Mortality vs income",
            "Median household income ($)", "Rate", moneyPoints(), 430, 300).toSvg();
        List<double[]> ticks = xTickPositions(svg);
        assertTrue(ticks.size() >= 2, "expected some x tick labels, got " + ticks.size());
        // "$100,000"-width labels need roughly 45px; anything closer than that overprints.
        for (int i = 1; i < ticks.size(); i++) {
            double gap = ticks.get(i)[0] - ticks.get(i - 1)[0];
            assertTrue(gap > 40,
                "x tick labels only " + gap + "px apart — they will overprint");
        }
    }

    @Test void thinningIsDrivenByTheSpaceAvailable() {
        // Tick COUNT comes from the data range, not the panel width — niceTicks never sees the
        // width — so a wider panel cannot gain labels. Thinning only ever removes, and must do
        // so only when the labels would actually collide.
        int narrow = xTickPositions(ChartRenderer.layoutPoints("scatter", "t",
            "Median household income ($)", "Rate", moneyPoints(), 300, 300).toSvg()).size();
        int roomy = xTickPositions(ChartRenderer.layoutPoints("scatter", "t",
            "Median household income ($)", "Rate", moneyPoints(), 430, 300).toSvg()).size();
        assertTrue(narrow < roomy,
            "a panel too tight for its labels must drop some; got " + narrow + " vs " + roomy);
        assertTrue(narrow >= 2, "but never below a readable minimum; got " + narrow);
    }
}
