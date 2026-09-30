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

import org.apache.calcite.adapter.askamerica.ChartScene.Anchor;
import org.apache.calcite.adapter.askamerica.ChartScene.Dot;
import org.apache.calcite.adapter.askamerica.ChartScene.Element;
import org.apache.calcite.adapter.askamerica.ChartScene.Group;
import org.apache.calcite.adapter.askamerica.ChartScene.HitTarget;
import org.apache.calcite.adapter.askamerica.ChartScene.Hover;
import org.apache.calcite.adapter.askamerica.ChartScene.Label;
import org.apache.calcite.adapter.askamerica.ChartScene.Line;
import org.apache.calcite.adapter.askamerica.ChartScene.Path;
import org.apache.calcite.adapter.askamerica.ChartScene.Rect;

import java.awt.Color;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Turns chart data into a laid-out {@link ChartScene}.
 *
 * <p>All geometry lives here, computed once and handed to both output backends. Two decisions
 * are deliberate departures from what the previous renderer did, both because a run of the
 * comparative eval showed them costing the reader:
 *
 * <p><b>No category label is ever dropped.</b> The old renderer thinned labels when they
 * collided, so an eight-bar chart printed four names and left the other four bars anonymous —
 * including, in the observed run, the one bar the caller had specifically annotated as not
 * being a state. A chart whose bars cannot be identified has not communicated its data. Labels
 * are rotated, and shortened only as a last resort, but every category keeps one.
 *
 * <p><b>No scientific notation on an axis.</b> Money in the hundreds of thousands rendered as
 * {@code 1E5}, which is unreadable in the one place a chart has to be exact. Ticks are grouped
 * integers up to a million and compact SI beyond it.
 */
final class ChartLayout {

    private ChartLayout() {}

    /** Categorical palette: distinguishable in sequence and at small sizes. */
    private static final Color[] PALETTE = {
        new Color(0x25, 0x63, 0xeb), new Color(0xea, 0x58, 0x0c),
        new Color(0x05, 0x96, 0x69), new Color(0x7c, 0x3a, 0xed),
        new Color(0xdc, 0x26, 0x26), new Color(0x08, 0x91, 0xb2),
        new Color(0xca, 0x8a, 0x04), new Color(0xdb, 0x27, 0x77),
    };

    private static final Color BACKGROUND = Color.WHITE;
    private static final Color INK = new Color(0x1f, 0x23, 0x28);
    private static final Color AXIS = new Color(0x9c, 0xa3, 0xaf);
    private static final Color GRID = new Color(0xe5, 0xe7, 0xeb);

    /**
     * Empty space kept between the title and the plot, and at the very bottom.
     *
     * <p>Reserved rather than reclaimed. The SVG hands a caller ids and classes and invites
     * annotations, and the first agent to accept the invitation put a callout straight through
     * the top gridline label and a footnote off the right edge — not carelessly, but because
     * the scaffold packed every pixel and then asked for more. Giving up ~34px of plot height
     * buys a place to put the sentence that qualifies the chart, which on a chart worth
     * annotating is the better trade.
     */
    private static final int ANNOTATION_BAND = 20;
    private static final int FOOTNOTE_BAND = 16;

    private static final int TITLE_SIZE = 15;
    private static final int TICK_SIZE = 11;
    /** Bar charts with more categories than this default to horizontal bars. */
    private static final int AUTO_HORIZONTAL_CATEGORIES = 8;
    /** Pixels per category row in a horizontal bar chart of single-line labels. */
    private static final int HBAR_ROW = 17;
    private static final int AXIS_TITLE_SIZE = 12;
    private static final int AXIS_TITLE_MIN = 8;
    /** Smallest hover area, in px, so a thin bar or a 3px point is still easy to hit. */
    private static final double MIN_HIT = 16;

    static Color color(int index) {
        return PALETTE[Math.floorMod(index, PALETTE.length)];
    }

    /** Layout of a line or bar chart over a shared category axis. */
    static ChartScene categoryChart(String type, String title, String xLabel, String yLabel,
            List<String> categories, List<ChartRenderer.SeriesSpec> series, int width,
            int height) {
        return categoryChart(type, title, xLabel, yLabel, categories, series, width, height,
            null);
    }

    /**
     * As above, with an optional y-axis domain forced from outside.
     *
     * <p>Exists for dashboards. Two panels drawn side by side with independently fitted axes
     * invite a comparison the picture does not support — the taller bar can be the smaller
     * number — and that is the failure a reader is least likely to catch, because nothing on
     * either panel looks wrong. A composer that means two panels to be compared computes one
     * domain across both and passes it here.
     */
    static ChartScene categoryChart(String type, String title, String xLabel, String yLabel,
            List<String> categories, List<ChartRenderer.SeriesSpec> series, int width,
            int height, double[] forcedDomain) {
        return categoryChart(type, title, xLabel, yLabel, categories, series, width, height,
            forcedDomain, ChartRenderer.BarOptions.DEFAULT);
    }

    /**
     * As above, with a bar chart's orientation, ordering and value labels.
     *
     * <p>{@code sort} orders by the first series; the other series follow their categories.
     * Horizontal bars take {@code x_label} as the category-axis title and {@code y_label} as the
     * value-axis title, so each names what its own axis carries whichever way the bars run.
     */
    static ChartScene categoryChart(String type, String title, String xLabel, String yLabel,
            List<String> categories, List<ChartRenderer.SeriesSpec> series, int width,
            int height, double[] forcedDomain, ChartRenderer.BarOptions opts) {
        ValueFormat fmt = opts.valueFormat;
        boolean bar = "bar".equals(type);
        if (bar && !"none".equals(opts.sort)) {
            List<String> sortedCats = new ArrayList<>();
            List<ChartRenderer.SeriesSpec> sortedSeries = new ArrayList<>();
            sortByFirstSeries(categories, series, "desc".equals(opts.sort), sortedCats,
                sortedSeries);
            categories = sortedCats;
            series = sortedSeries;
        }
        if (bar && isHorizontal(opts, categories, width)) {
            return horizontalBarChart(title, xLabel, yLabel, categories, series, width, height,
                forcedDomain, opts);
        }
        ChartScene scene = new ChartScene(width, height, BACKGROUND);

        double min = 0;
        double max = Double.NEGATIVE_INFINITY;
        for (ChartRenderer.SeriesSpec s : series) {
            for (Double v : s.values) {
                if (v != null) {
                    max = Math.max(max, v);
                    min = Math.min(min, v);
                }
            }
        }
        if (max == Double.NEGATIVE_INFINITY) {
            max = 1;
        }
        // A bar is drawn from the zero line to its value (see the bar loop below): min already
        // forces the domain to include zero so a positive-only series' bars start at the axis
        // floor, but an all-negative series needs the SAME guarantee on the top end, or the
        // rounded domain's max stays negative, zero's pixel-y lands above the plot's top edge,
        // and every bar is drawn straight through the title. Confirmed live: a bar chart of
        // month-over-month declines (every value negative) rendered its bars overlapping the
        // chart title. Lines have no such rectangle to mis-draw, so this is bar-only.
        if ("bar".equals(type) && max < 0) {
            max = 0;
        }
        Ticks ticks = forcedDomain == null
            ? niceTicks(min, max, fmt)
            : niceTicks(Math.min(min, forcedDomain[0]), Math.max(max, forcedDomain[1]), fmt);

        int legendHeight = legendBand(seriesNames(series), width);
        // Rotate rather than drop: measure first, then reserve the depth the choice needs.
        int widest = 0;
        for (String c : categories) {
            widest = Math.max(widest, ChartScene.textWidth(c, TICK_SIZE, false));
        }
        int tickLabelWidth = 0;
        for (String t : ticks.labels) {
            tickLabelWidth = Math.max(tickLabelWidth, ChartScene.textWidth(t, TICK_SIZE, false));
        }
        double right = 22;
        double titleBottom = title == null || title.isEmpty() ? 12 : 36;
        double top = titleBottom + ANNOTATION_BAND;

        // left -> slotWidth -> rotate -> bottom -> plotH, and wrapping the y title needs plotH
        // to decide on a second line: a genuine cycle. Broken with one provisional pass that
        // assumes a single-line title. The second line costs ~14px of left margin, which shifts
        // slotWidth by a fraction of a category and would have to sit exactly on the rotate
        // threshold to change anything; the final pass below uses the real reserve regardless.
        double provisionalLeft = 18 + (yLabel == null || yLabel.isEmpty() ? 0 : 18)
            + tickLabelWidth + 10;
        double provisionalSlot = (width - provisionalLeft - right)
            / Math.max(1, categories.size());
        // Two lines cost 14px more than one; rotation costs up to a third of the chart's height.
        // Wrap first, so a label only tilts when even two lines cannot hold it.
        int wrapWidth = (int) provisionalSlot - 6;
        boolean wraps = widest > provisionalSlot - 6 && wrapWidth > 8
            && wrapsWithoutLoss(categories, wrapWidth, 2);
        boolean rotate = widest > provisionalSlot - 6 && !wraps;
        // A rotated label's full depth is widest*0.72+22, but a fixed 96px cap clipped that
        // depth for genuinely long labels — the reserved band stayed 96px while the label
        // itself still needed more, so its tail rendered past the reserved margin, off the
        // bottom of the canvas or into the legend below it. The cap now scales with the
        // chart's own height instead of a constant, and any label that still overflows it gets
        // shortened (see rotatedLabelMaxWidth below) rather than silently clipped.
        double maxRotatedDepth = Math.max(60, height * 0.32);
        double rotatedDepth = rotate ? Math.min(maxRotatedDepth, widest * 0.72 + 22)
            : wraps ? 44 : 30;
        double bottom = rotatedDepth
            + (xLabel == null || xLabel.isEmpty() ? 8 : 22) + legendHeight + FOOTNOTE_BAND;
        double plotH = height - top - bottom;

        double left = 18 + yTitleReserve(yLabel, plotH) + tickLabelWidth + 10;
        double slotWidth = (width - left - right) / Math.max(1, categories.size());
        double plotW = width - left - right;

        addFrame(scene, title, xLabel, yLabel, left, top, plotW, plotH, width, height, ticks,
            legendHeight);
        scene.bounds(left, top, plotW, plotH, titleBottom + 4, top - 4, height - 4);

        // The text pixel-width a rotated label may actually use before it needs shortening —
        // inverse of the widest*0.72+22 depth formula above, so a label is only ever ellipsised
        // when the height-relative cap genuinely can't fit it, not whenever any label rotates.
        int rotatedLabelMaxWidth = (int) Math.max(20, (maxRotatedDepth - 22) / 0.72);

        // Category ticks. Every category gets a label — never thinned/dropped, only rotated
        // and, as a last resort past the margin's own cap, shortened to fit.
        Group xTicks = new Group().at("x-axis-labels");
        for (int i = 0; i < categories.size(); i++) {
            double cx = left + slotWidth * (i + 0.5);
            String text = categories.get(i);
            if (wraps) {
                List<String> lines = wrapLabel(text, (int) slotWidth - 6, 2);
                for (int li = 0; li < lines.size(); li++) {
                    Label wl = new Label(cx, top + plotH + 18 + li * 14, lines.get(li), INK,
                        TICK_SIZE, Anchor.MIDDLE, 0, false);
                    wl.styled("tick").at(li == 0 ? "xtick-" + ChartScene.slug(text)
                        : "xtick-" + ChartScene.slug(text) + "-" + (li + 1));
                    xTicks.add(wl);
                }
                continue;
            }
            String shown = rotate
                ? fitTo(text, rotatedLabelMaxWidth)
                : fitTo(text, (int) slotWidth - 6);
            Label lab = rotate
                ? new Label(cx, top + plotH + 14, shown, INK, TICK_SIZE, Anchor.END, -45, false)
                : new Label(cx, top + plotH + 18, shown, INK, TICK_SIZE, Anchor.MIDDLE, 0, false);
            lab.styled("tick").at("xtick-" + ChartScene.slug(text));
            xTicks.add(lab);
        }
        scene.add(xTicks);

        for (int si = 0; si < series.size(); si++) {
            ChartRenderer.SeriesSpec s = series.get(si);
            Color c = color(si);
            Group g = new Group().at("series-" + ChartScene.slug(s.name))
                .styled("series");
            if (bar) {
                double groupPad = slotWidth * 0.18;
                double barW = (slotWidth - groupPad * 2) / series.size();
                for (int i = 0; i < categories.size(); i++) {
                    Double v = i < s.values.size() ? s.values.get(i) : null;
                    if (v == null) {
                        continue;
                    }
                    double y = valueToY(v, ticks, top, plotH);
                    double zero = valueToY(0, ticks, top, plotH);
                    double x = left + slotWidth * i + groupPad + barW * si;
                    Element barMark = new Rect(x, Math.min(y, zero), barW - 1,
                        Math.abs(zero - y), c)
                        .at("mark-" + ChartScene.slug(s.name) + "-"
                            + ChartScene.slug(categories.get(i)))
                        .styled("bar");
                    double hitW = Math.max(barW - 1, MIN_HIT);
                    g.add(new Hover(barMark,
                        HitTarget.rect(x + (barW - 1 - hitW) / 2, top, hitW, plotH),
                        tooltip(s.tooltips, i, categories.get(i) + " — " + s.name + ": "
                            + ValueFormat.render(fmt, v))));
                    if (opts.valueLabels) {
                        String vt = opts.format(v);
                        double ly = v >= 0 ? y - 4 : y + 14;
                        g.add(new Label(x + (barW - 1) / 2, ly, vt, INK, TICK_SIZE,
                            Anchor.MIDDLE, 0, true).styled("value-label")
                            .at("value-" + ChartScene.slug(s.name) + "-"
                                + ChartScene.slug(categories.get(i))));
                    }
                }
            } else {
                Path p = new Path(c, null, 2);
                for (int i = 0; i < categories.size(); i++) {
                    Double v = i < s.values.size() ? s.values.get(i) : null;
                    if (v == null) {
                        continue;
                    }
                    p.to(left + slotWidth * (i + 0.5), valueToY(v, ticks, top, plotH));
                }
                g.add(p.at("line-" + ChartScene.slug(s.name)).styled("line"));
                for (int i = 0; i < categories.size(); i++) {
                    Double v = i < s.values.size() ? s.values.get(i) : null;
                    if (v == null) {
                        continue;
                    }
                    double cx = left + slotWidth * (i + 0.5);
                    double cy = valueToY(v, ticks, top, plotH);
                    Element dot = new Dot(cx, cy, 3, c, 1.0)
                        .at("mark-" + ChartScene.slug(s.name) + "-"
                            + ChartScene.slug(categories.get(i)))
                        .styled("point");
                    g.add(new Hover(dot, HitTarget.circle(cx, cy, MIN_HIT / 2),
                        tooltip(s.tooltips, i, categories.get(i) + " — " + s.name + ": "
                            + ValueFormat.render(fmt, v))));
                }
            }
            scene.add(g);
        }

        if (series.size() > 1) {
            addLegend(scene, seriesNames(series), height - 10 - FOOTNOTE_BAND, width);
        }
        return scene;
    }

    /**
     * Horizontal bars: categories down the left, values along the bottom, every bar growing from
     * one shared zero line so a negative value extends left of it.
     *
     * <p>A category label wraps onto a second line before anything is shortened. When the caller
     * allows it ({@link ChartRenderer.BarOptions#growHeight}) the chart grows to give every
     * category its own row rather than squeezing rows below a readable height; a dashboard panel
     * has a fixed cell, so there the rows share the cell and the label falls back to one line
     * when a row is too short for two.
     */
    private static ChartScene horizontalBarChart(String title, String categoryTitle,
            String valueTitle, List<String> categories, List<ChartRenderer.SeriesSpec> series,
            int width, int height, double[] forcedDomain, ChartRenderer.BarOptions opts) {
        int n = categories.size();
        int k = series.size();
        double min = 0;
        double max = 0;
        boolean any = false;
        for (ChartRenderer.SeriesSpec s : series) {
            for (Double v : s.values) {
                if (v != null) {
                    any = true;
                    max = Math.max(max, v);
                    min = Math.min(min, v);
                }
            }
        }
        if (!any) {
            max = 1;
        }
        ValueFormat fmt = opts.valueFormat;
        Ticks ticks = forcedDomain == null
            ? niceTicks(min, max, fmt)
            : niceTicks(Math.min(min, forcedDomain[0]), Math.max(max, forcedDomain[1]), fmt);

        int legendHeight = legendBand(seriesNames(series), width);
        double titleBottom = title == null || title.isEmpty() ? 12 : 36;
        double top = titleBottom + ANNOTATION_BAND;
        double bottom = 30 + (valueTitle == null || valueTitle.isEmpty() ? 8 : 22)
            + legendHeight + FOOTNOTE_BAND;

        // A value label sits just past its bar's end, outside the plot's own rectangle when the
        // bar reaches the domain edge, so the margin on that side has to hold it.
        int posLabelW = 0;
        int negLabelW = 0;
        if (opts.valueLabels) {
            for (ChartRenderer.SeriesSpec s : series) {
                for (Double v : s.values) {
                    if (v != null) {
                        int w = ChartScene.textWidth(opts.format(v), TICK_SIZE, true) + 6;
                        if (v >= 0) {
                            posLabelW = Math.max(posLabelW, w);
                        } else {
                            negLabelW = Math.max(negLabelW, w);
                        }
                    }
                }
            }
        }

        int widest = 0;
        for (String c : categories) {
            widest = Math.max(widest, ChartScene.textWidth(c, TICK_SIZE, false));
        }
        int labelMax = Math.max(20, Math.min(widest, (int) (width * 0.38)));
        double availH = height - top - bottom;
        int maxLines = opts.growHeight || availH / n >= 2 * TICK_SIZE + 8 ? 2 : 1;
        List<List<String>> wrapped = new ArrayList<>();
        int labelW = 0;
        int lineCount = 1;
        for (String c : categories) {
            List<String> lines = wrapLabel(c, labelMax, maxLines);
            wrapped.add(lines);
            lineCount = Math.max(lineCount, lines.size());
            for (String l : lines) {
                labelW = Math.max(labelW, ChartScene.textWidth(l, TICK_SIZE, false));
            }
        }

        if (opts.growHeight) {
            double rowMin = Math.max(lineCount > 1 ? 2 * TICK_SIZE + 8 : HBAR_ROW,
                k == 1 ? HBAR_ROW : k * 10 + 8);
            double needed = rowMin * n;
            if (availH < needed) {
                height += (int) Math.ceil(needed - availH);
            }
        }
        ChartScene scene = new ChartScene(width, height, BACKGROUND);
        double plotH = height - top - bottom;
        double labelRight = (categoryTitle == null || categoryTitle.isEmpty() ? 8 : 26) + labelW;
        double left = labelRight + 8 + negLabelW;
        double right = 22 + posLabelW;
        double plotW = width - left - right;
        double slotH = plotH / n;

        if (title != null && !title.isEmpty()) {
            int titleSize = fittedTitleSize(title, width);
            scene.add(new Label(width / 2.0, 26, fittedTitleText(title, width, titleSize), INK,
                titleSize, Anchor.MIDDLE, 0, true).at("chart-title").styled("title"));
        }
        Group grid = new Group().at("gridlines");
        for (int i = 0; i < ticks.values.size(); i++) {
            double x = valueToX(ticks.values.get(i), ticks, left, plotW);
            grid.add(new Line(x, top, x, top + plotH, GRID, 1, true).styled("grid"));
            grid.add(new Label(x, top + plotH + 16, ticks.labels.get(i), INK, TICK_SIZE,
                Anchor.MIDDLE, 0, false).styled("tick").at("value-tick-" + i));
        }
        scene.add(grid);
        double zeroX = valueToX(0, ticks, left, plotW);
        scene.add(new Group().at("axes")
            .add(new Line(zeroX, top, zeroX, top + plotH, AXIS, 1, false).styled("axis"))
            .add(new Line(left, top + plotH, left + plotW, top + plotH, AXIS, 1, false)
                .styled("axis")));
        if (valueTitle != null && !valueTitle.isEmpty()) {
            int vSize = fittedAxisTitleSize(valueTitle, plotW);
            scene.add(new Label(left + plotW / 2, height - 8 - legendHeight - FOOTNOTE_BAND,
                fittedAxisTitle(valueTitle, plotW, vSize), INK, vSize, Anchor.MIDDLE, 0, false)
                .at("value-axis-title").styled("axis-title"));
        }
        if (categoryTitle != null && !categoryTitle.isEmpty()) {
            int cSize = fittedAxisTitleSize(categoryTitle, plotH);
            scene.add(new Label(14, top + plotH / 2,
                fittedAxisTitle(categoryTitle, plotH, cSize), INK, cSize, Anchor.MIDDLE, -90,
                false).at("category-axis-title").styled("axis-title"));
        }
        scene.bounds(left, top, plotW, plotH, titleBottom + 4, top - 4, height - 4);

        Group cats = new Group().at("category-axis-labels");
        for (int i = 0; i < n; i++) {
            List<String> lines = wrapped.get(i);
            double cy = top + slotH * (i + 0.5);
            double first = cy - (lines.size() - 1) * (TICK_SIZE + 2) / 2.0 + 4;
            for (int li = 0; li < lines.size(); li++) {
                String slug = ChartScene.slug(categories.get(i));
                cats.add(new Label(labelRight, first + li * (TICK_SIZE + 2), lines.get(li), INK,
                    TICK_SIZE, Anchor.END, 0, false).styled("tick")
                    .at(li == 0 ? "xtick-" + slug : "xtick-" + slug + "-" + (li + 1)));
            }
        }
        scene.add(cats);

        double groupPad = slotH * 0.18;
        double barH = (slotH - groupPad * 2) / k;
        for (int si = 0; si < k; si++) {
            ChartRenderer.SeriesSpec s = series.get(si);
            Color c = color(si);
            Group g = new Group().at("series-" + ChartScene.slug(s.name)).styled("series");
            for (int i = 0; i < n; i++) {
                Double v = i < s.values.size() ? s.values.get(i) : null;
                if (v == null) {
                    continue;
                }
                double vx = valueToX(v, ticks, left, plotW);
                double y = top + slotH * i + groupPad + barH * si;
                String slug = ChartScene.slug(s.name) + "-" + ChartScene.slug(categories.get(i));
                Element barMark = new Rect(Math.min(vx, zeroX), y, Math.abs(vx - zeroX),
                    barH - 1, c).at("mark-" + slug).styled("bar");
                double hitH = Math.max(barH - 1, MIN_HIT);
                g.add(new Hover(barMark,
                    HitTarget.rect(left, y + (barH - 1 - hitH) / 2, plotW, hitH),
                    tooltip(s.tooltips, i, categories.get(i) + " — " + s.name + ": "
                        + ValueFormat.render(fmt, v))));
                if (opts.valueLabels) {
                    boolean negative = v < 0;
                    g.add(new Label(negative ? vx - 4 : vx + 4, y + barH / 2 + 4,
                        opts.format(v), INK, TICK_SIZE, negative ? Anchor.END : Anchor.START, 0,
                        true).styled("value-label").at("value-" + slug));
                }
            }
            scene.add(g);
        }

        if (k > 1) {
            addLegend(scene, seriesNames(series), height - 10 - FOOTNOTE_BAND, width);
        }
        return scene;
    }

    private static double valueToX(double v, Ticks t, double left, double plotW) {
        return left + (v - t.min) / (t.max - t.min) * plotW;
    }

    /** Layout of a pie chart: one series, one slice per category. */
    /**
     * Shrinks a panel title until it fits the panel it sits on.
     *
     * <p>Titles are centred on the panel, so one wider than its panel overflows in <em>both</em>
     * directions and is clipped at each end — "10-year nominal dollar rise, top 8 states" arrives
     * as "0-year nominal dollar rise, top 8 states (2014-2024". A centred overflow is worse than a
     * left-aligned one because it damages the start of the string, which is the part a reader uses
     * to tell one panel from another.
     *
     * <p>Verified against {@link ChartScene#textWidth}, the same measurement {@link
     * #fittedTitleText} uses to decide whether to truncate — a character-count guess here that
     * disagreed with that measurement was exactly how a title could pass this method's fitted
     * size straight into truncation instead of ever being drawn at a smaller, genuinely-fitting
     * size (observed 2026-09-04 once {@code textWidth}'s bold safety margin made the two
     * disagree for a title that fits after shrinking).
     */
    private static int fittedTitleSize(String title, double width) {
        if (title == null || title.isEmpty() || width <= 0) {
            return TITLE_SIZE;
        }
        double maxWidth = width - 12;
        int size = TITLE_SIZE;
        while (size > 9 && ChartScene.textWidth(title, size, true) > maxWidth) {
            size--;
        }
        return size;
    }

    /**
     * The title text to actually draw, ellipsised if shrinking to {@link #fittedTitleSize} still
     * does not make it fit.
     *
     * <p>{@code fittedTitleSize} only ever shrinks the font — down to a 9px floor — with no
     * fallback once that floor is reached, which is exactly the failure its own javadoc describes:
     * a title centred on a panel narrower than the text needs clips at BOTH ends ("10-year nominal
     * dollar rise, top 8 states" arrived as "0-year nominal dollar rise, top 8 states (2014-2024").
     * Truncating with a trailing ellipsis at the already-chosen size — the same fallback the board
     * header and axis titles already have — keeps the readable start of the string intact instead.
     */
    private static String fittedTitleText(String title, double width, int size) {
        if (title == null || title.isEmpty() || width <= 0) {
            return title;
        }
        double maxWidth = width - 12;
        if (ChartScene.textWidth(title, size, true) <= maxWidth) {
            return title;
        }
        String t = title;
        while (t.length() > 1 && ChartScene.textWidth(t + "…", size, true) > maxWidth) {
            t = t.substring(0, t.length() - 1);
        }
        return t + "…";
    }

    static ChartScene pieChart(String title, List<String> categories,
            List<Double> values, List<String> tooltips, ValueFormat fmt, int width,
            int height) {
        ChartScene scene = new ChartScene(width, height, BACKGROUND);
        double top = title == null || title.isEmpty() ? 16 : 44;
        if (title != null && !title.isEmpty()) {
            int titleSize = fittedTitleSize(title, width);
            scene.add(new Label(width / 2.0, 26, fittedTitleText(title, width, titleSize), INK,
                titleSize, Anchor.MIDDLE, 0, true).at("chart-title").styled("title"));
        }
        double total = 0;
        for (Double v : values) {
            total += v == null ? 0 : Math.max(0, v);
        }
        if (total <= 0) {
            total = 1;
        }
        scene.bounds(0, top, width, height - top - FOOTNOTE_BAND,
            title == null || title.isEmpty() ? 8 : 32, top - 4, height - 4);
        double cx = width / 2.0;
        double cy = top + (height - top - 20) / 2.0;
        double r = Math.min(width, height - top) * 0.32;

        double angle = -Math.PI / 2;
        Group slices = new Group().at("slices");
        Group labels = new Group().at("slice-labels");
        for (int i = 0; i < categories.size(); i++) {
            double v = i < values.size() && values.get(i) != null
                ? Math.max(0, values.get(i)) : 0;
            double sweep = v / total * Math.PI * 2;
            Path wedge = new Path(null, color(i), 0);
            wedge.to(cx, cy);
            int steps = Math.max(2, (int) (sweep / 0.08));
            for (int k = 0; k <= steps; k++) {
                double a = angle + sweep * k / steps;
                wedge.to(cx + Math.cos(a) * r, cy + Math.sin(a) * r);
            }
            Element slice = wedge.at("mark-" + ChartScene.slug(categories.get(i)))
                .styled("slice");
            slices.add(new Hover(slice, null, tooltip(tooltips, i, categories.get(i) + ": "
                + ValueFormat.render(fmt, v)
                + String.format(Locale.ROOT, " (%.1f%%)", v / total * 100))));

            double mid = angle + sweep / 2;
            double lx = cx + Math.cos(mid) * (r + 14);
            double ly = cy + Math.sin(mid) * (r + 14) + 4;
            String pct = String.format(Locale.ROOT, "%s %.0f%%", categories.get(i),
                v / total * 100);
            labels.add(new Label(lx, ly, pct, INK, TICK_SIZE,
                Math.cos(mid) < -0.1 ? Anchor.END : Math.cos(mid) > 0.1
                    ? Anchor.START : Anchor.MIDDLE, 0, false)
                .at("slice-label-" + ChartScene.slug(categories.get(i)))
                .styled("value-label"));
            angle += sweep;
        }
        scene.add(slices);
        scene.add(labels);
        return scene;
    }

    /** Layout of a scatter or bubble chart over two numeric axes. */
    static ChartScene pointChart(boolean bubble, String title, String xLabel, String yLabel,
            List<ChartRenderer.PointSeriesSpec> series, int width, int height,
            ValueFormat fmt) {
        ChartScene scene = new ChartScene(width, height, BACKGROUND);
        double xmin = Double.POSITIVE_INFINITY;
        double xmax = Double.NEGATIVE_INFINITY;
        double ymin = Double.POSITIVE_INFINITY;
        double ymax = Double.NEGATIVE_INFINITY;
        double smax = 0;
        for (ChartRenderer.PointSeriesSpec s : series) {
            for (Double v : s.x) {
                xmin = Math.min(xmin, v);
                xmax = Math.max(xmax, v);
            }
            for (Double v : s.y) {
                ymin = Math.min(ymin, v);
                ymax = Math.max(ymax, v);
            }
            if (bubble && s.size != null) {
                for (Double v : s.size) {
                    smax = Math.max(smax, Math.abs(v));
                }
            }
        }
        if (xmin > xmax) {
            xmin = 0;
            xmax = 1;
        }
        if (ymin > ymax) {
            ymin = 0;
            ymax = 1;
        }
        Ticks yt = niceTicks(ymin, ymax, fmt);
        Ticks xt = niceTicks(xmin, xmax);

        boolean identity = isIdentityScatter(series);
        // Highlight groups replace the series legend: with one series the groups are the only
        // thing a legend can say, and they need an entry even when there is just one.
        Map<String, List<String>> groups = series.get(0).highlight;
        List<String> legendNames = groups != null ? new ArrayList<>(groups.keySet())
            : identity || series.size() < 2 ? new ArrayList<String>() : pointSeriesNames(series);
        int legendHeight = legendNames.isEmpty() ? 0
            : legendRows(legendNames, width).size() * LEGEND_ROW_H + 4;
        int tickLabelWidth = 0;
        for (String t : yt.labels) {
            tickLabelWidth = Math.max(tickLabelWidth, ChartScene.textWidth(t, TICK_SIZE, false));
        }
        double right = 26;
        double titleBottom = title == null || title.isEmpty() ? 12 : 36;
        double top = titleBottom + ANNOTATION_BAND;
        double bottom = 34 + (xLabel == null || xLabel.isEmpty() ? 8 : 22) + legendHeight
            + FOOTNOTE_BAND;
        double plotH = height - top - bottom;
        // Nothing here depends on the left margin, so the y title can be measured against the
        // real plot height before the margin is set.
        double left = 18 + yTitleReserve(yLabel, plotH) + tickLabelWidth + 10;
        double plotW = width - left - right;

        addFrame(scene, title, xLabel, yLabel, left, top, plotW, plotH, width, height, yt,
            legendHeight);
        scene.bounds(left, top, plotW, plotH, titleBottom + 4, top - 4, height - 4);

        // Numeric x labels are centred on their tick and never rotated, so on a narrow panel
        // wide ones (money, populations) run into each other and print as one unreadable
        // string — a live board rendered "35,00040,00045,00050,000". A category axis avoids
        // this by rotating when its slots get tight; this axis has no such escape, so thin the
        // labels instead. Every Nth tick is kept, chosen as the smallest N that clears the
        // widest label. The axis still communicates its scale: gridlines are horizontal here,
        // so nothing depends on a label being present at every tick.
        int widestXLabel = 0;
        for (String t : xt.labels) {
            widestXLabel = Math.max(widestXLabel, ChartScene.textWidth(t, TICK_SIZE, false));
        }
        int xStep = 1;
        if (xt.values.size() > 1) {
            double slot = plotW / (xt.values.size() - 1);
            while (xStep < xt.values.size() && slot * xStep < widestXLabel + 8) {
                xStep++;
            }
        }
        Group xTicks = new Group().at("x-axis-labels");
        for (int i = 0; i < xt.values.size(); i++) {
            if (i % xStep != 0) {
                continue;
            }
            double v = xt.values.get(i);
            double x = left + (v - xt.min) / (xt.max - xt.min) * plotW;
            xTicks.add(new Label(x, top + plotH + 18, xt.labels.get(i), INK, TICK_SIZE,
                Anchor.MIDDLE, 0, false).styled("tick").at("xtick-" + i));
        }
        scene.add(xTicks);

        List<double[]> labelBoxes = new ArrayList<>();
        List<double[]> placedLabels = new ArrayList<>();
        Group labelGroup = new Group().at("point-labels").styled("labels");
        int pointBase = 0;
        for (int si = 0; si < series.size(); si++) {
            ChartRenderer.PointSeriesSpec s = series.get(si);
            Color c = identity ? color(0) : color(si);
            Group g = new Group().at("series-" + ChartScene.slug(s.name))
                .styled("series");
            int n = s.x.size();
            double[] px = new double[n];
            double[] py = new double[n];
            double[] pr = new double[n];
            for (int i = 0; i < n; i++) {
                px[i] = left + (s.x.get(i) - xt.min) / (xt.max - xt.min) * plotW;
                py[i] = valueToY(s.y.get(i), yt, top, plotH);
                pr[i] = 4;
                if (bubble && s.size != null && smax > 0) {
                    pr[i] = 4 + Math.sqrt(Math.abs(s.size.get(i)) / smax) * 18;
                }
            }
            int[] group = highlightGroups(s);
            boolean[] labelled = labelledPoints(s, group);
            // Plain points first, so emphasised ones sit on top of them.
            for (int pass = 0; pass < 2; pass++) {
                for (int i = 0; i < n; i++) {
                    boolean emphasised = labelled[i] || group[i] >= 0;
                    if (emphasised != (pass == 1)) {
                        continue;
                    }
                    Color fill = c;
                    double alpha = bubble ? 0.55 : 0.85;
                    if (s.highlight != null) {
                        fill = group[i] >= 0 ? color(group[i]) : AXIS;
                        if (group[i] < 0) {
                            alpha = bubble ? 0.35 : 0.6;
                        }
                    }
                    Element dot = new Dot(px[i], py[i], pr[i], fill, alpha)
                        .at("mark-" + ChartScene.slug(s.name) + "-" + i).styled("point");
                    String hover = s.name + ": (" + ValueFormat.plain(s.x.get(i)) + ", "
                        + ValueFormat.render(fmt, s.y.get(i)) + ")"
                        + (bubble && s.size != null
                            ? ", size " + ValueFormat.plain(s.size.get(i)) : "");
                    g.add(new Hover(dot,
                        HitTarget.circle(px[i], py[i], Math.max(pr[i], MIN_HIT / 2)),
                        tooltip(s.tooltips, i, hover)));
                }
            }
            scene.add(g);
            for (int i = 0; i < n; i++) {
                if (labelled[i]) {
                    labelBoxes.add(new double[]{px[i], py[i], pr[i], pointBase + i, i});
                }
            }
            placeLabels(labelGroup, labelBoxes, placedLabels, s, left, top, plotW, plotH);
            labelBoxes.clear();
            pointBase += n;
        }
        scene.add(labelGroup);

        if (!legendNames.isEmpty()) {
            addLegend(scene, legendNames, height - 10 - FOOTNOTE_BAND, width);
        }
        return scene;
    }

    /** Index of the highlight group each point belongs to (first match wins), or -1. */
    private static int[] highlightGroups(ChartRenderer.PointSeriesSpec s) {
        int[] group = new int[s.x.size()];
        java.util.Arrays.fill(group, -1);
        if (s.highlight == null) {
            return group;
        }
        int gi = 0;
        for (List<String> names : s.highlight.values()) {
            for (int i = 0; i < group.length; i++) {
                if (group[i] < 0 && names.contains(s.labels.get(i))) {
                    group[i] = gi;
                }
            }
            gi++;
        }
        return group;
    }

    /** Which points of the series get a text label under its label_mode. */
    private static boolean[] labelledPoints(ChartRenderer.PointSeriesSpec s, int[] group) {
        int n = s.x.size();
        boolean[] out = new boolean[n];
        switch (s.labelMode) {
        case "all":
            java.util.Arrays.fill(out, true);
            break;
        case "highlighted":
            for (int i = 0; i < n; i++) {
                out[i] = group[i] >= 0;
            }
            break;
        case "extremes":
            for (int i : furthestPoints(s)) {
                out[i] = true;
            }
            break;
        default:
            break;
        }
        return out;
    }

    /**
     * The labelCount points furthest from the least-squares line through the series, or from
     * its centre (in axis-normalised units) when there are too few points or no x spread to
     * fit a line.
     */
    private static List<Integer> furthestPoints(ChartRenderer.PointSeriesSpec s) {
        int n = s.x.size();
        double mx = 0;
        double my = 0;
        for (int i = 0; i < n; i++) {
            mx += s.x.get(i) / n;
            my += s.y.get(i) / n;
        }
        double sxx = 0;
        double sxy = 0;
        double sx2 = 0;
        double sy2 = 0;
        for (int i = 0; i < n; i++) {
            double dx = s.x.get(i) - mx;
            double dy = s.y.get(i) - my;
            sxx += dx * dx;
            sxy += dx * dy;
            sy2 += dy * dy;
        }
        final double[] dist = new double[n];
        boolean line = n >= 3 && sxx > 0;
        double slope = line ? sxy / sxx : 0;
        for (int i = 0; i < n; i++) {
            double dx = s.x.get(i) - mx;
            double dy = s.y.get(i) - my;
            if (line) {
                dist[i] = Math.abs(dy - slope * dx);
            } else {
                double nx = sxx > 0 ? dx / Math.sqrt(sxx) : 0;
                double ny = sy2 > 0 ? dy / Math.sqrt(sy2) : 0;
                dist[i] = Math.hypot(nx, ny);
            }
        }
        List<Integer> order = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            order.add(i);
        }
        java.util.Collections.sort(order, new java.util.Comparator<Integer>() {
            @Override public int compare(Integer a, Integer b) {
                return Double.compare(dist[b], dist[a]);
            }
        });
        return order.subList(0, Math.min(s.labelCount, n));
    }

    /**
     * Places one label per entry of {points} (px, py, radius, label index, point index),
     * to the right of its point and stepped down (then up) until it clears every label already
     * placed. The plot edge flips it to the left. {placed} accumulates across series. A label that fits nowhere is an error rather
     * than a silently dropped or overprinted one.
     */
    private static void placeLabels(Group out, List<double[]> points, List<double[]> placed,
            ChartRenderer.PointSeriesSpec s, double left, double top, double plotW,
            double plotH) {
        int step = TICK_SIZE + 1;
        // Left to right, so a nudged label never pushes one already settled.
        java.util.Collections.sort(points, new java.util.Comparator<double[]>() {
            @Override public int compare(double[] a, double[] b) {
                int c = Double.compare(a[0], b[0]);
                return c != 0 ? c : Double.compare(a[1], b[1]);
            }
        });
        for (double[] pt : points) {
            String text = s.labels.get((int) pt[4]);
            int w = ChartScene.textWidth(text, TICK_SIZE, false);
            boolean done = false;
            for (int side = 0; side < 2 && !done; side++) {
                double x0 = side == 0 ? pt[0] + pt[2] + 3 : pt[0] - pt[2] - 3 - w;
                for (int k = 0; k < 40 && !done; k++) {
                    double dy = ((k + 1) / 2) * step * (k % 2 == 1 ? 1 : -1);
                    double base = pt[1] + 4 + dy;
                    double[] box = {x0, base - TICK_SIZE, x0 + w, base + 3};
                    if (box[0] < left || box[2] > left + plotW || box[1] < top
                        || box[3] > top + plotH || overlaps(box, placed)) {
                        continue;
                    }
                    placed.add(box);
                    out.add(new Label(x0, base, text, INK, TICK_SIZE, Anchor.START, 0, false)
                        .styled("point-label").at("label-" + (int) pt[3]));
                    done = true;
                }
            }
            if (!done) {
                throw new IllegalArgumentException("no room to label point '" + text
                    + "' without overlapping another label — label fewer points "
                    + "(label_mode 'highlighted' or 'extremes') or enlarge the chart.");
            }
        }
    }

    private static boolean overlaps(double[] box, List<double[]> placed) {
        for (double[] o : placed) {
            if (box[0] < o[2] && o[0] < box[2] && box[1] < o[3] && o[1] < box[3]) {
                return true;
            }
        }
        return false;
    }

    // ── shared pieces ────────────────────────────────────────────────────────

    private static void addFrame(ChartScene scene, String title, String xLabel, String yLabel,
            double left, double top, double plotW, double plotH, int width, int height,
            Ticks ticks, int legendHeight) {
        if (title != null && !title.isEmpty()) {
            int titleSize = fittedTitleSize(title, width);
            scene.add(new Label(width / 2.0, 26, fittedTitleText(title, width, titleSize), INK,
                titleSize, Anchor.MIDDLE, 0, true).at("chart-title").styled("title"));
        }
        Group grid = new Group().at("gridlines");
        for (int i = 0; i < ticks.values.size(); i++) {
            double y = valueToY(ticks.values.get(i), ticks, top, plotH);
            grid.add(new Line(left, y, left + plotW, y, GRID, 1, true).styled("grid"));
            grid.add(new Label(left - 8, y + 4, ticks.labels.get(i), INK, TICK_SIZE,
                Anchor.END, 0, false).styled("tick").at("ytick-" + i));
        }
        scene.add(grid);
        scene.add(new Group().at("axes")
            .add(new Line(left, top, left, top + plotH, AXIS, 1, false).styled("axis"))
            .add(new Line(left, top + plotH, left + plotW, top + plotH, AXIS, 1, false)
                .styled("axis")));
        if (xLabel != null && !xLabel.isEmpty()) {
            // Above the legend when there is one, or it prints straight through the swatches.
            int xSize = fittedAxisTitleSize(xLabel, plotW);
            scene.add(new Label(left + plotW / 2, height - 8 - legendHeight - FOOTNOTE_BAND,
                fittedAxisTitle(xLabel, plotW, xSize), INK, xSize, Anchor.MIDDLE, 0, false)
                .at("x-axis-title").styled("axis-title"));
        }
        List<String> yLines = yTitleLines(yLabel, plotH);
        if (!yLines.isEmpty()) {
            // Rotated, so its budget is the PLOT HEIGHT — not the chart width. That is the
            // smaller number on a wide panel, which is why the y title is the one that clips.
            String longest = yLines.get(0);
            for (String l : yLines) {
                if (ChartScene.textWidth(l, AXIS_TITLE_SIZE, false)
                    > ChartScene.textWidth(longest, AXIS_TITLE_SIZE, false)) {
                    longest = l;
                }
            }
            int ySize = fittedAxisTitleSize(longest, plotH);
            double lx = 14;
            for (int i = 0; i < yLines.size(); i++) {
                scene.add(new Label(lx, top + plotH / 2,
                    fittedAxisTitle(yLines.get(i), plotH, ySize), INK, ySize, Anchor.MIDDLE, -90,
                    false).at(i == 0 ? "y-axis-title" : "y-axis-title-" + (i + 1))
                    .styled("axis-title"));
                lx += ySize + 2;
            }
        }
    }

    /**
     * Largest size at which an axis title fits the axis it labels, down to
     * {@link #AXIS_TITLE_MIN}. Axis titles were drawn at a fixed size from a fixed origin with
     * the available span in scope and unused, so a long one simply ran off the end — the same
     * defect the panel titles, captions and stat values each had in turn. It shows on the y
     * title first because rotation bounds it by the plot HEIGHT rather than the chart width.
     */
    private static int fittedAxisTitleSize(String text, double span) {
        if (text == null || text.isEmpty() || span <= 0) {
            return AXIS_TITLE_SIZE;
        }
        int size = AXIS_TITLE_SIZE;
        while (size > AXIS_TITLE_MIN && ChartScene.textWidth(text, size, false) > span) {
            size--;
        }
        return size;
    }

    /**
     * The axis title, ellipsised if it still overruns at {@link #AXIS_TITLE_MIN}. Truncating is
     * the last resort and deliberately visible: a title cut with no ellipsis reads as a complete
     * (and wrong) label, which is how "…income chang" reached a published board.
     */
    private static String fittedAxisTitle(String text, double span, int size) {
        if (text == null || text.isEmpty() || span <= 0
            || ChartScene.textWidth(text, size, false) <= span) {
            return text;
        }
        String s = text;
        while (s.length() > 1 && ChartScene.textWidth(s + "…", size, false) > span) {
            s = s.substring(0, s.length() - 1);
        }
        return s + "…";
    }

    private static final int LEGEND_GAP = 16;
    private static final int LEGEND_ROW_H = 20;

    /**
     * Most legend rows a panel will give up. The legend used to wrap without limit and the panel
     * reserved whatever it asked for, so a scatter naming 51 states produced a legend that
     * swallowed the plot — the marks were squeezed into a sliver and the panel title was drawn
     * through. Beyond this the remainder is summarised rather than listed; a key too long to
     * scan is not serving a reader anyway.
     */
    private static final int LEGEND_MAX_ROWS = 3;

    /**
     * Above this many single-point groups, a scatter is plotting identities rather than
     * categories, and a colour key is worse than none: fifty-one hues are not distinguishable
     * from one another, so the legend costs the panel its plot and returns nothing usable. Such
     * a scatter is drawn in one colour with no legend. The names are not lost — every mark keeps
     * its {@code mark-<name>-0} id, which is what the annotation band exists to attach to.
     */
    private static final int SCATTER_IDENTITY_MIN = 12;

    /**
     * Most lines a rotated y-axis title may take. Each costs a column of left margin, so this
     * is a bound on how much of the plot the label may eat in order to stay whole. Past it,
     * ellipsis is the honest answer and the caller should shorten the title.
     */
    private static final int Y_TITLE_MAX_LINES = 3;

    /** Width one legend entry occupies, swatch and trailing gap included. */
    private static int legendItemWidth(String name) {
        return 12 + 4 + ChartScene.textWidth(name, TICK_SIZE, false) + LEGEND_GAP;
    }

    /**
     * Packs legend entries into rows that fit the panel, and returns where each row starts.
     *
     * <p>The legend used to be a single centred row whatever its length. A four-series panel in a
     * narrow column then ran its last entry off the right edge — the swatch drew, the name did
     * not, and the chart showed a coloured square identifying nothing. That is worse than a
     * cramped legend: a key that silently loses an entry makes the reader mis-attribute a line.
     *
     * <p>Rows rather than shrinking, because a legend is already at the smallest size on the
     * board; taking it below the tick size would make it unreadable to save space the panel can
     * simply grow into.
     */
    private static List<List<String>> legendRows(List<String> names, int width) {
        List<List<String>> rows = new ArrayList<List<String>>();
        List<String> row = new ArrayList<String>();
        int used = 0;
        int avail = Math.max(60, width - 16);
        for (int i = 0; i < names.size(); i++) {
            String n = names.get(i);
            int w = legendItemWidth(n);
            if (!row.isEmpty() && used + w - LEGEND_GAP > avail) {
                if (rows.size() + 1 == LEGEND_MAX_ROWS && i < names.size()) {
                    // Last row we are willing to spend: say what is not shown rather than
                    // silently dropping it, so the reader knows the key is partial.
                    rows.add(row);
                    List<String> tail = new ArrayList<String>();
                    tail.add("+" + (names.size() - i) + " more");
                    rows.add(tail);
                    return rows;
                }
                rows.add(row);
                row = new ArrayList<String>();
                used = 0;
            }
            row.add(n);
            used += w;
        }
        if (!row.isEmpty()) {
            rows.add(row);
        }
        return rows;
    }

    /**
     * The rotated y-axis title, wrapped to two lines when one will not fit.
     *
     * <p>Rotation bounds this title by the plot HEIGHT, which on a short panel is small enough
     * that shrinking to the legibility floor still leaves it overrunning — and the fallback then
     * ellipsised it to things like "% change ...", which names nothing. Two lines cost about
     * fourteen pixels of left margin and keep the words. Only splits on a space, and only when
     * the wrap actually fits; otherwise the caller ellipsises as before, which is still better
     * than a silent cut.
     */
    private static List<String> yTitleLines(String yLabel, double plotH) {
        List<String> out = new ArrayList<String>();
        if (yLabel == null || yLabel.isEmpty()) {
            return out;
        }
        if (plotH <= 0 || ChartScene.textWidth(yLabel, AXIS_TITLE_MIN, false) <= plotH) {
            out.add(yLabel);
            return out;
        }
        String[] words = yLabel.split(" ");
        for (int k = 2; k <= Y_TITLE_MAX_LINES && k <= words.length; k++) {
            List<String> attempt = splitInto(words, k);
            int widest = 0;
            for (String line : attempt) {
                widest = Math.max(widest, ChartScene.textWidth(line, AXIS_TITLE_MIN, false));
            }
            if (widest <= plotH) {
                return attempt;
            }
        }
        out.add(yLabel);
        return out;
    }

    /**
     * Split into {@code k} lines, keeping the longest line as short as possible. Greedy on a
     * running width target rather than exhaustive: the candidates here are a handful of words,
     * and an axis title that needs cleverer breaking than this is one the caller should shorten.
     */
    private static List<String> splitInto(String[] words, int k) {
        int total = 0;
        for (String w : words) {
            total += ChartScene.textWidth(w + " ", AXIS_TITLE_MIN, false);
        }
        int target = total / k;
        List<String> lines = new ArrayList<String>();
        StringBuilder cur = new StringBuilder();
        int used = 0;
        for (int i = 0; i < words.length; i++) {
            int w = ChartScene.textWidth(words[i] + " ", AXIS_TITLE_MIN, false);
            boolean lastLine = lines.size() == k - 1;
            if (cur.length() > 0 && !lastLine && used + w / 2 > target) {
                lines.add(cur.toString());
                cur = new StringBuilder();
                used = 0;
            }
            if (cur.length() > 0) {
                cur.append(' ');
            }
            cur.append(words[i]);
            used += w;
        }
        if (cur.length() > 0) {
            lines.add(cur.toString());
        }
        return lines;
    }

    /** Left-margin pixels the y-axis title needs; each extra line costs one more column. */
    private static int yTitleReserve(String yLabel, double plotH) {
        List<String> lines = yTitleLines(yLabel, plotH);
        if (lines.isEmpty()) {
            return 0;
        }
        return 18 + (lines.size() - 1) * (AXIS_TITLE_SIZE + 2);
    }

    /** True when a scatter is plotting one mark per name — identities, not categories. */
    private static boolean isIdentityScatter(List<ChartRenderer.PointSeriesSpec> series) {
        if (series.size() < SCATTER_IDENTITY_MIN) {
            return false;
        }
        for (ChartRenderer.PointSeriesSpec s : series) {
            if (s.x.size() > 1) {
                return false;
            }
        }
        return true;
    }

    /** Vertical band the legend needs: zero for a single series, else one band per packed row. */
    private static int legendBand(List<String> names, int width) {
        if (names.size() <= 1) {
            return 0;
        }
        return legendRows(names, width).size() * LEGEND_ROW_H + 4;
    }

    private static void addLegend(ChartScene scene, List<String> names, double y, int width) {
        List<List<String>> rows = legendRows(names, width);
        Group legend = new Group().at("legend");
        // Rows are laid out bottom-up from the baseline the caller gave us, so the last row keeps
        // the position a single-row legend would have had and earlier rows stack above it.
        double rowY = y - (rows.size() - 1) * LEGEND_ROW_H;
        int idx = 0;
        for (List<String> row : rows) {
            int total = 0;
            for (String n : row) {
                total += legendItemWidth(n);
            }
            double x = Math.max(8, (width - (total - LEGEND_GAP)) / 2.0);
            for (String n : row) {
                legend.add(new Rect(x, rowY - 8, 11, 11, color(idx))
                    .at("legend-swatch-" + ChartScene.slug(n)));
                legend.add(new Label(x + 16, rowY + 1, n, INK, TICK_SIZE, Anchor.START, 0, false)
                    .styled("legend-label").at("legend-label-" + ChartScene.slug(n)));
                x += legendItemWidth(n);
                idx++;
            }
            rowY += LEGEND_ROW_H;
        }
        scene.add(legend);
    }

    private static List<String> seriesNames(List<ChartRenderer.SeriesSpec> series) {
        List<String> out = new ArrayList<>();
        for (ChartRenderer.SeriesSpec s : series) {
            out.add(s.name);
        }
        return out;
    }

    private static List<String> pointSeriesNames(List<ChartRenderer.PointSeriesSpec> series) {
        List<String> out = new ArrayList<>();
        for (ChartRenderer.PointSeriesSpec s : series) {
            out.add(s.name);
        }
        return out;
    }


    /** Horizontal when asked, or under "auto" when there are many categories or wide labels. */
    private static boolean isHorizontal(ChartRenderer.BarOptions opts, List<String> categories,
            int width) {
        if ("horizontal".equals(opts.orientation)) {
            return true;
        }
        if ("vertical".equals(opts.orientation)) {
            return false;
        }
        if (categories.size() > AUTO_HORIZONTAL_CATEGORIES) {
            return true;
        }
        int widest = 0;
        for (String c : categories) {
            widest = Math.max(widest, ChartScene.textWidth(c, TICK_SIZE, false));
        }
        // The slot a vertical layout would give each label, before any y-axis title or tick
        // labels take their share — so this errs toward horizontal only when a label is
        // genuinely wider than its column.
        double slot = (width - 60.0) / Math.max(1, categories.size());
        return widest > slot - 6;
    }

    /** Categories ordered by the first series' value, missing values last. */
    private static void sortByFirstSeries(List<String> categories,
            List<ChartRenderer.SeriesSpec> series, boolean descending, List<String> outCats,
            List<ChartRenderer.SeriesSpec> outSeries) {
        final List<Double> key = series.get(0).values;
        List<Integer> order = new ArrayList<>();
        for (int i = 0; i < categories.size(); i++) {
            order.add(i);
        }
        final int sign = descending ? -1 : 1;
        java.util.Collections.sort(order, (a, b) -> {
            Double va = key.get(a);
            Double vb = key.get(b);
            if (va == null || vb == null) {
                return va == null ? (vb == null ? 0 : 1) : -1;
            }
            return sign * Double.compare(va, vb);
        });
        for (int i : order) {
            outCats.add(categories.get(i));
        }
        for (ChartRenderer.SeriesSpec s : series) {
            List<Double> vals = new ArrayList<>();
            for (int i : order) {
                vals.add(i < s.values.size() ? s.values.get(i) : null);
            }
            outSeries.add(new ChartRenderer.SeriesSpec(s.name, vals));
        }
    }

    /** Whether every label fits within {@code maxLines} lines of {@code maxWidth} unshortened. */
    private static boolean wrapsWithoutLoss(List<String> labels, int maxWidth, int maxLines) {
        for (String l : labels) {
            List<String> lines = wrapLabel(l, maxWidth, maxLines);
            StringBuilder joined = new StringBuilder();
            for (String line : lines) {
                if (line.endsWith("…") || ChartScene.textWidth(line, TICK_SIZE, false) > maxWidth) {
                    return false;
                }
                joined.append(joined.length() == 0 ? "" : " ").append(line);
            }
            if (!joined.toString().equals(l.trim().replaceAll("\\s+", " "))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Breaks a label into at most {@code maxLines} lines that fit {@code maxWidth}, shortening
     * with an ellipsis only what still will not fit. Words are never split across lines.
     */
    static List<String> wrapLabel(String text, int maxWidth, int maxLines) {
        List<String> lines = new ArrayList<>();
        if (ChartScene.textWidth(text, TICK_SIZE, false) <= maxWidth) {
            lines.add(text);
            return lines;
        }
        String cur = "";
        for (String w : text.trim().split("\\s+")) {
            String cand = cur.isEmpty() ? w : cur + " " + w;
            if (cur.isEmpty() || ChartScene.textWidth(cand, TICK_SIZE, false) <= maxWidth) {
                cur = cand;
            } else {
                lines.add(cur);
                cur = w;
            }
        }
        lines.add(cur);
        while (lines.size() > maxLines) {
            String last = lines.remove(lines.size() - 1);
            lines.set(lines.size() - 1, lines.get(lines.size() - 1) + " " + last);
        }
        for (int i = 0; i < lines.size(); i++) {
            lines.set(i, fitTo(lines.get(i), maxWidth));
        }
        return lines;
    }

    private static double valueToY(double v, Ticks t, double top, double plotH) {
        return top + plotH - (v - t.min) / (t.max - t.min) * plotH;
    }

    /** Shortens a label only when rotation is not in play and it still cannot fit. */
    private static String fitTo(String text, int maxWidth) {
        if (maxWidth <= 8 || ChartScene.textWidth(text, TICK_SIZE, false) <= maxWidth) {
            return text;
        }
        String s = text;
        while (s.length() > 1
            && ChartScene.textWidth(s + "…", TICK_SIZE, false) > maxWidth) {
            s = s.substring(0, s.length() - 1);
        }
        return s + "…";
    }

    /** An axis range rounded to human numbers, with its rendered tick labels. */
    static final class Ticks {
        final double min;
        final double max;
        final List<Double> values = new ArrayList<>();
        final List<String> labels = new ArrayList<>();

        Ticks(double min, double max) {
            this.min = min;
            this.max = max;
        }
    }

    /** The caller's tooltip for mark {@code i}, or the generated default when it gave none. */
    private static String tooltip(List<String> supplied, int i, String generated) {
        String t = supplied == null ? null : supplied.get(i);
        return t == null ? generated : t;
    }

    /** Axis bounds and ticks on 1/2/5×10ⁿ steps, the spacing people read without thinking. */
    static Ticks niceTicks(double min, double max) {
        return niceTicks(min, max, null);
    }

    /** As above, labelling each tick with {@code fmt} when the caller supplied one. */
    static Ticks niceTicks(double min, double max, ValueFormat fmt) {
        if (min == max) {
            max = min + (min == 0 ? 1 : Math.abs(min) * 0.1);
        }
        // Step from the raw range. Rounding the range up first and then dividing compounds
        // two roundings: a 105,382 maximum became a 200,000 span, a 50,000 step and a 150,000
        // axis, leaving a third of the plot empty and the data squashed into the bottom.
        double step = niceNum((max - min) / 5, true);
        double lo = Math.floor(min / step) * step;
        double hi = Math.ceil(max / step) * step;
        Ticks t = new Ticks(lo, hi);
        for (double v = lo; v <= hi + step * 0.5; v += step) {
            double rounded = Math.abs(v) < step * 1e-9 ? 0 : v;
            t.values.add(rounded);
            t.labels.add(fmt == null ? formatTick(rounded, step) : fmt.format(rounded));
        }
        return t;
    }

    private static double niceNum(double range, boolean round) {
        double exp = Math.floor(Math.log10(range));
        double f = range / Math.pow(10, exp);
        double nf;
        if (round) {
            nf = f < 1.5 ? 1 : f < 3 ? 2 : f < 7 ? 5 : 10;
        } else {
            nf = f <= 1 ? 1 : f <= 2 ? 2 : f <= 5 ? 5 : 10;
        }
        return nf * Math.pow(10, exp);
    }

    /**
     * A tick as a reader would write it: grouped digits, then compact SI past a million.
     *
     * <p>Never scientific notation. Household income near $100,000 rendered as {@code 1E5} on
     * the previous renderer, which is precisely the wrong place to make a reader decode an
     * exponent.
     */
    static String formatTick(double v, double step) {
        double abs = Math.abs(v);
        if (abs >= 1e9) {
            return trim(v / 1e9) + "B";
        }
        if (abs >= 1e6) {
            return trim(v / 1e6) + "M";
        }
        if (step >= 1 && v == Math.rint(v)) {
            return String.format(Locale.ROOT, "%,d", (long) v);
        }
        int decimals = step >= 1 ? 0 : Math.min(4, (int) Math.ceil(-Math.log10(step)));
        return String.format(Locale.ROOT, "%,." + decimals + "f", v);
    }

    private static String trim(double v) {
        String s = String.format(Locale.ROOT, "%.1f", v);
        return s.endsWith(".0") ? s.substring(0, s.length() - 2) : s;
    }
}
