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

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.imageio.ImageIO;

/**
 * Composes several charts and stat tiles into one dashboard, emitted as a single SVG.
 *
 * <p>Panels are placed, not redrawn. Each chart is laid out by {@link ChartLayout} at its own
 * natural size and then nested as an {@code <svg>} with its own {@code viewBox}, so every
 * coordinate inside a panel is the one that was computed from — and can still be checked
 * against — that panel's data. Composition never touches plotted geometry.
 *
 * <p>The one thing this does that hand-assembly reliably gets wrong is <b>shared scales</b>.
 * Two bar charts side by side with independently fitted axes invite a comparison the picture
 * does not support: the taller bar can be the smaller number, and nothing on either panel looks
 * wrong, so a reader has no cue to distrust it. Panels naming the same {@code scale_group} are
 * given one domain spanning all of them.
 */
final class DashboardLayout {

    private DashboardLayout() {}

    private static final Color BACKGROUND = Color.WHITE;
    private static final Color INK = new Color(0x1f, 0x23, 0x28);
    private static final Color MUTED = new Color(0x6b, 0x72, 0x80);
    private static final Color PANEL_EDGE = new Color(0xe5, 0xe7, 0xeb);
    private static final Color UP = new Color(0x05, 0x96, 0x69);
    private static final Color DOWN = new Color(0xdc, 0x26, 0x26);

    private static final int GUTTER = 16;
    private static final int PAD = 20;
    private static final int CAPTION_H = 18;
    /** A stat tile holds a label, a number and a delta — three lines, not a chart. */
    private static final int STAT_ROW_H = 108;

    /** One cell of the dashboard: either a laid-out chart or a stat tile. */
    static final class Panel {
        String kind = "chart";
        int span = 1;
        String caption;
        String scaleGroup;
        // chart
        ChartScene scene;
        String chartType;
        String title;
        String xLabel;
        String yLabel;
        List<String> categories;
        List<ChartRenderer.SeriesSpec> series;
        List<ChartRenderer.PointSeriesSpec> points;
        // stat
        String label;
        String value;
        String delta;
        String deltaDirection;
    }

    /** The composed dashboard, held as its panels plus the chrome around them. */
    static final class Dashboard {
        final int width;
        final int height;
        private final String title;
        private final String subtitle;
        private final String footnote;
        private final List<Panel> panels;
        private final List<double[]> rects = new ArrayList<>();

        Dashboard(int width, int height, String title, String subtitle, String footnote,
                List<Panel> panels) {
            this.width = width;
            this.height = height;
            this.title = title;
            this.subtitle = subtitle;
            this.footnote = footnote;
            this.panels = panels;
        }

        void place(double x, double y, double w, double h) {
            rects.add(new double[]{x, y, w, h});
        }

        /**
         * The dashboard as one self-contained SVG.
         *
         * <p>Written to drop straight into an artifact or an HTML page: one stylesheet at the
         * top rather than one per panel, ids namespaced per panel so nothing collides, and a
         * {@code prefers-color-scheme} block so it survives a dark page.
         */
        String toSvg() {
            StringBuilder sb = new StringBuilder(16384);
            sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
            sb.append("<!--\n")
                .append("  Dashboard from AskAmerica compose_dashboard. Self-contained: no\n")
                .append("  external assets, no scripts. Drop it into an artifact or an HTML\n")
                .append("  page as-is.\n\n")
                .append("  Each panel is a nested <svg> with its own viewBox, so a panel's\n")
                .append("  internal coordinates are exactly what its own data produced. Panel\n")
                .append("  ids are namespaced p1-, p2-, ... so the per-chart editing contract\n")
                .append("  still works: p2-mark-california, p3-series-nominal, and so on.\n\n")
                .append("  Safe to edit: panel captions (<text class=\"caption\">), the\n")
                .append("  dashboard title/subtitle/footnote, any panel's fills, and the\n")
                .append("  per-panel <g id=\"pN-annotations\"> groups, which paint last.\n")
                .append("  Do NOT move a panel's internal geometry, and do NOT change one\n")
                .append("  panel's axis domain: panels sharing a scale_group were given one\n")
                .append("  domain deliberately so they can be compared.\n")
                .append("-->\n");
            sb.append("<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 ")
                .append(width).append(' ').append(height)
                .append("\" width=\"").append(width).append("\" height=\"").append(height)
                .append("\" font-family=\"system-ui, -apple-system, Segoe UI, Helvetica, "
                    + "Arial, sans-serif\">\n");
            sb.append("  <style>\n")
                .append("    .dash-title { font-size: 20px; font-weight: 650 }\n")
                .append("    .dash-subtitle { font-size: 13px }\n")
                .append("    .caption { font-size: 11px }\n")
                .append("    .footnote { font-size: 11px; font-style: italic }\n")
                .append("    .stat-label { font-size: 12px }\n")
                .append("    .stat-value { font-size: 30px; font-weight: 650 }\n")
                .append("    .stat-delta { font-size: 13px; font-weight: 600 }\n")
                .append("    .title { font-size: 15px; font-weight: 600 }\n")
                .append("    .axis-title { font-size: 12px }\n")
                .append("    .tick { font-size: 11px }\n")
                .append("    .value-label { font-size: 11px; font-weight: 600 }\n")
                .append("    .callout { font-size: 11px; font-style: italic }\n")
                .append("    @media (prefers-color-scheme: dark) {\n")
                .append("      .dash-bg, .chart-bg { fill: #16181d }\n")
                .append("      .panel-edge { stroke: #2c3038 }\n")
                .append("      .dash-title, .dash-subtitle, .caption, .footnote, .stat-label,\n")
                .append("      .stat-value, .title, .axis-title, .tick, .value-label,\n")
                .append("      .callout, .legend-label { fill: #e6e8eb }\n")
                .append("      .axis { stroke: #6b7280 }\n")
                .append("      .grid { stroke: #2c3038 }\n")
                .append("    }\n")
                .append("  </style>\n");
            sb.append("  <rect class=\"dash-bg\" x=\"0\" y=\"0\" width=\"").append(width)
                .append("\" height=\"").append(height).append("\" fill=\"")
                .append(ChartScene.hex(BACKGROUND)).append("\"/>\n");

            double y = PAD + 8;
            if (title != null && !title.isEmpty()) {
                sb.append(text("dash-title", PAD, y + 14, title, INK, "start"));
                y += 26;
            }
            if (subtitle != null && !subtitle.isEmpty()) {
                sb.append(text("dash-subtitle", PAD, y + 12, subtitle, MUTED, "start"));
            }

            for (int i = 0; i < panels.size(); i++) {
                Panel p = panels.get(i);
                double[] r = rects.get(i);
                String prefix = "p" + (i + 1) + "-";
                double panelH = r[3] - (p.caption == null ? 0 : CAPTION_H);
                if ("stat".equals(p.kind)) {
                    sb.append(statSvg(p, r[0], r[1], r[2], panelH, prefix));
                } else {
                    sb.append("  <rect class=\"panel-edge\" x=\"").append(ChartScene.num(r[0]))
                        .append("\" y=\"").append(ChartScene.num(r[1]))
                        .append("\" width=\"").append(ChartScene.num(r[2]))
                        .append("\" height=\"").append(ChartScene.num(panelH))
                        .append("\" fill=\"none\" stroke=\"")
                        .append(ChartScene.hex(PANEL_EDGE)).append("\" rx=\"6\"/>\n");
                    p.scene.writeSvgNested(sb, r[0], r[1], r[2], panelH, prefix);
                }
                if (p.caption != null) {
                    sb.append(text("caption", r[0] + 4, r[1] + panelH + 13, p.caption, MUTED,
                        "start"));
                }
            }

            if (footnote != null && !footnote.isEmpty()) {
                sb.append(text("footnote", PAD, height - 10, footnote, MUTED, "start"));
            }
            sb.append("  <g id=\"annotations\"><!-- dashboard-level callouts here --></g>\n");
            sb.append("</svg>\n");
            return sb.toString();
        }

        /** The same dashboard rasterised, for hosts that display images but not SVG. */
        byte[] toPng() throws IOException {
            BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
            Graphics2D g = img.createGraphics();
            g.setRenderingHint(RenderingHints.KEY_ANTIALIASING,
                RenderingHints.VALUE_ANTIALIAS_ON);
            g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING,
                RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
            g.setColor(BACKGROUND);
            g.fillRect(0, 0, width, height);

            double y = PAD + 8;
            if (title != null && !title.isEmpty()) {
                draw(g, title, PAD, y + 14, 20, true, INK);
                y += 26;
            }
            if (subtitle != null && !subtitle.isEmpty()) {
                draw(g, subtitle, PAD, y + 12, 13, false, MUTED);
            }
            for (int i = 0; i < panels.size(); i++) {
                Panel p = panels.get(i);
                double[] r = rects.get(i);
                double panelH = r[3] - (p.caption == null ? 0 : CAPTION_H);
                if ("stat".equals(p.kind)) {
                    statPng(g, p, r[0], r[1], r[2], panelH);
                } else {
                    g.setColor(PANEL_EDGE);
                    g.drawRoundRect((int) r[0], (int) r[1], (int) r[2], (int) panelH, 6, 6);
                    p.scene.drawInto(g, r[0], r[1], r[2], panelH);
                }
                if (p.caption != null) {
                    draw(g, p.caption, r[0] + 4, r[1] + panelH + 13, 11, false, MUTED);
                }
            }
            if (footnote != null && !footnote.isEmpty()) {
                draw(g, footnote, PAD, height - 10, 11, false, MUTED);
            }
            g.dispose();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ImageIO.write(img, "png", out);
            return out.toByteArray();
        }

        private String statSvg(Panel p, double x, double y, double w, double h, String prefix) {
            StringBuilder sb = new StringBuilder();
            sb.append("  <g id=\"").append(prefix).append("stat\">\n");
            sb.append("  <rect class=\"panel-edge\" x=\"").append(ChartScene.num(x))
                .append("\" y=\"").append(ChartScene.num(y))
                .append("\" width=\"").append(ChartScene.num(w))
                .append("\" height=\"").append(ChartScene.num(h))
                .append("\" fill=\"none\" stroke=\"").append(ChartScene.hex(PANEL_EDGE))
                .append("\" rx=\"6\"/>\n");
            double cy = y + h / 2;
            if (p.label != null) {
                sb.append(text("stat-label", x + 16, cy - 18, p.label, MUTED, "start"));
            }
            if (p.value != null) {
                sb.append(text("stat-value", x + 16, cy + 12, p.value, INK, "start"));
            }
            if (p.delta != null) {
                Color c = "down".equals(p.deltaDirection) ? DOWN
                    : "up".equals(p.deltaDirection) ? UP : MUTED;
                sb.append(text("stat-delta", x + 16, cy + 32, p.delta, c, "start"));
            }
            sb.append("  </g>\n");
            return sb.toString();
        }

        private void statPng(Graphics2D g, Panel p, double x, double y, double w, double h) {
            g.setColor(PANEL_EDGE);
            g.drawRoundRect((int) x, (int) y, (int) w, (int) h, 6, 6);
            double cy = y + h / 2;
            if (p.label != null) {
                draw(g, p.label, x + 16, cy - 18, 12, false, MUTED);
            }
            if (p.value != null) {
                draw(g, p.value, x + 16, cy + 12, 30, true, INK);
            }
            if (p.delta != null) {
                Color c = "down".equals(p.deltaDirection) ? DOWN
                    : "up".equals(p.deltaDirection) ? UP : MUTED;
                draw(g, p.delta, x + 16, cy + 32, 13, true, c);
            }
        }

        private static void draw(Graphics2D g, String s, double x, double y, int size,
                boolean bold, Color c) {
            g.setFont(new java.awt.Font(java.awt.Font.SANS_SERIF,
                bold ? java.awt.Font.BOLD : java.awt.Font.PLAIN, size));
            g.setColor(c);
            g.drawString(s, (float) x, (float) y);
        }

        private static String text(String cls, double x, double y, String s, Color fill,
                String anchor) {
            return "  <text class=\"" + cls + "\" x=" + '"' + ChartScene.num(x) + '"'
                + " y=\"" + ChartScene.num(y) + "\" fill=\"" + ChartScene.hex(fill) + "\""
                + ("start".equals(anchor) ? "" : " text-anchor=\"" + anchor + "\"")
                + ">" + ChartScene.escape(s) + "</text>\n";
        }
    }

    /**
     * Lays panels onto a grid and returns the composed dashboard.
     *
     * <p>Shared scales are resolved first, because a panel's layout depends on the domain it is
     * given: every panel naming a group contributes its extremes, and the union is handed back
     * to each of them.
     */
    static Dashboard compose(String title, String subtitle, String footnote, List<Panel> panels,
            int columns, int width, int height) {
        if (panels.isEmpty()) {
            throw new IllegalArgumentException("panels must not be empty");
        }
        int cols = Math.max(1, Math.min(columns, 4));

        Map<String, double[]> domains = new LinkedHashMap<>();
        for (Panel p : panels) {
            if (p.scaleGroup == null || !"chart".equals(p.kind) || p.series == null) {
                continue;
            }
            double[] d = domains.get(p.scaleGroup);
            if (d == null) {
                d = new double[]{Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
                domains.put(p.scaleGroup, d);
            }
            for (ChartRenderer.SeriesSpec s : p.series) {
                for (Double v : s.values) {
                    if (v != null) {
                        d[0] = Math.min(d[0], v);
                        d[1] = Math.max(d[1], v);
                    }
                }
            }
        }

        double headerH = PAD + 8
            + (title == null || title.isEmpty() ? 0 : 26)
            + (subtitle == null || subtitle.isEmpty() ? 0 : 18) + 10;
        double footH = (footnote == null || footnote.isEmpty() ? 0 : 22) + PAD;

        // Assign to rows first, because a row's height depends on what is in it.
        List<List<Panel>> rowList = new ArrayList<>();
        List<Panel> row = new ArrayList<>();
        int used = 0;
        for (Panel p : panels) {
            int span = Math.max(1, Math.min(p.span, cols));
            if (used + span > cols && !row.isEmpty()) {
                rowList.add(row);
                row = new ArrayList<>();
                used = 0;
            }
            row.add(p);
            used += span;
        }
        if (!row.isEmpty()) {
            rowList.add(row);
        }

        // A row of stat tiles needs a headline number and two small lines, not a chart's worth
        // of vertical space. Giving every row the same height made three tiles occupy as much
        // of the board as the chart that explained them, which inverts what the reader should
        // look at first.
        double cellW = (width - PAD * 2 - GUTTER * (cols - 1)) / (double) cols;
        double[] rowH = new double[rowList.size()];
        double fixed = 0;
        int flexible = 0;
        for (int i = 0; i < rowList.size(); i++) {
            boolean allStats = true;
            for (Panel p : rowList.get(i)) {
                allStats &= "stat".equals(p.kind);
            }
            if (allStats) {
                rowH[i] = STAT_ROW_H + (hasCaption(rowList.get(i)) ? CAPTION_H : 0);
                fixed += rowH[i];
            } else {
                flexible++;
            }
        }
        double slack = height - headerH - footH - GUTTER * (rowList.size() - 1) - fixed;
        double chartH = flexible == 0 ? 0 : Math.max(180, slack / flexible);
        for (int i = 0; i < rowH.length; i++) {
            if (rowH[i] == 0) {
                rowH[i] = chartH;
            }
        }

        // Panels are laid out at their placed pixel size rather than a fixed nominal size, so
        // type in a small cell is sized for that cell instead of being scaled down with it.
        for (int ri = 0; ri < rowList.size(); ri++) {
            for (Panel p : rowList.get(ri)) {
            if (!"chart".equals(p.kind)) {
                continue;
            }
            int span = Math.max(1, Math.min(p.span, cols));
            int w = (int) Math.round(cellW * span + GUTTER * (span - 1));
            int h = (int) Math.round(rowH[ri] - (p.caption == null ? 0 : CAPTION_H));
            double[] forced = p.scaleGroup == null ? null : domains.get(p.scaleGroup);
            if (forced != null && forced[0] > forced[1]) {
                forced = null;
            }
            if (p.points != null && !p.points.isEmpty()) {
                p.scene = ChartRenderer.layoutPoints(p.chartType, p.title, p.xLabel, p.yLabel,
                    p.points, w, h);
            } else if ("pie".equals(p.chartType)) {
                p.scene = ChartLayout.pieChart(p.title, p.categories, p.series.get(0).values,
                    w, h);
            } else {
                p.scene = ChartLayout.categoryChart(
                    p.chartType == null ? "line" : p.chartType, p.title, p.xLabel, p.yLabel,
                    p.categories, p.series, w, h, forced);
            }
            }
        }

        Dashboard dash = new Dashboard(width, height, title, subtitle, footnote, panels);
        double y = headerH;
        for (int ri = 0; ri < rowList.size(); ri++) {
            double x = PAD;
            for (Panel p : rowList.get(ri)) {
                int span = Math.max(1, Math.min(p.span, cols));
                double w = cellW * span + GUTTER * (span - 1);
                dash.place(x, y, w, rowH[ri]);
                x += w + GUTTER;
            }
            y += rowH[ri] + GUTTER;
        }
        return dash;
    }

    private static boolean hasCaption(List<Panel> row) {
        for (Panel p : row) {
            if (p.caption != null) {
                return true;
            }
        }
        return false;
    }

    /** Default canvas for a panel count, when the caller does not pick one. */
    static int[] defaultSize(List<Panel> panels, int columns) {
        int cols = Math.max(1, Math.min(columns, 4));
        int statUnits = 0;
        int chartUnits = 0;
        for (Panel p : panels) {
            int span = Math.max(1, Math.min(p.span, cols));
            if ("stat".equals(p.kind)) {
                statUnits += span;
            } else {
                chartUnits += span;
            }
        }
        int statRows = (int) Math.ceil(statUnits / (double) cols);
        int chartRows = (int) Math.ceil(chartUnits / (double) cols);
        return new int[]{Math.min(2000, 420 * cols + 40),
            Math.min(2000, 130 + statRows * (STAT_ROW_H + GUTTER) + chartRows * 320)};
    }

    static String fmt(double v) {
        return String.format(Locale.ROOT, "%,.0f", v);
    }
}
