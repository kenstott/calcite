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

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.geom.AffineTransform;
import java.awt.geom.Ellipse2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import javax.imageio.ImageIO;

/**
 * A laid-out chart, held as drawing primitives and written out to either SVG or PNG.
 *
 * <p>Two backends over one scene, rather than two renderers, because the whole point of
 * returning both is that the caller can trust the markup it edits to be the picture the reader
 * saw. Rendering the same data twice through separate libraries makes that a hope; rendering
 * one scene twice makes it arithmetic — every coordinate is computed once, in
 * {@link ChartRenderer}, and each backend only transcribes it.
 *
 * <p>The SVG side carries structure the PNG cannot: a stable id on every mark, a class on every
 * label, and a stylesheet at the top. That is what makes the returned SVG editable in the way
 * the tool's contract promises — restyling a series or greying one bar is a targeted change to
 * a named element, not a rewrite of a wall of anonymous paths.
 */
final class ChartScene {

    /** Text alignment relative to the anchor point. */
    enum Anchor { START, MIDDLE, END }

    /** Base class for anything drawable. {@code id} and {@code cls} reach the SVG only. */
    abstract static class Element {
        String id;
        String cls;

        Element at(String elementId) {
            this.id = elementId;
            return this;
        }

        Element styled(String className) {
            this.cls = className;
            return this;
        }

        abstract void writeSvg(StringBuilder sb, String indent);

        abstract void drawPng(Graphics2D g);
    }

    /** A group of elements, emitted as {@code <g>} in SVG and flattened in the raster. */
    static final class Group extends Element {
        final List<Element> children = new ArrayList<>();

        Group add(Element e) {
            children.add(e);
            return this;
        }

        @Override Group at(String elementId) {
            super.at(elementId);
            return this;
        }

        @Override Group styled(String className) {
            super.styled(className);
            return this;
        }

        @Override void writeSvg(StringBuilder sb, String indent) {
            sb.append(indent).append("<g").append(attr("id", id)).append(attr("class", cls))
                .append(">\n");
            for (Element c : children) {
                c.writeSvg(sb, indent + "  ");
            }
            sb.append(indent).append("</g>\n");
        }

        @Override void drawPng(Graphics2D g) {
            for (Element c : children) {
                c.drawPng(g);
            }
        }
    }

    /** An axis-aligned rectangle — a bar, the plot background, a legend swatch. */
    static final class Rect extends Element {
        final double x;
        final double y;
        final double w;
        final double h;
        final Color fill;

        Rect(double x, double y, double w, double h, Color fill) {
            this.x = x;
            this.y = y;
            this.w = w;
            this.h = h;
            this.fill = fill;
        }

        @Override void writeSvg(StringBuilder sb, String indent) {
            sb.append(indent).append("<rect").append(attr("id", id)).append(attr("class", cls))
                .append(" x=\"").append(num(x)).append("\" y=\"").append(num(y))
                .append("\" width=\"").append(num(w)).append("\" height=\"").append(num(h))
                .append("\" fill=\"").append(hex(fill)).append("\"/>\n");
        }

        @Override void drawPng(Graphics2D g) {
            g.setColor(fill);
            g.fillRect((int) Math.round(x), (int) Math.round(y),
                (int) Math.round(w), (int) Math.round(h));
        }
    }

    /** A straight line — an axis, a gridline, a leader for a callout. */
    static final class Line extends Element {
        final double x1;
        final double y1;
        final double x2;
        final double y2;
        final Color stroke;
        final double width;
        final boolean dashed;

        Line(double x1, double y1, double x2, double y2, Color stroke, double width,
                boolean dashed) {
            this.x1 = x1;
            this.y1 = y1;
            this.x2 = x2;
            this.y2 = y2;
            this.stroke = stroke;
            this.width = width;
            this.dashed = dashed;
        }

        @Override void writeSvg(StringBuilder sb, String indent) {
            sb.append(indent).append("<line").append(attr("id", id)).append(attr("class", cls))
                .append(" x1=\"").append(num(x1)).append("\" y1=\"").append(num(y1))
                .append("\" x2=\"").append(num(x2)).append("\" y2=\"").append(num(y2))
                .append("\" stroke=\"").append(hex(stroke))
                .append("\" stroke-width=\"").append(num(width)).append("\"");
            if (dashed) {
                sb.append(" stroke-dasharray=\"3 3\"");
            }
            sb.append("/>\n");
        }

        @Override void drawPng(Graphics2D g) {
            g.setColor(stroke);
            g.setStroke(dashed
                ? new BasicStroke((float) width, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER,
                    10f, new float[]{3f, 3f}, 0f)
                : new BasicStroke((float) width));
            g.drawLine((int) Math.round(x1), (int) Math.round(y1),
                (int) Math.round(x2), (int) Math.round(y2));
            g.setStroke(new BasicStroke(1f));
        }
    }

    /** A connected run of points — a line series, or a filled pie slice / area. */
    static final class Path extends Element {
        final List<double[]> points = new ArrayList<>();
        final Color stroke;
        final Color fill;
        final double width;

        Path(Color stroke, Color fill, double width) {
            this.stroke = stroke;
            this.fill = fill;
            this.width = width;
        }

        Path to(double x, double y) {
            points.add(new double[]{x, y});
            return this;
        }

        @Override void writeSvg(StringBuilder sb, String indent) {
            StringBuilder d = new StringBuilder();
            for (int i = 0; i < points.size(); i++) {
                d.append(i == 0 ? "M " : "L ").append(num(points.get(i)[0])).append(' ')
                    .append(num(points.get(i)[1])).append(' ');
            }
            if (fill != null) {
                d.append("Z");
            }
            sb.append(indent).append("<path").append(attr("id", id)).append(attr("class", cls))
                .append(" d=\"").append(d.toString().trim())
                .append("\" fill=\"").append(fill == null ? "none" : hex(fill))
                .append("\" stroke=\"").append(stroke == null ? "none" : hex(stroke))
                .append("\" stroke-width=\"").append(num(width))
                .append("\" stroke-linejoin=\"round\" stroke-linecap=\"round\"/>\n");
        }

        @Override void drawPng(Graphics2D g) {
            java.awt.geom.Path2D.Double p = new java.awt.geom.Path2D.Double();
            for (int i = 0; i < points.size(); i++) {
                double[] pt = points.get(i);
                if (i == 0) {
                    p.moveTo(pt[0], pt[1]);
                } else {
                    p.lineTo(pt[0], pt[1]);
                }
            }
            if (fill != null) {
                p.closePath();
                g.setColor(fill);
                g.fill(p);
            }
            if (stroke != null) {
                g.setColor(stroke);
                g.setStroke(new BasicStroke((float) width, BasicStroke.CAP_ROUND,
                    BasicStroke.JOIN_ROUND));
                g.draw(p);
                g.setStroke(new BasicStroke(1f));
            }
        }
    }

    /** A filled circle — a scatter point, a bubble, a line-series marker. */
    static final class Dot extends Element {
        final double cx;
        final double cy;
        final double r;
        final Color fill;
        final double opacity;

        Dot(double cx, double cy, double r, Color fill, double opacity) {
            this.cx = cx;
            this.cy = cy;
            this.r = r;
            this.fill = fill;
            this.opacity = opacity;
        }

        @Override void writeSvg(StringBuilder sb, String indent) {
            sb.append(indent).append("<circle").append(attr("id", id)).append(attr("class", cls))
                .append(" cx=\"").append(num(cx)).append("\" cy=\"").append(num(cy))
                .append("\" r=\"").append(num(r))
                .append("\" fill=\"").append(hex(fill)).append("\"");
            if (opacity < 1.0) {
                sb.append(" fill-opacity=\"").append(num(opacity)).append("\"");
            }
            sb.append("/>\n");
        }

        @Override void drawPng(Graphics2D g) {
            g.setColor(opacity < 1.0
                ? new Color(fill.getRed(), fill.getGreen(), fill.getBlue(),
                    (int) Math.round(opacity * 255))
                : fill);
            g.fill(new Ellipse2D.Double(cx - r, cy - r, r * 2, r * 2));
        }
    }

    /** A run of text, optionally rotated about its anchor. */
    static final class Label extends Element {
        final double x;
        final double y;
        final String text;
        final Color fill;
        final int size;
        final Anchor anchor;
        final double rotate;
        final boolean bold;

        Label(double x, double y, String text, Color fill, int size, Anchor anchor,
                double rotate, boolean bold) {
            this.x = x;
            this.y = y;
            this.text = text;
            this.fill = fill;
            this.size = size;
            this.anchor = anchor;
            this.rotate = rotate;
            this.bold = bold;
        }

        @Override void writeSvg(StringBuilder sb, String indent) {
            sb.append(indent).append("<text").append(attr("id", id)).append(attr("class", cls))
                .append(" x=\"").append(num(x)).append("\" y=\"").append(num(y))
                .append("\" fill=\"").append(hex(fill))
                .append("\" font-size=\"").append(size).append("\"");
            if (bold) {
                sb.append(" font-weight=\"600\"");
            }
            if (anchor != Anchor.START) {
                sb.append(" text-anchor=\"")
                    .append(anchor == Anchor.MIDDLE ? "middle" : "end").append("\"");
            }
            if (rotate != 0) {
                sb.append(" transform=\"rotate(").append(num(rotate)).append(' ')
                    .append(num(x)).append(' ').append(num(y)).append(")\"");
            }
            sb.append('>').append(escape(text)).append("</text>\n");
        }

        @Override void drawPng(Graphics2D g) {
            g.setFont(new Font(Font.SANS_SERIF, bold ? Font.BOLD : Font.PLAIN, size));
            g.setColor(fill);
            FontMetrics fm = g.getFontMetrics();
            int w = fm.stringWidth(text);
            double dx = anchor == Anchor.MIDDLE ? -w / 2.0 : anchor == Anchor.END ? -w : 0;
            if (rotate == 0) {
                g.drawString(text, (float) (x + dx), (float) y);
                return;
            }
            AffineTransform saved = g.getTransform();
            g.rotate(Math.toRadians(rotate), x, y);
            g.drawString(text, (float) (x + dx), (float) y);
            g.setTransform(saved);
        }
    }

    // ── the scene ────────────────────────────────────────────────────────────

    final int width;
    final int height;
    final Color background;
    private final List<Element> elements = new ArrayList<>();

    /**
     * Where the data marks live, and the bands deliberately left empty around them.
     *
     * <p>Recorded so {@link #toSvg()} can hand the caller real coordinates instead of an
     * invitation to guess. A caller told only that annotations are welcome puts one wherever it
     * seems natural and lands on a tick label — which is exactly what happened the first time
     * an agent used this scaffold: its callout printed over the top gridline label and its
     * footnote ran off the right edge.
     */
    private double plotX;
    private double plotY;
    private double plotW;
    private double plotH;
    private double annotationBandTop;
    private double annotationBandBottom;
    private double footnoteY;

    ChartScene(int width, int height, Color background) {
        this.width = width;
        this.height = height;
        this.background = background;
    }

    /** Records the plot rectangle and the free bands reserved above it and below it. */
    ChartScene bounds(double x, double y, double w, double h,
            double bandTop, double bandBottom, double footnote) {
        this.plotX = x;
        this.plotY = y;
        this.plotW = w;
        this.plotH = h;
        this.annotationBandTop = bandTop;
        this.annotationBandBottom = bandBottom;
        this.footnoteY = footnote;
        return this;
    }

    ChartScene add(Element e) {
        elements.add(e);
        return this;
    }

    /**
     * The scene as SVG, opening with the contract that says what may be edited.
     *
     * <p>The header is not decoration. A caller handed anonymous markup either leaves it alone
     * or rewrites it wholesale; naming the safe edits, and naming the one unsafe one, is what
     * turns the returned scaffold into something a caller will actually adjust rather than
     * replace. The geometry warning matters most — plotted coordinates are derived from the
     * data the caller passed, so moving them makes the chart disagree with its own source.
     */
    String toSvg() {
        StringBuilder sb = new StringBuilder(4096);
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        sb.append("<!--\n")
            .append("  Chart scaffold from AskAmerica render_chart. Self-contained: no external\n")
            .append("  assets, no scripts. Safe and expected to edit:\n")
            .append("    * add annotations   — <text class=\"callout\"> and <line> leaders\n")
            .append("    * restyle a series  — the <g id=\"series-*\"> groups\n")
            .append("    * de-emphasise one  — change fill on an id=\"mark-*\" element\n")
            .append("    * add value labels  — <text class=\"value-label\"> at a mark's x/y\n")
            .append("    * retitle / relabel — <text class=\"title|axis-title|tick\">\n")
            .append("\n")
            .append("  WHERE THERE IS ROOM. These bands are reserved and contain nothing, so\n")
            .append("  anything you put in them cannot collide with a label or a gridline:\n")
            .append(band("    * annotation band  ", annotationBandTop, annotationBandBottom))
            .append(band("    * footnote line    ", footnoteY - 10, footnoteY))
            .append("      (both span the full width, x from 8 to ").append(num(width - 8))
            .append(")\n")
            .append("    * append to <g id=\"annotations\"> at the end — it paints last, so\n")
            .append("      nothing can cover what you add there\n")
            .append("  The plot rectangle itself is x ").append(num(plotX)).append("..")
            .append(num(plotX + plotW)).append(", y ").append(num(plotY)).append("..")
            .append(num(plotY + plotH)).append(". Annotating INSIDE it is fine and often\n")
            .append("  right — a leader line to one mark, say — but that is where the data and\n")
            .append("  the gridlines are, so place deliberately rather than by eye.\n")
            .append("\n")
            .append("  Do NOT move plotted geometry: every x/y below is computed from the data\n")
            .append("  you passed, so shifting a mark makes the picture disagree with the\n")
            .append("  numbers it came from. Change what a mark says, not where it sits.\n")
            .append("-->\n");
        sb.append("<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 ")
            .append(width).append(' ').append(height)
            .append("\" width=\"").append(width).append("\" height=\"").append(height)
            .append("\" font-family=\"system-ui, -apple-system, Segoe UI, Helvetica, Arial, "
                + "sans-serif\">\n");
        sb.append("  <style>\n")
            .append("    .title { font-size: 15px; font-weight: 600 }\n")
            .append("    .axis-title { font-size: 12px }\n")
            .append("    .tick { font-size: 11px }\n")
            .append("    .value-label { font-size: 11px; font-weight: 600 }\n")
            .append("    .callout { font-size: 11px; font-style: italic }\n")
            .append("    @media (prefers-color-scheme: dark) {\n")
            .append("      .chart-bg { fill: #16181d }\n")
            .append("      .title, .axis-title, .tick, .value-label, .callout, .legend-label "
                + "{ fill: #e6e8eb }\n")
            .append("      .axis { stroke: #6b7280 }\n")
            .append("      .grid { stroke: #2c3038 }\n")
            .append("    }\n")
            .append("  </style>\n");
        sb.append("  <rect class=\"chart-bg\" x=\"0\" y=\"0\" width=\"").append(width)
            .append("\" height=\"").append(height).append("\" fill=\"")
            .append(hex(background)).append("\"/>\n");
        for (Element e : elements) {
            e.writeSvg(sb, "  ");
        }
        // Last, so anything appended here paints over the chart rather than under it.
        sb.append("  <g id=\"annotations\"><!-- add callouts and leaders here --></g>\n");
        sb.append("</svg>\n");
        return sb.toString();
    }

    /** The same scene rasterised, for hosts that display images but not SVG. */
    byte[] toPng() throws IOException {
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = image.createGraphics();
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING,
            RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_STROKE_CONTROL, RenderingHints.VALUE_STROKE_PURE);
        g.setColor(background);
        g.fillRect(0, 0, width, height);
        for (Element e : elements) {
            e.drawPng(g);
        }
        g.dispose();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ImageIO.write(image, "png", out);
        return out.toByteArray();
    }

    // ── shared helpers ───────────────────────────────────────────────────────

    /** Width of a string at a given size, used by layout to decide if labels fit. */
    static int textWidth(String text, int size, boolean bold) {
        BufferedImage probe = new BufferedImage(1, 1, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = probe.createGraphics();
        g.setFont(new Font(Font.SANS_SERIF, bold ? Font.BOLD : Font.PLAIN, size));
        int w = g.getFontMetrics().stringWidth(text);
        g.dispose();
        return w;
    }

    /** One reserved-band line for the header, or nothing when the band was never set. */
    private static String band(String label, double top, double bottom) {
        if (bottom - top < 4) {
            return "";
        }
        return label + "y " + num(top) + ".." + num(bottom) + "\n";
    }

    private static String attr(String name, String value) {
        return value == null ? "" : " " + name + "=\"" + escape(value) + "\"";
    }

    static String hex(Color c) {
        return String.format(Locale.ROOT, "#%02x%02x%02x", c.getRed(), c.getGreen(), c.getBlue());
    }

    /** Trims trailing zeros so coordinates stay readable in the markup a caller edits. */
    static String num(double v) {
        if (v == Math.rint(v) && Math.abs(v) < 1e9) {
            return Long.toString((long) v);
        }
        return String.format(Locale.ROOT, "%.2f", v);
    }

    static String escape(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace("\"", "&quot;");
    }

    /** A stable, readable id fragment for a category or series name. */
    static String slug(String name) {
        String s = name == null ? "" : name.toLowerCase(Locale.ROOT)
            .replaceAll("[^a-z0-9]+", "-").replaceAll("(^-|-$)", "");
        return s.isEmpty() ? "unnamed" : s;
    }
}
