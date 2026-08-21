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

import java.util.List;

/**
 * Wraps an answer — narrative, dashboard and sources — into one self-contained HTML page.
 *
 * <p>A chart alone is not the deliverable. The answer to one of these questions is a finding,
 * the figures behind it, the caveats that qualify it, and where every number came from; a bare
 * dashboard makes the reader hold the prose in their head and take the sourcing on trust. This
 * page carries all of it, served from the same loopback server, at the same twenty-token cost as
 * a link to the chart on its own.
 *
 * <p>Section bodies are HTML written by the caller, which is deliberate: asking a model to emit
 * HTML directly is cheaper and more faithful than shipping a Markdown renderer and hoping the
 * dialects agree. The safety of that choice rests on how the page is served rather than on
 * sanitising the input — {@link ArtifactServer} sends
 * {@code default-src 'none'; style-src 'unsafe-inline'; img-src data:}, so a stray
 * {@code <script>} cannot execute and nothing on the page can reach the network. The page never
 * leaves the machine that generated it.
 *
 * <p>The dashboard is inlined as SVG rather than linked, so the page is one file a reader can
 * save, mail, or print and still see the chart.
 */
final class ReportPage {

    private ReportPage() {}

    /** One narrative section: a heading and a body the caller wrote as HTML. */
    static final class Section {
        final String heading;
        final String html;

        Section(String heading, String html) {
            this.heading = heading;
            this.html = html;
        }
    }

    /** One citation: what it is, where it is, and optionally why it is being cited. */
    static final class Source {
        final String label;
        final String url;
        final String note;

        Source(String label, String url, String note) {
            this.label = label;
            this.url = url;
            this.note = note;
        }
    }

    /**
     * The AskAmerica mark, from {@code web/icon.svg}, inlined at {@code size} pixels.
     *
     * <p>Drawn rather than approximated. The first version of this page used a CSS gradient
     * square standing in for the logo, which reads as a coloured tile and drops the question
     * mark the mark is built around — a placeholder nobody would notice was wrong until they
     * looked at it next to the real thing. Rendered at 20px rather than the 16px a footer would
     * normally use, because below that the hook closes up and stops reading as a "?".
     */
    private static String markSvg(int size) {
        return "<svg class=\"mark\" width=\"" + size + "\" height=\"" + size + "\" "
            + "viewBox=\"0 0 100 100\" role=\"img\" aria-label=\"AskAmerica\">"
            + "<defs><clipPath id=\"rp-tile\">"
            + "<rect width=\"100\" height=\"100\" rx=\"22\"/></clipPath></defs>"
            + "<g clip-path=\"url(#rp-tile)\">"
            + "<polygon fill=\"#1a3a8a\" points=\"0,0 100,0 0,100\"/>"
            + "<polygon fill=\"#d5322f\" points=\"100,0 100,100 0,100\"/></g>"
            + "<path d=\"M33 34 A17 17 0 1 1 61 46 C55 52 50 55 50 63\" fill=\"none\" "
            + "stroke=\"#fff\" stroke-width=\"12\" stroke-linecap=\"round\"/>"
            + "<polygon fill=\"#fff\" points=\"50,68 52.82,76.12 61.4,76.29 54.57,81.48 "
            + "57.05,89.71 50,84.8 42.95,89.71 45.43,81.48 38.6,76.29 47.18,76.12\"/></svg>";
    }

    static String render(String title, String subtitle, List<Section> sections,
            String dashboardSvg, String svgDownloadUrl, List<Source> sources, String footnote,
            String byline) {
        StringBuilder sb = new StringBuilder(32768);
        sb.append("<!doctype html>\n<html lang=\"en\">\n<head>\n")
            .append("<meta charset=\"utf-8\">\n")
            .append("<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n")
            .append("<title>").append(esc(title == null ? "AskAmerica report" : title))
            .append("</title>\n<style>\n")
            .append(css())
            .append("</style>\n</head>\n<body>\n<main>\n");

        sb.append("<header>\n");
        if (title != null && !title.isEmpty()) {
            sb.append("<h1>").append(esc(title)).append("</h1>\n");
        }
        if (subtitle != null && !subtitle.isEmpty()) {
            sb.append("<p class=\"subtitle\">").append(esc(subtitle)).append("</p>\n");
        }
        sb.append("</header>\n");

        // The dashboard sits directly under the header: a reader who wants only the answer gets
        // it without scrolling, and the prose then explains what they have already seen.
        if (dashboardSvg != null && !dashboardSvg.isEmpty()) {
            sb.append("<figure class=\"board\">\n")
                .append(stripXmlDecl(dashboardSvg));
            if (svgDownloadUrl != null && !svgDownloadUrl.isEmpty()) {
                // Same origin, so the download attribute is honoured and the reader gets a
                // file rather than a tab full of markup.
                sb.append("<figcaption><a class=\"dl\" href=\"").append(esc(svgDownloadUrl))
                    .append("\" download=\"askamerica-dashboard.svg\" ")
                    .append("title=\"Download this chart as SVG\">")
                    .append("<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" ")
                    .append("aria-hidden=\"true\"><path d=\"M8 1v8M4.5 6.5 8 10l3.5-3.5\" ")
                    .append("fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.6\" ")
                    .append("stroke-linecap=\"round\" stroke-linejoin=\"round\"/>")
                    .append("<path d=\"M2.5 12v1.5h11V12\" fill=\"none\" ")
                    .append("stroke=\"currentColor\" stroke-width=\"1.6\" ")
                    .append("stroke-linecap=\"round\"/></svg> SVG</a></figcaption>\n");
            }
            sb.append("</figure>\n");
        }

        if (sections != null) {
            for (Section s : sections) {
                sb.append("<section>\n");
                if (s.heading != null && !s.heading.isEmpty()) {
                    sb.append("<h2>").append(esc(s.heading)).append("</h2>\n");
                }
                if (s.html != null) {
                    sb.append(s.html).append('\n');
                }
                sb.append("</section>\n");
            }
        }

        if (sources != null && !sources.isEmpty()) {
            sb.append("<section class=\"sources\">\n<h2>Sources</h2>\n<ol>\n");
            for (Source src : sources) {
                sb.append("<li>");
                if (src.url != null && !src.url.isEmpty()) {
                    sb.append("<a href=\"").append(esc(src.url)).append("\">")
                        .append(esc(src.label == null || src.label.isEmpty()
                            ? src.url : src.label)).append("</a>");
                } else {
                    sb.append(esc(src.label == null ? "" : src.label));
                }
                if (src.note != null && !src.note.isEmpty()) {
                    sb.append(" <span class=\"note\">— ").append(esc(src.note)).append("</span>");
                }
                sb.append("</li>\n");
            }
            sb.append("</ol>\n</section>\n");
        }

        sb.append("<footer>\n");
        if (footnote != null && !footnote.isEmpty()) {
            sb.append("<p class=\"footnote\">").append(esc(footnote)).append("</p>\n");
        }
        sb.append("<p class=\"brand\">")
            .append(markSvg(20))
            .append("<strong>AskAmerica</strong> · <a href=\"https://askamerica.ai\">")
            .append("askamerica.ai</a>");
        if (byline != null && !byline.isEmpty()) {
            sb.append("<span class=\"byline\">").append(esc(byline)).append("</span>");
        }
        sb.append("</p>\n</footer>\n</main>\n")
            // A print hint, not a print button. window.print() needs script and the
            // CSP forbids it (javascript: URLs included), so a clickable button here
            // would look like a control and do nothing. The shortcut is the honest
            // affordance: faint until hovered, and absent from the printed page.
            .append("<aside class=\"printhint\" aria-hidden=\"true\">"
                + "Print \u00b7 <kbd>Ctrl</kbd>/<kbd>\u2318</kbd>+<kbd>P</kbd></aside>\n")
            .append("</body>\n</html>\n");
        return sb.toString();
    }

    /**
     * An inlined {@code <svg>} may not carry its own XML declaration.
     *
     * <p>A declaration is only legal at the very start of a document, and the dashboard's own
     * serialiser emits one because it is normally a standalone file. Left in place mid-page it
     * renders as stray text above the chart.
     */
    private static String stripXmlDecl(String svg) {
        String t = svg.trim();
        if (t.startsWith("<?xml")) {
            int close = t.indexOf("?>");
            if (close > 0) {
                t = t.substring(close + 2).trim();
            }
        }
        return t;
    }

    private static String css() {
        return ""
            + ":root{--ink:#1f2328;--muted:#6b7280;--rule:#e5e7eb;--bg:#fff;--link:#1a3a8a}\n"
            + "@media(prefers-color-scheme:dark){:root{--ink:#e6e8eb;--muted:#9aa4b2;"
            + "--rule:#2c3038;--bg:#16181d;--link:#8ab4f8}}\n"
            + "*{box-sizing:border-box}\n"
            + "body{margin:0;background:var(--bg);color:var(--ink);"
            + "font:16px/1.65 system-ui,-apple-system,'Segoe UI',Helvetica,Arial,sans-serif}\n"
            + "main{max-width:60rem;margin:0 auto;padding:2.5rem 1.5rem 4rem}\n"
            + "h1{font-size:1.9rem;line-height:1.25;margin:0 0 .4rem;letter-spacing:-.01em}\n"
            + "h2{font-size:1.15rem;margin:2.2rem 0 .6rem;letter-spacing:-.005em}\n"
            + "h3{font-size:1rem;margin:1.4rem 0 .4rem}\n"
            + ".subtitle{color:var(--muted);margin:0 0 1.6rem;font-size:.95rem}\n"
            + ".board{margin:0 0 2rem;padding:0;border:1px solid var(--rule);border-radius:10px;"
            + "overflow:hidden;background:var(--bg)}\n"
            + ".board svg{display:block;width:100%;height:auto}\n"
            + "p{margin:0 0 .9rem}\n"
            + "table{border-collapse:collapse;width:100%;margin:.8rem 0 1.2rem;font-size:.92rem}\n"
            + "th,td{text-align:left;padding:.45rem .6rem;border-bottom:1px solid var(--rule)}\n"
            + "th{font-weight:600;color:var(--muted);font-size:.82rem;text-transform:uppercase;"
            + "letter-spacing:.04em}\n"
            // Screen-only interaction. Everything in this block is scoped away from print
            // deliberately: the page's strongest property is that it prints well, and a
            // sticky header or a scroll container is meaningless on paper at best and
            // clips content at worst. The print rules below restore each one.
            + "@media screen{\n"
            // Sticky headers: these reports are mostly 51-row state tables, and a header
            // scrolling out of view is the commonest way one becomes unreadable. Needs an
            // opaque background (it scrolls over cells) and a box-shadow rather than a
            // border, because border-collapse:collapse will not paint a border on a stuck
            // element.
            + "th{position:sticky;top:0;z-index:1;background:var(--bg);"
            + "box-shadow:inset 0 -1px 0 var(--rule)}\n"
            + "tbody tr:hover{background:color-mix(in srgb,var(--rule) 30%,transparent)}\n"
            // A long query scrolls sideways in its own box rather than widening the page.
            + "pre{overflow-x:auto}\n"
            + "}\n"
            // Progressive disclosure. Section bodies routinely carry an audit trail — the
            // exact SQL behind each figure — which has to be present but should not stand
            // between the reader and the finding. <details> is the only interactive control
            // available: the CSP forbids script, so a disclosure that works without it is
            // the whole toolkit.
            + "details{border:1px solid var(--rule);border-radius:8px;margin:.9rem 0}\n"
            + "summary{cursor:pointer;padding:.5rem .7rem;font-size:.82rem;font-weight:600;"
            + "color:var(--muted);text-transform:uppercase;letter-spacing:.04em}\n"
            + "summary:hover{color:var(--link)}\n"
            + "summary:focus-visible{outline:2px solid var(--link);outline-offset:-2px}\n"
            + "details[open] summary{border-bottom:1px solid var(--rule)}\n"
            + "details > *:not(summary){margin:.7rem}\n"
            + "pre{padding:.7rem;border-radius:6px;"
            + "font:.82rem/1.5 ui-monospace,SFMono-Regular,Menlo,monospace;"
            + "background:color-mix(in srgb,var(--rule) 45%,transparent)}\n"
            + "pre code{background:none;padding:0;font-size:inherit}\n"
            + "abbr[title]{text-decoration:underline dotted;text-underline-offset:2px;"
            + "cursor:help}\n"
            + ".printhint{position:fixed;right:1rem;bottom:1rem;padding:.35rem .6rem;"
            + "border:1px solid var(--rule);border-radius:999px;background:var(--bg);"
            + "font-size:.72rem;color:var(--muted);letter-spacing:.03em;opacity:.35;"
            + "transition:opacity .15s}\n"
            + ".printhint:hover{opacity:1}\n"
            + ".printhint kbd{font:inherit;font-weight:600;color:var(--ink)}\n"
            + "@media(prefers-reduced-motion:reduce){.printhint{transition:none}}\n"
            // Print. The page printing cleanly is a property worth protecting, so each
            // screen affordance is undone rather than left to degrade on its own.
            + "@media print{\n"
            // A collapsed <details> cannot be opened on paper, and the audit trail is the
            // part most worth having there. Show the content and drop the chrome that only
            // made sense as a control.
            + "details{border:0;margin:.6rem 0}\n"
            + "details > *:not(summary){display:block;margin:.4rem 0}\n"
            + "details[open] summary{border-bottom:0}\n"
            + "summary{padding:0;list-style:none}\n"
            + "summary::-webkit-details-marker{display:none}\n"
            // overflow-x:auto CLIPS on paper — there is no scrollbar to reach the rest of
            // the line. Wrap instead, and keep a border since browsers drop backgrounds.
            + "pre{overflow:visible;white-space:pre-wrap;word-break:break-word;"
            + "border:1px solid var(--rule)}\n"
            // The paper equivalent of a sticky header: repeat it on every page a long
            // table spills across, and keep a row from splitting across the break.
            + "thead{display:table-header-group}\n"
            + "tr{break-inside:avoid}\n"
            + ".printhint{display:none}\n"
            + "}\n"
            + "code{font:.88em ui-monospace,SFMono-Regular,Menlo,monospace;"
            + "background:color-mix(in srgb,var(--rule) 55%,transparent);"
            + "padding:.1em .35em;border-radius:4px}\n"
            + "blockquote{margin:1rem 0;padding:.2rem 0 .2rem 1rem;"
            + "border-left:3px solid var(--rule);color:var(--muted)}\n"
            + "a{color:var(--link)}\n"
            + ".sources ol{padding-left:1.2rem;font-size:.92rem}\n"
            + ".sources li{margin:.3rem 0}\n"
            + ".note{color:var(--muted)}\n"
            + "footer{margin-top:3rem;padding-top:1rem;border-top:1px solid var(--rule);"
            + "font-size:.85rem;color:var(--muted)}\n"
            + ".footnote{font-style:italic}\n"
            + ".brand{display:flex;align-items:center;gap:.45rem;margin:.8rem 0 0}\n"
            + ".brand strong{color:var(--ink)}\n"
            + ".byline{margin-left:auto}\n"
            + ".mark{flex:none;display:block}\n"
            + "figcaption{display:flex;justify-content:flex-end;padding:.4rem .6rem;"
            + "border-top:1px solid var(--rule)}\n"
            + ".dl{display:inline-flex;align-items:center;gap:.3rem;font-size:.78rem;"
            + "text-decoration:none;color:var(--muted);letter-spacing:.03em}\n"
            + ".dl:hover{color:var(--link)}\n";
    }

    static String esc(String s) {
        return s == null ? "" : s.replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace("\"", "&quot;");
    }
}
