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

import java.util.ArrayList;
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
    /**
     * One reader-operable row filter: a labelled checkbox that hides the rows carrying
     * {@code className}.
     *
     * <p>The class is applied by the caller inside its own section HTML — the engine only emits
     * the control and the rule. That keeps the author deciding which rows are a coherent group,
     * which is a judgement about the data rather than about the page.
     */
    static final class Filter {
        final String label;
        final String className;
        final String note;

        Filter(String label, String className, String note) {
            this.label = label;
            this.className = className;
            this.note = note;
        }
    }

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
        final String sql;

        Source(String label, String url, String note) {
            this(label, url, note, null);
        }

        Source(String label, String url, String note, String sql) {
            this.label = label;
            this.url = url;
            this.note = note;
            this.sql = sql;
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
            String byline, List<Filter> filters) {
        // Sections are rewritten FIRST: sorting a table produces the CSS that drives it, and the
        // <style> block is emitted before the body. Doing it in place would mean writing the
        // rules after the stylesheet had already closed.
        StringBuilder sortCss = new StringBuilder();
        List<Section> prepared = new ArrayList<>();
        if (sections != null) {
            int n = 0;
            for (Section sec : sections) {
                prepared.add(new Section(sec.heading,
                    withSortControls(sec.html, "s" + (++n) + "-", sortCss)));
            }
        }
        sections = prepared;

        StringBuilder sb = new StringBuilder(32768);
        sb.append("<!doctype html>\n<html lang=\"en\">\n<head>\n")
            .append("<meta charset=\"utf-8\">\n")
            .append("<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n")
            .append("<title>").append(esc(title == null ? "AskAmerica report" : title))
            .append("</title>\n<style>\n")
            .append(css())
            .append(filterCss(filters))
            .append(sortCss.length() == 0 ? "" : "@media screen{\n" + sortCss + "}\n")
            .append("</style>\n</head>\n<body>\n");
        // The checkboxes sit BEFORE <main> so the sibling combinator in filterCss can reach the
        // rows. They are visually hidden, not display:none — a display:none input cannot be
        // focused, and the label would stop being keyboard-operable.
        if (filters != null) {
            for (Filter f : filters) {
                sb.append("<input type=\"checkbox\" class=\"fltbox\" id=\"flt-")
                    .append(esc(f.className)).append("\">\n");
            }
        }
        sb.append("<main>\n");
        if (filters != null && !filters.isEmpty()) {
            sb.append("<div class=\"filterbar\"><span class=\"fltlead\">Show</span>");
            for (Filter f : filters) {
                sb.append("<label class=\"fltlabel\" for=\"flt-").append(esc(f.className))
                    .append("\">").append(esc(f.label));
                if (f.note != null && !f.note.isEmpty()) {
                    sb.append("<span class=\"fltnote\">").append(esc(f.note)).append("</span>");
                }
                sb.append("</label>");
            }
            sb.append("</div>\n");
        }

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
                if (src.sql != null && !src.sql.isEmpty()) {
                    sb.append("<details class=\"sqltoggle\"><summary>Show SQL</summary>")
                        .append("<pre><code>").append(esc(src.sql)).append("</code></pre>")
                        .append("</details>");
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

    /**
     * Per-filter rules, scoped to {@code @media screen} — which is the whole of how print is
     * kept correct.
     *
     * <p>A printout must be the complete document, not a snapshot of whichever toggles happened
     * to be set when someone hit print. Two people printing the same report would otherwise get
     * different tables, and neither page would say so. Worse, the prose states an {@code n}: a
     * filtered print silently disagrees with its own narrative.
     *
     * <p>Putting the hiding rule inside {@code @media screen} means print never sees it at all —
     * no {@code !important} override to fight, no reliance on {@code display:revert} restoring
     * the right value for a {@code <tr>}. The rows are simply never hidden on paper.
     */
    // ── Automatic per-table sorting ───────────────────────────────────────────

    private static final java.util.regex.Pattern TAG =
        java.util.regex.Pattern.compile("<[^>]*>");

    /**
     * Give every table in a section a sort control per column, without the caller doing anything.
     *
     * <p>Sorting happens HERE, at render time, not in the browser: the rows are already in hand,
     * Java orders them once, and the page ships one {@code <tbody>} per column. The CSP forbids
     * script, so a browser could not reorder them anyway — but even with script this is the
     * better division, because the server knows which columns are numeric and a DOM sorter has to
     * guess from text.
     *
     * <p>Parsed by hand rather than with jsoup, which build.gradle.kts excludes from this jar.
     * The rule that makes that safe is conservative bail-out: any table that is nested, ragged,
     * headerless, tiny, enormous or otherwise not obviously a plain data grid is returned
     * BYTE-FOR-BYTE UNCHANGED. Failing to sort a table costs a convenience; corrupting a caller's
     * markup costs the report.
     *
     * <p>Row markup is copied verbatim — the {@code <tr>} strings are reordered and rewrapped,
     * never rewritten — so cell contents, classes and inline styles survive intact, including the
     * classes the {@code filters} feature keys on.
     */
    private static String withSortControls(String html, String prefix, StringBuilder css) {
        if (html == null || html.isEmpty()) {
            return html;
        }
        StringBuilder out = new StringBuilder(html.length() + 4096);
        String lower = html.toLowerCase(java.util.Locale.ROOT);
        int pos = 0;
        int seq = 0;
        while (true) {
            int start = lower.indexOf("<table", pos);
            if (start < 0) {
                break;
            }
            int end = lower.indexOf("</table>", start);
            if (end < 0) {
                break;
            }
            end += 8;
            out.append(html, pos, start);
            String table = html.substring(start, end);
            String rebuilt = sortableTable(table, prefix + "t" + (++seq), css);
            out.append(rebuilt == null ? table : rebuilt);
            pos = end;
        }
        out.append(html.substring(pos));
        return out.toString();
    }

    /** Returns the rewritten table, or null to leave the original untouched. */
    private static String sortableTable(String table, String id, StringBuilder css) {
        String low = table.toLowerCase(java.util.Locale.ROOT);
        if (low.indexOf("<table", 1) >= 0) {
            return null;
        }
        List<String> rows = new ArrayList<>();
        List<Integer> ends = new ArrayList<>();
        int p = 0;
        while (true) {
            int rs = low.indexOf("<tr", p);
            if (rs < 0) {
                break;
            }
            int re = low.indexOf("</tr>", rs);
            if (re < 0) {
                return null;
            }
            re += 5;
            rows.add(table.substring(rs, re));
            ends.add(re);
            p = re;
        }
        int hdr = -1;
        for (int i = 0; i < rows.size(); i++) {
            if (rows.get(i).toLowerCase(java.util.Locale.ROOT).contains("<th")) {
                hdr = i;
                break;
            }
        }
        if (hdr < 0) {
            return null;
        }
        List<String> headers = cells(rows.get(hdr), "th");
        List<String> body = new ArrayList<>();
        for (int i = hdr + 1; i < rows.size(); i++) {
            if (rows.get(i).toLowerCase(java.util.Locale.ROOT).contains("<td")) {
                body.add(rows.get(i));
            }
        }
        int cols = headers.size();
        // Below four rows sorting is noise; beyond these bounds the emitted copies stop being
        // free. Both are judgement calls, stated rather than tuned.
        if (cols < 2 || cols > 10 || body.size() < 4 || body.size() > 200) {
            return null;
        }
        String[][] grid = new String[body.size()][];
        for (int i = 0; i < body.size(); i++) {
            List<String> c = cells(body.get(i), "td");
            if (c.size() != cols) {
                return null;
            }
            grid[i] = c.toArray(new String[0]);
        }

        // The column header IS the control. A separate button bar duplicates something the
        // reader is already looking at, and clicking a header to sort is the convention every
        // data grid has used for thirty years — so each <th>'s content is wrapped in a <label>
        // pointing at that column's radio. The original <th ...> tag is reused verbatim so
        // class, colspan and style survive.
        StringBuilder ctl = new StringBuilder();
        for (int i = 0; i <= cols; i++) {
            ctl.append("<input type=\"radio\" name=\"").append(id).append("\" class=\"srtbox\" id=\"")
               .append(id).append("-s").append(i).append("\"").append(i == 0 ? " checked" : "")
               .append(">");
        }

        List<String> thTags = cellTags(rows.get(hdr), "th");
        if (thTags.size() != cols) {
            return null;
        }
        StringBuilder hrow = new StringBuilder("<tr>");
        for (int c = 0; c < cols; c++) {
            hrow.append(thTags.get(c))
                .append("<label class=\"srth\" for=\"").append(id).append("-s").append(c + 1)
                .append("\">").append(headers.get(c))
                .append("<span class=\"srtcue\" aria-hidden=\"true\">")
                .append(numericColumn(grid, c) ? "\u2193" : "\u2191")
                .append("</span></label></th>");
        }
        hrow.append("</tr>");

        // A reset back to the author's chosen order, revealed only once a sort is active. It has
        // to exist: the order the author wrote is a decision about the data, and a radio group
        // cannot be un-checked by clicking a label again.
        String reset = "<label class=\"srtreset\" for=\"" + id + "-s0\">reset order</label>";

        String head = stripStray(table.substring(0, ends.get(hdr)));
        head = head.substring(0, head.length() - rows.get(hdr).length()) + hrow;
        StringBuilder bodies = new StringBuilder("<tbody class=\"srt s0\">");
        for (String r : body) {
            bodies.append(r);
        }
        bodies.append("</tbody>");
        for (int c = 0; c < cols; c++) {
            final int col = c;
            final boolean num = numericColumn(grid, c);
            final String[][] g = grid;
            Integer[] order = new Integer[body.size()];
            for (int i = 0; i < order.length; i++) {
                order[i] = i;
            }
            java.util.Arrays.sort(order, (x, y) -> {
                if (num) {
                    Double a = parseNum(g[x][col]);
                    Double b = parseNum(g[y][col]);
                    if (a == null && b == null) {
                        return 0;
                    }
                    if (a == null) {
                        return 1;
                    }
                    if (b == null) {
                        return -1;
                    }
                    return Double.compare(b, a);
                }
                return stripTags(g[x][col]).compareToIgnoreCase(stripTags(g[y][col]));
            });
            bodies.append("<tbody class=\"srt s").append(c + 1).append("\">");
            for (Integer i : order) {
                bodies.append(body.get(i));
            }
            bodies.append("</tbody>");
            String sel = "#" + id + "-s" + (c + 1) + ":checked ~ ";
            css.append(sel).append("table tbody.s0{display:none}\n")
               .append(sel).append("table tbody.s").append(c + 1)
               .append("{display:table-row-group}\n")
               .append(sel).append("table label[for=\"").append(id).append("-s").append(c + 1)
               .append("\"]{color:var(--link)}\n")
               .append(sel).append("table label[for=\"").append(id).append("-s").append(c + 1)
               .append("\"] .srtcue{opacity:1}\n")
               .append(sel).append(".srtreset{display:inline-block}\n");
        }
        return ctl + head + bodies.toString() + "</table>" + reset;
    }

    private static String stripStray(String s) {
        return s.replaceAll("(?i)</?tbody[^>]*>", "");
    }

    /** Opening tags of each cell, so a rebuilt row keeps class/colspan/style. */
    private static List<String> cellTags(String row, String tag) {
        List<String> out = new ArrayList<>();
        String low = row.toLowerCase(java.util.Locale.ROOT);
        int p = 0;
        while (true) {
            int cs = low.indexOf("<" + tag, p);
            if (cs < 0) {
                break;
            }
            int gt = low.indexOf('>', cs);
            if (gt < 0) {
                break;
            }
            out.add(row.substring(cs, gt + 1));
            p = gt + 1;
        }
        return out;
    }

    private static List<String> cells(String row, String tag) {
        List<String> out = new ArrayList<>();
        String low = row.toLowerCase(java.util.Locale.ROOT);
        int p = 0;
        while (true) {
            int cs = low.indexOf("<" + tag, p);
            if (cs < 0) {
                break;
            }
            int gt = low.indexOf('>', cs);
            int ce = low.indexOf("</" + tag + ">", gt);
            if (gt < 0 || ce < 0) {
                break;
            }
            out.add(row.substring(gt + 1, ce));
            p = ce + tag.length() + 3;
        }
        return out;
    }

    private static String stripTags(String s) {
        return TAG.matcher(s == null ? "" : s).replaceAll("").trim();
    }

    private static final java.util.regex.Pattern BLOCK_BREAK = java.util.regex.Pattern.compile(
        "(?i)</p>|</li>|</tr>|</h[1-6]>|<br\\s*/?>");

    /**
     * A section's HTML flattened to plain text, for a caller with no browser to open the report
     * in — the tool result text a model reads, not the page itself. Unlike {@link #stripTags},
     * this keeps paragraph/list/row breaks as newlines so the result reads as prose rather than
     * one run-on line, and unescapes the handful of entities a model's own HTML is likely to
     * contain. Not a general HTML-to-text converter: nested/complex markup degrades gracefully to
     * a run-on line rather than throwing, which is the right failure mode for a text summary that
     * is a convenience, not the deliverable.
     */
    static String sectionPlainText(String html) {
        if (html == null || html.isEmpty()) {
            return "";
        }
        String withBreaks = BLOCK_BREAK.matcher(html).replaceAll("\n");
        String noTags = stripTags(withBreaks);
        String unescaped = noTags
            .replace("&nbsp;", " ")
            .replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&#39;", "'");
        return unescaped.replaceAll("[ \\t]+", " ")
            .replaceAll("\n[ \\t]+", "\n")
            .replaceAll("\n{3,}", "\n\n")
            .trim();
    }

    /**
     * A column sorts numerically only if MOST of its non-empty cells parse. One stray "n/a"
     * should not turn a column of dollars into a string sort, and two state names containing a
     * digit should not make a name column numeric.
     */
    private static boolean numericColumn(String[][] grid, int col) {
        int ok = 0;
        int seen = 0;
        for (String[] row : grid) {
            String v = stripTags(row[col]);
            if (v.isEmpty()) {
                continue;
            }
            seen++;
            if (parseNum(v) != null) {
                ok++;
            }
        }
        return seen > 0 && ok * 2 > seen;
    }

    private static Double parseNum(String raw) {
        String v = stripTags(raw);
        if (v.isEmpty()) {
            return null;
        }
        boolean paren = v.startsWith("(") && v.endsWith(")");
        StringBuilder d = new StringBuilder();
        for (char ch : v.toCharArray()) {
            if ((ch >= '0' && ch <= '9') || ch == '.' || ch == '-') {
                d.append(ch);
            }
        }
        if (d.length() == 0) {
            return null;
        }
        try {
            double val = Double.parseDouble(d.toString());
            return paren ? -val : val;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String filterCss(List<Filter> filters) {
        if (filters == null || filters.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("@media screen{\n");
        for (Filter f : filters) {
            String c = cssIdent(f.className);
            sb.append("#flt-").append(c).append(":checked ~ main .").append(c)
                .append("{display:none}\n");
            sb.append("#flt-").append(c).append(":checked ~ main label[for=\"flt-").append(c)
                .append("\"]{background:var(--bg);color:var(--muted);"
                    + "box-shadow:inset 0 0 0 1px var(--rule)}\n");
        }
        sb.append("}\n");
        return sb.toString();
    }

    /** Only [a-z0-9_-] survives, so a caller-supplied class cannot break out of the selector. */
    private static String cssIdent(String s) {
        if (s == null) {
            return "x";
        }
        StringBuilder out = new StringBuilder();
        for (char ch : s.toLowerCase(java.util.Locale.ROOT).toCharArray()) {
            if ((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_') {
                out.append(ch);
            }
        }
        return out.length() == 0 ? "x" : out.toString();
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
            // The checkbox is offscreen rather than display:none — a display:none input takes
            // no focus, and the label stops being reachable by keyboard.
            // Non-default sort bodies are hidden in EVERY medium, and only revealed by a
            // checked radio inside @media screen. Print therefore shows exactly one copy of the
            // rows — the order the author wrote — instead of one copy per column.
            + "tbody.srt:not(.s0){display:none}\n"
            + ".srtbox{position:absolute;left:-9999px;width:1px;height:1px}\n"
            // The header cell is the control, so it has to look like one: pointer cursor, a
            // hover colour, and a direction cue that is faint until that column is the active
            // sort. Nothing else is added to the page.
            + "th .srth{display:inline-flex;align-items:center;gap:.3rem;cursor:pointer;"
            + "color:inherit;font:inherit;letter-spacing:inherit;text-transform:inherit}\n"
            + "th .srth:hover{color:var(--link)}\n"
            + "th .srth:focus-within{outline:2px solid var(--link);outline-offset:2px}\n"
            + ".srtcue{font-size:.85em;opacity:.28;line-height:1}\n"
            + ".srtreset{display:none;margin:-.6rem 0 1.2rem;font-size:.75rem;cursor:pointer;"
            + "color:var(--muted);text-decoration:underline;text-underline-offset:2px}\n"
            + ".srtreset:hover{color:var(--link)}\n"
            + ".fltbox{position:absolute;left:-9999px;width:1px;height:1px}\n"
            + ".filterbar{display:flex;flex-wrap:wrap;align-items:center;gap:.5rem;"
            + "margin:0 0 1.4rem;padding:.6rem .75rem;border:1px solid var(--rule);"
            + "border-radius:8px}\n"
            + ".fltlead{font-size:.72rem;font-weight:600;letter-spacing:.06em;"
            + "text-transform:uppercase;color:var(--muted);margin-right:.15rem}\n"
            + ".fltlabel{display:inline-flex;flex-direction:column;cursor:pointer;"
            + "font-size:.8rem;line-height:1.3;padding:.3rem .6rem;border-radius:999px;"
            + "background:color-mix(in srgb,var(--rule) 55%,transparent);color:var(--ink)}\n"
            + ".fltlabel:hover{color:var(--link)}\n"
            + ".fltbox:focus-visible + * .fltlabel,"
            + ".fltlabel:focus-within{outline:2px solid var(--link);outline-offset:2px}\n"
            + ".fltnote{font-size:.68rem;color:var(--muted)}\n"
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
            // The filter bar is a control, not content. The rows it hides are NOT
            // hidden here — see filterCss: the hiding rule lives in @media screen, so a
            // printout is always the complete table regardless of toggle state, and
            // always agrees with the n stated in the prose.
            + ".filterbar{display:none}\n"
            + ".srtcue{display:none}\n"
            + ".srtreset{display:none}\n"
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
            + ".sqltoggle{display:inline-block;margin-left:.4rem}\n"
            + ".sqltoggle summary{display:inline;cursor:pointer;font-size:.82rem;"
            + "color:var(--link)}\n"
            + ".sqltoggle summary:hover{text-decoration:underline}\n"
            + ".sqltoggle[open] summary{margin-bottom:.4rem}\n"
            + ".sqltoggle pre{margin:.3rem 0 0;padding:.6rem .8rem;overflow-x:auto;"
            + "background:color-mix(in srgb,var(--rule) 40%,transparent);border-radius:6px;"
            + "font-size:.82rem;white-space:pre-wrap;word-break:break-word}\n"
            + ".sqltoggle pre code{background:none;padding:0}\n"
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
