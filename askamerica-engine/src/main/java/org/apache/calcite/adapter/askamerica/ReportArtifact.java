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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

/**
 * The destination-neutral report payload: the same narrative sections and dashboard panels
 * {@code preview_report} takes, restated as data plus prescriptive layout hints for a chatbot
 * that draws the charts itself (for example as a Claude Artifact).
 *
 * <p>Panels are read by the same reader {@code preview_report} and {@code compose_dashboard}
 * use, so one report object drives both destinations without reshaping. The hints are computed
 * only from the panel data: a chatbot never has to read diagnostics to decide how to present a
 * value, and a missing value is passed through as a distinct suppressed cell rather than a
 * plotted zero.
 */
final class ReportArtifact {
    /** Bar charts with more categories than this read better as horizontal bars. */
    static final int HORIZONTAL_CATEGORY_COUNT = 8;
    /** Bar charts with a category label longer than this read better as horizontal bars. */
    static final int HORIZONTAL_LABEL_LENGTH = 12;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ReportArtifact() {
    }

    /**
     * A validation as data: the article under review and its claims sorted into the blocks
     * {@link ClaimScoring} scores them in — the author's own claims, each speaker's, then the
     * unscored audit of the sources the piece relays. A scored block carries its honesty and
     * bias scores, computed from its own claims. Claims keep the number they have in the order
     * given, so a block's table and the saved page agree.
     */
    static ObjectNode validation(String sourceUrl, JsonNode claims) {
        ObjectNode out = MAPPER.createObjectNode();
        putIfPresent(out, "source_url", sourceUrl);
        out.put("scope", "this article only");
        ArrayNode groups = out.putArray("groups");
        for (String b : ClaimScoring.blocks(claims)) {
            ObjectNode tally = MAPPER.createObjectNode();
            ArrayNode inBlock = MAPPER.createArrayNode();
            int n = 0;
            for (JsonNode claim : claims) {
                n++;
                if (!b.equals(ClaimScoring.block(claim))) {
                    continue;
                }
                String verdict = claim.path("verdict").asText("").trim()
                    .toLowerCase(java.util.Locale.ROOT);
                tally.put(verdict, tally.path(verdict).asInt(0) + 1);
                ObjectNode numbered = MAPPER.createObjectNode();
                numbered.put("n", n);
                numbered.setAll((ObjectNode) claim);
                inBlock.add(numbered);
            }
            ObjectNode group = groups.addObject();
            if (!b.isEmpty()) {
                group.put("group", ClaimScoring.group(inBlock.get(0)));
                putIfPresent(group, "speaker", ClaimScoring.blockSpeaker(b));
                group.put("label", ClaimScoring.label(b));
            }
            if (ClaimScoring.isScored(b)) {
                group.set("score", ClaimScoring.score(claims, b));
            }
            group.set("tally", tally);
            group.set("claims", inBlock);
        }
        return out;
    }

    /** Builds the payload; {@code sections}, {@code sources} and {@code panels} may be empty,
     *  never null. */
    static ObjectNode build(String title, String subtitle, String footnote, String byline,
            List<ReportPage.Section> sections, List<ReportPage.Source> sources,
            List<DashboardLayout.Panel> panels, int columns) {
        ObjectNode out = MAPPER.createObjectNode();
        out.put("schema_version", 1);
        putIfPresent(out, "title", title);
        putIfPresent(out, "subtitle", subtitle);
        putIfPresent(out, "footnote", footnote);
        putIfPresent(out, "byline", byline);
        out.put("columns", columns);
        ArrayNode secs = out.putArray("sections");
        for (ReportPage.Section s : sections) {
            ObjectNode n = secs.addObject();
            putIfPresent(n, "heading", s.heading);
            n.put("html", s.html);
        }
        ArrayNode srcs = out.putArray("sources");
        for (ReportPage.Source s : sources) {
            ObjectNode n = srcs.addObject();
            putIfPresent(n, "label", s.label);
            putIfPresent(n, "url", s.url);
            putIfPresent(n, "note", s.note);
            putIfPresent(n, "sql", s.sql);
            putIfPresent(n, "tool", s.tool);
            putIfPresent(n, "params", s.toolParams);
        }
        ArrayNode ps = out.putArray("panels");
        for (DashboardLayout.Panel p : panels) {
            ps.add(panel(p));
        }
        return out;
    }

    private static ObjectNode panel(DashboardLayout.Panel p) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("kind", p.kind);
        n.put("span", p.span);
        putIfPresent(n, "caption", p.caption);
        putIfPresent(n, "scale_group", p.scaleGroup);
        if ("stat".equals(p.kind)) {
            putIfPresent(n, "label", p.label);
            putIfPresent(n, "value", p.value);
            putIfPresent(n, "delta", p.delta);
            n.put("delta_direction", p.deltaDirection);
            return n;
        }
        n.put("chart_type", p.chartType);
        putIfPresent(n, "title", p.title);
        putIfPresent(n, "x_label", p.xLabel);
        putIfPresent(n, "y_label", p.yLabel);
        ObjectNode hints = n.putObject("hints");
        ArrayNode suppressed = hints.putArray("suppressed_cells");
        if (p.points != null) {
            ArrayNode series = n.putArray("points");
            for (ChartRenderer.PointSeriesSpec s : p.points) {
                ObjectNode sn = series.addObject();
                sn.put("name", s.name);
                putNumbers(sn.putArray("x"), s.x);
                putNumbers(sn.putArray("y"), s.y);
                if (s.size != null) {
                    putNumbers(sn.putArray("size"), s.size);
                }
                if (s.labels != null) {
                    ArrayNode ln = sn.putArray("labels");
                    for (String l : s.labels) {
                        ln.add(l);
                    }
                    sn.put("label_mode", s.labelMode);
                    sn.put("label_count", s.labelCount);
                }
                if (s.highlight != null) {
                    ObjectNode hn = sn.putObject("highlight");
                    for (java.util.Map.Entry<String, java.util.List<String>> g
                        : s.highlight.entrySet()) {
                        ArrayNode names = hn.putArray(g.getKey());
                        for (String l : g.getValue()) {
                            names.add(l);
                        }
                    }
                }
                for (int i = 0; i < s.y.size(); i++) {
                    if (s.y.get(i) == null || (i < s.x.size() && s.x.get(i) == null)) {
                        ObjectNode c = suppressed.addObject();
                        c.put("series", s.name);
                        c.put("index", i);
                    }
                }
            }
            return n;
        }
        ArrayNode cats = n.putArray("categories");
        int longest = 0;
        for (String c : p.categories) {
            cats.add(c);
            longest = Math.max(longest, c.length());
        }
        ArrayNode series = n.putArray("series");
        for (ChartRenderer.SeriesSpec s : p.series) {
            ObjectNode sn = series.addObject();
            sn.put("name", s.name);
            putNumbers(sn.putArray("values"), s.values);
            for (int i = 0; i < s.values.size(); i++) {
                if (s.values.get(i) == null && i < p.categories.size()) {
                    ObjectNode c = suppressed.addObject();
                    c.put("series", s.name);
                    c.put("category", p.categories.get(i));
                }
            }
        }
        if ("bar".equals(p.chartType)) {
            boolean horizontal = p.categories.size() > HORIZONTAL_CATEGORY_COUNT
                    || longest > HORIZONTAL_LABEL_LENGTH;
            hints.put("orientation_hint", horizontal ? "horizontal" : "vertical");
        }
        return n;
    }

    private static void putNumbers(ArrayNode into, List<Double> values) {
        for (Double v : values) {
            if (v == null) {
                into.addNull();
            } else {
                into.add(v);
            }
        }
    }

    private static void putIfPresent(ObjectNode n, String key, String value) {
        if (value != null) {
            n.put(key, value);
        }
    }
}
