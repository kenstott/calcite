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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

/**
 * The destination-neutral report payload: the same narrative sections and dashboard panels
 * {@code publish_report} takes, restated as data plus prescriptive layout hints for a chatbot
 * that draws the charts itself (for example as a Claude Artifact).
 *
 * <p>Panels are read by the same reader {@code publish_report} and {@code compose_dashboard}
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

    /** Builds the payload; {@code sections} and {@code panels} may be empty, never null. */
    static ObjectNode build(String title, String subtitle, String footnote, String byline,
            List<ReportPage.Section> sections, List<DashboardLayout.Panel> panels, int columns) {
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
