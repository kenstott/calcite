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

import org.apache.calcite.adapter.govdata.GovDataCatalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * In-memory metadata catalog loaded once from the bundled {@code /catalog.json}.
 *
 * <p>Backs the MCP discovery tools — {@code list_schemas}, {@code list_tables},
 * {@code describe_table}, {@code search_catalog} — so they answer instantly and
 * completely with schema/table/column names + descriptions, without initializing
 * any live schema connection (which can take minutes on first use).
 *
 * <p>Source of truth: the govdata {@code *-schema.yaml} files. Regenerate the
 * bundled resource with {@code govdata/scripts/generate_catalog.py}.
 */
final class Catalog {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static volatile JsonNode root;

    private Catalog() {
    }

    private static JsonNode root() {
        JsonNode r = root;
        if (r == null) {
            synchronized (Catalog.class) {
                r = root;
                if (r == null) {
                    JsonNode built = null;
                    try {
                        // Read the catalog straight from the driver's baked-in schema YAMLs.
                        built = GovDataCatalog.build(schemaList());
                    } catch (Throwable ignored) {
                        // fall through to empty catalog
                    }
                    root = (built != null && built.isArray()) ? built : MAPPER.createArrayNode();
                    r = root;
                }
            }
        }
        return r;
    }

    private static List<String> schemaList() {
        String src = System.getenv("ASKAMERICA_SCHEMAS");
        if (src == null || src.trim().isEmpty()) {
            src = McpServer.DEFAULT_SCHEMAS;
        }
        List<String> out = new ArrayList<>();
        for (String s : src.split(",")) {
            String t = s.trim().toLowerCase(Locale.ROOT);
            if (!t.isEmpty()) {
                out.add(t);
            }
        }
        return out;
    }

    static boolean available() {
        return root().size() > 0;
    }

    private static String txt(JsonNode n) {
        return (n == null || n.isNull() || n.isMissingNode()) ? "" : n.asText("");
    }

    private static JsonNode schemaNode(String schema) {
        String s = schema.toLowerCase(Locale.ROOT);
        for (JsonNode sc : root()) {
            if (s.equals(txt(sc.get("schema")).toLowerCase(Locale.ROOT))) {
                return sc;
            }
        }
        return null;
    }

    private static JsonNode tableNode(JsonNode sc, String table) {
        String t = table.toLowerCase(Locale.ROOT);
        for (JsonNode tb : sc.path("tables")) {
            if (t.equals(txt(tb.get("name")).toLowerCase(Locale.ROOT))) {
                return tb;
            }
        }
        return null;
    }

    private static JsonNode tableNodeOf(String schema, String table) {
        JsonNode sc = schemaNode(schema);
        return sc == null ? null : tableNode(sc, table);
    }

    // ── Static description overlay ────────────────────────────────────────────
    // The live information_schema carries table/column REMARKS but not view or
    // (untruncated) schema descriptions. These supply the authored text from the
    // YAML to fill those gaps; callers prefer these over REMARKS.

    static String schemaDescription(String schema) {
        JsonNode sc = schemaNode(schema);
        return (sc != null && sc.hasNonNull("comment")) ? sc.get("comment").asText() : null;
    }

    static String tableDescription(String schema, String table) {
        JsonNode tb = tableNodeOf(schema, table);
        return (tb != null && tb.hasNonNull("comment")) ? tb.get("comment").asText() : null;
    }

    static String columnDescription(String schema, String table, String column) {
        JsonNode tb = tableNodeOf(schema, table);
        if (tb == null) {
            return null;
        }
        String col = column.toLowerCase(Locale.ROOT);
        for (JsonNode c : tb.path("columns")) {
            if (col.equals(txt(c.get("name")).toLowerCase(Locale.ROOT)) && c.hasNonNull("comment")) {
                return c.get("comment").asText();
            }
        }
        return null;
    }

    /** Keyword search across schema/table/column names + descriptions; ranked, capped. */
    static ArrayNode search(String query, int limit) {
        String[] toks = query.toLowerCase(Locale.ROOT).split("\\s+");
        List<ObjectNode> hits = new ArrayList<>();
        for (JsonNode sc : root()) {
            String schema = txt(sc.get("schema"));
            String sComment = txt(sc.get("comment"));
            int ss = score(toks, schema, sComment);
            if (ss > 0) {
                hits.add(match("schema", schema, null, null, null, sComment, ss));
            }
            for (JsonNode tb : sc.path("tables")) {
                String table = txt(tb.get("name"));
                String tType = tb.path("type").asText("table");
                String tComment = txt(tb.get("comment"));
                int ts = score(toks, schema + " " + table, tComment);
                if (ts > 0) {
                    hits.add(match("table", schema, table, tType, null, tComment, ts));
                }
                for (JsonNode c : tb.path("columns")) {
                    String col = txt(c.get("name"));
                    String cComment = txt(c.get("comment"));
                    // Score column on its own name + comment only, so a query token that
                    // matches the table name doesn't drag in all of its columns.
                    int cs = score(toks, col, cComment);
                    if (cs > 0) {
                        hits.add(match("column", schema, table, txt(c.get("type")), col, cComment, cs));
                    }
                }
            }
        }
        hits.sort((a, b) -> Integer.compare(b.path("score").asInt(), a.path("score").asInt()));
        ArrayNode arr = MAPPER.createArrayNode();
        int n = 0;
        for (ObjectNode h : hits) {
            if (n++ >= limit) {
                break;
            }
            arr.add(h);
        }
        return arr;
    }

    private static int score(String[] toks, String name, String comment) {
        String n = name.toLowerCase(Locale.ROOT);
        String c = comment.toLowerCase(Locale.ROOT);
        int s = 0;
        for (String tk : toks) {
            if (tk.isEmpty()) {
                continue;
            }
            if (n.equals(tk)) {
                s += 10;
            } else if (n.contains(tk)) {
                s += 5;
            }
            if (c.contains(tk)) {
                s += 1;
            }
        }
        return s;
    }

    private static ObjectNode match(String kind, String schema, String table,
            String type, String column, String comment, int score) {
        ObjectNode o = MAPPER.createObjectNode();
        o.put("kind", kind);
        o.put("schema", schema);
        if (table != null) {
            o.put("table", table);
        }
        if (column != null) {
            o.put("column", column);
        }
        if (type != null && !type.isEmpty()) {
            o.put("type", type);
        }
        if (comment != null && !comment.isEmpty()) {
            o.put("description", comment);
        }
        o.put("score", score);
        return o;
    }
}
