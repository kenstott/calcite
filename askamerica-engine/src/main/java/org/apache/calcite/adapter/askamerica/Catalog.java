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

import org.apache.calcite.adapter.file.etl.VariableResolver;
import org.apache.calcite.adapter.govdata.GovDataCatalog;

import java.time.Year;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

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

    /** Every column name known to the catalog, lowercased. Empty when no catalog is loaded. */
    static java.util.Set<String> allColumnNames() {
        java.util.Set<String> cached = ALL_COLUMN_NAMES;
        if (cached != null) {
            return cached;
        }
        java.util.Set<String> names = new java.util.HashSet<>();
        for (JsonNode sc : root()) {
            for (JsonNode tb : sc.path("tables")) {
                for (JsonNode c : tb.path("columns")) {
                    String n = txt(c.get("name"));
                    if (!n.isEmpty()) {
                        names.add(n.toLowerCase(Locale.ROOT));
                    }
                }
            }
        }
        java.util.Set<String> frozen = java.util.Collections.unmodifiableSet(names);
        ALL_COLUMN_NAMES = frozen;
        return frozen;
    }

    private static volatile java.util.Set<String> ALL_COLUMN_NAMES;

    /** Table and view names known to the catalog for a schema; empty if unknown. */
    static List<String> tableNames(String schema) {
        List<String> out = new ArrayList<>();
        JsonNode sc = schemaNode(schema);
        if (sc != null) {
            for (JsonNode tb : sc.path("tables")) {
                String name = txt(tb.get("name"));
                if (!name.isEmpty()) {
                    out.add(name);
                }
            }
        }
        return out;
    }

    // ── Coverage window ───────────────────────────────────────────────────────

    /**
     * Effective year window for a table, resolved from its declared {@code year}
     * dimension, or null when the table declares no year range (unpartitioned
     * reference tables and most views).
     *
     * <p>This is the <em>declared</em> window — what the schema says should be
     * ingested — not a scan of loaded rows. It exists so a caller can tell an
     * empty result inside the window ("no matching rows") from one outside it
     * ("the source does not publish that year yet"), which is otherwise
     * indistinguishable. Bounds that cannot be resolved are omitted rather than
     * guessed, so a partial window never reads as a confident one.
     */
    static ObjectNode coverage(String schema, String table) {
        JsonNode tb = tableNodeOf(schema, table);
        if (tb == null) {
            return null;
        }
        JsonNode cov = tb.get("coverage");
        if (cov == null || !cov.isObject()) {
            return null;
        }

        int currentYear = Year.now(ZoneOffset.UTC).getValue();
        Integer minYear = intOrNull(cov.get("minYear"));
        Integer maxYear = intOrNull(cov.get("maxYear"));
        Integer dataLag = intOrNull(cov.get("dataLag"));
        int lag = (dataLag != null && dataLag > 0) ? dataLag.intValue() : 0;

        Integer start = resolveYear(cov.path("start").asText(null), currentYear);

        // An omitted end is not an unknown one: GovDataUtils.getEndYear runs the range
        // through the current year and lets dataLag pull the ceiling back, so most
        // yearRange blocks simply leave it out. An end that IS declared but will not
        // resolve stays absent below — that one really is unknown.
        Integer end;
        if (!cov.hasNonNull("end")) {
            end = Integer.valueOf(currentYear);
        } else {
            end = resolveYear(cov.path("end").asText(null), currentYear);
        }

        // minYear/maxYear and the declared start/end are all expressed in iterated
        // (publish-year) terms, matching DimensionIterator#resolveYearRange's own
        // clamping of `start`/`effectiveEnd` against them before it ever computes a
        // data year — so apply those clamps first, in that same unit.
        if (start != null && minYear != null) {
            start = Math.max(start, minYear);
        }
        if (end != null && maxYear != null) {
            end = Math.min(end, maxYear);
        }
        if (end != null) {
            end = Math.min(end, currentYear);
        }

        // DimensionIterator#injectEffectiveYear stamps effective_year = year - dataLag
        // into every combination of a YEAR_RANGE dimension with dataLag>0, and that
        // effective_year — not the publish year iterated above — is what most schemas
        // template into the URL and write as the table's own year/partition column.
        // Reported bounds must shift by the same amount or first_year/last_year name a
        // year earlier than any row the table actually holds.
        if (lag > 0) {
            if (start != null) {
                start = Integer.valueOf(start.intValue() - lag);
            }
            if (end != null) {
                end = Integer.valueOf(end.intValue() - lag);
            }
        }

        ObjectNode out = MAPPER.createObjectNode();
        out.put("column", cov.path("column").asText("year"));
        if (start != null) {
            out.put("first_year", start);
        }
        if (end != null) {
            out.put("last_year", end);
        }
        if (minYear != null) {
            // Declared in the same publish-year terms as start/end (see the yearRange
            // comments in econ-schema.yaml, e.g. "minYear hard-floors the publish year
            // at 2014 (=> data floor 2013)") — shift it the same way so it stays
            // comparable to first_year rather than naming a year later than the data
            // actually starts.
            out.put("source_earliest_year", Integer.valueOf(minYear.intValue() - lag));
        }
        if (dataLag != null && dataLag > 0) {
            out.put("publication_lag_years", dataLag);
        }
        out.put("basis", "declared");
        // partitionColumn coverage states a ceiling but no floor — the caller should not
        // read its absence as "starts at the beginning of time".
        out.put("declared_from", cov.path("form").asText("yearRange"));
        out.put("note", coverageNote(start, end, dataLag, currentYear));
        return out;
    }

    private static String coverageNote(Integer start, Integer end, Integer dataLag,
            int currentYear) {
        StringBuilder sb = new StringBuilder();
        sb.append("Declared ingest window from the schema definition, not a row scan; "
            + "a year inside it may still be mid-backfill. ");
        if (start != null && end != null) {
            sb.append("Query years ").append(start).append('-').append(end).append(". ");
        }
        if (dataLag != null && dataLag > 0 && end != null && end < currentYear) {
            sb.append("The source publishes about ").append(dataLag)
                .append(dataLag == 1 ? " year" : " years").append(" behind, so ")
                .append(end + 1);
            if (end + 1 < currentYear) {
                sb.append('-').append(currentYear).append(" do not exist");
            } else {
                sb.append(" does not exist");
            }
            sb.append(" upstream yet — an empty result there is expected, not a gap. ");
        }
        sb.append("Report the window when a question falls outside it rather than "
            + "presenting an empty result as zero. If an 'observed' block is present it is "
            + "the measured min/max actually loaded — trust it over this declared range, "
            + "since a backfill can lag what the schema intends; status 'measuring' means "
            + "the scan has not finished, so call describe_table again for it.");
        return sb.toString();
    }

    /**
     * Resolve a declared bound, which reaches here in any of the forms the schema YAMLs
     * use: a bare year, a {@code ${VAR:default}} reference, the literal {@code current},
     * or a {@code ${CURRENT_YEAR}} reference left unresolved because nothing set it.
     *
     * <p>The last two both mean "through now" — the same reading
     * {@code SecSchemaFactory} applies to an unset {@code CURRENT_YEAR}. Anything still
     * unparseable returns null so the caller omits the bound; a year is never invented.
     */
    private static Integer resolveYear(String raw, int currentYear) {
        if (raw == null || raw.trim().isEmpty()) {
            return null;
        }
        // The YAMLs use three interchangeable spellings for a bound, so all three have to
        // be run to a fixed point:
        //   ${VAR:default}      — resolveEnvVars
        //   {env:VAR:default}   — substitute (a separate resolver entry point)
        //   ${CURRENT_YEAR}     — left unresolved by both; means the current year
        // Filling CURRENT_YEAR also un-nests ${GOVDATA_END_YEAR:${CURRENT_YEAR}}, whose
        // inner braces defeat resolveEnvVars' pattern, into ${GOVDATA_END_YEAR:2026} —
        // so a GOVDATA_END_YEAR that IS set still wins on the following pass.
        String resolved = raw.trim();
        for (int i = 0; i < MAX_RESOLVE_PASSES; i++) {
            String next = VariableResolver.substitute(resolved, null);
            next = VariableResolver.resolveEnvVars(next);
            next = CURRENT_YEAR_REF.matcher(next).replaceAll(String.valueOf(currentYear));
            if (next.equals(resolved)) {
                break;
            }
            resolved = next;
        }
        resolved = resolved.trim();
        if ("current".equalsIgnoreCase(resolved)) {
            return Integer.valueOf(currentYear);
        }
        try {
            return Integer.valueOf(resolved);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static final int MAX_RESOLVE_PASSES = 5;

    /**
     * An unset {@code ${CURRENT_YEAR}} / {@code ${GOVDATA_CURRENT_YEAR}} placeholder,
     * which means the current year — the same reading {@code SecSchemaFactory} applies.
     */
    private static final java.util.regex.Pattern CURRENT_YEAR_REF =
        java.util.regex.Pattern.compile("\\$\\{(GOVDATA_)?CURRENT_YEAR\\}");

    private static Integer intOrNull(JsonNode n) {
        return (n != null && n.isNumber()) ? Integer.valueOf(n.intValue()) : null;
    }

    /**
     * Common short English words excluded from search scoring, shared with
     * {@link ExternalSources}. Without this, a stopword like "by" is a substring of
     * dozens of unrelated {@code *_by_state}-style table names/comments across this
     * catalog, and its incidental hits outrank a genuinely on-topic table whose own
     * name/comment never happens to contain the query's stopwords at all.
     */
    static final Set<String> STOPWORDS = new HashSet<>(Arrays.asList(
        "a", "an", "the", "and", "or", "but", "for", "nor", "so", "yet", "of", "to", "in", "on",
        "at", "by", "with", "from", "into", "onto", "is", "are", "was", "were", "be", "been",
        "being", "this", "that", "these", "those", "it", "its", "as", "if", "than", "then"));

    /** Keyword search across schema/table/column names + descriptions; ranked, capped. */
    static ArrayNode search(String query, int limit) {
        List<String> toks = new ArrayList<>();
        for (String tk : query.toLowerCase(Locale.ROOT).split("\\s+")) {
            if (!tk.isEmpty() && !STOPWORDS.contains(tk)) {
                toks.add(tk);
            }
        }
        List<ObjectNode> hits = new ArrayList<>();
        for (JsonNode sc : root()) {
            String schema = txt(sc.get("schema"));
            String sComment = txt(sc.get("comment"));
            int ss = score(toks, schema, sComment, NAME_WEIGHT);
            if (ss > 0) {
                hits.add(match("schema", schema, null, null, null, sComment, ss));
            }
            for (JsonNode tb : sc.path("tables")) {
                String table = txt(tb.get("name"));
                String tType = tb.path("type").asText("table");
                String tComment = txt(tb.get("comment"));
                int ts = score(toks, schema + " " + table, tComment, NAME_WEIGHT);
                if (ts > 0) {
                    hits.add(match("table", schema, table, tType, null, tComment, ts));
                }
                for (JsonNode c : tb.path("columns")) {
                    String col = txt(c.get("name"));
                    String cComment = txt(c.get("comment"));
                    // Score column on its own name + comment only, so a query token that
                    // matches the table name doesn't drag in all of its columns. Weighted
                    // lower than a schema/table match (see COLUMN_NAME_WEIGHT): a column
                    // literally named "state" exists on dozens of unrelated tables across
                    // this catalog, so its bare name matching one query token is much
                    // weaker evidence of topical relevance than the table's own name or
                    // comment actually describing the query's subject.
                    int cs = score(toks, col, cComment, COLUMN_NAME_WEIGHT);
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

    /** Full weight for a schema/table name match. */
    private static final int NAME_WEIGHT = 10;

    /**
     * Weight for a column name match — well below {@link #NAME_WEIGHT}. A bare column
     * name is reused verbatim across dozens of unrelated tables in this catalog (e.g.
     * "state", "year", "value"), so its exact match is far weaker evidence that a table
     * is genuinely about the query's topic than the table's own name or comment saying
     * so; scoring it the same let single-token column hits outrank a table whose
     * name/comment actually described the query (search_catalog("campaign contributions
     * by state") never surfacing any fec.* table).
     */
    private static final int COLUMN_NAME_WEIGHT = 3;

    private static int score(List<String> toks, String name, String comment, int nameWeight) {
        String n = name.toLowerCase(Locale.ROOT);
        String c = comment.toLowerCase(Locale.ROOT);
        int s = 0;
        for (String tk : toks) {
            if (tk.isEmpty()) {
                continue;
            }
            if (n.equals(tk)) {
                s += nameWeight;
            } else if (n.contains(tk)) {
                s += nameWeight / 2;
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
