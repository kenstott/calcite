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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds the {@code diagnostics} envelope that rides alongside every analytical result.
 *
 * <p>The envelope exists because the server cannot see the user's intent or the surrounding
 * conversation — only the form of the query and the shape of what came back. A result that
 * carries its own defects is self-scoping: even a poorly-framed question yields an honestly
 * bounded answer, because the facts needed to caveat it arrive in the same response rather
 * than requiring a follow-up the host may never make.
 *
 * <p>Two boundaries hold this to facts rather than opinions:
 *
 * <ul>
 *   <li>Every warning type comes from a fixed catalog ({@code small_n}, {@code low_coverage},
 *       {@code row_fanout}, {@code grain_mismatch}, {@code vintage_misalignment},
 *       {@code broken_field}, {@code no_pushdown}, {@code collinear_controls}) and is derived
 *       from something the server measured — a row count, a declared coverage window, a key
 *       fan-out, a value outside its own domain. Nothing here judges whether a question was
 *       worth asking or which specification is correct; relocating that judgment into the
 *       server would reproduce the prior-laundering problem it exists to prevent.</li>
 *   <li>The absence of a warning is never a claim of validity, only that no listed defect was
 *       detected. That sentence ships inside every envelope, because an empty warnings array
 *       otherwise reads as a clean bill of health.</li>
 * </ul>
 *
 * <p>The envelope is strictly additive: it travels as its own content block, so the data
 * payload a host receives is byte-identical to what it received before diagnostics existed.
 * A host that ignores the block is no worse off; a host that reads it can re-query with a
 * control, change grain, or hedge.
 */
final class QuestionDiagnostics {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static final String INFO = "info";
    static final String CAUTION = "caution";
    static final String HIGH = "high";

    /**
     * Unit count at or below which an analysis is flagged as resting on few observations.
     * Set just above the 51 state-level units that motivate it, so a state or state-plus-DC
     * panel trips it and a county-grain analysis does not.
     */
    private static final int SMALL_N = 60;

    /** SUM/COUNT/AVG — the aggregates that double-count when levels are mixed. */
    private static final Pattern ADDITIVE_AGG = Pattern.compile(
        "\\b(sum|count|avg|mean|total)\\s*\\(", Pattern.CASE_INSENSITIVE);

    /** Fraction of nulls in a returned column at which the column is reported as unusable. */
    private static final double NULL_DOMINANT = 0.9;

    /** Absolute correlation between two covariates at which they are reported as duplicative. */
    private static final double COLLINEAR_R = 0.95;

    /** Mean of a coverage-percent column below which the underlying universe is incomplete. */
    private static final double LOW_COVERAGE_PCT = 80.0;

    /**
     * Separator between the parts of a composite key while counting fan-out. A character no
     * data value carries, so two key parts cannot run together into a third key's string and
     * hide a duplication behind a falsely-high distinct count.
     */
    private static final String KEY_SEP = "\u001f";

    /** Stands in for a null key part, which is itself a value worth grouping by. */
    private static final String NULL_KEY = "\u0000";

    static final String BASIS_NOTE =
        "Derived from measured facts only (row counts, declared coverage, key fan-out, value "
        + "domains). The absence of a warning is not a claim of validity — only that no listed "
        + "defect was detected.";

    private QuestionDiagnostics() {
    }

    // ── SQL surface reading ───────────────────────────────────────────────────

    private static final Pattern TABLE_REF = Pattern.compile(
        "(?i)\\b(?:FROM|JOIN)\\s+\"?([a-zA-Z_][a-zA-Z0-9_]*)\"?\\s*\\.\\s*\"?([a-zA-Z_][a-zA-Z0-9_]*)\"?");

    private static final Pattern YEAR_LITERAL = Pattern.compile("\\b(19\\d{2}|20\\d{2})\\b");

    private static final Set<String> META_SCHEMAS = new HashSet<>(
        Arrays.asList("information_schema", "pg_catalog", "metadata"));

    /** Aggregates that measure an association between exactly two columns. */
    private static final Pattern BIVARIATE_AGG = Pattern.compile(
        "(?i)\\b(corr|covar_pop|covar_samp|regr_slope|regr_intercept|regr_r2|regr_sxy|"
        + "regr_avgx|regr_avgy)\\s*\\(");

    private static final Pattern ANY_STAT_AGG = Pattern.compile(
        "(?i)\\b(corr|covar_pop|covar_samp|regr_[a-z0-9_]+|median|quantile_cont|quantile_disc|"
        + "mode|stddev_samp|stddev_pop|var_samp|var_pop|skewness|kurtosis|mad)\\s*\\(");

    private static final Pattern COUNT_COLUMN = Pattern.compile(
        "(?i)^(n|n_obs|nobs|num|count|cnt|row_count|rows|observations|obs|regr_count|"
        + "n_[a-z0-9_]+|[a-z0-9_]+_count|num_[a-z0-9_]+)$");

    /** {@code schema.table} references in the SQL, meta-schemas excluded, in first-seen order. */
    static Set<String> referencedTables(String sql) {
        Set<String> out = new LinkedHashSet<>();
        if (sql == null) {
            return out;
        }
        Matcher m = TABLE_REF.matcher(sql);
        while (m.find()) {
            String schema = m.group(1).toLowerCase(Locale.ROOT);
            if (META_SCHEMAS.contains(schema)) {
                continue;
            }
            out.add(schema + "." + m.group(2).toLowerCase(Locale.ROOT));
        }
        return out;
    }

    private static boolean hasJoin(String sql) {
        return sql != null && Pattern.compile("(?i)\\bJOIN\\b").matcher(sql).find();
    }

    private static boolean collapsesRows(String sql) {
        return sql != null
            && (Pattern.compile("(?i)\\bGROUP\\s+BY\\b").matcher(sql).find()
                || Pattern.compile("(?i)\\bSELECT\\s+DISTINCT\\b").matcher(sql).find());
    }

    static boolean bivariateAssociation(String sql) {
        return sql != null && BIVARIATE_AGG.matcher(sql).find();
    }

    private static boolean anyStatAggregate(String sql) {
        return sql != null && ANY_STAT_AGG.matcher(sql).find();
    }

    /** Distinct four-digit years appearing as literals in the SQL. */
    static Set<Integer> yearLiterals(String sql) {
        Set<Integer> out = new LinkedHashSet<>();
        if (sql == null) {
            return out;
        }
        Matcher m = YEAR_LITERAL.matcher(sql);
        while (m.find()) {
            out.add(Integer.valueOf(m.group(1)));
        }
        return out;
    }

    // ── Result-shape reading ──────────────────────────────────────────────────

    /** Column names of the first row, which the JDBC path guarantees every row shares. */
    private static List<String> columnsOf(ArrayNode rows) {
        List<String> out = new ArrayList<>();
        if (rows == null || rows.size() == 0) {
            return out;
        }
        java.util.Iterator<String> it = rows.get(0).fieldNames();
        while (it.hasNext()) {
            out.add(it.next());
        }
        return out;
    }

    /**
     * The unit of analysis implied by the returned columns. Read off identifier columns
     * rather than the SQL, because a join can change the grain without changing which
     * tables are named.
     */
    static String grainOf(List<String> columns) {
        boolean county = false;
        boolean state = false;
        boolean zcta = false;
        boolean tract = false;
        for (String raw : columns) {
            String c = raw.toLowerCase(Locale.ROOT);
            if (c.contains("tract")) {
                tract = true;
            } else if (c.contains("zcta") || c.equals("zip") || c.contains("zip_code")) {
                zcta = true;
            } else if (c.contains("county_fips") || c.equals("county") || c.contains("county_name")) {
                county = true;
            } else if (c.contains("state_fips") || c.equals("state") || c.equals("state_code")
                    || c.equals("state_abbr") || c.equals("stusps") || c.contains("state_name")) {
                state = true;
            }
        }
        // Finest identifier present wins: a county row that also carries its state is a
        // county-grain row, not a state-grain one.
        if (tract) {
            return "tract";
        }
        if (zcta) {
            return "zcta";
        }
        if (county) {
            return "county";
        }
        if (state) {
            return "state";
        }
        return null;
    }

    /**
     * The observation count the result rests on, which is not the returned row count when
     * the query aggregated. {@code SELECT corr(y, x), COUNT(*) AS n FROM ...} returns one
     * row describing 51 observations; reading n as 1 would flag every aggregate as tiny and
     * miss the case the flag is for.
     *
     * <p>Returns -1 when the result states a statistic without the count it rests on. That
     * is reported as an unknown n rather than assumed, because the whole point of the count
     * is to let a reader judge whether the coefficient means anything.
     */
    static int observationCount(ArrayNode rows, String sql) {
        if (rows == null) {
            return -1;
        }
        if (rows.size() != 1) {
            return rows.size();
        }
        JsonNode row = rows.get(0);
        java.util.Iterator<Map.Entry<String, JsonNode>> it = row.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            if (COUNT_COLUMN.matcher(e.getKey().toLowerCase(Locale.ROOT)).matches()
                    && e.getValue().isIntegralNumber()) {
                return e.getValue().asInt();
            }
        }
        // A single row that computed no aggregate really is a single observation.
        return anyStatAggregate(sql) ? -1 : 1;
    }

    // ── Declared primary keys (for fan-out detection) ─────────────────────────

    private static final ConcurrentHashMap<String, List<String>> PK_CACHE =
        new ConcurrentHashMap<>();

    private static final Pattern SAFE_IDENT = Pattern.compile("[a-z_][a-z0-9_]*");

    /**
     * Declared primary key of a table, lowercased, or an empty list when it declares none.
     * Cached because fan-out detection asks for every table a query names, on every call,
     * and these constraints do not change within a server lifetime.
     */
    static List<String> primaryKey(Connection conn, String schema, String table)
            throws java.sql.SQLException {
        String key = schema + "." + table;
        List<String> cached = PK_CACHE.get(key);
        if (cached != null) {
            return cached;
        }
        if (conn == null || !SAFE_IDENT.matcher(schema).matches()
                || !SAFE_IDENT.matcher(table).matches()) {
            return Collections.emptyList();
        }
        List<String> pk = new ArrayList<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT k.column_name FROM information_schema.key_column_usage k "
                 + "JOIN information_schema.table_constraints tc "
                 + "  ON k.constraint_name = tc.constraint_name "
                 + " AND k.table_schema = tc.table_schema "
                 + " AND k.table_name = tc.table_name "
                 // Both sides restated so each metadata scan prunes from its own predicates;
                 // without it the constraints scan walks every table in every schema.
                 + "WHERE lower(k.table_schema) = '" + schema + "' "
                 + "  AND lower(k.table_name) = '" + table + "' "
                 + "  AND lower(tc.table_schema) = '" + schema + "' "
                 + "  AND lower(tc.table_name) = '" + table + "' "
                 + "  AND tc.constraint_type = 'PRIMARY KEY' "
                 + "ORDER BY k.ordinal_position")) {
            while (rs.next()) {
                pk.add(rs.getString(1).toLowerCase(Locale.ROOT));
            }
        }
        List<String> immutable = Collections.unmodifiableList(pk);
        PK_CACHE.put(key, immutable);
        return immutable;
    }

    // ── Name matching where entity resolution exists ──────────────────────────

    /**
     * Ways to match one organisation name against another. LIKE is the visible member of the
     * family; every one of these has the same two failure modes, and the similarity functions
     * are worse in one respect — a threshold looks principled, so the result reads as measured
     * rather than guessed.
     */
    private static final String[][] NAME_MATCHERS = {
        {"(?:NOT\\s+)?LIKE", "LIKE"},
        {"(?:NOT\\s+)?ILIKE", "ILIKE"},
        {"(?:NOT\\s+)?SIMILAR\\s+TO", "SIMILAR TO"},
        {"!?~\\*?", "regex operator"},
        {"regexp_(?:matches|full_match|extract|replace)", "regexp function"},
        {"levenshtein|damerau_levenshtein|editdist3", "edit distance"},
        {"jaro_similarity|jaro_winkler_similarity|jaro|jaro_winkler", "Jaro/Jaro-Winkler"},
        {"hamming|mismatches", "Hamming distance"},
        {"jaccard", "Jaccard similarity"},
        {"similarity", "trigram similarity"},
        {"(?:array_|list_)?cosine_similarity|cosine_distance", "cosine similarity"},
        {"contains|starts_with|ends_with|strpos|instr|position", "substring test"},
    };

    private static final Pattern[] MATCHER_PATTERNS = new Pattern[NAME_MATCHERS.length];

    static {
        for (int i = 0; i < NAME_MATCHERS.length; i++) {
            MATCHER_PATTERNS[i] = Pattern.compile(
                "\\b" + NAME_MATCHERS[i][0] + "\\b|" + NAME_MATCHERS[i][0],
                Pattern.CASE_INSENSITIVE);
        }
    }

    /** Column-name fragments that mark a free-text organisation name. */
    private static final String[] ORG_NAME_HINTS = {
        "assignee_organization", "company_name", "legal_name", "org_name", "own_name",
        "owner_name", "lead_sponsor", "sponsor", "carrier_name", "borrower_name",
        "labeler_name", "recalling_firm", "firm_name", "entity_name", "registrant",
        "manufacturer", "employer_name", "operator_name", "parent_name", "conm",
    };

    private static boolean looksLikeOrgNameColumn(String col) {
        String c = col.toLowerCase(Locale.ROOT);
        for (String h : ORG_NAME_HINTS) {
            if (c.equals(h) || c.endsWith("_" + h) || c.contains(h)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Matching a company by {@code LIKE} is wrong in both directions at once, and looks right.
     *
     * <p>It over-matches: {@code LIKE '%CAPITAL ONE%'} pulls in "MAK Capital One L.L.C.", an
     * unrelated fund. And it under-matches, which is the half that does the damage — a firm's
     * filings are spread across subsidiaries whose names share no substring with the parent.
     * ExxonMobil's patents sit under "ExxonMobil Chemical Patents Inc." and "Exxon Research and
     * Engineering Company"; Capital One files 6,217 as "Capital One Services, LLC" and 215 as
     * "Capital One Financial Corporation". A pattern anchored on the registrant name finds the
     * 215 and reports it as the firm's patent output.
     *
     * <p>Neither failure raises an error. The query returns rows, the rows are real, and nothing
     * indicates which ones are missing — so this warning exists to say that a resolution path is
     * available, not that the query is malformed.
     */
    private static void nameMatchingWithoutResolution(String sql, ArrayNode warnings) {
        if (sql == null) {
            return;
        }
        // Find org-name columns in the statement, then read the text around each one for a
        // matching construct. Anchoring on the column rather than the operator is what lets
        // one pass cover infix operators (LIKE, ~) and function calls (levenshtein(...))
        // without a separate rule for each shape.
        List<String> hits = new ArrayList<>();
        List<String> methods = new ArrayList<>();
        Matcher ident = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*").matcher(sql);
        while (ident.find()) {
            String col = ident.group();
            if (!looksLikeOrgNameColumn(col)) {
                continue;
            }
            int from = Math.max(0, ident.start() - 90);
            int to = Math.min(sql.length(), ident.end() + 90);
            String window = sql.substring(from, to);
            for (int i = 0; i < MATCHER_PATTERNS.length; i++) {
                if (MATCHER_PATTERNS[i].matcher(window).find()) {
                    if (!hits.contains(col)) {
                        hits.add(col);
                    }
                    if (!methods.contains(NAME_MATCHERS[i][1])) {
                        methods.add(NAME_MATCHERS[i][1]);
                    }
                }
            }
        }
        if (hits.isEmpty()) {
            return;
        }
        StringBuilder note = new StringBuilder();
        note.append("This query identifies an organisation by matching its name — ")
            .append(String.join(", ", methods))
            .append(" on ")
            .append(String.join(", ", hits))
            .append(". That is wrong in two directions at once and neither raises an error. ")
            .append("It over-matches — a pattern on a common word pulls in unrelated entities ")
            .append("that happen to contain it. More seriously it UNDER-matches: a firm's rows ")
            .append("are spread across subsidiaries whose names share no substring with the ")
            .append("parent, so the result silently omits most of the firm and still looks ")
            .append("complete. ");
        note.append("**FIRST: check whether you need to match names at all.** ")
            .append("`ref.canonical_org_entity` is a wide table with one nullable foreign key ")
            .append("per source — sec_cik, patents_assignee_id, fec_committee_id, ")
            .append("eia_utility_id, fmcsa_dot_number, exempt_org_ein, ipeds_unitid and more, ")
            .append("each with a _confidence sibling. If you are defining a SET rather than ")
            .append("looking up one named company, select on those columns being non-null and ")
            .append("join to the source tables directly — an exact join with no matching, no ")
            .append("candidate review and no failure modes to reason about. Name matching is ")
            .append("for when a person handed you a name. ");
        note.append("IF YOU DO NEED TO MATCH NAMES, HOW MUCH CARE DEPENDS ON THE ROLE THE ")
            .append("ENTITY PLAYS. ");
        note.append("If the set of firms IS the study — you are ranking them, correlating ")
            .append("across them, or reporting a per-firm figure — resolution is the analysis, ")
            .append("not a lookup, and it belongs in its own step: ")
            .append("(1) call `resolve_entity` with a `terms` ARRAY for the whole cast at once ")
            .append("— it resolves them in a single scan and returns the inputs that matched ")
            .append("NOTHING, which is the half you cannot otherwise see; ")
            .append("(2) READ the candidates and reject the ones that are not your firm; ")
            .append("(3) expand each confirmed entity with `entity_relationships` (it also ")
            .append("takes a `leis` array) — subsidiaries rarely share a name with the parent, ")
            .append("so this is the only step that recovers what no name match can reach; ")
            .append("(4) pin the confirmed identifier set; ")
            .append("(5) run the analysis joining on those identifiers, with no name matching ")
            .append("anywhere in it; ")
            .append("(6) sanity-check the resolved totals against a figure you already know, ")
            .append("because expansion only reaches entities the ownership graph has edges for. ");
        note.append("Do NOT fold resolution into the analysis query. Fused, a wrong number ")
            .append("cannot be attributed — you cannot tell whether the analysis is wrong or ")
            .append("whether one of forty entities was a mismatch. And filtering rows out ")
            .append("afterwards only removes over-matches; the rows you are MISSING were never ")
            .append("in the result to remove. ");
        note.append("If the entity is incidental — one well-known firm used as a filter, or ")
            .append("context around a result that does not depend on the firm set being ")
            .append("complete — this is lighter. Resolve the one name, glance at what came ")
            .append("back, and proceed. The full sequence is not worth it for a filter, but say ")
            .append("in the write-up which you did, because a reader cannot tell a confirmed ")
            .append("entity set from a convenient one by looking at the numbers. ");
        note.append("**Resolution is a match, not a fact — read the candidates before using ")
            .append("them.** No resolver is precise: `high` confidence means the name ")
            .append("normalised to the same string, not that it is the organisation you meant. ")
            .append("\"CAPITAL ONE PARTNERS\" scores an exact normalised match against ")
            .append("\"Capital One\" and is a different company. A resolver moves the ")
            .append("judgement from string matching to candidate review; it does not remove it. ")
            .append("Where a name resolves to several entities, deduplicate before aggregating ")
            .append("or those firms are counted twice.");
        ObjectNode w = warning("name_matching_without_resolution", CAUTION, note.toString());
        ArrayNode cols = MAPPER.createArrayNode();
        for (String c : hits) {
            cols.add(c);
        }
        w.set("columns", cols);
        warnings.add(w);
    }

    // ── Geography level mixing ────────────────────────────────────────────────

    /** Geographic identifier columns whose NULLs mean "this row is at a coarser level". */
    private static final String[] GEO_ID_COLS = {
        "county_fips", "county_code", "county", "county_name", "tract", "tract_fips",
        "zcta", "zip", "zip_code", "cbsa", "cbsa_code", "place_fips", "place",
    };

    /**
     * Some tables stack levels in one relation: a state total, then that state's counties,
     * with the county identifier NULL on the coarser row. Both are legitimate rows, so a SUM
     * over them counts every county twice — once alone and once inside its state — and raises
     * no error.
     *
     * <p>This is a hazard in the RESULT, not necessarily a defect in the table. A table may
     * well expose a level column to filter on — {@code census.pep_population} has a real
     * {@code geography} partition column, and its mixed rows are correct — but a caller who
     * did not filter still holds a result that double-counts. The warning is aimed at that
     * result, and says how to filter rather than implying the table is broken.
     *
     * <p>Measured from the returned rows: a geographic identifier that is NULL in some and
     * populated in others is the signature. A column that is NULL everywhere is simply not
     * loaded and belongs to a different warning.
     */
    private static void geographyLevelMixing(String sql, ArrayNode rows, List<String> columns,
            ArrayNode warnings) {
        if (rows == null || rows.size() < 2) {
            return;
        }
        for (String raw : columns) {
            String c = raw.toLowerCase(Locale.ROOT);
            boolean isGeoId = false;
            for (String g : GEO_ID_COLS) {
                if (c.equals(g) || c.endsWith("_" + g)) {
                    isGeoId = true;
                    break;
                }
            }
            if (!isGeoId) {
                continue;
            }
            int nulls = 0;
            int filled = 0;
            for (JsonNode row : rows) {
                JsonNode v = row.get(raw);
                if (v == null || v.isNull() || v.asText().trim().isEmpty()) {
                    nulls++;
                } else {
                    filled++;
                }
            }
            if (nulls == 0 || filled == 0) {
                continue;
            }
            boolean additive = additiveAggregate(sql);
            ObjectNode w = warning("geography_level_mixing", additive ? HIGH : CAUTION,
                "This result mixes geographic levels in one column: " + raw + " is populated on "
                + filled + " row(s) and NULL on " + nulls + ". A NULL geographic identifier "
                + "normally means the row is a coarser total — a state row sitting beside its "
                + "own counties — so the finer rows are counted twice, once alone and once "
                + "inside the total. "
                + (additive ? "This result applies an additive aggregate over those rows, so "
                    + "the figure is inflated by the duplication and nothing errors. " : "")
                + "Filter to one level before aggregating: " + raw + " IS NOT NULL for the "
                + "finer grain, or " + raw + " IS NULL for the coarser one.");
            w.put("column", raw);
            w.put("rows_with_value", filled);
            w.put("rows_null", nulls);
            warnings.add(w);
            return;
        }
    }

    // ── Rollup contamination ──────────────────────────────────────────────────

    /** Exact dimension values that denote a pre-aggregated total rather than a part. */
    private static final Set<String> ROLLUP_EXACT = new HashSet<>(Arrays.asList(
        "total", "totals", "all", "overall", "any", "united states", "u.s.", "us", "usa",
        "national", "nation", "all sectors", "all industries", "all types", "all ages",
        "all races", "both sexes", "all workers", "all items", "all other", "combined"));

    /**
     * Census divisions and regions. These are rollups of states, but unlike "All Sectors" the
     * words are ordinary English — a column of time zones legitimately contains "Pacific" and
     * "Mountain". They therefore count as rollups only when the same column also holds real
     * state names, which is what makes the row a division sitting beside its own members.
     */
    private static final Set<String> CENSUS_ROLLUPS = new HashSet<>(Arrays.asList(
        "new england", "middle atlantic", "east north central", "west north central",
        "south atlantic", "east south central", "west south central", "mountain", "pacific",
        "northeast", "midwest", "south", "west"));

    private static final Set<String> STATE_NAMES = new HashSet<>(Arrays.asList(
        "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut",
        "delaware", "florida", "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa",
        "kansas", "kentucky", "louisiana", "maine", "maryland", "massachusetts", "michigan",
        "minnesota", "mississippi", "missouri", "montana", "nebraska", "nevada",
        "new hampshire", "new jersey", "new mexico", "new york", "north carolina",
        "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania", "rhode island",
        "south carolina", "south dakota", "tennessee", "texas", "utah", "vermont",
        "virginia", "washington", "west virginia", "wisconsin", "wyoming"));

    private static boolean looksLikeRollup(String v) {
        if (ROLLUP_EXACT.contains(v)) {
            return true;
        }
        return v.startsWith("total ") || v.startsWith("all ") || v.endsWith(" total")
            || v.endsWith(", total") || v.endsWith(" all");
    }

    private static boolean additiveAggregate(String sql) {
        return sql != null && ADDITIVE_AGG.matcher(sql).find();
    }

    /**
     * A tall table that keeps rollup rows in the same column as the parts they sum.
     *
     * <p>"All sectors" beside each sector, a United States row beside the states. Summing
     * without filtering to one level double-counts, silently, and the inflated figure still
     * looks plausible — this is the mechanism behind the ~26x overcount measured in
     * {@code energy.eia_electricity_generation}.
     *
     * <p>Only labelled rollups are detected. Numeric sentinels (a sector code of 99 meaning
     * "all") are indistinguishable from a real code without table metadata, so this warning
     * does not guess at them; its silence is not a claim that none are present.
     */
    private static void rollupContamination(String sql, ArrayNode rows, List<String> columns,
            ArrayNode warnings) {
        if (rows == null || rows.size() < 2) {
            return;
        }
        for (String raw : columns) {
            List<String> rollups = new ArrayList<>();
            int rollupRows = 0;
            int otherRows = 0;
            // Does this column hold real states? Only then does "Pacific" mean a census
            // division rather than a time zone.
            boolean hasStates = false;
            for (JsonNode row : rows) {
                JsonNode v = row.get(raw);
                if (v != null && v.isTextual()
                        && STATE_NAMES.contains(v.asText().trim().toLowerCase(Locale.ROOT))) {
                    hasStates = true;
                    break;
                }
            }
            for (JsonNode row : rows) {
                JsonNode v = row.get(raw);
                if (v == null || v.isNull() || !v.isTextual()) {
                    continue;
                }
                String val = v.asText().trim().toLowerCase(Locale.ROOT);
                if (val.isEmpty()) {
                    continue;
                }
                if (looksLikeRollup(val) || (hasStates && CENSUS_ROLLUPS.contains(val))) {
                    rollupRows++;
                    String label = v.asText().trim();
                    if (!rollups.contains(label) && rollups.size() < 6) {
                        rollups.add(label);
                    }
                } else {
                    otherRows++;
                }
            }
            if (rollupRows == 0 || otherRows == 0) {
                continue;
            }
            boolean additive = additiveAggregate(sql);
            ObjectNode w = warning("rollup_contamination", additive ? HIGH : CAUTION,
                "Column " + raw + " holds pre-aggregated rollup rows in the same column as the "
                + "parts they sum: " + String.join(", ", rollups) + " appears on " + rollupRows
                + " row(s) alongside " + otherRows + " other value(s). "
                + (additive
                    ? "This result sums or counts across them, so every part is counted twice "
                      + "— once alone and once inside its rollup — and no error is raised. "
                    : "Any SUM or COUNT over these rows will count every part twice, once alone "
                      + "and once inside its rollup. ")
                + "Filter to a single level before aggregating: exclude the rollup values, or "
                + "select only them if the total is what you want. Inspect the distinct values "
                + "of " + raw + " first — SELECT DISTINCT " + raw + " — rather than assuming "
                + "the column holds only leaves.");
            w.put("column", raw);
            ArrayNode found = MAPPER.createArrayNode();
            for (String r : rollups) {
                found.add(r);
            }
            w.set("rollup_values", found);
            w.put("rollup_rows", rollupRows);
            w.put("other_rows", otherRows);
            warnings.add(w);
            return;
        }
    }

    // ── Mixed unit kinds ──────────────────────────────────────────────────────

    /** FIPS code, full name and postal abbreviation for each non-state unit. */
    private static final String[][] NON_STATE_UNITS = {
        {"11", "district of columbia", "dc", "District of Columbia"},
        {"72", "puerto rico", "pr", "Puerto Rico"},
        {"60", "american samoa", "as", "American Samoa"},
        {"66", "guam", "gu", "Guam"},
        {"69", "northern mariana islands", "mp", "Northern Mariana Islands"},
        {"78", "united states virgin islands", "vi", "U.S. Virgin Islands"},
    };

    /**
     * Fifty states plus DC is not fifty-one states, and the difference is not pedantry.
     *
     * <p>DC is a city being compared against states. On density, land area and urbanisation it
     * is an extreme outlier — roughly nine times the density of the next unit — so a ranking or
     * a correlation can turn on it while looking like a statement about states. On many other
     * measures it is unremarkable and excluding it would throw away a real observation. Which
     * of those is true depends on the variable, so this warning reports the composition and
     * leaves the decision where it belongs, with the caller.
     *
     * <p>The territories are a different case: they are almost never wanted in a state
     * comparison and are usually present because {@code WHERE} said nothing rather than because
     * anyone chose them.
     *
     * <p>Fires only when the result is actually a cross-unit statistic. Listing DC in a table of
     * rows is not an error, and warning about it there would train the reader to skip the whole
     * diagnostics block.
     */
    private static void mixedUnitKinds(String sql, ArrayNode rows, List<String> columns,
            String grain, ArrayNode warnings) {
        if (rows == null || rows.size() == 0) {
            return;
        }
        boolean crossUnit = bivariateAssociation(sql) || anyStatAggregate(sql);
        // Detect by VALUE, not by column name. A state identifier travels under many names
        // across these schemas — state, state_fips, stusps, geo_name, name — and gating on a
        // known list means the detector goes quiet on exactly the tables nobody thought of.
        // "Puerto Rico" in a cell is the measurement; which header it sits under is not.
        List<String> idCols = new ArrayList<>();
        for (String raw : columns) {
            String c = raw.toLowerCase(Locale.ROOT);
            if (c.contains("state") || c.contains("geo") || c.contains("name")
                    || c.contains("fips") || c.contains("area")) {
                idCols.add(raw);
            }
        }
        if (idCols.isEmpty()) {
            return;
        }
        boolean dc = false;
        List<String> territories = new ArrayList<>();
        int matchedRows = 0;
        // Remember which column actually carried the non-state value, so the suggested fix
        // names this result's column rather than a plausible-looking guess.
        String hitCol = null;
        for (JsonNode row : rows) {
            boolean rowIsNonState = false;
            for (String col : idCols) {
                JsonNode v = row.get(col);
                if (v == null || v.isNull()) {
                    continue;
                }
                String val = v.asText().trim().toLowerCase(Locale.ROOT);
                for (String[] unit : NON_STATE_UNITS) {
                    if (val.equals(unit[0]) || val.equals(unit[1]) || val.equals(unit[2])) {
                        rowIsNonState = true;
                        if (hitCol == null) {
                            hitCol = col;
                        }
                        if ("11".equals(unit[0])) {
                            dc = true;
                        } else if (!territories.contains(unit[3])) {
                            territories.add(unit[3]);
                        }
                    }
                }
            }
            if (rowIsNonState) {
                matchedRows++;
            }
        }
        if (!dc && territories.isEmpty()) {
            return;
        }
        // At county grain DC is one county-equivalent among 3,144 and is not an outlier of
        // kind; warning there would be noise on every national county query.
        if ("county".equals(grain) || "tract".equals(grain) || "zcta".equals(grain)) {
            return;
        }
        int total = rows.size();
        int states = total - matchedRows;
        StringBuilder note = new StringBuilder();
        String severity;
        if (!territories.isEmpty()) {
            severity = CAUTION;
            note.append("This result mixes states with ")
                .append(String.join(", ", territories))
                .append(dc ? " and the District of Columbia" : "")
                .append(". The territories are not states and are rarely wanted in a "
                    + "state comparison — they are usually present because the WHERE clause "
                    + "said nothing, not because anyone chose them. ");
        } else {
            severity = crossUnit ? CAUTION : INFO;
            note.append("This result includes the District of Columbia alongside states. ");
        }
        if (dc) {
            note.append("DC is a city measured against states: an extreme outlier on density, "
                + "land area and urbanisation, and unremarkable on many other measures. Which "
                + "it is here depends on the variable, so check whether it carries the result "
                + "rather than assuming either way. ");
        }
        note.append("Composition: ").append(total).append(" units — ").append(states)
            .append(" states");
        if (dc) {
            note.append(" + DC");
        }
        if (!territories.isEmpty()) {
            note.append(" + ").append(territories.size()).append(" territor")
                .append(territories.size() == 1 ? "y" : "ies");
        }
        note.append(". ");
        if (crossUnit) {
            note.append("Because this result is a statistic computed ACROSS those units, the "
                + "composition is part of the answer: report n, and report the statistic with "
                + "and without any unit that is not the same kind of thing as the rest. ");
        }
        String col = hitCol == null ? "state_fips" : hitCol;
        note.append("To restrict to the 50 states, filter on ").append(col)
            .append(": the territories are FIPS 60, 66, 69, 72 and 78, and DC is 11 ")
            .append("(by name, exclude ").append(dc ? "'District of Columbia'" : "")
            .append(dc && !territories.isEmpty() ? " and " : "")
            .append(territories.isEmpty() ? "" : "'" + String.join("', '", territories) + "'")
            .append(").");
        ObjectNode w = warning("mixed_unit_kinds", severity, note.toString());
        w.put("units_total", total);
        w.put("states", states);
        w.put("includes_dc", dc);
        w.put("unit_column", col);
        ArrayNode terr = MAPPER.createArrayNode();
        for (String t : territories) {
            terr.add(t);
        }
        w.set("territories", terr);
        warnings.add(w);
    }

    // ── Envelope assembly ─────────────────────────────────────────────────────

    private static ObjectNode warning(String type, String severity, String note) {
        ObjectNode w = MAPPER.createObjectNode();
        w.put("type", type);
        w.put("severity", severity);
        w.put("note", note);
        return w;
    }

    private static ObjectNode envelope(ArrayNode warnings) {
        ObjectNode diag = MAPPER.createObjectNode();
        diag.set("warnings", warnings);
        diag.put("basis", BASIS_NOTE);
        ObjectNode out = MAPPER.createObjectNode();
        out.set("diagnostics", diag);
        return out;
    }

    /**
     * Diagnostics for a {@code query} result.
     *
     * @param conn      catalog connection, used only to read declared primary keys; null
     *                  disables fan-out detection rather than failing the call
     * @param sql       the SQL as the caller wrote it
     * @param rows      the returned rows
     * @param rowLimit  the cap applied to this call, so a result that hit the cap is not
     *                  read as a complete count of units
     */
    static ObjectNode forQuery(Connection conn, String sql, ArrayNode rows, int rowLimit) {
        ArrayNode warnings = MAPPER.createArrayNode();
        List<String> columns = columnsOf(rows);
        String grain = grainOf(columns);
        int n = observationCount(rows, sql);
        boolean capped = rows != null && rowLimit > 0 && rows.size() >= rowLimit;

        emptyOrLowCoverage(sql, rows, warnings);
        coveragePercentColumns(rows, columns, warnings);
        outOfCoverageYears(sql, rows, warnings);
        smallNAndGrain(sql, grain, n, capped, warnings);
        uncontrolledAssociation(sql, warnings);
        brokenFields(rows, columns, warnings);
        rowFanout(conn, sql, rows, columns, warnings);
        vintageMisalignment(sql, warnings);
        mixedUnitKinds(sql, rows, columns, grain, warnings);
        geographyLevelMixing(sql, rows, columns, warnings);
        rollupContamination(sql, rows, columns, warnings);
        nameMatchingWithoutResolution(sql, warnings);

        ObjectNode out = envelope(warnings);
        ObjectNode diag = (ObjectNode) out.get("diagnostics");
        if (grain != null) {
            diag.put("grain", grain);
        }
        if (n >= 0) {
            diag.put("n", n);
            diag.put("n_basis", rows != null && rows.size() == 1 && anyStatAggregate(sql)
                ? "count column in the aggregate row"
                : (capped ? "rows returned, capped at the limit — a floor, not a count"
                          : "rows returned"));
        } else {
            diag.putNull("n");
            diag.put("n_basis",
                "not reported — this result states a statistic without the observation count "
                + "it rests on. Add COUNT(*) AS n so the coefficient can be judged.");
        }
        ObjectNode vintage = vintageBlock(sql);
        if (vintage != null) {
            diag.set("vintage", vintage);
        }
        return out;
    }

    // ── Individual checks ─────────────────────────────────────────────────────

    private static void emptyOrLowCoverage(String sql, ArrayNode rows, ArrayNode warnings) {
        if (rows != null && rows.size() == 0) {
            warnings.add(warning("low_coverage", HIGH,
                "The query returned no rows. This snapshot is versioned, not live: an empty "
                + "result outside a table's declared coverage window means the source has not "
                + "published that period, which is not the same as a zero. Call describe_table "
                + "to read the window before reporting this as an absence, and do not "
                + "substitute a figure from outside this corpus."));
        }
    }

    private static void coveragePercentColumns(ArrayNode rows, List<String> columns,
            ArrayNode warnings) {
        if (rows == null || rows.size() == 0) {
            return;
        }
        for (String col : columns) {
            String lower = col.toLowerCase(Locale.ROOT);
            if (!lower.contains("coverage")) {
                continue;
            }
            double sum = 0;
            int count = 0;
            for (JsonNode row : rows) {
                JsonNode v = row.get(col);
                if (v != null && v.isNumber()) {
                    sum += v.asDouble();
                    count++;
                }
            }
            if (count > 0 && sum / count < LOW_COVERAGE_PCT) {
                ObjectNode w = warning("low_coverage", CAUTION,
                    "The universe behind these rows is partial, so totals computed from them "
                    + "understate the true total and are not comparable across units whose "
                    + "coverage differs.");
                w.put("column", col);
                w.put("mean_coverage", Math.round(sum / count * 100.0) / 100.0);
                warnings.add(w);
            }
        }
    }

    private static void outOfCoverageYears(String sql, ArrayNode rows, ArrayNode warnings) {
        Set<Integer> years = yearLiterals(sql);
        if (years.isEmpty()) {
            return;
        }
        boolean empty = rows != null && rows.size() == 0;
        for (String ref : referencedTables(sql)) {
            String[] parts = ref.split("\\.", 2);
            ObjectNode cov = Catalog.coverage(parts[0], parts[1]);
            if (cov == null || !cov.has("first_year") || !cov.has("last_year")) {
                continue;
            }
            int first = cov.get("first_year").asInt();
            int last = cov.get("last_year").asInt();
            for (Integer y : years) {
                if (y.intValue() >= first && y.intValue() <= last) {
                    continue;
                }
                ObjectNode w = warning("low_coverage", empty ? HIGH : CAUTION,
                    "The SQL names a year outside this table's declared coverage window. Rows "
                    + "for it were never ingested, so their absence says nothing about the "
                    + "underlying quantity — report it as not published rather than as zero or "
                    + "as a decline.");
                w.put("table", ref);
                w.put("year", y.intValue());
                w.put("declared_first_year", first);
                w.put("declared_last_year", last);
                warnings.add(w);
            }
        }
    }

    private static void smallNAndGrain(String sql, String grain, int n, boolean capped,
            ArrayNode warnings) {
        boolean causalShape = bivariateAssociation(sql) || anyStatAggregate(sql) || grain != null;
        if (!causalShape || n < 0) {
            return;
        }
        // A result that filled its row cap reports a floor on the unit count, not the count,
        // so it cannot support a claim that there are few units.
        if (n > 0 && n <= SMALL_N && !capped) {
            ObjectNode w = warning("small_n", CAUTION,
                "This analysis rests on few units. At this count, an estimate is compatible "
                + "with a wide range of true values and cannot separate more candidate "
                + "explanations than it has observations.");
            w.put("n", n);
            warnings.add(w);
        }
        if (grain != null && (bivariateAssociation(sql) || anyStatAggregate(sql))
                && n > 0 && n <= SMALL_N && !capped) {
            ObjectNode w = warning("grain_mismatch", CAUTION,
                "The unit of analysis is " + grain + ". An association measured across " + n
                + " " + grain + " units is a statement about " + grain + "s, not about the "
                + "people or entities inside them — inferring the latter from the former is "
                + "the ecological fallacy. A finer grain, where one is available for both "
                + "series, admits more units and fewer between-unit confounds.");
            w.put("grain", grain);
            w.put("n", n);
            warnings.add(w);
        }
    }

    /**
     * A SQL association aggregate conditions on nothing, by construction: {@code corr} and
     * the {@code regr_*} family take exactly two arguments, so there is nowhere to put a
     * covariate. That makes "this coefficient is unadjusted" a fact about the operator rather
     * than a judgment about the model.
     */
    private static void uncontrolledAssociation(String sql, ArrayNode warnings) {
        if (!bivariateAssociation(sql)) {
            return;
        }
        warnings.add(warning("uncontrolled_confound", CAUTION,
            "corr() and the regr_*() aggregates take exactly two columns, so this result is "
            + "not conditioned on any covariate — anything that moves both series produces the "
            + "same coefficient as a direct relationship would. To adjust for covariates, use "
            + "ols_regression; for repeated observations of the same unit, panel_fixed_effects "
            + "or robust_regression with cluster_col."));
    }

    private static void brokenFields(ArrayNode rows, List<String> columns, ArrayNode warnings) {
        if (rows == null || rows.size() == 0) {
            return;
        }
        int total = rows.size();
        for (String col : columns) {
            String lower = col.toLowerCase(Locale.ROOT);
            int nulls = 0;
            int outOfDomain = 0;
            Double example = null;
            boolean proportion = (lower.contains("pct") || lower.contains("percent")
                || lower.contains("share"))
                // A percent *change* is legitimately negative and legitimately over 100.
                && !lower.contains("change") && !lower.contains("growth")
                && !lower.contains("delta") && !lower.contains("diff");
            boolean count = lower.equals("population") || lower.endsWith("_count")
                || lower.startsWith("count_") || lower.endsWith("_total")
                || lower.startsWith("total_");
            for (JsonNode row : rows) {
                JsonNode v = row.get(col);
                if (v == null || v.isNull()) {
                    nulls++;
                    continue;
                }
                if (!v.isNumber()) {
                    continue;
                }
                double d = v.asDouble();
                if ((proportion && (d < 0 || d > 100)) || (count && d < 0)) {
                    outOfDomain++;
                    if (example == null) {
                        example = Double.valueOf(d);
                    }
                }
            }
            if (outOfDomain > 0) {
                ObjectNode w = warning("broken_field", HIGH,
                    "Values in this column fall outside the domain its name declares. Treat the "
                    + "column as unreliable for this query rather than reading the out-of-range "
                    + "values as extremes.");
                w.put("column", col);
                w.put("issue", "out_of_domain");
                w.put("rows_affected", outOfDomain);
                w.put("example_value", example.doubleValue());
                warnings.add(w);
            }
            if (total >= 20 && nulls >= (int) Math.ceil(NULL_DOMINANT * total)) {
                ObjectNode w = warning("broken_field", CAUTION,
                    "This column is almost entirely null in the returned rows. Any aggregate "
                    + "over it describes the small non-null remainder, not the population the "
                    + "query selected.");
                w.put("column", col);
                w.put("issue", "null_dominant");
                w.put("null_rows", nulls);
                w.put("total_rows", total);
                warnings.add(w);
            }
        }
    }

    /**
     * A join that produced more than one row per declared key. The defect is silent by
     * construction: joining {@code geo.counties} on {@code county_fips} alone, when its
     * declared key is {@code (county_fips, year)} and it holds one copy of every county per
     * TIGER vintage, multiplies every downstream sum by the number of vintages with no error
     * and no visible symptom.
     *
     * <p>Only fires when the query joins and does not collapse rows itself — a plain scan at
     * a table's own grain, or a GROUP BY that deliberately aggregates duplicates away, is not
     * this defect.
     */
    private static void rowFanout(Connection conn, String sql, ArrayNode rows,
            List<String> columns, ArrayNode warnings) {
        if (conn == null || rows == null || rows.size() < 2 || !hasJoin(sql)
                || collapsesRows(sql)) {
            return;
        }
        for (String ref : referencedTables(sql)) {
            String[] parts = ref.split("\\.", 2);
            List<String> pk;
            try {
                pk = primaryKey(conn, parts[0], parts[1]);
            } catch (java.sql.SQLException e) {
                // Reported in the envelope by the caller rather than swallowed here.
                throw new DiagnosticsException("primary key lookup failed for " + ref, e);
            }
            ObjectNode w = detectFanout(ref, pk, rows, columns);
            if (w != null) {
                warnings.add(w);
            }
        }
    }

    /**
     * The fan-out measurement itself, separated from where the declared key comes from so it
     * can be exercised against a known key without a live catalog.
     *
     * @return the warning, or null when this table's rows show no duplication
     */
    static ObjectNode detectFanout(String table, List<String> pk, ArrayNode rows,
            List<String> columns) {
        if (pk == null || pk.size() < 2 || rows == null || rows.size() < 2) {
            return null;
        }
        Map<String, String> lowerToActual = new HashMap<>();
        for (String c : columns) {
            lowerToActual.put(c.toLowerCase(Locale.ROOT), c);
        }
        List<String> present = new ArrayList<>();
        List<String> missing = new ArrayList<>();
        for (String k : pk) {
            if (lowerToActual.containsKey(k)) {
                present.add(lowerToActual.get(k));
            } else {
                missing.add(k);
            }
        }
        // Fan-out is only detectable when part of the key survived into the result and
        // part did not — a fully present key cannot duplicate, and a fully absent one
        // leaves nothing to count by.
        if (present.isEmpty() || missing.isEmpty()) {
            return null;
        }
        Map<String, Integer> counts = new LinkedHashMap<>();
        int max = 0;
        String worst = null;
        for (JsonNode row : rows) {
            StringBuilder key = new StringBuilder();
            for (String c : present) {
                JsonNode v = row.get(c);
                // Unit separator, so two key parts cannot run together into a
                // third key's string and hide a duplication behind a false
                // distinct-key count.
                key.append(v == null || v.isNull() ? NULL_KEY : v.asText())
                    .append(KEY_SEP);
            }
            String k = key.toString();
            Integer prev = counts.get(k);
            int next = (prev == null ? 0 : prev.intValue()) + 1;
            counts.put(k, Integer.valueOf(next));
            if (next > max) {
                max = next;
                worst = k;
            }
        }
        if (max <= 1) {
            return null;
        }
        ObjectNode w = warning("row_fanout", HIGH,
            "The join produced more than one row per declared key of " + table + ", "
            + "because " + String.join(", ", missing) + " is part of that key and is "
            + "not in the join or the result. Every SUM, COUNT, or AVG over these rows "
            + "is multiplied by the duplication. Add the missing key column(s) to the "
            + "join condition, or filter " + table + " to one value of them.");
        w.put("table", table);
        ArrayNode keyCols = MAPPER.createArrayNode();
        for (String c : present) {
            keyCols.add(c);
        }
        w.set("key", keyCols);
        ArrayNode missingCols = MAPPER.createArrayNode();
        for (String c : missing) {
            missingCols.add(c);
        }
        w.set("missing_key_columns", missingCols);
        w.put("max_rows_per_key", max);
        w.put("distinct_keys", counts.size());
        if (worst != null) {
            w.put("example_key", worst.replace(KEY_SEP, "|").replace(NULL_KEY, "?"));
        }
        return w;
    }

    /** Declared coverage windows of the tables a query joins, when they differ. */
    private static void vintageMisalignment(String sql, ArrayNode warnings) {
        if (!hasJoin(sql)) {
            return;
        }
        Map<String, int[]> windows = declaredWindows(sql);
        if (windows.size() < 2) {
            return;
        }
        int overlapFirst = Integer.MIN_VALUE;
        int overlapLast = Integer.MAX_VALUE;
        boolean differ = false;
        int[] previous = null;
        for (int[] w : windows.values()) {
            overlapFirst = Math.max(overlapFirst, w[0]);
            overlapLast = Math.min(overlapLast, w[1]);
            if (previous != null && (previous[0] != w[0] || previous[1] != w[1])) {
                differ = true;
            }
            previous = w;
        }
        if (!differ) {
            return;
        }
        boolean disjoint = overlapFirst > overlapLast;
        ObjectNode w = warning("vintage_misalignment", disjoint ? HIGH : INFO,
            disjoint
                ? "The joined tables have no year in common, so any row this join produces "
                  + "pairs observations from different periods."
                : "The joined tables cover different year ranges. Outside the overlap, one side "
                  + "contributes no rows, so a trend or comparison spanning the full range "
                  + "changes composition partway through rather than measuring a change in the "
                  + "quantity.");
        ObjectNode tables = MAPPER.createObjectNode();
        for (Map.Entry<String, int[]> e : windows.entrySet()) {
            tables.put(e.getKey(), e.getValue()[0] + "-" + e.getValue()[1]);
        }
        w.set("declared_windows", tables);
        if (!disjoint) {
            w.put("overlap", overlapFirst + "-" + overlapLast);
        }
        warnings.add(w);
    }

    private static Map<String, int[]> declaredWindows(String sql) {
        Map<String, int[]> windows = new LinkedHashMap<>();
        for (String ref : referencedTables(sql)) {
            String[] parts = ref.split("\\.", 2);
            ObjectNode cov = Catalog.coverage(parts[0], parts[1]);
            if (cov != null && cov.has("first_year") && cov.has("last_year")) {
                windows.put(ref, new int[]{
                    cov.get("first_year").asInt(), cov.get("last_year").asInt()});
            }
        }
        return windows;
    }

    /** Declared coverage of every table the query names, for the envelope's vintage block. */
    private static ObjectNode vintageBlock(String sql) {
        Map<String, int[]> windows = declaredWindows(sql);
        if (windows.isEmpty()) {
            return null;
        }
        ObjectNode out = MAPPER.createObjectNode();
        out.put("basis", "declared ingest windows from the schema definitions, not a row scan");
        ObjectNode tables = MAPPER.createObjectNode();
        for (Map.Entry<String, int[]> e : windows.entrySet()) {
            tables.put(e.getKey(), e.getValue()[0] + "-" + e.getValue()[1]);
        }
        out.set("tables", tables);
        return out;
    }

    // ── Statistical tool results ──────────────────────────────────────────────

    /**
     * Diagnostics for one of the matrix-algebra stats tools. These see facts the
     * {@code query} path cannot: the exact observation count after null-dropping, how many
     * rows the drop removed, and the covariate matrix itself — which is what makes
     * collinearity between two requested controls measurable rather than guessed at.
     *
     * @param sql          the SQL the tool ran to build its design matrix
     * @param covariates   names of the columns entered as adjustment (predictors/controls)
     * @param covariateCols the covariate columns, row-major, as the estimator saw them
     * @param n            observations actually used
     * @param totalRows    rows the SQL returned before null-dropping
     * @param dropped      rows dropped for a null in a required column
     */
    static ObjectNode forExtraction(String sql, List<String> covariates,
            double[][] covariateCols, int n, int totalRows, int dropped) {
        ArrayNode warnings = MAPPER.createArrayNode();

        if (n > 0 && n <= SMALL_N) {
            ObjectNode w = warning("small_n", CAUTION,
                "This estimate rests on few observations. Standard errors here are wide even "
                + "when the point estimate looks decisive, and a model cannot separate more "
                + "candidate explanations than it has observations.");
            w.put("n", n);
            warnings.add(w);
        }

        if (covariates != null && covariates.size() == 1) {
            warnings.add(warning("uncontrolled_confound", CAUTION,
                "One predictor and no other covariate: this estimate is not conditioned on "
                + "anything, so it carries the effect of every unmodelled variable correlated "
                + "with both sides. Naming the one or two structural confounds that matter "
                + "changes what the coefficient means."));
        }

        if (totalRows > 0 && dropped > 0) {
            double share = (double) dropped / totalRows;
            if (share >= 0.2) {
                ObjectNode w = warning("broken_field", share >= 0.5 ? HIGH : CAUTION,
                    "A large share of the rows the SQL returned were dropped for a null in a "
                    + "required column. The estimate describes the surviving rows, which are "
                    + "not a random sample of the ones selected unless the nulls are unrelated "
                    + "to the outcome.");
                w.put("issue", "null_dominant");
                w.put("rows_returned_by_sql", totalRows);
                w.put("rows_dropped_for_null", dropped);
                w.put("dropped_share", Math.round(share * 1000.0) / 1000.0);
                warnings.add(w);
            }
        }

        collinearCovariates(covariates, covariateCols, warnings);
        outOfCoverageYears(sql, null, warnings);
        vintageMisalignment(sql, warnings);

        ObjectNode out = envelope(warnings);
        ObjectNode diag = (ObjectNode) out.get("diagnostics");
        diag.put("n", n);
        diag.put("n_basis", "observations entering the estimator, after null-dropping");
        ObjectNode vintage = vintageBlock(sql);
        if (vintage != null) {
            diag.set("vintage", vintage);
        }
        return out;
    }

    /**
     * Near-duplicate covariates, measured as a pairwise Pearson correlation on the design
     * matrix. Two controls at r above the threshold divide one variable's explanatory work
     * between them: individual coefficients and their standard errors become unstable while
     * the fit barely moves, which reads as "neither matters" rather than "these are the same
     * variable twice".
     */
    private static void collinearCovariates(List<String> covariates, double[][] cols,
            ArrayNode warnings) {
        if (covariates == null || cols == null || covariates.size() < 2 || cols.length < 3) {
            return;
        }
        int k = covariates.size();
        for (int a = 0; a < k; a++) {
            for (int b = a + 1; b < k; b++) {
                double r = pearson(cols, a, b);
                if (Double.isNaN(r) || Math.abs(r) < COLLINEAR_R) {
                    continue;
                }
                ObjectNode w = warning("collinear_controls", CAUTION,
                    "These two covariates are near-duplicates of each other in this sample. "
                    + "Their individual coefficients and standard errors are unstable — the "
                    + "pair explains jointly what neither can be credited with separately. "
                    + "Drop one, or combine them, unless you specifically need both.");
                ArrayNode pair = MAPPER.createArrayNode();
                pair.add(covariates.get(a));
                pair.add(covariates.get(b));
                w.set("covariates", pair);
                w.put("r", Math.round(r * 1000.0) / 1000.0);
                warnings.add(w);
            }
        }
    }

    /** Pearson correlation between two columns of a row-major matrix; NaN if either is constant. */
    private static double pearson(double[][] rows, int a, int b) {
        int n = rows.length;
        double sumA = 0;
        double sumB = 0;
        for (double[] row : rows) {
            sumA += row[a];
            sumB += row[b];
        }
        double meanA = sumA / n;
        double meanB = sumB / n;
        double cov = 0;
        double varA = 0;
        double varB = 0;
        for (double[] row : rows) {
            double da = row[a] - meanA;
            double db = row[b] - meanB;
            cov += da * db;
            varA += da * da;
            varB += db * db;
        }
        if (varA <= 0 || varB <= 0) {
            return Double.NaN;
        }
        return cov / Math.sqrt(varA * varB);
    }

    // ── Refusals ──────────────────────────────────────────────────────────────

    /**
     * The envelope attached to a hard refusal. Refusal is reserved for the un-runnable or
     * corrupt-by-construction — a statistic that cannot compute, not a question that is
     * merely imperfect, which still gets its data plus warnings.
     */
    static ObjectNode forRefusal(String type, String note, String runnableAlternative) {
        ArrayNode warnings = MAPPER.createArrayNode();
        ObjectNode w = warning(type, HIGH, note);
        w.put("runnable_alternative", runnableAlternative);
        warnings.add(w);
        ObjectNode out = envelope(warnings);
        ((ObjectNode) out.get("diagnostics")).put("refused", true);
        return out;
    }

    /**
     * True when a statistical aggregate could not be evaluated at all.
     *
     * <p>This used to mean "the inputs span two schemas, so it cannot push down", and the
     * remedy was to align the series elsewhere. That is no longer the situation: these
     * aggregates have Java implementations, so a query that cannot push down simply runs
     * locally and returns the same answer. Reaching here means BOTH paths failed, which is a
     * defect rather than a shape the caller should work around.
     */
    static boolean isPushdownFailure(String compactMessage) {
        return compactMessage != null
            && compactMessage.contains("could not be evaluated");
    }

    // ── critique_query ────────────────────────────────────────────────────────

    /**
     * Form-level critique of a proposed query, without running it. Everything here is read
     * off the SQL text and the declared catalog, so it costs nothing and can be called
     * before an expensive query rather than after it. It reports defects it can see; it does
     * not rewrite the question, and silence from it is not approval.
     */
    static ObjectNode critique(Connection conn, String sql) {
        ArrayNode warnings = MAPPER.createArrayNode();
        Set<String> tables = referencedTables(sql);

        if (tables.isEmpty()) {
            warnings.add(warning("no_pushdown", HIGH,
                "The SQL names no schema-qualified table. Tables must be referenced as "
                + "schema.table (e.g. sec.filing_metadata); call list_schemas or "
                + "search_catalog to find the right one."));
        }

        if (bivariateAssociation(sql)
                && !Pattern.compile("(?i)\\bCOUNT\\s*\\(").matcher(sql).find()
                && !Pattern.compile("(?i)\\bregr_count\\s*\\(").matcher(sql).find()) {
            warnings.add(warning("small_n", CAUTION,
                "This computes an association but returns no observation count, so the result "
                + "cannot be judged for significance. Add COUNT(*) AS n."));
        }

        if (bivariateAssociation(sql)) {
            uncontrolledAssociation(sql, warnings);
        }

        // A <> or = against a nullable column drops every NULL row under three-valued logic,
        // which silently narrows the universe rather than filtering within it.
        if (Pattern.compile("(?i)<>\\s*'").matcher(sql).find()
                && !Pattern.compile("(?i)IS\\s+NULL").matcher(sql).find()) {
            warnings.add(warning("broken_field", CAUTION,
                "A <> comparison against a nullable column drops every NULL row (standard SQL "
                + "three-valued logic), so the result is a subset of what the filter reads as "
                + "selecting. Add OR <column> IS NULL if NULLs belong in the universe."));
        }

        outOfCoverageYears(sql, null, warnings);
        vintageMisalignment(sql, warnings);
        partialKeyJoin(conn, sql, warnings);

        ObjectNode out = envelope(warnings);
        ObjectNode diag = (ObjectNode) out.get("diagnostics");
        ArrayNode refs = MAPPER.createArrayNode();
        for (String t : tables) {
            refs.add(t);
        }
        diag.set("tables", refs);
        ObjectNode vintage = vintageBlock(sql);
        if (vintage != null) {
            diag.set("vintage", vintage);
        }
        diag.put("checked", "form and declared catalog only — this query was NOT executed, so "
            + "defects that only appear in the returned rows (fan-out, out-of-domain values, "
            + "null-dominant columns) are not covered here. Run the query to get those.");
        return out;
    }

    /**
     * A join whose ON clause names only part of a multi-column declared key. This is the
     * fan-out defect caught before it runs: the query has not executed, so there are no rows
     * to count, but the missing key column is visible in the SQL.
     */
    private static void partialKeyJoin(Connection conn, String sql, ArrayNode warnings) {
        if (conn == null || !hasJoin(sql)) {
            return;
        }
        for (String ref : referencedTables(sql)) {
            String[] parts = ref.split("\\.", 2);
            List<String> pk;
            try {
                pk = primaryKey(conn, parts[0], parts[1]);
            } catch (java.sql.SQLException e) {
                throw new DiagnosticsException("primary key lookup failed for " + ref, e);
            }
            ObjectNode w = detectPartialKeyJoin(ref, pk, sql);
            if (w != null) {
                warnings.add(w);
            }
        }
    }

    /**
     * The partial-key test itself, separated from the catalog lookup so it can be exercised
     * against a known key. A key column the SQL never names cannot be in the join condition,
     * which is what makes this readable off the text alone.
     *
     * @return the warning, or null when the SQL names the whole key or none of it
     */
    static ObjectNode detectPartialKeyJoin(String table, List<String> pk, String sql) {
        if (pk == null || pk.size() < 2 || sql == null) {
            return null;
        }
        String lower = sql.toLowerCase(Locale.ROOT);
        List<String> missing = new ArrayList<>();
        int mentioned = 0;
        for (String k : pk) {
            if (Pattern.compile("(?i)\\b" + Pattern.quote(k) + "\\b").matcher(lower).find()) {
                mentioned++;
            } else {
                missing.add(k);
            }
        }
        if (mentioned == 0 || missing.isEmpty()) {
            return null;
        }
        ObjectNode w = warning("row_fanout", CAUTION,
            "The declared key of " + table + " has " + pk.size() + " columns and this SQL "
            + "names only " + mentioned + " of them. If the join keys on the partial "
            + "key, it will return one row per value of the missing column(s) and "
            + "multiply every downstream SUM, COUNT, and AVG — silently, with no error. "
            + "Either join on the full key or filter " + table + " to a single value of "
            + "the missing column(s).");
        w.put("table", table);
        ArrayNode pkCols = MAPPER.createArrayNode();
        for (String c : pk) {
            pkCols.add(c);
        }
        w.set("declared_key", pkCols);
        ArrayNode missingCols = MAPPER.createArrayNode();
        for (String c : missing) {
            missingCols.add(c);
        }
        w.set("key_columns_not_named", missingCols);
        return w;
    }

    /**
     * Envelope for the case where a diagnostic check itself failed. The failure is reported
     * rather than dropped: a silently-empty warnings array is indistinguishable from a clean
     * result, which would make a broken check look like a passing one.
     */
    static ObjectNode incomplete(String reason) {
        ArrayNode warnings = MAPPER.createArrayNode();
        ObjectNode w = warning("diagnostics_incomplete", INFO,
            "A diagnostic check did not complete, so this result carries no diagnostics. "
            + "Absence of warnings here reflects the failed check, not a clean result.");
        w.put("reason", reason);
        warnings.add(w);
        return envelope(warnings);
    }

    /** Raised when a diagnostic check cannot complete; surfaced, never swallowed. */
    static final class DiagnosticsException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        DiagnosticsException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
