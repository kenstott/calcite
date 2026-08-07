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
     * True when an error is the cross-schema push-down failure. Both shapes reduce to the
     * same limitation: a statistical aggregate's inputs span more than one govdata schema,
     * and each schema is its own DuckDB catalog, so the join cannot be pushed down as one
     * query and the aggregate has nowhere to run.
     */
    static boolean isPushdownFailure(String compactMessage) {
        return compactMessage != null
            && compactMessage.contains("failed to push down to the DuckDB engine");
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
