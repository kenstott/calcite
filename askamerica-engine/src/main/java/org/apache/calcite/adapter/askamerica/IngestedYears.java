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
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Years actually present in a table, measured once per session and held in memory.
 *
 * <p>The declared window in {@link Catalog#coverage} says what the schema intends to
 * ingest; it can diverge from reality in both directions — a backfill still running,
 * or a year loaded ahead of the declared lag. Only a scan settles it, and a scan is
 * too slow to sit in the path of a {@code describe_table} call.
 *
 * <p>So the measurement is lazy and out of band: the first {@code describe_table} for a
 * table schedules a probe and returns the declared window alone; once the probe lands,
 * every later call for that table also carries the observed window. Probes run one at a
 * time on a single daemon thread, so a burst of describes cannot stampede the warehouse,
 * and a failed probe is recorded as failed rather than retried on every call.
 */
final class IngestedYears {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Terminal states are cached too — a probe that failed must not be re-run per call. */
    private static final Map<String, Result> CACHE = new ConcurrentHashMap<>();

    private static final ExecutorService PROBES =
        Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "askamerica-year-probe");
                t.setDaemon(true);
                return t;
            }
        });

    private IngestedYears() {
    }

    /** Outcome of one probe: measured bounds, or the reason there are none. */
    private static final class Result {
        final Integer first;
        final Integer last;
        final String status;

        Result(Integer first, Integer last, String status) {
            this.first = first;
            this.last = last;
            this.status = status;
        }
    }

    private static String key(String schema, String table) {
        return schema + "." + table;
    }

    /**
     * Observed window for a table, or null if no probe has completed yet. Schedules the
     * probe on first ask, so the answer is available to the next caller.
     */
    static ObjectNode observed(String schema, String table, String column) {
        String k = key(schema, table);
        Result r = CACHE.get(k);
        if (r == null) {
            schedule(k, schema, table, column);
            return null;
        }
        if (r.status != null) {
            ObjectNode out = MAPPER.createObjectNode();
            out.put("status", r.status);
            return out;
        }
        ObjectNode out = MAPPER.createObjectNode();
        if (r.first != null) {
            out.put("first_year", r.first);
        }
        if (r.last != null) {
            out.put("last_year", r.last);
        }
        out.put("status", "measured");
        return out;
    }

    /** Reserve the slot before submitting, so concurrent describes schedule one probe. */
    private static void schedule(final String k, final String schema, final String table,
            final String column) {
        if (CACHE.putIfAbsent(k, new Result(null, null, "measuring")) != null) {
            return;
        }
        PROBES.submit(new Runnable() {
            @Override public void run() {
                CACHE.put(k, probe(schema, table, column));
            }
        });
    }

    /** A plausible 4-digit year, or null — a bound outside this range is not reported. */
    private static Integer year(String raw) {
        if (raw == null) {
            return null;
        }
        try {
            int y = Integer.parseInt(raw.trim());
            return (y >= 1800 && y <= 2200) ? Integer.valueOf(y) : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Result probe(String schema, String table, String column) {
        // The column comes from the catalog's own partition metadata and the table/schema
        // through safeIdent, so none of this is caller-controlled text.
        // The column is referenced through an alias so it sits dot-adjacent: "year" is a
        // reserved identifier, and quoteReservedIdentifiers only rewrites dot-adjacent
        // tokens — a bare MIN(year) would reach the parser unquoted and fail.
        String sql = McpServer.quoteReservedIdentifiers(
            "SELECT MIN(t." + column + "), MAX(t." + column + ") FROM "
            + schema + "." + table + " t");
        long started = System.currentTimeMillis();
        try {
            // The all-schemas connection describe_table already opened, rather than a
            // per-schema one — a fresh connection costs minutes on first use, which would
            // strand the probe long after the caller stopped looking for it.
            Connection c = McpServer.getCatalogConnection();
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {
                if (!rs.next()) {
                    return new Result(null, null, "empty");
                }
                // Read as text, not getInt: a hive partition column arrives as VARCHAR on
                // most of these tables, and getInt throws outright on the string accessor.
                // MIN/MAX over 4-character years orders the same either way.
                Integer first = year(rs.getString(1));
                Integer last = year(rs.getString(2));
                if (first == null || last == null) {
                    // Either the table is empty or the column holds something that is not a
                    // year. Both are reportable states; neither justifies inventing bounds.
                    return new Result(null, null,
                        rs.getString(1) == null ? "empty" : "unrecognized");
                }
                McpServer.logLine("[askamerica-mcp] year-probe " + schema + "." + table
                    + " -> " + first + "-" + last
                    + " in " + (System.currentTimeMillis() - started) + "ms");
                return new Result(first, last, null);
            }
        } catch (Exception e) {
            McpServer.logLine("[askamerica-mcp] year-probe failed " + schema + "." + table
                + ": " + e.getMessage());
            return new Result(null, null, "unavailable");
        }
    }
}
