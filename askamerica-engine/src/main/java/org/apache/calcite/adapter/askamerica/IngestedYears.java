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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
 *
 * <p>The probe counts rows per year rather than taking bare MIN/MAX, because bounds alone
 * cannot show an interior hole — a table loaded for 2015-2017 and 2021-2023 reports exactly
 * the same window as one loaded for all nine years. {@code data_coverage} needs that
 * difference; {@link #observed} still returns only the bounds, so {@code describe_table}'s
 * output is unchanged.
 */
final class IngestedYears {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * One entry per table, holding the probe itself rather than its outcome, so the
     * asynchronous {@link #observed} path and the blocking {@link #measure} path share a
     * single scan instead of racing to start two. Terminal states stay cached too — a probe
     * that failed must not be re-run per call.
     */
    private static final Map<String, Future<Result>> PROBES = new ConcurrentHashMap<>();

    private static final ExecutorService POOL =
        Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "askamerica-year-probe");
                t.setDaemon(true);
                return t;
            }
        });

    /**
     * How long {@code data_coverage} waits for a scan before answering "still measuring".
     * A caller who asked for a row scan can afford to wait for one, but not unboundedly:
     * probes are serialized, so a queue ahead of this one can outlast any client timeout.
     */
    private static final long MEASURE_TIMEOUT_SECONDS = 120;

    private IngestedYears() {
    }

    /** Outcome of one probe: rows per year, or the reason there are none. */
    static final class Result {
        final Integer first;
        final Integer last;
        final String status;
        /** Year to row count, ascending. Empty unless {@code status} is null (measured). */
        final Map<Integer, Long> yearCounts;
        /** Rows whose year column held something that is not a 4-digit year. */
        final long unparsedRows;

        Result(Integer first, Integer last, String status, Map<Integer, Long> yearCounts,
                long unparsedRows) {
            this.first = first;
            this.last = last;
            this.status = status;
            this.yearCounts = yearCounts == null
                ? Collections.<Integer, Long>emptyMap() : yearCounts;
            this.unparsedRows = unparsedRows;
        }

        Result(Integer first, Integer last, String status) {
            this(first, last, status, null, 0);
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
        // Whether the probe already existed, read before creating it: the first ask returns
        // null so describe_table says nothing at all about an unmeasured table, rather than
        // reporting a state the caller cannot act on.
        boolean known = PROBES.containsKey(key(schema, table));
        Future<Result> f = probeFor(schema, table, column);
        if (!known) {
            return null;
        }
        if (!f.isDone()) {
            ObjectNode out = MAPPER.createObjectNode();
            out.put("status", "measuring");
            return out;
        }
        return bounds(settled(f));
    }

    private static ObjectNode bounds(Result r) {
        ObjectNode out = MAPPER.createObjectNode();
        if (r.status != null) {
            out.put("status", r.status);
            return out;
        }
        if (r.first != null) {
            out.put("first_year", r.first);
        }
        if (r.last != null) {
            out.put("last_year", r.last);
        }
        out.put("status", "measured");
        return out;
    }

    /**
     * Blocking counterpart to {@link #observed}: waits for the scan rather than reporting
     * that one is under way, up to {@link #MEASURE_TIMEOUT_SECONDS}. Returns null if the
     * wait ran out, which the caller must report as unmeasured rather than as empty.
     */
    static Result measure(String schema, String table, String column) {
        Future<Result> f = probeFor(schema, table, column);
        try {
            return f.get(MEASURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            return new Result(null, null, "unavailable");
        }
    }

    /** Whatever a completed probe settled on; a probe that threw reads as unavailable. */
    private static Result settled(Future<Result> f) {
        try {
            return f.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Result(null, null, "unavailable");
        } catch (ExecutionException e) {
            return new Result(null, null, "unavailable");
        }
    }

    /** One probe per table, started on first ask by whichever path asks first. */
    private static Future<Result> probeFor(final String schema, final String table,
            final String column) {
        String k = key(schema, table);
        Future<Result> existing = PROBES.get(k);
        if (existing != null) {
            return existing;
        }
        FutureTask<Result> task = new FutureTask<>(new Callable<Result>() {
            @Override public Result call() {
                return probe(schema, table, column);
            }
        });
        Future<Result> prior = PROBES.putIfAbsent(k, task);
        if (prior != null) {
            return prior;
        }
        POOL.submit(task);
        return task;
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
        // reserved identifier, and normalizeCallerSql only rewrites dot-adjacent
        // tokens — a bare GROUP BY year would reach the parser unquoted and fail.
        String sql = McpServer.normalizeCallerSql(
            "SELECT t." + column + ", COUNT(*) FROM " + schema + "." + table + " t "
            + "GROUP BY t." + column + " ORDER BY t." + column);
        long started = System.currentTimeMillis();
        try {
            // The all-schemas connection describe_table already opened, rather than a
            // per-schema one — a fresh connection costs minutes on first use, which would
            // strand the probe long after the caller stopped looking for it.
            Connection c = McpServer.getCatalogConnection();
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {
                Map<Integer, Long> counts = new LinkedHashMap<>();
                long unparsed = 0;
                boolean anyRow = false;
                while (rs.next()) {
                    anyRow = true;
                    // Read as text, not getInt: a hive partition column arrives as VARCHAR on
                    // most of these tables, and getInt throws outright on the string accessor.
                    Integer y = year(rs.getString(1));
                    long n = rs.getLong(2);
                    if (y == null) {
                        // Either a null partition value or a column that does not hold years.
                        // Counted, not dropped — an unreadable year is itself a coverage fact.
                        unparsed += n;
                    } else {
                        Long prior = counts.get(y);
                        counts.put(y, Long.valueOf(prior == null ? n : prior.longValue() + n));
                    }
                }
                if (!anyRow) {
                    return new Result(null, null, "empty");
                }
                if (counts.isEmpty()) {
                    return new Result(null, null, "unrecognized", null, unparsed);
                }
                List<Integer> years = new ArrayList<>(counts.keySet());
                Collections.sort(years);
                Map<Integer, Long> ordered = new LinkedHashMap<>();
                for (Integer y : years) {
                    ordered.put(y, counts.get(y));
                }
                Integer first = years.get(0);
                Integer last = years.get(years.size() - 1);
                McpServer.logLine("[askamerica-mcp] year-probe " + schema + "." + table
                    + " -> " + first + "-" + last + " (" + years.size() + " years)"
                    + " in " + (System.currentTimeMillis() - started) + "ms");
                return new Result(first, last, null, ordered, unparsed);
            }
        } catch (Exception e) {
            McpServer.logLine("[askamerica-mcp] year-probe failed " + schema + "." + table
                + ": " + e.getMessage());
            return new Result(null, null, "unavailable");
        }
    }

    /**
     * The measured picture for {@code data_coverage}: which years hold rows, how many, and
     * which years inside the loaded span hold none.
     */
    static ObjectNode detail(Result r) {
        ObjectNode out = bounds(r);
        if (r.status != null) {
            if (r.unparsedRows > 0) {
                out.put("rows_with_unreadable_year", r.unparsedRows);
            }
            return out;
        }
        ArrayNode present = MAPPER.createArrayNode();
        ObjectNode counts = MAPPER.createObjectNode();
        long total = 0;
        for (Map.Entry<Integer, Long> e : r.yearCounts.entrySet()) {
            present.add(e.getKey().intValue());
            counts.put(String.valueOf(e.getKey()), e.getValue().longValue());
            total += e.getValue().longValue();
        }
        out.set("years_present", present);
        out.set("rows_by_year", counts);
        out.put("rows_total", total);
        if (r.unparsedRows > 0) {
            out.put("rows_with_unreadable_year", r.unparsedRows);
        }
        out.set("interior_gaps", gaps(r));
        return out;
    }

    /** Years between the first and last loaded year that hold no rows at all. */
    static ArrayNode gaps(Result r) {
        ArrayNode out = MAPPER.createArrayNode();
        if (r.status != null || r.first == null || r.last == null) {
            return out;
        }
        for (int y = r.first.intValue(); y <= r.last.intValue(); y++) {
            if (!r.yearCounts.containsKey(Integer.valueOf(y))) {
                out.add(y);
            }
        }
        return out;
    }

    /** Declared years carrying no rows — the declared-versus-loaded difference. */
    static ArrayNode missingVersusDeclared(Result r, Integer declaredFirst,
            Integer declaredLast) {
        ArrayNode out = MAPPER.createArrayNode();
        if (r.status != null || declaredFirst == null || declaredLast == null) {
            return out;
        }
        for (int y = declaredFirst.intValue(); y <= declaredLast.intValue(); y++) {
            if (!r.yearCounts.containsKey(Integer.valueOf(y))) {
                out.add(y);
            }
        }
        return out;
    }
}
