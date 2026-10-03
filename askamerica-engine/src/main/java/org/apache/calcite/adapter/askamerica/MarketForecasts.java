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

import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The forecast builder for prediction-market events: from an event's driver and rules text it
 * picks the catalog series and the transform of it the event settles on, reads the series
 * through the SQL runner, and builds the distribution of the settlement value from the
 * series' own history.
 *
 * <p>The distribution is always empirical: a sample of past changes of the same transform,
 * applied to the latest level the catalog holds. It says how the series has moved over as many
 * periods as separate the catalog's last row from the settlement period; it is not a model of
 * why it would move now. The method and the sample count are returned with it.
 *
 * <p>The builder never fills in what it cannot read. A spec that cannot be resolved from the
 * rules and no override names what is missing and fails; a series the rules name that the
 * catalog does not carry returns the flag {@code series_not_sourced} and no forecast. Where the
 * rules and the series disagree (seasonal adjustment, precision, units, a last period earlier
 * than the period before settlement) the forecast is built anyway and the disagreement is
 * returned as a flag the report must state.
 */
final class MarketForecasts {

    static final String TOOL = "forecast_market_event";
    /** Fewest samples a forecast may rest on; fewer is an error, not a thin forecast. */
    static final int MIN_SAMPLES = 8;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    static final Set<String> KEYS = new TreeSet<>(Arrays.asList(
        "source", "event_id", "as_of", "table", "series", "series_column", "date_column",
        "value_column", "series_sql", "transform", "round", "frequency", "seasonally_adjusted",
        "change_mode", "settlement_period", "window_start", "years"));

    /** What the event settles on, computed from the series. */
    enum Transform {
        /** The series' value at the settlement period. */
        LEVEL("level"),
        /** Percent change from the previous month, at the settlement month. */
        MOM("mom_pct"),
        /** Percent change from twelve months earlier, at the settlement month. */
        YOY("yoy_pct"),
        /** Difference from the previous period, in the series' units (payrolls added). */
        CHANGE("change"),
        /** The highest value the series reaches from its last row to the settlement date. */
        MAX_PATH("max_path"),
        /** The lowest value the series reaches from its last row to the settlement date. */
        MIN_PATH("min_path");

        final String key;

        Transform(String key) {
            this.key = key;
        }

        static Transform of(String key) {
            for (Transform t : values()) {
                if (t.key.equals(key)) {
                    return t;
                }
            }
            List<String> keys = new ArrayList<>();
            for (Transform t : values()) {
                keys.add(t.key);
            }
            throw new IllegalArgumentException("transform must be one of " + keys + ", got '"
                + key + "'");
        }
    }

    /** A daily series' latest row is stale when older than this many calendar days. */
    static final int STALE_DAILY_DAYS = 5;
    /** A weekly series' latest row is stale when older than this many calendar days: a
     *  print has been missed. */
    static final int STALE_WEEKLY_DAYS = 7;
    /** A monthly series is stale when the month after its last period ended more than this
     *  many days ago. */
    static final int STALE_MONTHLY_DAYS = 45;

    enum Freq {
        MONTHLY("monthly", 45), WEEKLY("weekly", 9), DAILY("daily", 5);

        final String key;
        final int staleDays;

        Freq(String key, int staleDays) {
            this.key = key;
            this.staleDays = staleDays;
        }

        static Freq of(String key) {
            for (Freq f : values()) {
                if (f.key.equals(key)) {
                    return f;
                }
            }
            throw new IllegalArgumentException("frequency must be monthly, weekly or daily, "
                + "got '" + key + "'");
        }

        /**
         * Default history window, in years: enough rows to sample, under the row cap. Monthly
         * is 5: backtested on four Kalshi series against 3, 8 and 15 years, a window that
         * reaches back past the current regime scored worse on payrolls and no better on the
         * rest, and 3 years leaves too few rows for the tails.
         */
        int defaultYears() {
            return this == MONTHLY ? 5 : this == WEEKLY ? 10 : 5;
        }
    }

    /** A catalog series, or one the caller described. */
    static final class Series {
        String id;
        String table;
        String seriesColumn = "series";
        /** Null for a BLS-style table keyed by year and period (M01-M12). */
        String dateColumn;
        String valueColumn = "value";
        String sql;
        Freq freq;
        /** Null when unknown: the caller gave an override without saying. */
        Boolean seasonallyAdjusted;
        /** Units of the series itself; null when unknown. */
        String unit;
        /** True when changes are proportional (prices, indexes), false for rates in percent;
         *  null when unknown. */
        Boolean ratio;
        String label;
        Transform defaultTransform;
        /** What a unit of the series counts when its values are in thousands (jobs), so the
         *  forecast can be put in the units a venue strikes in; null when not declared. */
        String countUnit;
        /** Days after a monthly period ends by which its first print is out; null when not
         *  declared (a caller's own series). Only an as_of cutoff reads it. */
        Integer publishLagDays;

        Series copy() {
            Series s = new Series();
            s.id = id;
            s.table = table;
            s.seriesColumn = seriesColumn;
            s.dateColumn = dateColumn;
            s.valueColumn = valueColumn;
            s.sql = sql;
            s.freq = freq;
            s.seasonallyAdjusted = seasonallyAdjusted;
            s.unit = unit;
            s.ratio = ratio;
            s.label = label;
            s.defaultTransform = defaultTransform;
            s.countUnit = countUnit;
            s.publishLagDays = publishLagDays;
            return s;
        }
    }

    private static Series bls(String table, String id, boolean sa, String unit, boolean ratio,
            Transform dflt, String label) {
        Series s = new Series();
        s.table = table;
        s.id = id;
        s.freq = Freq.MONTHLY;
        s.seasonallyAdjusted = sa;
        s.unit = unit;
        s.ratio = ratio;
        s.defaultTransform = dflt;
        s.label = label;
        return s;
    }

    private static Series counted(Series s, String countUnit) {
        s.countUnit = countUnit;
        return s;
    }

    private static Series fred(String id, Freq freq, boolean sa, String unit, boolean ratio,
            Transform dflt, String label) {
        Series s = bls("econ.fred_indicators", id, sa, unit, ratio, dflt, label);
        s.dateColumn = "date";
        s.freq = freq;
        return s;
    }

    /** The series this builder resolves to; every id and table is in the econ or fred schema
     *  YAML (MarketForecastsTest checks them against it). */
    static final List<Series> CATALOG = Collections.unmodifiableList(Arrays.asList(
        bls("econ.inflation_metrics", "CUUR0000SA0", false, "index", true, null,
            "CPI-U all items, not seasonally adjusted"),
        bls("econ.inflation_metrics", "CUUR0000SA0L1E", false, "index", true, null,
            "CPI-U core (less food and energy), not seasonally adjusted"),
        bls("econ.food_cpi", "CUUR0000SAF11", false, "index", true, null,
            "CPI-U food at home, not seasonally adjusted"),
        bls("econ.food_cpi", "CUUR0000SEFV", false, "index", true, null,
            "CPI-U food away from home, not seasonally adjusted"),
        bls("econ.employment_statistics", "LNS14000000", true, "percent", false,
            Transform.LEVEL, "unemployment rate, 16+, seasonally adjusted"),
        bls("econ.employment_statistics", "LNS13327709", true, "percent", false,
            Transform.LEVEL, "U-6 underutilization rate, seasonally adjusted"),
        counted(bls("econ.employment_statistics", "CES0000000001", true, "thousands", true,
            Transform.CHANGE, "total nonfarm payrolls, seasonally adjusted"), "jobs"),
        counted(bls("econ.employment_statistics", "CES0500000001", true, "thousands", true,
            Transform.CHANGE, "total private payrolls, seasonally adjusted"), "jobs"),
        fred("CPIAUCSL", Freq.MONTHLY, true, "index", true, null,
            "CPI-U all items, seasonally adjusted"),
        fred("CPIAUCNS", Freq.MONTHLY, false, "index", true, null,
            "CPI-U all items, not seasonally adjusted"),
        fred("CPILFESL", Freq.MONTHLY, true, "index", true, null,
            "CPI-U core (less food and energy), seasonally adjusted"),
        fred("PCEPI", Freq.MONTHLY, true, "index", true, null,
            "PCE price index, seasonally adjusted"),
        fred("PCEPILFE", Freq.MONTHLY, true, "index", true, null,
            "core PCE price index, seasonally adjusted"),
        fred("UNRATE", Freq.MONTHLY, true, "percent", false, Transform.LEVEL,
            "unemployment rate, seasonally adjusted"),
        fred("FEDFUNDS", Freq.MONTHLY, false, "percent", false, Transform.LEVEL,
            "federal funds rate, monthly average"),
        fred("ICSA", Freq.WEEKLY, true, "persons", true, Transform.LEVEL,
            "initial jobless claims, seasonally adjusted"),
        fred("CCSA", Freq.WEEKLY, true, "persons", true, Transform.LEVEL,
            "continuing jobless claims, seasonally adjusted"),
        fred("MORTGAGE30US", Freq.WEEKLY, false, "percent", false, Transform.LEVEL,
            "30-year fixed mortgage rate, weekly"),
        fred("DCOILWTICO", Freq.DAILY, false, "dollars", true, Transform.LEVEL,
            "WTI crude oil spot price, dollars per barrel"),
        fred("DGS3MO", Freq.DAILY, false, "percent", false, Transform.LEVEL,
            "3-month Treasury constant-maturity yield, daily"),
        fred("DGS2", Freq.DAILY, false, "percent", false, Transform.LEVEL,
            "2-year Treasury constant-maturity yield, daily"),
        fred("DGS5", Freq.DAILY, false, "percent", false, Transform.LEVEL,
            "5-year Treasury constant-maturity yield, daily"),
        fred("DGS10", Freq.DAILY, false, "percent", false, Transform.LEVEL,
            "10-year Treasury constant-maturity yield, daily"),
        fred("DGS30", Freq.DAILY, false, "percent", false, Transform.LEVEL,
            "30-year Treasury constant-maturity yield, daily"),
        fred("HOUST", Freq.MONTHLY, true, "thousands", true, Transform.LEVEL,
            "housing starts, seasonally adjusted annual rate"),
        fred("PERMIT", Freq.MONTHLY, true, "thousands", true, Transform.LEVEL,
            "building permits, seasonally adjusted annual rate"),
        fred("CSUSHPISA", Freq.MONTHLY, true, "index", true, null,
            "Case-Shiller U.S. national home price index, seasonally adjusted")));

    /** Days after month end by which each monthly catalog series has printed, by release:
     *  the latest day of the following month(s) the agency's schedule puts it on. */
    private static final Map<String, Integer> PUBLISH_LAG_DAYS = new LinkedHashMap<>();

    static {
        // BLS Consumer Price Index: mid-month, the 15th at the latest.
        for (String id : new String[]{"CUUR0000SA0", "CUUR0000SA0L1E", "CUUR0000SAF11",
            "CUUR0000SEFV", "CPIAUCSL", "CPIAUCNS", "CPILFESL"}) {
            PUBLISH_LAG_DAYS.put(id, 16);
        }
        // BLS Employment Situation: the first or second Friday.
        for (String id : new String[]{"LNS14000000", "LNS13327709", "CES0000000001",
            "CES0500000001", "UNRATE"}) {
            PUBLISH_LAG_DAYS.put(id, 10);
        }
        // BEA Personal Income and Outlays: the end of the following month.
        PUBLISH_LAG_DAYS.put("PCEPI", 31);
        PUBLISH_LAG_DAYS.put("PCEPILFE", 31);
        // Federal Reserve H.15 monthly average: the first business days of the month.
        PUBLISH_LAG_DAYS.put("FEDFUNDS", 4);
        // Census New Residential Construction: around the 18th.
        PUBLISH_LAG_DAYS.put("HOUST", 21);
        PUBLISH_LAG_DAYS.put("PERMIT", 21);
        // S&P Case-Shiller: the last Tuesday of the second month after.
        PUBLISH_LAG_DAYS.put("CSUSHPISA", 62);
        for (Series c : CATALOG) {
            if (c.freq == Freq.MONTHLY) {
                Integer lag = PUBLISH_LAG_DAYS.get(c.id);
                if (lag == null) {
                    throw new IllegalStateException("monthly catalog series " + c.id
                        + " declares no publication lag");
                }
                c.publishLagDays = lag;
            }
        }
    }

    /** Ids the rules may name in another form, and the catalog series that is the same
     *  quantity: the BLS seasonally adjusted CPI ids are carried by FRED. */
    private static final Map<String, String> ALIASES = new LinkedHashMap<>();

    static {
        ALIASES.put("CUSR0000SA0", "CPIAUCSL");
        ALIASES.put("CUSR0000SA0L1E", "CPILFESL");
    }

    /** A series id the rules name: BLS programs the catalog carries, and FRED oil prices. */
    private static final Pattern NAMED_ID = Pattern.compile(
        "\\b(APU[0-9A-Z]{6,}|CU[SU]R[0-9A-Z]{6,}|CES[0-9]{8,}|LN[SU][0-9]{7,}|WPU[0-9A-Z]{4,}"
        + "|DCOIL[A-Z0-9]+)\\b");

    private static final Pattern MONTH_NAMED;

    static {
        // A month by its name or its three-letter form ("Apr 2026" is how Kalshi titles some
        // events). The day may not be the first two digits of the year that follows.
        StringBuilder names = new StringBuilder("sept");
        for (Month m : Month.values()) {
            String name = m.name().toLowerCase(Locale.ROOT);
            names.append('|').append(name).append('|').append(name, 0, 3);
        }
        MONTH_NAMED = Pattern.compile("\\b(" + names
            + ")\\b\\.?(?:\\s+\\d{1,2}(?!\\d),?)?(?:\\s+(\\d{4}))?");
    }

    private static final Pattern SA_NEGATIVE = Pattern.compile(
        "not seasonally adjusted|non-?seasonally adjusted|unadjusted|\\bnsa\\b");
    private static final Pattern SA_POSITIVE = Pattern.compile(
        "seasonally adjusted|\\bsaar\\b|\\bs\\.a\\.");
    private static final Pattern MOM_TEXT = Pattern.compile(
        "month[- ]over[- ]month|\\bm/m\\b|\\bmom\\b|monthly (change|rate|increase|inflation)"
        + "|(from|compared (to|with)) the (previous|prior|preceding) month"
        + "|(one|1)[- ]month percent(age)? change");
    private static final Pattern YOY_TEXT = Pattern.compile(
        "year[- ]over[- ]year|\\byoy\\b|12[- ]month|annual (rate|inflation|change)"
        + "|from a year (ago|earlier)|compared (to|with) (the )?same month"
        + "|over the (last|past) year|year[- ]ending");
    /** "increases by more than X%" / "increases by above X%": a one-month change when no
     *  12-month wording is present. */
    private static final Pattern MOM_IMPLIED = Pattern.compile(
        "\\b(increases?|rises?)\\b[^.%]{0,40}-?\\d+(\\.\\d+)?\\s*%");
    /** "10-year", "30 year", "3-month": the tenor of a Treasury yield. */
    private static final Pattern TENOR = Pattern.compile(
        "\\b(\\d{1,2})[- ](year|month)\\b");
    private static final Pattern MAX_TEXT = Pattern.compile(
        "at any (time|point)|\\btouch(es)?\\b"
        + "|\\b(highest|maximum)\\b.{0,40}\\b(during|between|through|before)\\b"
        + "|\\bhow high\\b|\\babove\\b[^.]{0,80}\\bon any (business )?day\\b");
    private static final Pattern MIN_TEXT = Pattern.compile(
        "\\bhow low\\b|\\bbelow\\b[^.]{0,80}\\bon any (business )?day\\b"
        + "|\\b(lowest|minimum)\\b.{0,40}\\b(during|between|through|before)\\b");
    private static final Pattern CORE_TEXT = Pattern.compile(
        "\\bcore\\b|less food and energy|excluding food and energy|ex[- ]food and energy");
    private static final Pattern CPI_COMPONENT = Pattern.compile(
        "\\b(shelter|rent|food|energy|gasoline|apparel|used (cars|vehicles)|medical|airfares?|"
        + "electricity|new vehicles)\\b");
    private static final Pattern DEMOGRAPHIC = Pattern.compile(
        "\\b(black|hispanic|latino|asian|white|women|men|teen\\w*|veterans?|youth)\\b");

    private final PredictionMarkets.Fetcher fetcher;
    private final MarketTools.SqlRunner sql;
    private final Supplier<Instant> clock;

    MarketForecasts(PredictionMarkets.Fetcher fetcher, MarketTools.SqlRunner sql,
            Supplier<Instant> clock) {
        this.fetcher = fetcher;
        this.sql = sql;
        this.clock = clock;
    }

    // ─── Tool definition ───────────────────────────────────────────────────────

    private static ObjectNode prop(String type, String description) {
        ObjectNode p = MAPPER.createObjectNode();
        p.put("type", type);
        p.put("description", description);
        return p;
    }

    /** The MCP tool definition: name, description and input schema, as McpServer.tool(...)
     *  builds them. */
    static ObjectNode toolDef() {
        ObjectNode props = MAPPER.createObjectNode();
        props.set("source", prop("string", "kalshi or polymarket."));
        props.set("event_id", prop("string",
            "The event_id from find_market_candidates (Kalshi event ticker, Polymarket event "
            + "id)."));
        props.set("as_of", prop("string",
            "ISO date. Only series rows whose period ended on or before it are used (weekly "
            + "and daily rows: dated on or before it). For a backtest."));
        props.set("transform", prop("string",
            "What the event settles on: level, mom_pct, yoy_pct, change, max_path or min_path. Read "
            + "from the rules when they say; pass it when the rules do not parse."));
        props.set("series", prop("string",
            "Override the settlement series id, e.g. CUUR0000SA0. With table for a series the "
            + "builder does not know."));
        props.set("table", prop("string",
            "Override the table, e.g. econ.fred_indicators. Needs series."));
        props.set("series_column", prop("string", "Column holding the series id (default "
            + "series)."));
        props.set("date_column", prop("string",
            "Column holding the observation date; needed for a table the builder does not "
            + "know."));
        props.set("value_column", prop("string",
            "Column holding the value (default value)."));
        props.set("series_sql", prop("string",
            "A query returning columns date and value, one row per observation, in place of "
            + "table and series. Needs frequency."));
        props.set("frequency", prop("string",
            "monthly, weekly or daily; needed for series_sql or a series the builder does "
            + "not know."));
        props.set("seasonally_adjusted", prop("boolean",
            "Whether the overriding series is seasonally adjusted, so a mismatch with the "
            + "rules can be flagged."));
        props.set("change_mode", prop("string",
            "additive (rates in percent) or ratio (prices, indexes, counts); needed for "
            + "level or max_path on a series the builder does not know."));
        props.set("settlement_period", prop("string",
            "YYYY-MM for a monthly series, an ISO date for weekly or daily; overrides what is "
            + "read from the title, rules and close time."));
        props.set("window_start", prop("string",
            "max_path or min_path only: ISO date the event's window began; rows from it to the "
            + "last row set the bound the path extreme cannot fall inside."));
        props.set("round", prop("integer",
            "Decimals the rules settle on; read from the rules when they say."));
        props.set("years", prop("integer",
            "Years of history to sample (default 5 monthly, 10 weekly, 5 daily)."));
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("type", "object");
        schema.set("properties", props);
        ArrayNode required = schema.putArray("required");
        required.add("source");
        required.add("event_id");
        // Key order matches McpServer.tool(): name, description, inputSchema.
        ObjectNode out = MAPPER.createObjectNode();
        out.put("name", TOOL);
        out.put("description", DESCRIPTION);
        out.set("inputSchema", schema);
        return out;
    }

    private static final String DESCRIPTION =
        "Build the forecast distribution for a Kalshi or Polymarket event from the catalog's "
        + "own series. From the event's driver and rules it picks the settlement series and "
        + "the transform the event settles on (level, month-over-month percent, year-over-year "
        + "percent, change, or the maximum of the path to the settlement date), reads the "
        + "rows, and samples past changes of the same transform applied to the latest level. "
        + "Returns median, p05, p95, n, method, series, table, transform, last_period, "
        + "settlement_period, samples, round, fan (history and interval bands, the input of a "
        + "render_chart fan chart) and flags: seasonal_adjustment_mismatch, "
        + "last_period_before_settlement, rounding_mismatch, rounding_unverified, "
        + "units_mismatch, history_gap, series_not_sourced. Pass samples and round to "
        + "price_market_event. Any part of the spec can be overridden; when the rules do not "
        + "resolve it and nothing overrides it, the error names what is missing. as_of cuts "
        + "history off at a date. You MUST state every flag in the report. You MUST NOT price "
        + "an event whose result carries series_not_sourced against a guessed forecast.";

    // ─── Arguments ─────────────────────────────────────────────────────────────

    /** The arguments of one call, parsed and checked. */
    static final class Request {
        LocalDate asOf;
        String table;
        String series;
        String seriesColumn;
        String dateColumn;
        String valueColumn;
        String seriesSql;
        Transform transform;
        Integer round;
        Freq frequency;
        Boolean seasonallyAdjusted;
        Boolean ratio;
        String settlementPeriod;
        LocalDate windowStart;
        Integer years;

        static Request parse(JsonNode args) {
            Iterator<String> names = args.fieldNames();
            while (names.hasNext()) {
                String n = names.next();
                if (!KEYS.contains(n)) {
                    throw new IllegalArgumentException("unknown key '" + n + "' in "
                        + TOOL + "; allowed: " + KEYS);
                }
            }
            Request r = new Request();
            r.asOf = dateArg(args, "as_of");
            r.table = identifierArg(args, "table", true);
            r.series = textArg(args, "series");
            r.seriesColumn = identifierArg(args, "series_column", false);
            r.dateColumn = identifierArg(args, "date_column", false);
            r.valueColumn = identifierArg(args, "value_column", false);
            r.seriesSql = textArg(args, "series_sql");
            String t = textArg(args, "transform");
            r.transform = t == null ? null : Transform.of(t);
            if (has(args, "round")) {
                if (!args.get("round").isIntegralNumber() || args.get("round").asInt() < 0) {
                    throw new IllegalArgumentException("round must be a non-negative integer, "
                        + "got " + args.get("round"));
                }
                r.round = args.get("round").asInt();
            }
            String f = textArg(args, "frequency");
            r.frequency = f == null ? null : Freq.of(f);
            if (has(args, "seasonally_adjusted")) {
                if (!args.get("seasonally_adjusted").isBoolean()) {
                    throw new IllegalArgumentException("seasonally_adjusted must be true or "
                        + "false, got " + args.get("seasonally_adjusted"));
                }
                r.seasonallyAdjusted = args.get("seasonally_adjusted").asBoolean();
            }
            String mode = textArg(args, "change_mode");
            if (mode != null) {
                if (!"additive".equals(mode) && !"ratio".equals(mode)) {
                    throw new IllegalArgumentException("change_mode must be additive or ratio, "
                        + "got '" + mode + "'");
                }
                r.ratio = "ratio".equals(mode);
            }
            r.settlementPeriod = textArg(args, "settlement_period");
            r.windowStart = dateArg(args, "window_start");
            if (has(args, "years")) {
                if (!args.get("years").isIntegralNumber() || args.get("years").asInt() < 1
                        || args.get("years").asInt() > 60) {
                    throw new IllegalArgumentException("years must be an integer from 1 to 60, "
                        + "got " + args.get("years"));
                }
                r.years = args.get("years").asInt();
            }
            if (r.seriesSql != null && (r.table != null || r.series != null)) {
                throw new IllegalArgumentException("give series_sql, or table and series, "
                    + "not both");
            }
            if (r.table != null && r.series == null) {
                throw new IllegalArgumentException("table needs series: name the series id in "
                    + r.table);
            }
            return r;
        }
    }

    private static boolean has(JsonNode args, String name) {
        return args.has(name) && !args.get(name).isNull();
    }

    private static String textArg(JsonNode args, String name) {
        if (!has(args, name)) {
            return null;
        }
        String s = args.get(name).asText().trim();
        return s.isEmpty() ? null : s;
    }

    private static LocalDate dateArg(JsonNode args, String name) {
        String s = textArg(args, name);
        if (s == null) {
            return null;
        }
        try {
            return LocalDate.parse(s);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(name + " must be an ISO date (YYYY-MM-DD), got '"
                + s + "'");
        }
    }

    private static final Pattern IDENT = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** A table or column name that is safe to put in a query, or null when absent. */
    private static String identifierArg(JsonNode args, String name, boolean qualified) {
        String s = textArg(args, name);
        if (s == null) {
            return null;
        }
        String[] parts = s.split("\\.", -1);
        if (parts.length > (qualified ? 2 : 1)) {
            throw new IllegalArgumentException(name + " must be "
                + (qualified ? "schema.table" : "a column name") + ", got '" + s + "'");
        }
        for (String p : parts) {
            if (!IDENT.matcher(p).matches()) {
                throw new IllegalArgumentException(name + " must be "
                    + (qualified ? "schema.table" : "a column name") + ", got '" + s + "'");
            }
        }
        return s;
    }

    // ─── The result ────────────────────────────────────────────────────────────

    /** A built forecast: the distribution MarketPricing prices, and the report. */
    static final class Result {
        /** Null when the series is not sourced: there is nothing to price. Already rounded
         *  when a precision applies. */
        MarketPricing.Forecast forecast;
        /** {@link #forecast} before any rounding. */
        MarketPricing.Forecast unrounded;
        ObjectNode json;
    }

    /** The handler McpServer registers: reads the event, builds the forecast, returns the
     *  report as JSON. */
    String forecastMarketEvent(JsonNode args) throws Exception {
        Request req = Request.parse(args);
        String source = textArg(args, "source");
        String eventId = textArg(args, "event_id");
        if (source == null || eventId == null) {
            throw new IllegalArgumentException("source and event_id are required");
        }
        PredictionMarkets.LiveEvent live = PredictionMarkets.fetchEvent(fetcher, source,
            eventId);
        PredictionMarkets.Event ev = live.event;
        Result r = forecast(ev.eventTitle, ev.rules, ev.driver, ev.closeTime, req);
        r.json.put("source", source);
        r.json.put("event_id", eventId);
        return MAPPER.writeValueAsString(r.json);
    }

    // ─── Resolution ────────────────────────────────────────────────────────────

    /** What the rules and the caller together settle on. */
    private static final class Spec {
        Series series;
        Transform transform;
        String transformSource;
        String how;
        /** Set when the rules name a series the catalog does not carry. */
        String notSourcedId;
        String notSourcedLabel;
    }

    private static Series catalogSeries(String id, String table) {
        String key = ALIASES.getOrDefault(id, id);
        for (Series s : CATALOG) {
            if (s.id.equals(key) && (table == null || s.table.equals(table))) {
                return s.copy();
            }
        }
        return null;
    }

    private static Series pickSa(String saId, String nsaId, Boolean rulesSa,
            Transform transformHint) {
        // Rules that state the adjustment decide. Silent rules follow the BLS convention: the
        // 12-month change is published on the unadjusted index, the monthly change on the
        // adjusted one.
        boolean sa = rulesSa != null ? rulesSa : transformHint != Transform.YOY;
        return catalogSeries(sa ? saId : nsaId, null);
    }

    private static List<Transform> transformsIn(String text) {
        List<Transform> found = new ArrayList<>();
        if (MOM_TEXT.matcher(text).find()) {
            found.add(Transform.MOM);
        }
        if (YOY_TEXT.matcher(text).find()) {
            found.add(Transform.YOY);
        }
        if (MAX_TEXT.matcher(text).find()) {
            found.add(Transform.MAX_PATH);
        }
        if (MIN_TEXT.matcher(text).find()) {
            found.add(Transform.MIN_PATH);
        }
        return found;
    }

    /** The transform the title or the rules name, or null when they name none. */
    private static Transform transformOf(String title, String rules) {
        List<Transform> t = transformsIn(title.toLowerCase(Locale.ROOT));
        if (t.isEmpty()) {
            t = transformsIn(rules.toLowerCase(Locale.ROOT));
        }
        if (t.size() > 1) {
            List<String> keys = new ArrayList<>();
            for (Transform x : t) {
                keys.add(x.key);
            }
            throw new IllegalArgumentException("the rules match more than one transform "
                + keys + "; pass transform");
        }
        return t.isEmpty() ? null : t.get(0);
    }

    /** The series and transform an event settles on, as the builder reads its title and
     *  rules; or why it reads none. */
    static final class Quantity {
        /** Series id and transform, e.g. "CUUR0000SA0 yoy_pct"; null when not resolved. */
        final String key;
        /** Series id; set without {@link #key} when only the transform is unread. */
        final String series;
        final String reason;
        /** Set with {@link #key}. */
        Transform transform;
        /** The month a monthly event settles on; null when the title and rules name none. */
        YearMonth month;
        /** Decimals the rules publish the number to; null when they do not say. */
        Integer round;

        Quantity(String key, String series, String reason) {
            this.key = key;
            this.series = series;
            this.reason = reason;
        }
    }

    /** The series is read and the rules do not say how it is transformed. */
    private static final class TransformMissing extends IllegalArgumentException {
        final String series;

        TransformMissing(String series, String message) {
            super(message);
            this.series = series;
        }
    }

    static Quantity quantityOf(PredictionMarkets.Event e) {
        String title = e.eventTitle == null ? "" : e.eventTitle;
        String rules = e.rules == null ? "" : e.rules;
        Spec spec;
        try {
            spec = resolve(title, rules, e.driver, new Request(),
                rulesSeasonal((title + " " + rules).toLowerCase(Locale.ROOT)));
        } catch (TransformMissing x) {
            return new Quantity(null, x.series, x.getMessage());
        } catch (IllegalArgumentException x) {
            return new Quantity(null, null, x.getMessage());
        }
        if (spec.notSourcedId != null) {
            return new Quantity(null, spec.notSourcedId,
                spec.notSourcedLabel + " is not in the catalog");
        }
        Quantity q = new Quantity(spec.series.id + " " + spec.transform.key, spec.series.id,
            null);
        q.transform = spec.transform;
        q.month = spec.series.freq == Freq.MONTHLY ? monthOf(title, rules, e.closeTime) : null;
        q.round = rulesRound((title + " " + rules).toLowerCase(Locale.ROOT));
        return q;
    }

    // ─── One month's change against twelve months' ─────────────────────────────

    /** Series that are one index, before and after seasonal adjustment. */
    private static final String[][] ONE_INDEX = {
        {"CPIAUCSL", "CPIAUCNS", "CUUR0000SA0"},
        {"CPILFESL", "CUUR0000SA0L1E"},
    };
    /** Years of history the wedge's errors are measured over. */
    private static final int CONVERSION_YEARS = 12;
    /** The fewest months a wedge's error may be measured on. */
    private static final int MIN_WEDGE_ERRORS = 24;

    /**
     * Whether one event settles on a month's month-over-month percent change and the other on
     * the same index's year-over-year percent change: two numbers, one of which all but
     * fixes the other once the month before has printed.
     */
    static boolean convertible(Quantity a, Quantity b) {
        if (a.key == null || b.key == null || a.transform == b.transform
                || a.transform != Transform.MOM && a.transform != Transform.YOY
                || b.transform != Transform.MOM && b.transform != Transform.YOY) {
            return false;
        }
        if (a.series.equals(b.series)) {
            return true;
        }
        for (String[] index : ONE_INDEX) {
            List<String> ids = Arrays.asList(index);
            if (ids.contains(a.series) && ids.contains(b.series)) {
                return true;
            }
        }
        return false;
    }

    /**
     * How a month's month-over-month percent change {@code m} of one series fixes the
     * year-over-year percent change of a series of the same index: with N the year-over-year
     * series and t the month, {@code yoy = ((1 + n/100) * N[t-1]/N[t-12] - 1) * 100}, where
     * {@code n = m - wedge} is N's own one-month change. The wedge is what seasonal
     * adjustment adds to the month's change; it is not known until the month prints, and is
     * taken to be what it was in the same month a year earlier. {@code errors} holds how far
     * off that was in each month of the history.
     */
    static final class Conversion {
        String momSeries;
        String yoySeries;
        YearMonth month;
        /** N[t-1] / N[t-12]. */
        double ratio;
        double wedge;
        /** The wedge of a month less the wedge twelve months before it, per month. */
        double[] errors;
        String errorsFrom;
        String errorsTo;

        /** The intercept of {@code yoy = intercept + slope * (m - error)}. */
        double intercept() {
            return (ratio - 1) * 100 - ratio * wedge;
        }

        double slope() {
            return ratio;
        }

        ObjectNode toJson() {
            double[] abs = new double[errors.length];
            double sum = 0;
            double squares = 0;
            for (int i = 0; i < errors.length; i++) {
                abs[i] = Math.abs(errors[i]);
                sum += errors[i];
                squares += errors[i] * errors[i];
            }
            Arrays.sort(abs);
            double mean = sum / errors.length;
            ObjectNode o = MAPPER.createObjectNode();
            o.put("month", month.toString());
            o.put("month_over_month_series", momSeries);
            o.put("year_over_year_series", yoySeries);
            o.put("formula", "yoy = ((1 + (mom - wedge - error) / 100) * index_ratio - 1) * 100");
            o.put("index_ratio", PredictionMarkets.round(ratio, 6));
            o.put("index_ratio_is", yoySeries + " in " + month.minusMonths(1) + " over "
                + month.minusMonths(12));
            o.put("wedge", PredictionMarkets.round(wedge, 4));
            o.put("wedge_is", momSeries.equals(yoySeries) ? "zero: one series"
                : "the month-over-month percent change of " + momSeries + " less that of "
                    + yoySeries + " in " + month.minusMonths(12) + ", taken to repeat in "
                    + month);
            ObjectNode e = o.putObject("wedge_error");
            e.put("months", errors.length);
            e.put("from", errorsFrom);
            e.put("to", errorsTo);
            e.put("mean", PredictionMarkets.round(mean, 4));
            e.put("sd", PredictionMarkets.round(Math.sqrt(Math.max(0,
                squares / errors.length - mean * mean)), 4));
            e.put("p95_abs", PredictionMarkets.round(abs[(int) (0.95 * abs.length)], 4));
            e.put("max_abs", PredictionMarkets.round(abs[abs.length - 1], 4));
            e.put("is", "the wedge of each month less the wedge twelve months before it, in "
                + "percentage points, on the catalog's current (revised) values");
            return o;
        }
    }

    private Map<YearMonth, Double> monthly(String id, LocalDate today) throws Exception {
        Series s = catalogSeries(id, null);
        if (s == null) {
            throw new IllegalStateException(id + " is not in the catalog");
        }
        Map<YearMonth, Double> out = new LinkedHashMap<>();
        for (Obs o : load(s, seriesQuery(s, null, today, CONVERSION_YEARS), null)) {
            out.put(YearMonth.from(o.date), positive(o.value, id));
        }
        return out;
    }

    private static double needed(Map<YearMonth, Double> v, String id, YearMonth ym) {
        Double x = v.get(ym);
        if (x == null) {
            throw new IllegalStateException("the conversion needs " + id + " for " + ym
                + ", and the catalog has no value for it");
        }
        return x;
    }

    /** A month's one-month percent change, or null when either end is missing. */
    private static Double change(Map<YearMonth, Double> v, YearMonth ym) {
        Double now = v.get(ym);
        Double before = v.get(ym.minusMonths(1));
        return now == null || before == null ? null : 100 * (now / before - 1);
    }

    /**
     * The conversion between a month-over-month and a year-over-year event of one index and
     * one month.
     *
     * @throws IllegalStateException when the catalog's history cannot support it
     */
    Conversion conversion(Quantity mom, Quantity yoy) throws Exception {
        if (mom.month == null || yoy.month == null) {
            throw new IllegalStateException("the settlement month of an event is not named");
        }
        if (!mom.month.equals(yoy.month)) {
            throw new IllegalStateException("the two events settle on different months: "
                + mom.month + " and " + yoy.month);
        }
        YearMonth t = mom.month;
        LocalDate today = clock.get().atZone(ZoneOffset.UTC).toLocalDate();
        Map<YearMonth, Double> n = monthly(yoy.series, today);
        boolean one = mom.series.equals(yoy.series);
        Map<YearMonth, Double> s = one ? n : monthly(mom.series, today);
        if (n.containsKey(t)) {
            throw new IllegalStateException(yoy.series + " already has " + t + ": the "
                + "events' values are in the catalog. Query them instead of pricing them.");
        }
        if (!n.containsKey(t.minusMonths(1))) {
            throw new IllegalStateException(yoy.series + " has no value for "
                + t.minusMonths(1) + " yet: the month-over-month change of " + t + " fixes "
                + "its year-over-year change only once the month before has printed");
        }
        Conversion c = new Conversion();
        c.momSeries = mom.series;
        c.yoySeries = yoy.series;
        c.month = t;
        c.ratio = needed(n, yoy.series, t.minusMonths(1))
            / needed(n, yoy.series, t.minusMonths(12));
        if (one) {
            c.wedge = 0;
            c.errors = new double[]{0};
            c.errorsFrom = t.toString();
            c.errorsTo = t.toString();
            return c;
        }
        YearMonth last = t.minusMonths(12);
        needed(n, yoy.series, last.minusMonths(1));
        c.wedge = 100 * (needed(s, mom.series, last)
            / needed(s, mom.series, last.minusMonths(1)) - 1) - change(n, last);
        List<Double> errors = new ArrayList<>();
        for (YearMonth ym : n.keySet()) {
            YearMonth before = ym.minusMonths(12);
            Double a = change(s, ym);
            Double b = change(n, ym);
            Double a0 = change(s, before);
            Double b0 = change(n, before);
            if (a != null && b != null && a0 != null && b0 != null) {
                errors.add((a - b) - (a0 - b0));
                c.errorsFrom = c.errorsFrom == null ? ym.toString() : c.errorsFrom;
                c.errorsTo = ym.toString();
            }
        }
        if (errors.size() < MIN_WEDGE_ERRORS) {
            throw new IllegalStateException("the wedge between " + mom.series + " and "
                + yoy.series + " can be compared with the one a year before it in only "
                + errors.size() + " months of the catalog's history; the conversion needs "
                + MIN_WEDGE_ERRORS);
        }
        c.errors = toArray(errors);
        return c;
    }

    private static Spec resolve(String title, String rules, PredictionMarkets.Driver driver,
            Request req, Boolean rulesSa) {
        Spec spec = new Spec();
        String text = (title + " " + rules).toLowerCase(Locale.ROOT);
        Transform named = req.transform != null ? req.transform : transformOf(title, rules);
        if (req.seriesSql != null || req.series != null) {
            spec.series = overrideSeries(req);
            spec.how = req.seriesSql != null ? "series_sql given" : "series given";
        } else {
            Matcher id = NAMED_ID.matcher((title + " " + rules).toUpperCase(Locale.ROOT));
            Series fromRules = null;
            if (id.find()) {
                fromRules = catalogSeries(id.group(1), null);
                if (fromRules == null) {
                    spec.notSourcedId = id.group(1);
                    spec.notSourcedLabel = "the series the rules name, " + id.group(1);
                    return spec;
                }
                spec.how = "the rules name series " + id.group(1);
                spec.series = fromRules;
            } else {
                if (driver == null) {
                    throw new IllegalArgumentException("the event matches no driver, so no "
                        + "settlement series can be read from it: give series (and table), or "
                        + "series_sql");
                }
                String missing = driverSeries(driver.name, text, rulesSa, named, spec);
                if (spec.notSourcedId != null) {
                    return spec;
                }
                if (spec.series == null) {
                    throw new IllegalArgumentException(missing);
                }
                spec.how = "driver " + driver.name + " and the rules text";
            }
        }
        if (req.transform != null) {
            spec.transform = req.transform;
            spec.transformSource = "override";
        } else if (named != null) {
            spec.transform = named;
            spec.transformSource = "rules";
        } else if ("index".equals(spec.series.unit)
                && MOM_IMPLIED.matcher(text).find()) {
            // An index event that says "increases by more than X% in <month>" and names no
            // 12-month wording settles on the one-month percent change.
            spec.transform = Transform.MOM;
            spec.transformSource = "rules";
        } else if (spec.series.defaultTransform != null) {
            spec.transform = spec.series.defaultTransform;
            spec.transformSource = "series_default";
        } else {
            throw new TransformMissing(spec.series.id, "transform is missing: the rules do "
                + "not say whether " + spec.series.id + " settles on its level, "
                + "month-over-month change or year-over-year change. Pass transform (level, "
                + "mom_pct, yoy_pct, change, max_path, min_path)");
        }
        return spec;
    }

    /**
     * The catalog series for a driver and the rules text, or null in {@code spec.series}
     * with the reason as the return value.
     */
    private static String driverSeries(String driver, String text, Boolean rulesSa,
            Transform hint,
            Spec spec) {
        switch (driver) {
        case "inflation": {
            String stripped = text.replaceAll("food and energy", "");
            if (CPI_COMPONENT.matcher(stripped).find()) {
                return "the rules name a CPI component, not all items or core; the builder "
                    + "resolves only those. Pass series (and table), or series_sql";
            }
            spec.series = CORE_TEXT.matcher(text).find()
                ? pickSa("CPILFESL", "CUUR0000SA0L1E", rulesSa, hint)
                : pickSa("CPIAUCSL", "CUUR0000SA0", rulesSa, hint);
            return null;
        }
        case "pce":
            spec.series = catalogSeries(CORE_TEXT.matcher(text).find() ? "PCEPILFE" : "PCEPI",
                null);
            return null;
        case "unemployment":
            if (DEMOGRAPHIC.matcher(text).find()) {
                return "the rules name a demographic unemployment rate; the builder resolves "
                    + "only the headline rate (LNS14000000) and U-6. Pass series (and table)";
            }
            spec.series = catalogSeries(text.contains("u-6") ? "LNS13327709" : "LNS14000000",
                null);
            return null;
        case "payrolls":
            spec.series = catalogSeries(text.contains("private") ? "CES0500000001"
                : "CES0000000001", null);
            return null;
        case "jobless_claims":
            spec.series = catalogSeries(text.contains("continuing") ? "CCSA" : "ICSA", null);
            return null;
        case "mortgage_rate":
            if (Pattern.compile("15[- ]year").matcher(text).find()) {
                return "the rules name the 15-year mortgage rate; the catalog carries only "
                    + "the 30-year (MORTGAGE30US). Pass series (and table), or series_sql";
            }
            spec.series = catalogSeries("MORTGAGE30US", null);
            return null;
        case "energy_price":
            if (text.contains("wti") || text.contains("crude")) {
                if (text.contains("brent")) {
                    spec.notSourcedId = "DCOILBRENTEU";
                    spec.notSourcedLabel = "Brent crude oil price";
                    return null;
                }
                spec.series = catalogSeries("DCOILWTICO", null);
                return null;
            }
            if (text.contains("brent")) {
                spec.notSourcedId = "DCOILBRENTEU";
                spec.notSourcedLabel = "Brent crude oil price";
                return null;
            }
            return "the rules name an energy price other than WTI crude; the builder resolves "
                + "only WTI (DCOILWTICO). Pass series (and table), or series_sql";
        case "treasury_yield": {
            Matcher tenor = TENOR.matcher(text);
            List<String> ids = new ArrayList<>();
            while (tenor.find()) {
                String id = "DGS" + tenor.group(1)
                    + (tenor.group(2).startsWith("m") ? "MO" : "");
                if (!ids.contains(id)) {
                    ids.add(id);
                }
            }
            if (ids.size() != 1) {
                return "the rules name " + (ids.isEmpty() ? "no single Treasury tenor"
                    : "more than one Treasury tenor " + ids) + "; the builder resolves one "
                    + "constant-maturity yield (DGS<years>). Pass series (and table), or "
                    + "series_sql";
            }
            spec.series = catalogSeries(ids.get(0), null);
            if (spec.series == null) {
                spec.notSourcedId = ids.get(0);
                spec.notSourcedLabel = "the Treasury yield the rules name";
            }
            return null;
        }
        case "housing":
            if (text.contains("housing starts")) {
                spec.series = catalogSeries("HOUST", null);
            } else if (text.contains("building permits")) {
                spec.series = catalogSeries("PERMIT", null);
            } else if (text.contains("case-shiller") || text.contains("case shiller")) {
                spec.series = catalogSeries("CSUSHPISA", null);
            } else {
                return "the rules name a housing quantity the builder does not resolve "
                    + "(it resolves housing starts, building permits, Case-Shiller). Pass "
                    + "series (and table), or series_sql";
            }
            return null;
        case "food_prices":
            if (Pattern.compile("\\beggs?\\b").matcher(text).find()) {
                spec.notSourcedId = "APU0000708111";
                spec.notSourcedLabel = "average price of a dozen eggs (BLS average-price "
                    + "series, govdata-ops issue 851)";
                return null;
            }
            if (text.contains("food away")) {
                spec.series = catalogSeries("CUUR0000SEFV", null);
            } else if (text.contains("food at home") || text.contains("grocery")) {
                spec.series = catalogSeries("CUUR0000SAF11", null);
            } else {
                return "the rules name a food price the builder does not resolve (it resolves "
                    + "food at home and food away from home). Pass series (and table), or "
                    + "series_sql";
            }
            return null;
        default:
            return "driver '" + driver + "' has no settlement-series resolver in the "
                + "builder, so its series is missing: pass series (and table, date_column, "
                + "value_column, frequency), or series_sql (with frequency)";
        }
    }

    /** The series an override names or describes; every part the builder cannot read from
     *  the catalog must be given. */
    private static Series overrideSeries(Request req) {
        Series s;
        if (req.seriesSql != null) {
            if (req.frequency == null) {
                throw new IllegalArgumentException("frequency is missing: series_sql needs "
                    + "frequency (monthly, weekly or daily)");
            }
            s = new Series();
            s.id = "series_sql";
            s.sql = req.seriesSql;
            s.label = "series_sql";
        } else {
            s = catalogSeries(req.series, req.table);
            if (s == null) {
                if (req.table == null) {
                    throw new IllegalArgumentException("series '" + req.series + "' is not one "
                        + "the builder knows; name its table too");
                }
                if (req.dateColumn == null || req.valueColumn == null
                        || req.frequency == null) {
                    List<String> missing = new ArrayList<>();
                    if (req.dateColumn == null) {
                        missing.add("date_column");
                    }
                    if (req.valueColumn == null) {
                        missing.add("value_column");
                    }
                    if (req.frequency == null) {
                        missing.add("frequency");
                    }
                    throw new IllegalArgumentException(missing + " missing: series '"
                        + req.series + "' in " + req.table + " is not one the builder knows, "
                        + "so these cannot be read from the catalog");
                }
                s = new Series();
                s.id = req.series;
                s.table = req.table;
                s.label = req.series + " in " + req.table;
            }
        }
        if (req.seriesColumn != null) {
            s.seriesColumn = req.seriesColumn;
        }
        if (req.dateColumn != null) {
            s.dateColumn = req.dateColumn;
        }
        if (req.valueColumn != null) {
            s.valueColumn = req.valueColumn;
        }
        if (req.frequency != null) {
            s.freq = req.frequency;
        }
        if (req.seasonallyAdjusted != null) {
            s.seasonallyAdjusted = req.seasonallyAdjusted;
        }
        if (req.ratio != null) {
            s.ratio = req.ratio;
        }
        return s;
    }

    // ─── Rules text ────────────────────────────────────────────────────────────

    /** True when the rules say seasonally adjusted, false when they say not, null when silent. */
    static Boolean rulesSeasonal(String text) {
        if (SA_NEGATIVE.matcher(text).find()) {
            return false;
        }
        return SA_POSITIVE.matcher(text).find() ? Boolean.TRUE : null;
    }

    private static final Pattern ROUND_DECIMALS = Pattern.compile(
        "\\b(zero|no|one|two|three|four|\\d)[- ]decimals?( places?)?\\b");
    private static final Pattern ROUND_NEAREST = Pattern.compile(
        "nearest (tenth|hundredth|thousandth|whole|integer|0\\.(0*)1\\b)");

    /** Decimals the rules round to, or null when they do not say. */
    static Integer rulesRound(String text) {
        if (text.contains("single-decimal") || text.contains("single decimal")) {
            return 1;
        }
        Matcher m = ROUND_DECIMALS.matcher(text);
        if (m.find()) {
            switch (m.group(1)) {
            case "zero":
            case "no":
                return 0;
            case "one":
                return 1;
            case "two":
                return 2;
            case "three":
                return 3;
            case "four":
                return 4;
            default:
                return Integer.parseInt(m.group(1));
            }
        }
        m = ROUND_NEAREST.matcher(text);
        if (m.find()) {
            switch (m.group(1)) {
            case "tenth":
                return 1;
            case "hundredth":
                return 2;
            case "thousandth":
                return 3;
            case "whole":
            case "integer":
                return 0;
            default:
                return m.group(2).length() + 1;
            }
        }
        return null;
    }

    private static Set<String> rulesUnits(String text) {
        Set<String> u = new LinkedHashSet<>();
        if (text.contains("%") || text.contains("percent")) {
            u.add("percent");
        }
        if (text.contains("thousand")) {
            u.add("thousands");
        }
        if (text.contains("million")) {
            u.add("millions");
        }
        if (text.contains("$") || text.contains("dollar")) {
            u.add("dollars");
        }
        return u;
    }

    // ─── Rows ──────────────────────────────────────────────────────────────────

    /** One observation. {@code date} is the period start for a monthly series. */
    private static final class Obs {
        final LocalDate date;
        final double value;
        /** The day the observation is treated as known: for a monthly series the period's
         *  last day plus the series' publication lag (no lag when it declares none), the
         *  observation date otherwise. */
        final LocalDate knownBy;

        Obs(LocalDate date, double value, Series s) {
            this.date = date;
            this.value = value;
            this.knownBy = s.freq == Freq.MONTHLY
                ? YearMonth.from(date).atEndOfMonth().plusDays(
                    s.publishLagDays == null ? 0 : s.publishLagDays)
                : date;
        }
    }

    private static String quote(String s) {
        return "'" + s.replace("'", "''") + "'";
    }

    /** A column name as a quoted identifier. An unquoted {@code date} is a keyword to the
     *  engine's parser, and the query then costs minutes instead of seconds. */
    private static String column(String name) {
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    private static String seriesQuery(Series s, LocalDate asOf, LocalDate ref, int years) {
        if (s.sql != null) {
            return s.sql;
        }
        LocalDate from = ref.minusYears(years);
        String value = column(s.valueColumn) + " AS \"value\"";
        String where = " FROM " + s.table + " WHERE " + column(s.seriesColumn) + " = "
            + quote(s.id);
        if (s.dateColumn == null) {
            return "SELECT \"year\", \"period\", " + value + where
                + " AND \"period\" LIKE 'M%' AND \"period\" <> 'M13'"
                + " AND \"year\" >= " + from.getYear()
                + (asOf == null ? "" : " AND \"year\" <= " + asOf.getYear())
                + " ORDER BY \"year\", \"period\"";
        }
        String date = column(s.dateColumn);
        return "SELECT " + date + " AS \"date\", " + value + where
            + " AND " + date + " >= DATE '" + from + "'"
            + (asOf == null ? "" : " AND " + date + " <= DATE '" + asOf + "'")
            + " ORDER BY " + date;
    }

    private static double valueOf(JsonNode row, String field, String what, int n) {
        JsonNode v = row.get(field);
        if (v == null || !v.isNumber()) {
            throw new IllegalArgumentException(what + " row " + n + ": '" + field + "' is "
                + v + ", not a number");
        }
        return v.asDouble();
    }

    /** The rows of the series known on or before {@code asOf}, oldest first. */
    private List<Obs> load(Series s, String query, LocalDate asOf) throws Exception {
        ArrayNode rows = sql.rows(query, MarketTools.MAX_ROWS);
        if (rows.size() >= MarketTools.MAX_ROWS) {
            throw new IllegalArgumentException("the series query returned "
                + MarketTools.MAX_ROWS + " rows or more; lower years so no row is dropped "
                + "unseen");
        }
        List<Obs> out = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            JsonNode row = rows.get(i);
            // A null value is an observation the source never published (BLS skipped
            // October 2025): the period is missing, not the row malformed.
            if (row.has("value") && row.get("value").isNull()) {
                continue;
            }
            double v = valueOf(row, "value", s.id, i + 1);
            LocalDate d;
            if (s.dateColumn == null && s.sql == null) {
                JsonNode year = row.get("year");
                JsonNode period = row.get("period");
                if (year == null || !year.canConvertToInt() || period == null
                        || !period.asText().matches("M(0[1-9]|1[0-2])")) {
                    throw new IllegalArgumentException(s.id + " row " + (i + 1) + ": year "
                        + year + " and period " + period + " are not a year and M01-M12");
                }
                d = LocalDate.of(year.asInt(), Integer.parseInt(period.asText().substring(1)),
                    1);
            } else {
                JsonNode date = row.get("date");
                if (date == null || date.isNull() || date.asText().length() < 10) {
                    throw new IllegalArgumentException(s.id + " row " + (i + 1) + ": date is "
                        + date + ", not an ISO date");
                }
                d = LocalDate.parse(date.asText().substring(0, 10));
                if (s.freq == Freq.MONTHLY) {
                    d = d.withDayOfMonth(1);
                }
            }
            Obs o = new Obs(d, v, s);
            if (asOf == null || !o.knownBy.isAfter(asOf)) {
                out.add(o);
            }
        }
        out.sort(Comparator.comparing((Obs o) -> o.date));
        for (int i = 1; i < out.size(); i++) {
            if (out.get(i).date.equals(out.get(i - 1).date)) {
                throw new IllegalArgumentException(s.id + " has two rows for "
                    + out.get(i).date + "; the builder will not choose between them");
            }
        }
        return out;
    }

    // ─── Dates ─────────────────────────────────────────────────────────────────

    private static String period(LocalDate d, Freq f) {
        return f == Freq.MONTHLY ? YearMonth.from(d).toString() : d.toString();
    }

    /** The month an event settles on, from the title and then the rules, or null. */
    private static YearMonth monthOf(String title, String rules, String closeTime) {
        Matcher m = MONTH_NAMED.matcher(title.toLowerCase(Locale.ROOT));
        if (m.find()) {
            return monthWithYear(m, closeTime);
        }
        m = MONTH_NAMED.matcher(rules.toLowerCase(Locale.ROOT));
        while (m.find()) {
            if (m.group(2) != null) {
                return monthWithYear(m, closeTime);
            }
        }
        return null;
    }

    private static YearMonth monthWithYear(Matcher m, String closeTime) {
        String prefix = m.group(1).substring(0, 3).toUpperCase(Locale.ROOT);
        int month = 0;
        for (Month named : Month.values()) {
            if (named.name().startsWith(prefix)) {
                month = named.getValue();
            }
        }
        if (m.group(2) != null) {
            return YearMonth.of(Integer.parseInt(m.group(2)), month);
        }
        // No year given: the period ended on or before the event closed.
        if (closeTime == null || closeTime.length() < 10) {
            return null;
        }
        YearMonth close = YearMonth.from(LocalDate.parse(closeTime.substring(0, 10)));
        YearMonth candidate = YearMonth.of(close.getYear(), month);
        return candidate.isAfter(close) ? candidate.minusYears(1) : candidate;
    }

    // ─── Building ──────────────────────────────────────────────────────────────

    private static final class Flags {
        final ArrayNode out = MAPPER.createArrayNode();

        void add(String code, String message) {
            ObjectNode f = out.addObject();
            f.put("code", code);
            f.put("message", message);
        }
    }

    /**
     * Builds the forecast for an event given by its title, rules, driver and close time.
     * Package-private so a backtest can call it for an event it did not read live.
     *
     * @param driver the event's driver, or null when its title matched none
     * @param closeTime ISO timestamp the event closes, or null when {@code settlement_period}
     *     is given
     */
    Result forecast(String title, String rules, PredictionMarkets.Driver driver,
            String closeTime, Request req) throws Exception {
        String rulesText = rules == null ? "" : rules;
        String all = (title + " " + rulesText).toLowerCase(Locale.ROOT);
        Boolean rulesSa = rulesSeasonal(all);
        Spec spec = resolve(title, rulesText, driver, req, rulesSa);
        Flags flags = new Flags();
        ObjectNode json = MAPPER.createObjectNode();
        json.put("tool", TOOL);
        json.put("event_title", title);
        if (driver == null) {
            json.putNull("driver");
        } else {
            json.put("driver", driver.name);
        }
        if (req.asOf == null) {
            json.putNull("as_of");
        } else {
            json.put("as_of", req.asOf.toString());
        }
        json.put("as_of_rule", "a monthly row is used once its month has ended and its "
            + "release has printed (the series' publication lag in days after month end) on "
            + "or before as_of; a weekly or daily row once its date is on or before as_of");
        Result result = new Result();
        result.json = json;
        if (spec.notSourcedId != null) {
            json.put("status", "not_forecast");
            json.putNull("series");
            json.put("series_named", spec.notSourcedId);
            json.putNull("table");
            flags.add("series_not_sourced", "The rules settle on " + spec.notSourcedLabel
                + " (" + spec.notSourcedId + "), which the catalog does not carry, so no "
                + "forecast was built.");
            json.set("flags", flags.out);
            json.put("next", "No forecast exists for this event: say the series is not "
                + "sourced. Do not price it against a forecast of another series.");
            return result;
        }
        Series s = spec.series;
        Transform t = spec.transform;
        if ((t == Transform.MOM || t == Transform.YOY) && s.freq != Freq.MONTHLY) {
            throw new IllegalArgumentException(t.key + " needs a monthly series; "
                + s.id + " is " + s.freq.key);
        }
        if ((t == Transform.LEVEL || t == Transform.MAX_PATH || t == Transform.MIN_PATH)
                && s.ratio == null) {
            throw new IllegalArgumentException("change_mode is missing: " + t.key + " on "
                + s.id + " needs to know whether the series moves in proportion (ratio) or "
                + "by amounts (additive)");
        }
        LocalDate today = clock.get().atZone(ZoneOffset.UTC).toLocalDate();
        LocalDate ref = req.asOf != null ? req.asOf : today;
        int years = req.years != null ? req.years : s.freq.defaultYears();
        List<Obs> loaded = load(s, seriesQuery(s, req.asOf, ref, years), req.asOf);
        if (loaded.isEmpty()) {
            throw new IllegalStateException(s.id + " has no rows"
                + (req.asOf == null ? "" : " on or before " + req.asOf)
                + (s.table == null ? "" : " in " + s.table));
        }
        // A monthly series sits on a month grid. A month with no value is a hole: every
        // transform is computed only where its own endpoints both exist, nothing is filled.
        List<Obs> obs = loaded;
        Obs last = obs.get(obs.size() - 1);
        YearMonth firstYm = YearMonth.from(obs.get(0).date);
        int len = s.freq == Freq.MONTHLY
            ? (int) ChronoUnit.MONTHS.between(firstYm, YearMonth.from(last.date)) + 1
            : obs.size();
        double[] v = new double[len];
        LocalDate[] dates = new LocalDate[len];
        Arrays.fill(v, Double.NaN);
        for (int i = 0; i < len; i++) {
            dates[i] = s.freq == Freq.MONTHLY ? firstYm.plusMonths(i).atDay(1)
                : obs.get(i).date;
        }
        for (int i = 0; i < obs.size(); i++) {
            int at = s.freq == Freq.MONTHLY ? (int) ChronoUnit.MONTHS.between(firstYm,
                YearMonth.from(obs.get(i).date)) : i;
            v[at] = obs.get(i).value;
        }
        if (len > obs.size()) {
            List<String> missing = new ArrayList<>();
            for (int i = 0; i < len; i++) {
                if (Double.isNaN(v[i])) {
                    int j = i;
                    while (j + 1 < len && Double.isNaN(v[j + 1])) {
                        j++;
                    }
                    missing.add(i == j ? YearMonth.from(dates[i]).toString()
                        : YearMonth.from(dates[i]) + " to " + YearMonth.from(dates[j]));
                    i = j;
                }
            }
            flags.add("history_gap", s.id + " has no value for " + String.join(", ", missing)
                + "; each transform is computed only where its own endpoints exist.");
        }

        // Settlement period and how many observations lie between it and the last row.
        String settlement;
        int h;
        LocalDate settleDate;
        if (s.freq == Freq.MONTHLY) {
            YearMonth ym;
            if (req.settlementPeriod != null) {
                try {
                    ym = YearMonth.parse(req.settlementPeriod);
                } catch (DateTimeParseException e) {
                    throw new IllegalArgumentException("settlement_period must be YYYY-MM for "
                        + "a monthly series, got '" + req.settlementPeriod + "'");
                }
            } else {
                ym = monthOf(title, rulesText, closeTime);
                if (ym == null) {
                    throw new IllegalArgumentException("settlement period is missing: no "
                        + "month in the title or rules. Pass settlement_period (YYYY-MM)");
                }
            }
            settlement = ym.toString();
            settleDate = ym.atEndOfMonth();
            h = (int) ChronoUnit.MONTHS.between(YearMonth.from(last.date), ym);
        } else {
            String raw = req.settlementPeriod != null ? req.settlementPeriod : closeTime;
            if (raw == null || raw.length() < 10) {
                throw new IllegalArgumentException("settlement date is missing: no close time "
                    + "on the event. Pass settlement_period (YYYY-MM-DD)");
            }
            try {
                settleDate = LocalDate.parse(raw.substring(0, 10));
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("settlement_period must be an ISO date for "
                    + "a " + s.freq.key + " series, got '" + raw + "'");
            }
            settlement = settleDate.toString();
            long days = ChronoUnit.DAYS.between(last.date, settleDate);
            h = s.freq == Freq.WEEKLY ? (int) Math.ceil(days / 7.0)
                : (int) Math.round(days * 5 / 7.0);
        }
        if (h <= 0) {
            throw new IllegalStateException(s.id + " already has " + period(last.date, s.freq)
                + ", on or after the settlement period " + settlement + ": the event's value "
                + "is in the catalog. Query it instead of forecasting it.");
        }

        double[] samples;
        String method;
        double lastValue = last.value;
        // The series in the forecast's own terms, and its samples at any step short of the
        // settlement one, for the fan. A one-period change has no path: atStep stays null.
        double[] hist;
        IntFunction<double[]> atStep = null;
        switch (t) {
        case LEVEL:
            hist = v;
            atStep = k -> levelSamples(v, k, s.ratio, s.id);
            samples = levelSamples(v, h, s.ratio, s.id);
            method = "historical " + h + "-period " + (s.ratio ? "proportional" : "additive")
                + " changes applied to the latest level";
            break;
        case CHANGE: {
            List<Double> d = new ArrayList<>();
            hist = new double[len];
            Arrays.fill(hist, Double.NaN);
            for (int i = 1; i < len; i++) {
                if (!Double.isNaN(v[i]) && !Double.isNaN(v[i - 1])) {
                    d.add(v[i] - v[i - 1]);
                    hist[i] = v[i] - v[i - 1];
                }
            }
            samples = toArray(d);
            method = "historical one-period changes in the level";
            break;
        }
        case MOM: {
            List<Double> pct = new ArrayList<>();
            boolean align = !Boolean.TRUE.equals(s.seasonallyAdjusted);
            YearMonth target = YearMonth.parse(settlement);
            hist = new double[len];
            Arrays.fill(hist, Double.NaN);
            for (int i = 1; i < len; i++) {
                if (Double.isNaN(v[i]) || Double.isNaN(v[i - 1])) {
                    continue;
                }
                hist[i] = 100 * (positive(v[i], s.id) / positive(v[i - 1], s.id) - 1);
                if (!align || dates[i].getMonthValue() == target.getMonthValue()) {
                    pct.add(hist[i]);
                }
            }
            samples = toArray(pct);
            method = "historical month-over-month percent changes"
                + (align ? ", the settlement calendar month only (series not known to be "
                    + "seasonally adjusted)" : "");
            break;
        }
        case YOY: {
            if (len < 13) {
                throw new IllegalStateException(s.id + " has " + len + " months of history; "
                    + "year-over-year needs at least 13");
            }
            int base = len - 1 + h - 12;
            if (base >= len) {
                throw new IllegalStateException("year-over-year for " + settlement + " needs "
                    + "the value 12 months earlier, which is after the catalog's last period "
                    + period(last.date, s.freq) + "; settlement is more than 12 periods out");
            }
            if (Double.isNaN(v[base])) {
                throw new IllegalStateException("year-over-year for " + settlement + " needs "
                    + YearMonth.from(dates[base]) + ", and " + s.id + " has no value for it");
            }
            double[] y = new double[len];
            Arrays.fill(y, Double.NaN);
            for (int i = 12; i < len; i++) {
                if (!Double.isNaN(v[i]) && !Double.isNaN(v[i - 12])) {
                    y[i] = 100 * (positive(v[i], s.id) / positive(v[i - 12], s.id) - 1);
                }
            }
            lastValue = y[len - 1];
            if (Double.isNaN(lastValue)) {
                throw new IllegalStateException("the latest year-over-year change needs "
                    + YearMonth.from(dates[len - 13]) + ", and " + s.id + " has no value for "
                    + "it");
            }
            hist = y;
            double latest = lastValue;
            atStep = k -> yoySamples(y, k, latest);
            samples = yoySamples(y, h, latest);
            method = "historical " + h + "-period changes in the year-over-year percent, "
                + "applied to the latest year-over-year percent";
            break;
        }
        default: {
            boolean up = t == Transform.MAX_PATH;
            double floor = last.value;
            if (req.windowStart != null) {
                for (Obs o : obs) {
                    if (!o.date.isBefore(req.windowStart)) {
                        floor = up ? Math.max(floor, o.value) : Math.min(floor, o.value);
                    }
                }
            }
            hist = v;
            double bound = floor;
            atStep = k -> pathSamples(v, k, s, last.value, bound, up);
            samples = pathSamples(v, h, s, last.value, floor, up);
            method = "the " + (up ? "highest" : "lowest") + " value of historical " + h
                + "-period paths ("
                + (s.ratio ? "proportional" : "additive") + " moves from the latest level)"
                + (req.windowStart != null ? ", " + (up ? "floored" : "capped")
                    + " at the " + (up ? "highest" : "lowest") + " row since "
                    + req.windowStart : "");
            break;
        }
        }
        double scale = 1;
        if (samples.length < MIN_SAMPLES) {
            throw new IllegalStateException("only " + samples.length + " samples can be built "
                + "from " + obs.size() + " rows of " + s.id + " (" + MIN_SAMPLES + " needed); "
                + "raise years or give a longer series");
        }

        // Mismatches between what the rules settle on and what the series is.
        Boolean sa = s.seasonallyAdjusted;
        if (rulesSa != null && sa != null && !rulesSa.equals(sa)) {
            flags.add("seasonal_adjustment_mismatch", "The rules settle on a "
                + (rulesSa ? "seasonally adjusted" : "not seasonally adjusted") + " figure but "
                + s.id + " is " + (sa ? "seasonally adjusted" : "not seasonally adjusted")
                + ".");
        }
        boolean stale;
        String stalePeriod;
        if (t == Transform.MAX_PATH || t == Transform.MIN_PATH) {
            stale = ChronoUnit.DAYS.between(last.date, ref) > s.freq.staleDays;
            stalePeriod = ref.toString();
        } else {
            stale = h > 1;
            stalePeriod = s.freq == Freq.MONTHLY ? YearMonth.parse(settlement)
                .minusMonths(1).toString() : "the period before " + settlement;
        }
        if (stale) {
            flags.add("last_period_before_settlement", "The catalog's last period for " + s.id
                + " is " + period(last.date, s.freq) + ", earlier than " + stalePeriod
                + ", the period before settlement"
                + (t == Transform.MAX_PATH || t == Transform.MIN_PATH ? " reference date" : "") + "; the forecast "
                + "bridges " + h + " periods the catalog does not hold.");
        }
        LocalDate newest = s.freq == Freq.MONTHLY
            ? YearMonth.from(last.date).plusMonths(1).atEndOfMonth() : last.date;
        if (req.asOf != null && s.freq == Freq.MONTHLY && s.publishLagDays == null) {
            flags.add("publication_lag_unknown", s.id + " declares no publication lag, so "
                + "as_of " + req.asOf + " keeps every month that had ended by then, whether or "
                + "not its release had printed.");
        }
        long age = ChronoUnit.DAYS.between(newest, ref);
        int limit = s.freq == Freq.DAILY ? STALE_DAILY_DAYS
            : s.freq == Freq.WEEKLY ? STALE_WEEKLY_DAYS : STALE_MONTHLY_DAYS;
        if (age > limit) {
            flags.add("history_stale", "The latest row of " + s.id + " is for last_period "
                + period(last.date, s.freq) + ", " + (s.freq == Freq.MONTHLY
                    ? "whose following month ended " : "") + age + " days before " + ref
                + " (limit " + limit + " days for " + s.freq.key + " series); the forecast "
                + "starts from a value the market has since moved past.");
        }
        String outUnit = t == Transform.MOM || t == Transform.YOY ? "percent" : s.unit;
        Set<String> units = rulesUnits(all);
        if ("thousands".equals(outUnit) && units.isEmpty()) {
            if (s.countUnit != null && all.contains(s.countUnit)) {
                // The series is in thousands, the venue counts single jobs: put the samples
                // in the venue's units.
                for (int i = 0; i < samples.length; i++) {
                    samples[i] *= THOUSAND;
                }
                lastValue *= THOUSAND;
                scale = THOUSAND;
                outUnit = s.countUnit;
            } else {
                flags.add("units_mismatch", "The rules state no units and " + s.id + " is in "
                    + "thousands, so the strikes may be in single units: the forecast is "
                    + "unverified against them. Pass samples in the strikes' units.");
            }
        }
        if (outUnit != null && !units.isEmpty() && !units.contains(outUnit)) {
            flags.add("units_mismatch", "The rules state " + units + " but the forecast is in "
                + outUnit + ".");
        }
        Integer rulesRound = rulesRound(all);
        Integer round = req.round != null ? req.round : rulesRound;
        if (req.round != null && rulesRound != null && !req.round.equals(rulesRound)) {
            flags.add("rounding_mismatch", "round is " + req.round + " but the rules settle "
                + "to " + rulesRound + " decimals.");
        }
        if (round == null) {
            flags.add("rounding_unverified", "The rules state no rounding and none was given, "
                + "so values are compared with strikes unrounded.");
        }

        MarketPricing.Forecast f = MarketPricing.samples(samples);
        result.unrounded = f;
        if (round != null) {
            f = f.rounded(round);
        }
        result.forecast = f;
        ObjectNode stats = f.toJson();
        json.put("status", "forecast");
        json.put("series", s.id);
        json.put("series_label", s.label);
        if (s.table == null) {
            json.putNull("table");
        } else {
            json.put("table", s.table);
        }
        if (sa == null) {
            json.putNull("seasonally_adjusted");
        } else {
            json.put("seasonally_adjusted", sa);
        }
        if (outUnit == null) {
            json.putNull("units");
        } else {
            json.put("units", outUnit);
        }
        json.put("frequency", s.freq.key);
        json.put("transform", t.key);
        json.put("transform_source", spec.transformSource);
        json.put("spec_resolved_from", spec.how);
        json.put("last_period", period(last.date, s.freq));
        json.put("last_value", lastValue);
        json.put("settlement_period", settlement);
        json.put("steps_ahead", h);
        json.put("history_start", period(obs.get(0).date, s.freq));
        json.put("history_rows", obs.size());
        json.put("method", method);
        json.put("n", stats.get("n").asInt());
        json.set("median", stats.get("median"));
        json.set("p05", stats.get("p05"));
        json.set("p95", stats.get("p95"));
        if (round == null) {
            json.putNull("round");
        } else {
            json.put("round", round);
        }
        ArrayNode raw = json.putArray("samples");
        for (double x : samples) {
            raw.add(x);
        }
        json.set("fan", fan(hist, dates, s.freq, h, settlement, settleDate, samples, atStep,
            scale));
        json.set("flags", flags.out);
        json.put("next", "Price the event with price_market_event(source, event_id, "
            + "build_forecast=true) when no override was needed; otherwise with samples and "
            + "round from this result. State every flag, the method "
            + "and n, the series and table, last_period and what would make the forecast "
            + "wrong. The forecast is past changes applied to the latest level: it knows "
            + "nothing the series does not.");
        return result;
    }

    private static final double THOUSAND = 1000;
    /** Periods of history a fan shows before the forecast, and forecast steps it shows. */
    private static final int FAN_HISTORY = 24;
    private static final int FAN_STEPS = 12;

    /**
     * The forecast as a fan chart: the series' recent history in the forecast's own terms,
     * then the median and the 50% and 90% intervals of the samples at each step to settlement.
     * The fields are those of {@code render_chart} with chart_type=fan.
     *
     * <p>A forecast of one period's change has no path to settlement — {@code atStep} is null
     * — so its intervals stand at the settlement period alone.
     *
     * @param samples the samples at the settlement step, already in the output units
     * @param scale what a value of {@code hist} or {@code atStep} is multiplied by to reach
     *        those units
     */
    private static ObjectNode fan(double[] hist, LocalDate[] dates, Freq freq, int h,
            String settlement, LocalDate settleDate, double[] samples,
            IntFunction<double[]> atStep, double scale) {
        int len = hist.length;
        int from = Math.max(0, len - FAN_HISTORY);
        List<String> categories = new ArrayList<>();
        List<Double> history = new ArrayList<>();
        for (int i = from; i < len; i++) {
            categories.add(period(dates[i], freq));
            history.add(Double.isNaN(hist[i]) ? null : hist[i] * scale);
        }
        List<double[]> steps = new ArrayList<>();
        if (atStep != null) {
            int stride = (int) Math.ceil(h / (double) FAN_STEPS);
            for (int k = stride; k < h; k += stride) {
                LocalDate d = stepDate(dates[len - 1], freq, k);
                double[] x = atStep.apply(k);
                // A step whose date reaches the settlement date is the settlement step.
                if (d.isBefore(settleDate) && x.length >= MIN_SAMPLES) {
                    for (int i = 0; i < x.length; i++) {
                        x[i] *= scale;
                    }
                    categories.add(period(d, freq));
                    steps.add(x);
                }
            }
        }
        categories.add(settlement);
        steps.add(samples);

        ObjectNode fan = MAPPER.createObjectNode();
        ArrayNode cats = fan.putArray("categories");
        for (String c : categories) {
            cats.add(c);
        }
        int past = history.size();
        // The fan opens from the last value of the history, where there is a path from it.
        Double anchor = atStep == null ? null : history.get(past - 1);
        ArrayNode series = fan.putArray("series");
        ArrayNode hv = series.addObject().put("name", "history").putArray("values");
        ArrayNode mv = series.addObject().put("name", "median forecast").putArray("values");
        double[][] quantiles = {{0.25, 0.75}, {0.05, 0.95}};
        String[] names = {"50% interval", "90% interval"};
        ArrayNode bands = fan.putArray("bands");
        ArrayNode[] lows = new ArrayNode[names.length];
        ArrayNode[] highs = new ArrayNode[names.length];
        for (int b = 0; b < names.length; b++) {
            ObjectNode band = bands.addObject().put("name", names[b]);
            lows[b] = band.putArray("low");
            highs[b] = band.putArray("high");
        }
        for (int i = 0; i < past; i++) {
            boolean opens = i == past - 1 && anchor != null;
            fanValue(hv, history.get(i));
            fanValue(mv, opens ? anchor : null);
            for (int b = 0; b < names.length; b++) {
                fanValue(lows[b], opens ? anchor : null);
                fanValue(highs[b], opens ? anchor : null);
            }
        }
        for (double[] x : steps) {
            double[] ordered = x.clone();
            Arrays.sort(ordered);
            hv.addNull();
            int n = ordered.length;
            fanValue(mv, n % 2 == 1 ? ordered[n / 2] : (ordered[n / 2 - 1] + ordered[n / 2]) / 2);
            for (int b = 0; b < names.length; b++) {
                fanValue(lows[b], quantile(ordered, quantiles[b][0]));
                fanValue(highs[b], quantile(ordered, quantiles[b][1]));
            }
        }
        fan.put("note", "render_chart chart_type=fan takes categories, series and bands as "
            + "they are. The intervals are quantiles of the samples at each step"
            + (atStep == null ? "; this forecast is of one period's change, so they stand at "
                + "the settlement period alone" : "") + ". Add each strike as a "
            + "reference_lines value.");
        return fan;
    }

    private static void fanValue(ArrayNode to, Double v) {
        if (v == null) {
            to.addNull();
        } else {
            to.add(PredictionMarkets.round(v, 4));
        }
    }

    /** The same order statistic {@link MarketPricing.Forecast} reports its percentiles by. */
    private static double quantile(double[] ordered, double q) {
        return ordered[Math.min(ordered.length - 1, (int) (q * ordered.length))];
    }

    /** The date {@code k} observations after {@code last}; a daily series skips weekends. */
    private static LocalDate stepDate(LocalDate last, Freq freq, int k) {
        if (freq == Freq.MONTHLY) {
            return YearMonth.from(last).plusMonths(k).atEndOfMonth();
        }
        if (freq == Freq.WEEKLY) {
            return last.plusWeeks(k);
        }
        LocalDate d = last;
        for (int i = 0; i < k; ) {
            d = d.plusDays(1);
            if (d.getDayOfWeek().getValue() <= 5) {
                i++;
            }
        }
        return d;
    }

    private static double[] yoySamples(double[] y, int h, double latest) {
        List<Double> out = new ArrayList<>();
        for (int i = 0; i + h < y.length; i++) {
            if (!Double.isNaN(y[i]) && !Double.isNaN(y[i + h])) {
                out.add(latest + (y[i + h] - y[i]));
            }
        }
        return toArray(out);
    }

    /** The extreme of each historical {@code h}-period path replayed from the latest level. */
    private static double[] pathSamples(double[] v, int h, Series s, double latest,
            double floor, boolean up) {
        List<Double> out = new ArrayList<>();
        for (int i = 0; i + h < v.length; i++) {
            if (Double.isNaN(v[i])) {
                continue;
            }
            double max = floor;
            for (int k = 1; k <= h; k++) {
                if (Double.isNaN(v[i + k])) {
                    continue;
                }
                double x = s.ratio
                    ? latest * positive(v[i + k], s.id) / positive(v[i], s.id)
                    : latest + (v[i + k] - v[i]);
                max = up ? Math.max(max, x) : Math.min(max, x);
            }
            out.add(max);
        }
        return toArray(out);
    }

    private static double positive(double v, String id) {
        if (!(v > 0)) {
            throw new IllegalArgumentException(id + " has a value of " + v + "; a "
                + "proportional change needs positive values");
        }
        return v;
    }

    private static double[] levelSamples(double[] v, int h, boolean ratio, String id) {
        List<Double> out = new ArrayList<>();
        double last = v[v.length - 1];
        for (int i = 0; i + h < v.length; i++) {
            if (Double.isNaN(v[i]) || Double.isNaN(v[i + h])) {
                continue;
            }
            out.add(ratio ? last * positive(v[i + h], id) / positive(v[i], id)
                : last + (v[i + h] - v[i]));
        }
        return toArray(out);
    }

    private static double[] toArray(List<Double> l) {
        double[] a = new double[l.size()];
        for (int i = 0; i < a.length; i++) {
            a[i] = l.get(i);
        }
        return a;
    }
}
