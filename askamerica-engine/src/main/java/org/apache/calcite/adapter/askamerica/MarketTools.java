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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * The prediction-market tools of the MCP server: find events AskAmerica's data can price,
 * price one against a forecast, propose baskets of correlated events, and score a basket.
 *
 * <p>Every tool that lists events reads both venues, Kalshi and Polymarket, and reports what
 * it saw of each; lists and random draws alternate between them, so one venue's larger volume
 * never crowds the other out.
 */
final class MarketTools {

    /** Runs a catalog query and returns its rows, one object per row keyed by column label. */
    interface SqlRunner {
        ArrayNode rows(String sql, int limit) throws Exception;
    }

    /** The most rows a samples or scenarios query may return. */
    static final int MAX_ROWS = 5000;
    /** Events a search forecasts and prices before it may report that none is mispriced. */
    static final int MAX_DRAWS = 8;
    /** Counterparts named in an instruction; the full list is in other_venue. */
    static final int MAX_NAMED = 6;
    /** Order books read per priced event: its mispriced markets, largest edge first. */
    static final int BOOKS_READ = 3;
    static final String VOID_CLOSE = "settlement_close";
    static final String VOID_RELEASE = "settlement_series_release";
    static final List<String> VENUES =
        Collections.unmodifiableList(Arrays.asList("kalshi", "polymarket"));

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Set<String> EVENT_SPEC_KEYS = new HashSet<>(Arrays.asList(
        "source", "event_id", "samples", "samples_sql", "mean", "sd", "lower", "upper",
        "level", "price_median", "cumulative_vol_pct", "round", "conditions", "column",
        "fee_rate", "build_forecast"));

    private final PredictionMarkets.Fetcher fetcher;
    private final PredictionMarkets.ListingCache cache;
    private final SqlRunner sql;
    private final Supplier<Instant> clock;
    private final long listingWaitMillis;
    private final MarketForecasts builder;
    private final MarketBacktest backtest;
    /** Events priced against a forecast by this server process, with how many of their
     *  markets passed {@code min_edge}: what a "find a mispriced event" search has covered. */
    private final Map<String, Integer> forecastPriced = new LinkedHashMap<>();
    /** The other-venue counterparts of each event priced against a forecast, where that
     *  event is not itself one: a drawn event is owed one counterpart priced, not the whole
     *  driver family, and a counterpart owes nothing back. */
    private final Map<String, List<String>> counterparts = new LinkedHashMap<>();
    /** Whether a random draw was taken: the mark of a search, as against one named event. */
    private boolean randomSearch;

    MarketTools(PredictionMarkets.Fetcher fetcher, PredictionMarkets.ListingCache cache,
            SqlRunner sql, Supplier<Instant> clock, long listingWaitMillis) {
        this(fetcher, cache, sql, clock, listingWaitMillis,
            new MarketBacktest(fetcher, sql, clock));
    }

    /** @param backtest whose records grade the confidence of a forecast edge */
    MarketTools(PredictionMarkets.Fetcher fetcher, PredictionMarkets.ListingCache cache,
            SqlRunner sql, Supplier<Instant> clock, long listingWaitMillis,
            MarketBacktest backtest) {
        this.backtest = backtest;
        this.fetcher = fetcher;
        this.cache = cache;
        this.sql = sql;
        this.clock = clock;
        this.listingWaitMillis = listingWaitMillis;
        this.builder = new MarketForecasts(fetcher, sql, clock);
    }

    // ─── Arguments ─────────────────────────────────────────────────────────────

    private static boolean has(JsonNode args, String name) {
        return args.has(name) && !args.get(name).isNull();
    }

    private static int intArg(JsonNode args, String name, int dflt) {
        if (!has(args, name)) {
            return dflt;
        }
        if (!args.get(name).isIntegralNumber()) {
            throw new IllegalArgumentException(name + " must be an integer, got "
                + args.get(name));
        }
        return args.get(name).asInt();
    }

    private static double doubleArg(JsonNode args, String name) {
        if (!args.get(name).isNumber()) {
            throw new IllegalArgumentException(name + " must be a number, got "
                + args.get(name));
        }
        return args.get(name).asDouble();
    }

    private static boolean boolArg(JsonNode args, String name) {
        if (!has(args, name)) {
            return false;
        }
        if (!args.get(name).isBoolean()) {
            throw new IllegalArgumentException(name + " must be true or false, got "
                + args.get(name));
        }
        return args.get(name).asBoolean();
    }

    private static String textArg(JsonNode args, String name) {
        if (!has(args, name)) {
            return null;
        }
        String s = args.get(name).asText().trim();
        return s.isEmpty() ? null : s;
    }

    private static String requiredText(JsonNode args, String name) {
        String s = textArg(args, name);
        if (s == null) {
            throw new IllegalArgumentException(name + " is required");
        }
        return s;
    }

    // ─── Both venues ───────────────────────────────────────────────────────────

    /**
     * At most {@code n} of {@code ordered}, taken alternately from each venue in the order
     * given, and returned in that order. With both venues present and n at least 2, both are
     * in the result.
     */
    static List<PredictionMarkets.Event> balanced(List<PredictionMarkets.Event> ordered,
            int n) {
        if (ordered.size() <= n) {
            return ordered;
        }
        Map<String, Iterator<PredictionMarkets.Event>> byVenue = new LinkedHashMap<>();
        for (String v : VENUES) {
            List<PredictionMarkets.Event> of = new ArrayList<>();
            for (PredictionMarkets.Event e : ordered) {
                if (v.equals(e.source)) {
                    of.add(e);
                }
            }
            byVenue.put(v, of.iterator());
        }
        Set<PredictionMarkets.Event> chosen =
            Collections.newSetFromMap(new IdentityHashMap<>());
        while (chosen.size() < n) {
            for (Iterator<PredictionMarkets.Event> it : byVenue.values()) {
                if (chosen.size() < n && it.hasNext()) {
                    chosen.add(it.next());
                }
            }
        }
        List<PredictionMarkets.Event> out = new ArrayList<>();
        for (PredictionMarkets.Event e : ordered) {
            if (chosen.contains(e)) {
                out.add(e);
            }
        }
        return out;
    }

    private static ObjectNode countByVenue(List<PredictionMarkets.Event> events) {
        ObjectNode o = MAPPER.createObjectNode();
        for (String v : VENUES) {
            int n = 0;
            for (PredictionMarkets.Event e : events) {
                if (v.equals(e.source)) {
                    n++;
                }
            }
            o.put(v, n);
        }
        return o;
    }

    private static ObjectNode listingJson(PredictionMarkets.Listing listing) {
        ObjectNode o = MAPPER.createObjectNode();
        o.put("fetched_at", listing.fetchedAt.toString());
        o.put("kalshi_markets_read", listing.kalshiMarkets);
        o.put("polymarket_markets_read", listing.polymarketMarkets);
        o.put("coverage", "Kalshi: every open event. Polymarket: open events tagged "
            + PredictionMarkets.POLYMARKET_TAGS + ".");
        return o;
    }

    private static String pending(PredictionMarkets.ListingPendingException e)
            throws Exception {
        ObjectNode o = MAPPER.createObjectNode();
        o.put("status", "loading");
        o.put("pages_read", e.pagesRead);
        o.put("message", "The listing of open Kalshi and Polymarket markets is still being "
            + "read. It takes about a minute and is then kept for 15 minutes. Call this tool "
            + "again with the same arguments.");
        return MAPPER.writeValueAsString(o);
    }

    // ─── find_market_candidates ────────────────────────────────────────────────

    String findCandidates(JsonNode args) throws Exception {
        String driver = textArg(args, "driver");
        if (driver != null && PredictionMarkets.driverNamed(driver) == null) {
            List<String> names = new ArrayList<>();
            for (PredictionMarkets.Driver d : PredictionMarkets.DRIVERS) {
                names.add(d.name);
            }
            throw new IllegalArgumentException("driver must be one of " + names + ", got '"
                + driver + "'");
        }
        String basis = textArg(args, "basis");
        if (basis != null && !PredictionMarkets.BASES.contains(basis)) {
            throw new IllegalArgumentException("basis must be one of "
                + PredictionMarkets.BASES + ", got '" + basis + "'");
        }
        int within = intArg(args, "within", 180);
        int minDays = intArg(args, "min_days", 1);
        double minVolume = has(args, "min_volume") ? doubleArg(args, "min_volume") : 1000;
        int limit = intArg(args, "limit", 20);
        int sample = intArg(args, "sample", 0);
        if (sample > 0) {
            synchronized (forecastPriced) {
                randomSearch = true;
            }
        }
        boolean detail = boolArg(args, "detail");
        Set<String> exclude = new HashSet<>();
        for (JsonNode x : args.path("exclude")) {
            exclude.add(x.asText());
        }
        PredictionMarkets.Listing listing;
        try {
            listing = cache.get(boolArg(args, "refresh"), listingWaitMillis);
        } catch (PredictionMarkets.ListingPendingException e) {
            return pending(e);
        }
        List<PredictionMarkets.Event> all = PredictionMarkets.screen(listing.rows,
            clock.get(), within, minDays, minVolume);
        List<PredictionMarkets.Event> matched = new ArrayList<>();
        Map<String, Integer> byBasis = new TreeMap<>();
        for (PredictionMarkets.Event e : all) {
            if ((driver != null && !driver.equals(e.driver.name))
                    || (basis != null && !basis.equals(e.driver.basis))
                    || exclude.contains(e.eventId)) {
                continue;
            }
            matched.add(e);
            byBasis.merge(e.driver.basis, 1, Integer::sum);
        }
        ObjectNode out = MAPPER.createObjectNode();
        out.set("listing", listingJson(listing));
        out.put("matched", matched.size());
        out.set("matched_by_venue", countByVenue(matched));
        ObjectNode bb = out.putObject("matched_by_basis");
        for (Map.Entry<String, Integer> e : byBasis.entrySet()) {
            bb.put(e.getKey(), e.getValue());
        }
        List<PredictionMarkets.Event> shown;
        if (sample > 0) {
            long seed = has(args, "seed") ? args.get("seed").asLong() : System.nanoTime();
            Random rng = new Random(seed);
            List<PredictionMarkets.Event> shuffled = new ArrayList<>();
            for (String v : VENUES) {
                List<PredictionMarkets.Event> of = new ArrayList<>();
                for (PredictionMarkets.Event e : matched) {
                    if (v.equals(e.source)) {
                        of.add(e);
                    }
                }
                Collections.shuffle(of, rng);
                shuffled.addAll(of);
            }
            shown = balanced(shuffled, sample);
            out.put("seed", seed);
            out.put("selection", sample + " drawn at random, alternating Kalshi and "
                + "Polymarket; pass this seed to repeat the draw, or exclude the event_ids "
                + "already drawn to draw again");
        } else {
            shown = limit > 0 ? balanced(matched, limit) : matched;
            out.put("selection", "best basis first, then 24-hour volume, alternating Kalshi "
                + "and Polymarket");
        }
        out.put("shown", shown.size());
        out.set("shown_by_venue", countByVenue(shown));
        for (String v : VENUES) {
            if (out.get("matched_by_venue").get(v).asInt() == 0) {
                out.put("venue_note", v + " lists no event matching these filters; the events "
                    + "below are all from the other venue. Say so in the answer.");
            }
        }
        ArrayNode events = out.putArray("events");
        for (PredictionMarkets.Event e : shown) {
            events.add(detail ? e.toJson() : e.toSummaryJson());
        }
        out.put("next", "A candidate is where to look, not a mispricing. For each event: "
            + "price_market_event(source, event_id) with no forecast returns its rules and "
            + "quotes; build the forecast with the tool named in forecast_with; call "
            + "price_market_event again with the forecast. Volume is contracts on Kalshi and "
            + "dollars on Polymarket.");
        return MAPPER.writeValueAsString(out);
    }

    // ─── price_market_event ────────────────────────────────────────────────────

    /** A numeric column of a query result as doubles; the first column when unnamed. */
    private double[] sampleColumn(String query) throws Exception {
        ArrayNode rows = sql.rows(query, MAX_ROWS);
        if (rows.size() >= MAX_ROWS) {
            throw new IllegalArgumentException("samples_sql returned " + MAX_ROWS + " rows or "
                + "more; bound it below " + MAX_ROWS + " so no sample is dropped unseen");
        }
        double[] out = new double[rows.size()];
        for (int i = 0; i < out.length; i++) {
            Iterator<Map.Entry<String, JsonNode>> fields = rows.get(i).fields();
            if (!fields.hasNext()) {
                throw new IllegalArgumentException("samples_sql returned no columns");
            }
            Map.Entry<String, JsonNode> first = fields.next();
            if (!first.getValue().isNumber()) {
                throw new IllegalArgumentException("samples_sql row " + (i + 1) + ": column '"
                    + first.getKey() + "' is " + first.getValue() + ", not a number. The "
                    + "first column must be the settlement value, with no NULLs.");
            }
            out[i] = first.getValue().asDouble();
        }
        return out;
    }

    /** The forecast an event spec carries, or null when it carries none. */
    private MarketPricing.Forecast forecastOf(JsonNode spec) throws Exception {
        List<String> forms = new ArrayList<>();
        if (has(spec, "samples")) {
            forms.add("samples");
        }
        if (has(spec, "samples_sql")) {
            forms.add("samples_sql");
        }
        if (has(spec, "mean")) {
            forms.add("mean");
        }
        if (has(spec, "price_median")) {
            forms.add("price_median");
        }
        if (forms.size() > 1) {
            throw new IllegalArgumentException("give one forecast form, not " + forms);
        }
        for (String dependent : new String[]{"sd", "lower", "upper", "level"}) {
            if (has(spec, dependent) && !has(spec, "mean")) {
                throw new IllegalArgumentException(dependent + " needs mean");
            }
        }
        if (has(spec, "cumulative_vol_pct") && !has(spec, "price_median")) {
            throw new IllegalArgumentException("cumulative_vol_pct needs price_median");
        }
        MarketPricing.Forecast f;
        if (forms.isEmpty()) {
            return null;
        } else if ("samples".equals(forms.get(0))) {
            JsonNode arr = spec.get("samples");
            if (!arr.isArray()) {
                throw new IllegalArgumentException("samples must be an array of numbers");
            }
            double[] v = new double[arr.size()];
            for (int i = 0; i < v.length; i++) {
                if (!arr.get(i).isNumber()) {
                    throw new IllegalArgumentException("samples[" + i + "] is " + arr.get(i)
                        + ", not a number");
                }
                v[i] = arr.get(i).asDouble();
            }
            f = MarketPricing.samples(v);
        } else if ("samples_sql".equals(forms.get(0))) {
            f = MarketPricing.samples(sampleColumn(spec.get("samples_sql").asText()));
        } else if ("mean".equals(forms.get(0))) {
            double mean = doubleArg(spec, "mean");
            boolean interval = has(spec, "lower") || has(spec, "upper");
            if (has(spec, "sd") == interval) {
                throw new IllegalArgumentException("mean needs either sd, or lower and upper "
                    + "(with level, default 0.95)");
            }
            if (interval) {
                if (!has(spec, "lower") || !has(spec, "upper")) {
                    throw new IllegalArgumentException("lower and upper go together");
                }
                f = MarketPricing.normalFromInterval(mean, doubleArg(spec, "lower"),
                    doubleArg(spec, "upper"),
                    has(spec, "level") ? doubleArg(spec, "level") : 0.95);
            } else {
                f = MarketPricing.normal(mean, doubleArg(spec, "sd"));
            }
        } else {
            if (!has(spec, "cumulative_vol_pct")) {
                throw new IllegalArgumentException("price_median needs cumulative_vol_pct");
            }
            f = MarketPricing.lognormal(doubleArg(spec, "price_median"),
                doubleArg(spec, "cumulative_vol_pct"));
        }
        return has(spec, "round") ? f.rounded(intArg(spec, "round", 0)) : f;
    }

    private static Map<String, MarketPricing.Condition> conditionsOf(JsonNode spec) {
        Map<String, MarketPricing.Condition> given = new LinkedHashMap<>();
        if (has(spec, "conditions")) {
            JsonNode c = spec.get("conditions");
            if (!c.isObject()) {
                throw new IllegalArgumentException("conditions must be an object of market_id "
                    + "to condition, e.g. {\"1234\": {\"above\": 3.6}}");
            }
            Iterator<Map.Entry<String, JsonNode>> it = c.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                given.put(e.getKey(), MarketPricing.Condition.parse(e.getValue()));
            }
        }
        return given;
    }

    private MarketPricing.Priced price(JsonNode spec, double minEdge, boolean bothSides)
            throws Exception {
        return price(spec, minEdge, bothSides, false);
    }

    /**
     * Prices one event spec. With {@code ticketed} and a forecast, the order books of the
     * {@link #BOOKS_READ} mispriced markets with the largest edge are read and every priced
     * row carries days to settlement, annualized return and breakeven; a mispriced row
     * carries an order ticket and, where its book was read, its depth.
     */
    private MarketPricing.Priced price(JsonNode spec, double minEdge, boolean bothSides,
            boolean ticketed) throws Exception {
        String source = requiredText(spec, "source");
        String eventId = requiredText(spec, "event_id");
        PredictionMarkets.LiveEvent live =
            PredictionMarkets.fetchEvent(fetcher, source, eventId);
        MarketPricing.Forecast forecast = forecastOf(spec);
        ObjectNode built = null;
        if (boolArg(spec, "build_forecast")) {
            if (forecast != null || has(spec, "round")) {
                throw new IllegalArgumentException("build_forecast builds the forecast and "
                    + "its rounding from the rules: give it alone, or give a forecast");
            }
            PredictionMarkets.Event ev = live.event;
            MarketForecasts.Result r = builder.forecast(ev.eventTitle, ev.rules, ev.driver,
                ev.closeTime, new MarketForecasts.Request());
            forecast = r.forecast;
            built = r.json;
            built.remove("samples");
            built.remove("next");
            built.remove("tool");
        }
        Double feeRate = has(spec, "fee_rate") ? Double.valueOf(doubleArg(spec, "fee_rate"))
            : null;
        MarketPricing.Priced priced = MarketPricing.priceEvent(live, forecast,
            conditionsOf(spec), minEdge, feeRate, bothSides, textArg(spec, "column"));
        Instant now = clock.get();
        if (ticketed && forecast != null) {
            List<JsonNode> past = new ArrayList<>();
            for (JsonNode m : priced.json.path("priced_markets")) {
                if ("mispriced".equals(m.path("verdict").asText())) {
                    past.add(m);
                }
            }
            past.sort((a, b) -> Double.compare(b.get("edge").asDouble(),
                a.get("edge").asDouble()));
            Map<String, MarketHistory.OrderBook> books = new LinkedHashMap<>();
            for (JsonNode m : past.subList(0, Math.min(BOOKS_READ, past.size()))) {
                String id = m.get("market_id").asText();
                books.put(id, MarketHistory.orderBook(fetcher,
                    MarketHistory.resolve(fetcher, source, id, null)));
            }
            priced = MarketPricing.priceEvent(live, forecast, conditionsOf(spec), minEdge,
                feeRate, bothSides, textArg(spec, "column"), new MarketPricing.SizeOptions(now,
                    books, has(spec, "size") ? Double.valueOf(doubleArg(spec, "size")) : null));
            priced.json.put("books_read", books.size());
        }
        // The grid settlement values fall on, from the stated or the built rounding.
        Integer places = has(spec, "round") ? Integer.valueOf(intArg(spec, "round", 0))
            : forecast == null ? null : forecast.places;
        priced.json.set("structural_locks", MarketPricing.structuralLocks(live, feeRate,
            places == null ? null : Math.pow(10, -places), conditionsOf(spec)));
        if (built != null) {
            priced.json.set("forecast_built", built);
        }
        priced.json.put("quotes_read_at", now.toString());
        priced.json.put("venue_series", live.event.series);
        return priced;
    }

    /** Events of the other venue on the same driver closing within days of {@code event}. */
    private ObjectNode otherVenue(String source, String driver, String closeTime)
            throws Exception {
        ObjectNode o = MAPPER.createObjectNode();
        String other = "kalshi".equals(source) ? "polymarket" : "kalshi";
        o.put("venue", other);
        if (driver == null) {
            o.put("status", "not_screened");
            o.put("note", "This event's title matches no driver, so the listing cannot pair "
                + "it. Search " + other + " for the same question before reporting.");
            return o;
        }
        PredictionMarkets.Listing listing;
        try {
            listing = cache.get(false, listingWaitMillis);
        } catch (PredictionMarkets.ListingPendingException e) {
            o.put("status", "listing_loading");
            o.put("note", "The listing of both venues is still being read; call this tool "
                + "again to see whether " + other + " prices the same quantity.");
            return o;
        }
        LocalDate day = LocalDate.parse(closeTime.substring(0, 10));
        ArrayNode found = MAPPER.createArrayNode();
        for (PredictionMarkets.Event e : PredictionMarkets.screen(listing.rows, clock.get(),
                36500, 0, 0)) {
            if (other.equals(e.source) && e.driver.name.equals(driver)
                    && Math.abs(ChronoUnit.DAYS.between(day,
                        LocalDate.parse(e.closeTime.substring(0, 10))))
                        <= MarketBaskets.CROSS_VENUE_DAYS) {
                found.add(e.toSummaryJson());
            }
        }
        o.put("status", found.size() > 0 ? "found" : "none");
        o.set("counterparts", found);
        o.put("note", found.size() > 0
            ? other + " prices this driver within " + MarketBaskets.CROSS_VENUE_DAYS
                + " days of this event. Price each counterpart with the same forecast, and "
                + "test the pair with price_market_basket(lock=true, scenario_grid=true)."
            : other + " lists no event on this driver closing within "
                + MarketBaskets.CROSS_VENUE_DAYS + " days. Say that only this venue prices "
                + "it.");
        return o;
    }

    String priceEvent(JsonNode args) throws Exception {
        double minEdge = has(args, "min_edge") ? doubleArg(args, "min_edge") : 0.03;
        MarketPricing.Priced priced = price(args, minEdge, boolArg(args, "both_sides"), true);
        ObjectNode json = priced.json;
        int tickets = addReleaseVoid(json);
        json.set("other_venue", otherVenue(json.get("source").asText(),
            json.hasNonNull("driver") ? json.get("driver").asText() : null,
            json.get("close_time").asText()));
        if (json.get("forecast").isNull() && json.has("forecast_built")) {
            json.put("next", "No forecast was built: the series this event settles on is not "
                + "in the catalog (forecast_built.flags). You MUST NOT price it against a "
                + "forecast of another series. Say so, or draw another event.");
        } else if (json.get("forecast").isNull()) {
            json.put("next", "Quotes only — no forecast was given. Read rules for the exact "
                + "series, period, rounding and release date. "
                + (json.hasNonNull("basis")
                    ? "Call again with build_forecast=true. When that fails to resolve the "
                    + "series, the settlement series is in govdata_tables: you MUST call "
                    + "describe_table on each and query it before any web source, and you "
                    + "MUST build the forecast from those rows ("
                    + PredictionMarkets.forecastWith(json.get("basis").asText())
                    + "). The web is only for a print newer than the table's last period."
                    : "This event matches no driver: find the series with search_catalog.")
                + " Markets in markets_without_condition need an entry in conditions.");
        } else {
            if (json.get("implied_median") != null && !json.get("implied_median").isNull()) {
                json.put("check", "Compare forecast.median with implied_median, the market's "
                    + "own median. A large gap is more often a misread rule or a table that "
                    + "stops before the settlement period than an edge.");
            }
            json.set("chart_panel", chartPanel(json));
            json.put("chart_panel_use", "You MUST pass chart_panel in dashboard.panels of "
                + "the report for each event the report names.");
            json.set("search", searchProgress(json, minEdge));
            String source = json.get("source").asText();
            String series = json.hasNonNull("venue_series")
                ? json.get("venue_series").asText() : null;
            boolean built = json.has("forecast_built");
            json.set("confidence", MarketBacktest.confidence(backtest.record(source, series),
                built ? json.get("forecast_built").path("flags") : null, built, source,
                series));
            if (tickets > 0) {
                json.put("ticket_use", "Each mispriced market carries a ticket: the side, the "
                    + "limit_price (the most to pay and still clear min_edge after the fee), "
                    + "depth (contracts and dollars resting at or under that limit, read for "
                    + "the " + BOOKS_READ + " largest edges) and void_conditions. The quote is "
                    + "as of quote_time. You MUST report limit_price, depth and "
                    + "void_conditions with each opportunity. Asked later whether it is still "
                    + "there, you MUST call requote_market_opportunity with the ticket.");
            }
        }
        return MAPPER.writeValueAsString(json);
    }

    /**
     * Adds to every ticket the release that voids it: a ticket priced on a built forecast is
     * void once the catalog holds a period after the one the forecast started from. Returns
     * the number of tickets.
     */
    private static int addReleaseVoid(ObjectNode json) {
        JsonNode built = json.get("forecast_built");
        int tickets = 0;
        for (JsonNode m : json.path("priced_markets")) {
            if (!m.hasNonNull("ticket")) {
                continue;
            }
            tickets++;
            if (built != null && built.hasNonNull("series") && built.hasNonNull("last_period")) {
                ObjectNode v = ((ArrayNode) m.get("ticket").get("void_conditions")).addObject();
                v.put("type", VOID_RELEASE);
                v.set("series", built.get("series"));
                v.set("last_period", built.get("last_period"));
                v.put("void_when", "the catalog holds a period of the series after "
                    + "last_period: the forecast is then one release behind");
            }
        }
        return tickets;
    }

    // ─── requote_market_opportunity ────────────────────────────────────────────

    /**
     * Quotes a ticket of {@link #priceEvent} again: whether the contracts are still resting
     * at or under its limit, and whether one of its void conditions has occurred. Reads the
     * market, its order book and, for a release condition, the settlement series' last
     * period. The ticket's fair value is not recomputed.
     */
    String requote(JsonNode args) throws Exception {
        JsonNode ticket = args.get("ticket");
        if (ticket == null || !ticket.isObject()) {
            throw new IllegalArgumentException("ticket is required: the ticket object of a "
                + "mispriced market, as price_market_event returned it");
        }
        String source = requiredText(ticket, "source");
        String eventId = requiredText(ticket, "event_id");
        String marketId = requiredText(ticket, "market_id");
        Instant now = clock.get();
        ArrayNode voided = MAPPER.createArrayNode();
        Boolean released = null;
        for (JsonNode c : ticket.path("void_conditions")) {
            String type = c.path("type").asText();
            if (VOID_CLOSE.equals(type)) {
                if (!now.isBefore(PredictionMarkets.closeInstant(requiredText(c, "time")))) {
                    voided.addObject().put("type", type).put("why", "the market closed at "
                        + c.get("time").asText());
                }
            } else if (VOID_RELEASE.equals(type)) {
                String was = requiredText(c, "last_period");
                PredictionMarkets.Event ev =
                    PredictionMarkets.fetchEvent(fetcher, source, eventId).event;
                JsonNode nowBuilt = builder.forecast(ev.eventTitle, ev.rules, ev.driver,
                    ev.closeTime, new MarketForecasts.Request()).json;
                if (!nowBuilt.hasNonNull("last_period")) {
                    throw new IllegalStateException("the settlement series of " + source + " "
                        + eventId + " no longer resolves, so its last period cannot be read");
                }
                String is = nowBuilt.get("last_period").asText();
                released = !is.equals(was);
                if (released) {
                    voided.addObject().put("type", type).put("why", "the catalog's last "
                        + "period of " + c.path("series").asText() + " is now " + is
                        + ", the ticket was priced from " + was);
                }
            } else {
                throw new IllegalArgumentException("unknown void condition type '" + type + "'");
            }
        }
        MarketHistory.MarketRef ref = MarketHistory.resolve(fetcher, source, marketId, null);
        ObjectNode out;
        if (ref.open) {
            out = MarketPricing.requote(ticket, MarketHistory.orderBook(fetcher, ref));
            out.put("book_status", out.get("status").asText());
        } else {
            out = MAPPER.createObjectNode();
            out.put("market_id", marketId);
            out.put("book_status", "closed");
            out.put("venue_status", ref.status);
            out.put("status", "gone");
        }
        if (voided.size() > 0) {
            out.put("status", "void");
        }
        out.put("source", source);
        out.put("event_id", eventId);
        out.set("ticket_quote_time", ticket.get("quote_time"));
        out.put("requoted_at", now.toString());
        out.set("voided_by", voided);
        if (released == null) {
            out.putNull("release_since_quote");
        } else {
            out.put("release_since_quote", released);
        }
        String status = out.get("status").asText();
        out.put("next", "void".equals(status)
            ? "The ticket is void (voided_by). You MUST NOT report it as available. Price the "
                + "event again with price_market_event(build_forecast=true) for a new ticket."
            : "gone".equals(status)
            ? "Nothing rests at or under limit_price. Say the opportunity is gone at this "
                + "limit and give current_best_price and edge_at_current_best."
            : "partly_open".equals(status)
            ? "Only contracts_at_or_under_limit of the ticket size rest at or under "
                + "limit_price. Say so, with contracts_left."
            : "The ticket is open at its limit as of requoted_at. State that the fair value "
                + "is the ticket's and was not recomputed.");
        return MAPPER.writeValueAsString(out);
    }

    /**
     * Where a search for mispriced events stands after this pricing: what has been forecast
     * and priced so far, what passed, and the call that comes next. A search ends when enough
     * events pass or {@link #MAX_DRAWS} have been forecast and priced — not at the first
     * event that fails.
     */
    private ObjectNode searchProgress(ObjectNode json, double minEdge) {
        String source = json.get("source").asText();
        String key = source + ":" + json.get("event_id").asText();
        int passed = 0;
        ArrayNode exclude = MAPPER.createArrayNode();
        String owed;
        synchronized (forecastPriced) {
            forecastPriced.put(key, json.get("mispriced_markets").asInt());
            for (Map.Entry<String, Integer> e : forecastPriced.entrySet()) {
                exclude.add(e.getKey().substring(e.getKey().indexOf(':') + 1));
                if (e.getValue() > 0) {
                    passed++;
                }
            }
            boolean isCounterpart = false;
            for (List<String> named : counterparts.values()) {
                isCounterpart |= named.contains(key);
            }
            List<String> named = new ArrayList<>();
            for (JsonNode c : json.get("other_venue").path("counterparts")) {
                named.add(c.get("source").asText() + ":" + c.get("event_id").asText());
            }
            if (!isCounterpart && !named.isEmpty() && !counterparts.containsKey(key)) {
                counterparts.put(key, named);
            }
            owed = owedCounterparts();
        }
        ObjectNode o = MAPPER.createObjectNode();
        o.put("events_forecast_and_priced", exclude.size());
        o.put("events_with_a_market_past_min_edge", passed);
        o.put("min_edge", minEdge);
        o.set("priced_event_ids", exclude);
        StringBuilder next = new StringBuilder();
        if (owed != null) {
            next.append("You MUST price ").append(owed)
                .append(" before answering. You MUST read its rules first: reuse this "
                    + "forecast only when it settles on the same series, period and units; "
                    + "otherwise forecast the quantity its rules name. ");
        }
        if (passed == 0 && exclude.size() < MAX_DRAWS) {
            next.append("No event priced so far has a market with edge >= ").append(minEdge)
                .append(". When the question asks to find mispriced events you MUST NOT "
                    + "answer yet: call find_market_candidates(sample=1, exclude="
                    + "priced_event_ids), then forecast and price that event. Stop when the "
                    + "number asked for pass or ").append(MAX_DRAWS)
                .append(" events have been forecast and priced.");
        } else if (passed == 0) {
            next.append(MAX_DRAWS).append(" events forecast and priced and none passed: "
                + "report that, with the closest case.");
        } else {
            next.append(passed).append(" event(s) passed. When more were asked for, draw "
                + "again with exclude=priced_event_ids.");
        }
        o.put("next", next.toString().trim());
        return o;
    }

    /**
     * Why a report may not be published yet, or null. A {@code next} in a tool result is
     * advice a caller can ignore: measured live (2026-10-02), a search told "you MUST NOT
     * answer yet" after each failed event still reported "none found" after one or two draws,
     * with the other venue's counterparts unpriced. Applies only once a random draw was taken.
     */
    String searchGate() {
        synchronized (forecastPriced) {
            if (!randomSearch) {
                return null;
            }
            int passed = 0;
            for (int n : forecastPriced.values()) {
                passed += n > 0 ? 1 : 0;
            }
            String owed = owedCounterparts();
            StringBuilder b = new StringBuilder();
            if (passed == 0 && forecastPriced.size() < MAX_DRAWS) {
                b.append("the search for a mispriced market event is unfinished: ")
                    .append(forecastPriced.size()).append(" of ").append(MAX_DRAWS)
                    .append(" events have been forecast and priced and none has a market "
                        + "past min_edge. Call find_market_candidates(sample=1, exclude=")
                    .append(ids()).append("), forecast that event from its govdata_tables "
                        + "and price it with price_market_event; repeat until one passes or ")
                    .append(MAX_DRAWS).append(" are priced.");
            }
            if (owed != null) {
                b.append(b.length() > 0 ? " Also, " : "").append("no other-venue "
                    + "counterpart of an event already priced has been priced: price ")
                    .append(owed).append(". Read its rules and price it with "
                        + "price_market_event and a forecast of the quantity its rules name.");
            }
            return b.length() > 0 ? b.toString() : null;
        }
    }

    /** For each priced event with none of its counterparts priced, the one to price: named
     *  as a choice among the first few, the one whose rules settle on the same quantity. */
    private String owedCounterparts() {
        List<String> owed = new ArrayList<>();
        for (Map.Entry<String, List<String>> e : counterparts.entrySet()) {
            boolean done = false;
            for (String c : e.getValue()) {
                done |= forecastPriced.containsKey(c);
            }
            if (!done) {
                List<String> named = e.getValue();
                owed.add("one counterpart of " + e.getKey() + " (the one whose rules "
                    + "settle on the same quantity, among "
                    + String.join(", ", named.subList(0, Math.min(MAX_NAMED, named.size())))
                    + (named.size() > MAX_NAMED ? " and the rest of other_venue" : "") + ")");
            }
        }
        return owed.isEmpty() ? null : String.join("; ", owed);
    }

    /** Whether an event has been priced against a forecast since the last report. */
    boolean pricedWithForecast() {
        synchronized (forecastPriced) {
            return !forecastPriced.isEmpty();
        }
    }

    /**
     * A dashboard panel for one priced event: forecast fair value beside the YES ask, market
     * by market. Returned ready to pass on because a search that ends in a table of edges
     * was, measured live (2026-10-02, five runs), never once charted.
     */
    private static ObjectNode chartPanel(ObjectNode json) {
        ObjectNode p = MAPPER.createObjectNode();
        p.put("type", "chart");
        p.put("chart_type", "bar");
        p.put("title", json.path("event_title").asText(json.path("event_id").asText())
            + " (" + json.path("source").asText() + "): forecast fair value vs. YES ask");
        p.put("y_label", "probability of YES");
        ArrayNode categories = p.putArray("categories");
        ArrayNode fair = MAPPER.createArrayNode();
        ArrayNode ask = MAPPER.createArrayNode();
        for (JsonNode m : json.path("priced_markets")) {
            if (!m.hasNonNull("fair")) {
                continue;
            }
            String t = m.path("title").asText(m.path("market_id").asText());
            categories.add(t.length() > 28 ? t.substring(0, 27) + "…" : t);
            fair.add(m.get("fair").asDouble());
            if (m.hasNonNull("yes_ask")) {
                ask.add(m.get("yes_ask").asDouble());
            } else {
                ask.addNull();
            }
        }
        ArrayNode series = p.putArray("series");
        series.addObject().put("name", "Forecast fair value").set("values", fair);
        series.addObject().put("name", "YES ask").set("values", ask);
        return p;
    }

    /** Starts the next search from nothing: called once a report has cleared the gates. */
    void resetSearch() {
        synchronized (forecastPriced) {
            forecastPriced.clear();
            counterparts.clear();
            randomSearch = false;
        }
    }

    private List<String> ids() {
        List<String> ids = new ArrayList<>();
        for (String k : forecastPriced.keySet()) {
            ids.add(k.substring(k.indexOf(':') + 1));
        }
        return ids;
    }

    // ─── find_market_baskets ───────────────────────────────────────────────────

    String findBaskets(JsonNode args) throws Exception {
        String recipe = textArg(args, "recipe");
        if (recipe != null && !MarketBaskets.RECIPES.contains(recipe)) {
            throw new IllegalArgumentException("recipe must be one of " + MarketBaskets.RECIPES
                + ", got '" + recipe + "'");
        }
        Set<String> bases = new TreeSet<>();
        if (has(args, "basis")) {
            for (JsonNode b : args.get("basis")) {
                if (!PredictionMarkets.BASES.contains(b.asText())) {
                    throw new IllegalArgumentException("basis entries must be among "
                        + PredictionMarkets.BASES + ", got '" + b.asText() + "'");
                }
                bases.add(b.asText());
            }
        } else {
            bases.addAll(Arrays.asList("release", "climatology", "policy"));
        }
        double maxSpread = has(args, "max_spread") ? doubleArg(args, "max_spread") : 0.10;
        int limit = intArg(args, "limit", 10);
        int perBasket = intArg(args, "events_per_basket", 8);
        PredictionMarkets.Listing listing;
        try {
            listing = cache.get(boolArg(args, "refresh"), listingWaitMillis);
        } catch (PredictionMarkets.ListingPendingException e) {
            return pending(e);
        }
        List<PredictionMarkets.Event> candidates = PredictionMarkets.screen(listing.rows,
            clock.get(), intArg(args, "within", 180), intArg(args, "min_days", 1),
            has(args, "min_volume") ? doubleArg(args, "min_volume") : 1000);
        List<PredictionMarkets.Event> kept =
            MarketBaskets.filter(candidates, bases, maxSpread);
        List<MarketBaskets.Basket> baskets =
            MarketBaskets.propose(kept, recipe, textArg(args, "match"));
        ObjectNode out = MAPPER.createObjectNode();
        out.set("listing", listingJson(listing));
        out.put("candidates", candidates.size());
        out.set("candidates_by_venue", countByVenue(candidates));
        out.put("after_filters", kept.size());
        out.set("after_filters_by_venue", countByVenue(kept));
        out.put("filters", "basis in " + bases + ", median spread <= " + maxSpread
            + " with a two-sided quote");
        ObjectNode byRecipe = out.putObject("baskets_by_recipe");
        for (String r : MarketBaskets.RECIPES) {
            int n = 0;
            for (MarketBaskets.Basket b : baskets) {
                if (b.name.startsWith(r + ":")) {
                    n++;
                }
            }
            byRecipe.put(r, n);
        }
        out.put("baskets", baskets.size());
        ArrayNode arr = out.putArray("shown");
        for (int i = 0; i < baskets.size() && (limit <= 0 || i < limit); i++) {
            MarketBaskets.Basket b = baskets.get(i);
            ObjectNode o = b.toJson(perBasket);
            if (b.name.startsWith("cross_venue:")) {
                putRules(o, b.listed(perBasket));
            }
            arr.add(o);
        }
        out.put("next", "A basket's why is a hypothesis. cross_venue is the simplest "
            + "arbitrage and needs no forecast: price_market_basket with both events, "
            + "lock=true, scenario_grid=true, one shared column, and conditions stating every "
            + "market in that column's units. A cross_venue basket whose rules_match is not "
            + "'match' MUST NOT be reported as a lock. Any other recipe: measure the link in the "
            + "govdata_tables (correlation_matrix, fetch_aligned_series), forecast each event, "
            + "then price_market_basket with joint scenarios.");
        return MAPPER.writeValueAsString(out);
    }

    /** Most cross-venue pairs whose settlement rules are compared for one basket. */
    private static final int RULE_PAIRS = 6;

    /**
     * Compares the settlement rules of every pair of {@code events} on different venues and
     * writes {@code rules} and the basket's {@code rules_match}: 'differ' when any pair
     * differs, else 'unverified' when any is, else 'match'. Writes nothing for one venue.
     */
    private static void putRules(ObjectNode out, List<PredictionMarkets.Event> events) {
        ArrayNode pairs = MAPPER.createArrayNode();
        Set<String> verdicts = new TreeSet<>();
        int skipped = 0;
        for (int i = 0; i < events.size(); i++) {
            for (int j = i + 1; j < events.size(); j++) {
                PredictionMarkets.Event a = events.get(i);
                PredictionMarkets.Event b = events.get(j);
                if (a.source.equals(b.source)) {
                    continue;
                }
                if (pairs.size() >= RULE_PAIRS) {
                    skipped++;
                    continue;
                }
                ObjectNode diff = MarketRules.compare(a, b);
                ObjectNode p = pairs.addObject();
                p.put("a", a.source + ":" + a.eventId);
                p.put("b", b.source + ":" + b.eventId);
                p.set("rules_match", diff.get("rules_match"));
                p.set("differing", diff.get("differing"));
                p.set("unknown", diff.get("unknown"));
                verdicts.add(diff.get("rules_match").asText());
            }
        }
        if (pairs.size() == 0) {
            return;
        }
        out.put("rules_match", verdicts.contains(MarketRules.DIFFER) ? MarketRules.DIFFER
            : verdicts.contains(MarketRules.UNVERIFIED) || skipped > 0 ? MarketRules.UNVERIFIED
            : MarketRules.MATCH);
        out.set("rules", pairs);
        if (skipped > 0) {
            out.put("rule_pairs_not_compared", skipped);
        }
    }

    /** The legs a score lists, in its order. */
    private static List<MarketPricing.Leg> legsOf(JsonNode score, List<MarketPricing.Leg> legs) {
        List<MarketPricing.Leg> out = new ArrayList<>();
        for (JsonNode ref : score.get("legs")) {
            List<MarketPricing.Leg> found = new ArrayList<>();
            for (MarketPricing.Leg l : legs) {
                if (l.id.equals(ref.get("id").asText())
                        && l.source.equals(ref.get("source").asText())
                        && l.side.equals(ref.get("side").asText())) {
                    found.add(l);
                }
            }
            if (found.size() != 1) {
                throw new IllegalStateException("leg " + ref + " names " + found.size()
                    + " of the basket's legs; an event was given more than once");
            }
            out.add(found.get(0));
        }
        return out;
    }

    // ─── price_market_basket ───────────────────────────────────────────────────

    /** The key of {@code row} that names {@code column}: exact, else the one key equal to it
     *  ignoring case. */
    private static String keyFor(String column, Set<String> keys) {
        if (keys.contains(column)) {
            return column;
        }
        List<String> loose = new ArrayList<>();
        for (String k : keys) {
            if (k.equalsIgnoreCase(column)) {
                loose.add(k);
            }
        }
        if (loose.size() != 1) {
            throw new IllegalArgumentException("scenarios have no column '" + column
                + "'; columns are " + new TreeSet<>(keys));
        }
        return loose.get(0);
    }

    private static MarketPricing.Scenarios scenariosOf(JsonNode rows, Set<String> columns,
            String basis) {
        if (!rows.isArray() || rows.size() == 0) {
            throw new IllegalArgumentException("scenarios must be a non-empty array of "
                + "objects, one per scenario, with one number per event column");
        }
        Set<String> keys = new TreeSet<>();
        rows.get(0).fieldNames().forEachRemaining(keys::add);
        Map<String, String> keyOf = new LinkedHashMap<>();
        for (String c : columns) {
            keyOf.put(c, keyFor(c, keys));
        }
        boolean weighted = keys.contains("p");
        List<Map<String, Double>> out = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            Map<String, Double> row = new LinkedHashMap<>();
            for (Map.Entry<String, String> e : keyOf.entrySet()) {
                JsonNode v = rows.get(i).get(e.getValue());
                if (v == null || !v.isNumber()) {
                    throw new IllegalArgumentException("scenario " + (i + 1) + ": column '"
                        + e.getValue() + "' is " + v + ", not a number. Every event needs a "
                        + "value in every scenario: keep only periods where all series "
                        + "exist.");
                }
                row.put(e.getKey(), v.asDouble());
            }
            if (weighted) {
                JsonNode p = rows.get(i).get("p");
                if (p == null || !p.isNumber()) {
                    throw new IllegalArgumentException("scenario " + (i + 1)
                        + ": weight p is " + p + ", not a number");
                }
                row.put("p", p.asDouble());
            }
            out.add(row);
        }
        return MarketPricing.Scenarios.of(out, basis);
    }

    String priceBasket(JsonNode args) throws Exception {
        JsonNode specs = args.path("events");
        if (!specs.isArray() || specs.size() == 0) {
            throw new IllegalArgumentException("events must be a non-empty array of event "
                + "specs, each with source and event_id");
        }
        boolean lock = boolArg(args, "lock");
        boolean grid = boolArg(args, "scenario_grid");
        List<String> given = new ArrayList<>();
        for (String s : new String[]{"scenarios", "scenarios_sql"}) {
            if (has(args, s)) {
                given.add(s);
            }
        }
        if (grid) {
            given.add("scenario_grid");
        }
        if (given.size() != 1) {
            throw new IllegalArgumentException("give exactly one of scenarios, scenarios_sql "
                + "or scenario_grid=true, got " + given);
        }
        double minEdge = has(args, "min_edge") ? doubleArg(args, "min_edge") : 0.03;
        List<MarketPricing.Leg> legs = new ArrayList<>();
        List<PredictionMarkets.Event> pricedEvents = new ArrayList<>();
        ObjectNode out = MAPPER.createObjectNode();
        ArrayNode events = out.putArray("events");
        Set<String> venues = new TreeSet<>();
        for (JsonNode spec : specs) {
            if (!spec.isObject()) {
                throw new IllegalArgumentException("each entry of events must be an object");
            }
            Iterator<String> names = spec.fieldNames();
            while (names.hasNext()) {
                String n = names.next();
                if (!EVENT_SPEC_KEYS.contains(n)) {
                    throw new IllegalArgumentException("unknown key '" + n + "' in an event "
                        + "spec; allowed: " + new TreeSet<>(EVENT_SPEC_KEYS));
                }
            }
            MarketPricing.Priced priced = price(spec, minEdge, lock);
            if (!lock && priced.json.get("forecast").isNull()) {
                throw new IllegalArgumentException(spec.get("source").asText() + " event "
                    + spec.get("event_id").asText() + " has no forecast. Without lock=true a "
                    + "basket is built from mispriced legs, which need one.");
            }
            legs.addAll(priced.legs);
            pricedEvents.add(priced.event);
            venues.add(priced.json.get("source").asText());
            ObjectNode e = events.addObject();
            for (String f : new String[]{"source", "event_id", "event_title", "driver",
                "close_time", "forecast", "taker_fee_rates", "mispriced_markets",
                "markets_without_condition", "quotes_read_at"}) {
                e.set(f, priced.json.get(f));
            }
            e.put("legs", priced.legs.size());
        }
        ArrayNode v = out.putArray("venues");
        for (String s : venues) {
            v.add(s);
        }
        if (venues.size() < VENUES.size()) {
            out.put("venue_note", "Every event here is on " + venues + ". Check the other "
                + "venue for the same quantities (find_market_baskets recipe cross_venue) "
                + "before reporting this as the best basket.");
        }
        Set<String> columns = new TreeSet<>();
        for (MarketPricing.Leg l : legs) {
            columns.add(l.column);
        }
        MarketPricing.Scenarios scenarios;
        if (legs.isEmpty()) {
            throw new IllegalArgumentException("no leg to score: " + (lock
                ? "no market of these events has both a condition and a quote"
                : "no market of these events is mispriced by min_edge " + minEdge));
        } else if (grid) {
            scenarios = MarketPricing.grid(legs);
        } else if (has(args, "scenarios")) {
            scenarios = scenariosOf(args.get("scenarios"), columns, "scenarios argument");
        } else {
            ArrayNode rows = sql.rows(args.get("scenarios_sql").asText(), MAX_ROWS);
            if (rows.size() >= MAX_ROWS) {
                throw new IllegalArgumentException("scenarios_sql returned " + MAX_ROWS
                    + " rows or more; bound it below " + MAX_ROWS);
            }
            scenarios = scenariosOf(rows, columns, "scenarios_sql, " + rows.size() + " rows");
        }
        ObjectNode score = MarketPricing.scoreBasket(legs, scenarios, grid,
            intArg(args, "search", 0), lock,
            has(args, "min_yield") ? Double.valueOf(doubleArg(args, "min_yield")) : null,
            intArg(args, "top", 10));
        out.setAll(score);
        putRules(out, pricedEvents);
        if (columns.size() == 1) {
            // The curve of the best subset the search kept; of every leg when no search ran.
            JsonNode best = score.path("search").path("best").path(0);
            boolean searched = score.has("search");
            if (!searched || !best.isMissingNode()) {
                List<Map<String, Double>> points = MarketPricing.grid(legs).rows;
                double[] values = new double[points.size()];
                String column = columns.iterator().next();
                for (int i = 0; i < values.length; i++) {
                    values[i] = points.get(i).get(column);
                }
                ObjectNode curve = MarketBaskets.payoffCurve(
                    searched ? legsOf(best, legs) : legs, values);
                curve.put("of", searched ? "search.best[0]" : "all_legs");
                out.set("payoff_curve", curve);
            }
        }
        if (lock) {
            String rules = out.path("rules_match").asText(null);
            out.put("next", "A lock MUST be reported with its floor net of fees and its "
                + "payoff_curve." + (rules == null ? ""
                : MarketRules.MATCH.equals(rules)
                    ? " rules_match is 'match': the two venues' rule texts agree."
                    : " rules_match is '" + rules + "': a lock holding legs on both venues "
                        + "MUST be reported as not a lock, with the differing and unknown "
                        + "dimensions in rules.")
                + " With no subset kept, state that no lock exists at these quotes.");
        }
        out.put("limits", "Fees are a taker order at the quote. Not modelled: depth behind "
            + "the quote, position limits, cost of capital until settlement, and the two "
            + "venues settling in different currencies on different rule texts. A lock is "
            + "only as complete as its scenarios, and a cross-venue lock only as sound as the "
            + "two rule texts agreeing on source, period and rounding.");
        return MAPPER.writeValueAsString(out);
    }
}
