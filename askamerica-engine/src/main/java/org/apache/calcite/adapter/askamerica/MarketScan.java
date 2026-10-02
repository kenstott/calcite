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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * The search for mispriced events, run inside the engine: every candidate event is forecast by
 * {@link MarketForecasts} and priced by {@link MarketPricing}, and the caller gets the ranked
 * shortlist. One call replaces a draw, a rules read, a query, a forecast and a pricing call
 * per event.
 *
 * <p>An event is evaluated once and kept for the listing's lifetime, so a scan that runs out
 * of its time budget resumes where it stopped on the next call.
 */
final class MarketScan {
    static final String TOOL = "scan_market_opportunities";
    static final Set<String> KEYS = new TreeSet<>(Arrays.asList("driver", "min_edge", "within",
        "min_days", "min_volume", "min_market_volume", "limit", "max_events", "include_flagged",
        "refresh"));
    /** Flags that say the forecast is of a different quantity than the event settles on, or
     *  starts from a value the market has already moved past. */
    static final Set<String> BLOCKING_FLAGS = Collections.unmodifiableSet(new TreeSet<>(
        Arrays.asList("seasonal_adjustment_mismatch", "units_mismatch", "rounding_mismatch",
            "history_stale")));
    static final String INSIDE = "market_inside_baseline_range";
    static final String OUTSIDE = "market_outside_baseline_range";
    private static final int MARKETS_SHOWN = 3;
    private static final int EXAMPLES_SHOWN = 5;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PredictionMarkets.Fetcher fetcher;
    private final PredictionMarkets.ListingCache cache;
    private final MarketForecasts builder;
    private final MarketBacktest backtest;
    private final Supplier<Instant> clock;
    private final long listingWaitMillis;
    private final long budgetMillis;
    private final Duration ttl;
    /** One evaluation per event, keyed source:event_id. */
    private final Map<String, Row> rows = new LinkedHashMap<>();

    MarketScan(PredictionMarkets.Fetcher fetcher, PredictionMarkets.ListingCache cache,
            MarketTools.SqlRunner sql, Supplier<Instant> clock, long listingWaitMillis,
            long budgetMillis, Duration ttl) {
        this(fetcher, cache, sql, clock, listingWaitMillis, budgetMillis, ttl,
            new MarketBacktest(fetcher, sql, clock));
    }

    /** @param backtest whose records grade each opportunity and drop a series the venue's
     *                 price has beaten */
    MarketScan(PredictionMarkets.Fetcher fetcher, PredictionMarkets.ListingCache cache,
            MarketTools.SqlRunner sql, Supplier<Instant> clock, long listingWaitMillis,
            long budgetMillis, Duration ttl, MarketBacktest backtest) {
        this.backtest = backtest;
        this.fetcher = fetcher;
        this.cache = cache;
        this.builder = new MarketForecasts(fetcher, sql, clock);
        this.clock = clock;
        this.listingWaitMillis = listingWaitMillis;
        this.budgetMillis = budgetMillis;
        this.ttl = ttl;
    }

    /** What the scan made of one event. */
    private static final class Row {
        Instant at;
        /** priced, unresolved, not_sourced, no_condition or fetch_failed. */
        String outcome;
        String reason;
        ObjectNode json;
        /** Edge of the best market whose edge is outside sampling error; null when none. */
        Double bestEdge;
        boolean blocked;
        /** The market's implied median is outside the forecast's p05 to p95. */
        boolean outside;
        /** The baseline beat this series' price in the last backtest. */
        boolean backtested;
        /** The mispriced markets, best edge first. */
        List<JsonNode> past;
        /** json with the markets that count under this call's arguments. */
        ObjectNode shown;
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

    private static double doubleArg(JsonNode args, String name, double dflt) {
        if (!has(args, name)) {
            return dflt;
        }
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
        props.set("driver", prop("string",
            "Only events of this driver, e.g. inflation, unemployment, mortgage_rate. Omit for "
            + "every driver the engine can forecast."));
        props.set("min_edge", prop("number",
            "Smallest edge after fees, in probability points, to list (default 0.10)."));
        props.set("within", prop("integer", "Only events closing within this many days "
            + "(default 60)."));
        props.set("min_days", prop("integer", "Only events closing at least this many days "
            + "out (default 1)."));
        props.set("min_volume", prop("number", "Smallest event volume (default 1000)."));
        props.set("min_market_volume", prop("number",
            "Smallest volume of a market, in contracts, for its edge to count (default 500)."));
        props.set("limit", prop("integer", "Opportunities to return (default 5)."));
        props.set("max_events", prop("integer",
            "Most events to evaluate, most traded first (default 60)."));
        props.set("include_flagged", prop("boolean",
            "true: also list events whose forecast is of a different quantity than the rules "
            + "name (seasonal adjustment, units or rounding mismatch) or starts from a stale "
            + "value (history_stale)."));
        props.set("refresh", prop("boolean", "true: re-read the venues and re-evaluate."));
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("type", "object");
        schema.set("properties", props);
        schema.putArray("required");
        ObjectNode out = MAPPER.createObjectNode();
        out.put("name", TOOL);
        out.put("description", DESCRIPTION);
        out.set("inputSchema", schema);
        return out;
    }

    private static final String DESCRIPTION =
        "Find mispriced Kalshi and Polymarket events in one call. The engine forecasts every "
        + "candidate event from the catalog series its rules settle on, prices each market "
        + "net of the venue's taker fee, and returns the events with a market whose edge is "
        + "at least min_edge and outside sampling error, best edge first. Each opportunity "
        + "gives the market, side, price, fair, fee, edge, return_on_cost, volume, days to "
        + "settlement, and the forecast's series, table, transform, method, n, last_period "
        + "and flags. funnel counts the events matched, evaluated, forecast and passed; "
        + "not_forecast says why the rest were left out; structural lists events whose own "
        + "quotes contradict each other, which need no forecast. The forecast is past "
        + "changes of the series applied to its latest value: the baseline any participant "
        + "can compute, not private information. baseline_vs_market says whether the "
        + "market's implied median is inside the forecast's p05-p95 (the market is only more "
        + "certain than the baseline) or outside it. 'Mispriced by more than 10%' means edge >= "
        + "0.10. Can return status 'loading' or 'scanning'. You MUST call again with the same "
        + "arguments while status is 'loading' or 'scanning'. You MUST use this tool first "
        + "when asked to find mispriced events. You MUST vet at most 3 opportunities: read "
        + "each one's rules with price_market_event(source, event_id, build_forecast=true) "
        + "and confirm the forecast series, period and rounding are what the rules name. You "
        + "MUST state every flag and baseline_vs_market. When asked for a random event you MUST pick at random "
        + "among opportunities. When opportunities is empty you MUST say none was found and "
        + "report funnel. You MUST NOT report an opportunity you have not vetted.";

    // ─── The scan ──────────────────────────────────────────────────────────────

    private static String key(PredictionMarkets.Event e) {
        return e.source + ":" + e.eventId;
    }

    /** The first sentence of an error, short enough to group by. */
    private static String brief(String message) {
        String m = message == null ? "no message" : message;
        int cut = m.indexOf(": ");
        return cut > 0 && cut < 80 ? m.substring(0, cut) : m.length() > 80
            ? m.substring(0, 80) : m;
    }

    private Row evaluate(PredictionMarkets.Event listed) throws Exception {
        Row row = new Row();
        row.at = clock.get();
        PredictionMarkets.LiveEvent live;
        try {
            live = PredictionMarkets.fetchEvent(fetcher, listed.source, listed.eventId);
        } catch (IOException e) {
            row.outcome = "fetch_failed";
            row.reason = brief(e.getMessage());
            return row;
        }
        PredictionMarkets.Event ev = live.event;
        MarketForecasts.Result built;
        try {
            built = builder.forecast(ev.eventTitle, ev.rules, ev.driver, ev.closeTime,
                new MarketForecasts.Request());
        } catch (IllegalArgumentException e) {
            row.outcome = "unresolved";
            row.reason = brief(e.getMessage());
            return row;
        }
        if (built.forecast == null) {
            row.outcome = "not_sourced";
            row.reason = built.json.path("series_named").asText("series not in the catalog");
            return row;
        }
        MarketPricing.Priced priced = MarketPricing.priceEvent(live, built.forecast,
            Collections.<String, MarketPricing.Condition>emptyMap(), 0, null, false, null,
            new MarketPricing.SizeOptions(row.at, null, null));
        List<JsonNode> past = new ArrayList<>();
        boolean any = false;
        for (JsonNode m : priced.json.path("priced_markets")) {
            any |= m.path("fair").isNumber();
            if ("mispriced".equals(m.path("verdict").asText()) && m.path("edge").isNumber()) {
                past.add(m);
            }
        }
        if (!any) {
            row.outcome = "no_condition";
            row.reason = "no market of the event carries a strike the engine can read";
            return row;
        }
        past.sort(Comparator.comparingDouble((JsonNode m) -> m.get("edge").asDouble())
            .reversed());
        ObjectNode o = MAPPER.createObjectNode();
        o.put("source", ev.source);
        o.put("event_id", ev.eventId);
        o.put("event_title", ev.eventTitle);
        o.put("venue_series", ev.series);
        o.put("driver", ev.driver == null ? null : ev.driver.name);
        o.put("url", ev.url);
        o.put("close_time", ev.closeTime);
        o.put("days_to_settlement", PredictionMarkets.round(Duration.between(row.at,
            PredictionMarkets.closeInstant(ev.closeTime)).toMinutes() / 1440.0, 1));
        o.put("quotes_read_at", row.at.toString());
        if (ev.impliedMedian != null) {
            o.put("market_implied_median", ev.impliedMedian);
            JsonNode lo = built.json.path("p05");
            JsonNode hi = built.json.path("p95");
            if (lo.isNumber() && hi.isNumber()) {
                row.outside = ev.impliedMedian < lo.asDouble()
                    || ev.impliedMedian > hi.asDouble();
                o.put("baseline_vs_market", row.outside ? OUTSIDE : INSIDE);
            }
        }
        ObjectNode f = o.putObject("forecast");
        for (String name : new String[]{"series", "table", "transform", "method", "n",
            "median", "p05", "p95", "last_period", "settlement_period"}) {
            f.set(name, built.json.get(name));
        }
        ArrayNode codes = f.putArray("flags");
        for (JsonNode flag : built.json.path("flags")) {
            String code = flag.path("code").asText();
            codes.add(code);
            row.blocked |= BLOCKING_FLAGS.contains(code);
        }
        row.outcome = "priced";
        row.json = o;
        row.past = past;
        return row;
    }

    synchronized String scan(JsonNode args) throws Exception {
        Iterator<String> names = args.fieldNames();
        while (names.hasNext()) {
            String n = names.next();
            if (!KEYS.contains(n)) {
                throw new IllegalArgumentException("unknown key '" + n + "' in " + TOOL
                    + "; allowed: " + KEYS);
            }
        }
        String driver = has(args, "driver") ? args.get("driver").asText() : null;
        if (driver != null && PredictionMarkets.driverNamed(driver) == null) {
            throw new IllegalArgumentException("driver '" + driver + "' is not a driver; see "
                + "find_market_candidates");
        }
        double minEdge = doubleArg(args, "min_edge", 0.10);
        int within = intArg(args, "within", 60);
        int minDays = intArg(args, "min_days", 1);
        double minVolume = doubleArg(args, "min_volume", 1000);
        double minMarketVolume = doubleArg(args, "min_market_volume", 500);
        int limit = intArg(args, "limit", 5);
        int maxEvents = intArg(args, "max_events", 60);
        boolean includeFlagged = boolArg(args, "include_flagged");
        boolean refresh = boolArg(args, "refresh");

        PredictionMarkets.Listing listing;
        try {
            listing = cache.get(refresh, listingWaitMillis);
        } catch (PredictionMarkets.ListingPendingException e) {
            ObjectNode o = MAPPER.createObjectNode();
            o.put("status", "loading");
            o.put("pages_read", e.pagesRead);
            o.put("message", "The listing of open Kalshi and Polymarket markets is still "
                + "being read. Call this tool again with the same arguments.");
            return MAPPER.writeValueAsString(o);
        }
        Instant now = clock.get();
        if (refresh) {
            rows.clear();
        }
        List<PredictionMarkets.Event> matched = new ArrayList<>();
        List<PredictionMarkets.Event> structural = new ArrayList<>();
        for (PredictionMarkets.Event e : PredictionMarkets.screen(listing.rows, now, within,
                minDays, minVolume)) {
            if (driver != null && !driver.equals(e.driver.name)) {
                continue;
            }
            matched.add(e);
            if (!e.locks.isEmpty()) {
                structural.add(e);
            }
        }
        List<PredictionMarkets.Event> chosen = new ArrayList<>(matched);
        chosen.sort(Comparator.comparingLong((PredictionMarkets.Event e) -> e.volume24h)
            .reversed());
        if (chosen.size() > maxEvents) {
            chosen = chosen.subList(0, maxEvents);
        }

        long started = System.nanoTime();
        int evaluated = 0;
        int fresh = 0;
        boolean outOfTime = false;
        for (PredictionMarkets.Event e : chosen) {
            Row have = rows.get(key(e));
            if (have != null && Duration.between(have.at, now).compareTo(ttl) < 0) {
                evaluated++;
                continue;
            }
            // At least one event per call, so a scan always advances.
            if (fresh > 0 && (System.nanoTime() - started) / 1_000_000L > budgetMillis) {
                outOfTime = true;
                break;
            }
            rows.put(key(e), evaluate(e));
            evaluated++;
            fresh++;
        }

        ObjectNode out = MAPPER.createObjectNode();
        out.put("status", outOfTime ? "scanning" : "complete");
        out.put("listing_read_at", listing.fetchedAt.toString());
        out.put("min_edge", minEdge);
        Map<String, Integer> why = new TreeMap<>();
        Map<String, List<String>> examples = new TreeMap<>();
        List<Row> passed = new ArrayList<>();
        int forecast = 0;
        int flagged = 0;
        int beaten = 0;
        boolean unbacktested = false;
        for (PredictionMarkets.Event e : chosen) {
            Row r = rows.get(key(e));
            if (r == null) {
                continue;
            }
            if (!"priced".equals(r.outcome)) {
                String k = r.outcome + ": " + r.reason;
                why.merge(k, 1, Integer::sum);
                List<String> ex = examples.computeIfAbsent(k, x -> new ArrayList<String>());
                if (ex.size() < EXAMPLES_SHOWN) {
                    ex.add(key(e));
                }
                continue;
            }
            forecast++;
            ObjectNode o = r.json.deepCopy();
            ArrayNode markets = o.putArray("markets");
            int counted = 0;
            r.bestEdge = null;
            for (JsonNode m : r.past) {
                if (m.path("volume").asDouble(0) < minMarketVolume) {
                    continue;
                }
                if (counted == 0) {
                    r.bestEdge = m.get("edge").asDouble();
                }
                if (counted++ < MARKETS_SHOWN) {
                    ObjectNode shown = markets.addObject();
                    for (String f : new String[]{"market_id", "title", "side", "price",
                        "fair", "se", "fee", "edge", "return_on_cost",
                        "annualized_return_simple_365d", "breakeven_fair", "volume"}) {
                        if (m.has(f)) {
                            shown.set(f, m.get(f));
                        }
                    }
                }
            }
            o.put("markets_past_sampling_error", counted);
            r.shown = o;
            if (r.bestEdge == null || r.bestEdge < minEdge) {
                continue;
            }
            if (r.blocked && !includeFlagged) {
                flagged++;
                continue;
            }
            String series = o.hasNonNull("venue_series")
                ? o.get("venue_series").asText() : null;
            ObjectNode record = backtest.record(o.get("source").asText(), series);
            if (record != null && !includeFlagged
                && "market_beats_baseline".equals(record.path("verdict").asText())) {
                beaten++;
                continue;
            }
            ObjectNode confidence = MarketBacktest.confidence(record,
                o.get("forecast").get("flags"), true, o.get("source").asText(), series);
            confidence.remove("must");
            o.set("confidence", confidence);
            r.backtested = MarketBacktest.TIER_BACKTESTED.equals(
                confidence.get("tier").asText());
            unbacktested |= record == null && "kalshi".equals(o.get("source").asText());
            passed.add(r);
        }
        // A series whose baseline has beaten the price comes first; then a market outside
        // the baseline's own range, the sharper disagreement.
        passed.sort(Comparator.comparing((Row r) -> !r.backtested)
            .thenComparing((Row r) -> !r.outside)
            .thenComparing(Comparator.comparingDouble((Row r) -> r.bestEdge).reversed()));
        ObjectNode funnel = out.putObject("funnel");
        funnel.put("events_matched", matched.size());
        funnel.put("events_to_evaluate", chosen.size());
        funnel.put("events_evaluated", evaluated);
        funnel.put("events_forecast", forecast);
        funnel.put("events_passed", passed.size());
        funnel.put("events_past_min_edge_left_out_for_a_blocking_flag", flagged);
        funnel.put("events_past_min_edge_left_out_as_the_price_beat_the_baseline_in_backtest",
            beaten);
        ArrayNode opps = out.putArray("opportunities");
        for (int i = 0; i < passed.size() && i < limit; i++) {
            opps.add(passed.get(i).shown);
        }
        ArrayNode not = out.putArray("not_forecast");
        for (Map.Entry<String, Integer> e : why.entrySet()) {
            ObjectNode n = not.addObject();
            n.put("why", e.getKey());
            n.put("events", e.getValue());
            ArrayNode ex = n.putArray("examples");
            for (String id : examples.get(e.getKey())) {
                ex.add(id);
            }
        }
        ArrayNode locks = out.putArray("structural");
        for (int i = 0; i < structural.size() && i < EXAMPLES_SHOWN; i++) {
            PredictionMarkets.Event e = structural.get(i);
            ObjectNode s = locks.addObject();
            s.put("source", e.source);
            s.put("event_id", e.eventId);
            s.put("event_title", e.eventTitle);
            s.put("locks_basis", "before fees, from the listing's quotes; price_market_event "
                + "returns structural_locks net of fees");
            ArrayNode l = s.putArray("locks");
            for (ObjectNode lock : e.locks) {
                l.add(lock);
            }
        }
        out.put("forecast_is", "past changes of the settlement series applied to its latest "
            + "value: a baseline any participant can compute, not private information");
        out.put("baseline_vs_market_is", INSIDE + ": the market's implied median is inside "
            + "the forecast's p05-p95, so the edge says only that the market is more certain "
            + "than the baseline. " + OUTSIDE + ": the market's level is one the baseline "
            + "gives under 5% on that side.");
        if (outOfTime) {
            out.put("next", "The scan is unfinished: " + evaluated + " of " + chosen.size()
                + " events evaluated. You MUST call " + TOOL + " again with the same "
                + "arguments before answering.");
        } else if (passed.isEmpty()) {
            out.put("next", "No event has a market with edge >= " + minEdge + " outside "
                + "sampling error. Say none was found and report funnel and not_forecast. "
                + "An event in not_forecast can still be priced by hand: "
                + "forecast_market_event with the series, table and transform given.");
        } else {
            out.put("next", "Vet at most 3 of opportunities before reporting: "
                + "price_market_event(source, event_id, build_forecast=true), read rules, "
                + "confirm the forecast's series, period and rounding are what the rules "
                + "name, and state every flag, confidence and baseline_vs_market. "
                + (unbacktested ? "For a Kalshi opportunity whose confidence.backtest is null "
                    + "you MUST call " + MarketBacktest.TOOL + "(source, series=venue_series) "
                    + "and state its verdict. " : "")
                + "When asked for a random event, pick at random among opportunities.");
        }
        return MAPPER.writeValueAsString(out);
    }
}
