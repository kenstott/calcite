/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * Prices every basket the quotes alone can settle, in one pass, and lists the arb-like ones.
 *
 * <p>Two tiers. Within one event: a ladder whose strikes are priced out of order, a bucket
 * partition the strikes prove, and a set of markets the venue states has at most one winner.
 * Across venues: each Kalshi and Polymarket pair of one cross-venue basket, scored over every
 * outcome their conditions can tell apart, keeping subsets with a leg on each venue that lose
 * at none. A pair is a lock only when the forecast builder reads the same series and transform
 * from both events; a pair it reads differently is not priced, and a pair it cannot read on
 * one side is priced and listed apart as unverified.
 *
 * <p>A verified pair with no lock is then scored against the forecast of the number it settles
 * on: a near-lock is a basket of it that profits in both tails, loses only between two strikes,
 * and that the forecast expects to pay more than it costs with a loss no likelier than
 * {@code max_loss_probability}. Locks need no forecast; near-locks are a judgement of one,
 * kept only while the quotes agree: they too put the losing band under the cap, and the
 * forecast puts every leg within {@link #MAX_QUOTE_GAP} and {@link #MAX_QUOTE_RATIO} of its
 * quote.
 *
 * <p>An event is read once and kept for the listing's lifetime, so a scan that runs out of its
 * time budget resumes where it stopped on the next call.
 */
final class MarketBasketScan {
    static final String TOOL = "scan_market_baskets";
    static final Set<String> KEYS = new TreeSet<>(Arrays.asList("driver", "within", "min_days",
        "min_volume", "max_spread", "min_floor", "max_loss_probability", "search", "limit",
        "max_events", "refresh"));
    /** The status of a scan that read every event it chose. */
    static final String COMPLETE = "complete";
    /** Rule dimensions that say what number an event settles on. */
    static final List<String> QUANTITY = Arrays.asList("series", "settlement_period",
        "transform");
    private static final Set<String> BASES =
        new TreeSet<>(Arrays.asList("release", "climatology", "policy"));
    private static final String COLUMN = "v";
    private static final String VERIFIED = "verified";
    private static final String NEAR_RULE = " You MUST NOT report a near_locks entry as a "
        + "lock: state its loses_between, worst_profit, p_loss, market_p_loss and "
        + "expected_profit, and that p_loss and expected_profit are the engine's forecast. "
        + "For an entry whose same_quantity is 'converted' you MUST state its conversion's "
        + "wedge_error and that the two events settle on different numbers.";
    /** {@code same_quantity} of a pair on two numbers one conversion apart. */
    static final String CONVERTED = "converted";
    private static final int EXAMPLES_SHOWN = 5;
    private static final int CLOSEST_SHOWN = 3;
    private static final int PER_PAIR = 1;
    /** The furthest the forecast may put a near-lock leg's chance of winning from its price. */
    static final double MAX_QUOTE_GAP = 0.20;
    /**
     * The most the forecast may put a near-lock leg's winning, or its losing, over what the
     * price implies. The gap alone passes a leg quoted at 0.006 that the forecast puts at 0.17.
     */
    static final double MAX_QUOTE_RATIO = 2;
    private static final String STEP_TAKEN = "the change to be a multiple of 25 basis points";
    private static final String DEGREE_TAKEN = "the temperature to be a whole number of degrees";
    /** {@code same_quantity} of a pair on one station's temperature on one day, which each
     *  venue reads from a different record. */
    static final String TWO_MEASUREMENTS = "two_measurements";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PredictionMarkets.Fetcher fetcher;
    private final PredictionMarkets.ListingCache cache;
    private final MarketForecasts builder;
    private final Supplier<Instant> clock;
    private final long listingWaitMillis;
    private final long budgetMillis;
    private final Duration ttl;
    /** One read per event, keyed source:event_id. */
    private final Map<String, Row> rows = new LinkedHashMap<>();

    MarketBasketScan(PredictionMarkets.Fetcher fetcher, PredictionMarkets.ListingCache cache,
            MarketTools.SqlRunner sql, Supplier<Instant> clock, long listingWaitMillis,
            long budgetMillis, Duration ttl) {
        this.fetcher = fetcher;
        this.cache = cache;
        this.builder = new MarketForecasts(fetcher, sql, clock);
        this.clock = clock;
        this.listingWaitMillis = listingWaitMillis;
        this.budgetMillis = budgetMillis;
        this.ttl = ttl;
    }

    /** One event as its venue quoted it. */
    private static final class Row {
        Instant at;
        /** Null when the venue could not be read; {@link #reason} then says why. */
        PredictionMarkets.LiveEvent live;
        String reason;
        Map<String, MarketPricing.Condition> conditions;
        /** Set when the event is one FOMC meeting's decision; its conditions are then the
         *  change of the target rate in basis points. */
        MarketPricing.Decision decision;
        /** Set when the event is one day's highest or lowest temperature in one place. */
        MarketPricing.DailyExtreme extreme;
        /** Baskets inside the event, locking or not. */
        List<ObjectNode> baskets = new ArrayList<>();
        /** Whether the forecast of the event's quantity was asked for; it is built once. */
        boolean forecastTried;
        /** Null when it was not built; {@link #forecastReason} then says why. */
        MarketPricing.Forecast forecast;
        /** {@link #forecast} before the rules' rounding. */
        MarketPricing.Forecast unrounded;
        String forecastReason;
    }

    /** What pricing a pair leaves for the near-lock search. */
    private static final class Paired {
        List<MarketPricing.Leg> legs;
        Map<String, MarketPricing.Leg> byKey;
        int depth;
        ObjectNode rules;
        Instant at;
        /** The one series and transform both events resolve to, or null. */
        String settlesOn;
        /** Set when one event is a month's month-over-month change and the other the same
         *  index's year-over-year change: the pair is two numbers, and has no lock. */
        Row momRow;
        MarketForecasts.Quantity mom;
        MarketForecasts.Quantity yoy;
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
            "Only events of this driver, e.g. inflation, unemployment, output. Omit for every "
            + "driver."));
        props.set("within", prop("integer", "Only events closing within this many days "
            + "(default 180)."));
        props.set("min_days", prop("integer", "Only events closing at least this many days "
            + "out (default 1)."));
        props.set("min_volume", prop("number", "Smallest event volume (default 1000)."));
        props.set("max_spread", prop("number", "Widest median bid-ask spread of an event paired "
            + "across venues (default 0.10)."));
        props.set("min_floor", prop("number", "Smallest worst-case profit per unit of cost, "
            + "after fees, to list (default 0: any lock)."));
        props.set("max_loss_probability", prop("number", "Largest probability of a loss, "
            + "on the forecast and on the quotes, for a near_locks entry (default 0.10)."));
        props.set("search", prop("integer", "Most legs in a cross-venue basket, 2 to 4 "
            + "(default 3)."));
        props.set("limit", prop("integer", "Baskets to return (default 5)."));
        props.set("max_events", prop("integer", "Most events to read, cross-venue pairs first "
            + "(default 60)."));
        props.set("refresh", prop("boolean", "true: re-read the venues."));
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
        "Find baskets of Kalshi and Polymarket contracts that lock a profit after fees,"
        + " from the quotes alone. Within one event: strikes priced out of order, a "
        + "bucket partition the strikes prove, and NO on every market of an event the "
        + "venue states has at most one winner. Across venues: every Kalshi and "
        + "Polymarket pair find_market_baskets lists as cross_venue, scored over every "
        + "outcome their conditions can tell apart, keeping baskets with a leg on each "
        + "venue that lose at none. A pair is in baskets only when the engine resolves "
        + "both events to one settlement series and transform (settles_on); one it "
        + "resolves to different series is in not_priced; one it cannot resolve on one "
        + "side is in unverified with same_quantity_reason. Each basket gives its legs, "
        + "cost, floor_profit, floor (worst case per unit of cost) and, across venues, "
        + "rules_match. near_locks are arb-like bets, not locks: baskets of a verified "
        + "pair with no lock that profit outside loses_between, with worst_profit and, "
        + "from the engine's forecast, p_loss (at most max_loss_probability) and "
        + "expected_profit. funnel counts what was read; not_priced says why a pair was "
        + "left out; closest lists the exclusive events nearest a lock. size reads the "
        + "legs' order books: the sets that fill, their capital and profit in dollars. "
        + "Cost is paid at purchase; the annualized rate is over days_to_settlement. "
        + "Other recipes settle on different quantities and are counted, not priced: "
        + "price_market_basket takes them with scenarios_sql. You MUST call again with "
        + "the same arguments while status is 'loading' or 'scanning'. You MUST use this "
        + "tool first when asked to find a basket that locks a yield, an arbitrage or an "
        + "arb-like bet. You MUST report a cross_venue basket as not a lock unless its "
        + "rules_match is 'match', and state rules_differing and rules_unknown. You MUST "
        + "state cost, fees, floor, size and quotes_read_at for every basket reported. "
        + "You MUST NOT report an unverified or near_locks entry as a lock. When baskets "
        + "is empty you MUST say none was found and report funnel and not_priced.";

    // ─── One event ─────────────────────────────────────────────────────────────

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

    private static ObjectNode ref(PredictionMarkets.Event e) {
        ObjectNode o = MAPPER.createObjectNode();
        o.put("source", e.source);
        o.put("event_id", e.eventId);
        o.put("event_title", e.eventTitle);
        o.put("driver", e.driver == null ? null : e.driver.name);
        o.put("url", e.url);
        o.put("close_time", e.closeTime);
        return o;
    }

    /** A basket inside one event, from one side of a structural node. */
    private static ObjectNode within(String type, PredictionMarkets.Event e, Instant at,
            JsonNode legs, double cost, double floorProfit, String basis) {
        ObjectNode b = MAPPER.createObjectNode();
        b.put("type", type);
        b.putArray("venues").add(e.source);
        b.putArray("events").add(ref(e));
        ArrayNode out = b.putArray("legs");
        for (JsonNode l : legs) {
            ObjectNode leg = l.deepCopy();
            leg.put("source", e.source);
            out.add(leg);
        }
        b.put("cost", PredictionMarkets.round(cost, 4));
        b.put("floor_profit", PredictionMarkets.round(floorProfit, 4));
        b.put("floor", PredictionMarkets.round(floorProfit / cost, 4));
        b.put("basis", basis);
        b.put("quotes_read_at", at.toString());
        return b;
    }

    private Row read(PredictionMarkets.Event listed) {
        Row row = new Row();
        row.at = clock.get();
        PredictionMarkets.LiveEvent live;
        try {
            live = PredictionMarkets.fetchEvent(fetcher, listed.source, listed.eventId);
        } catch (IOException | IllegalStateException e) {
            row.reason = brief(e.getMessage());
            return row;
        }
        row.live = live;
        PredictionMarkets.Event e = live.event;
        row.decision = MarketPricing.Decision.of(e);
        row.extreme = MarketPricing.DailyExtreme.of(e);
        row.conditions = row.decision != null ? row.decision.conditions
            : row.extreme != null ? row.extreme.conditions
            : MarketPricing.labelConditions(e);
        Double step = row.decision != null ? MarketPricing.Decision.STEP : null;
        for (JsonNode n : MarketPricing.structuralLocks(live, null, step, row.conditions)) {
            if ("non_monotone_ladder".equals(n.get("type").asText())) {
                row.baskets.add(within("non_monotone_ladder", e, row.at, n.get("legs"),
                    n.get("cost").asDouble(), n.get("floor_profit").asDouble(),
                    "the wider strike is quoted below the narrower: YES on the wider and NO "
                    + "on the narrower pays at least 1"));
            } else if (n.get("lock").asBoolean()) {
                for (String side : new String[]{"buy_all_yes", "buy_all_no"}) {
                    JsonNode s = n.get(side);
                    if (s != null && !s.isNull() && s.get("lock").asBoolean()) {
                        row.baskets.add(within("bucket_partition", e, row.at, s.get("legs"),
                            s.get("cost").asDouble(), s.get("floor_profit").asDouble(),
                            "the strikes cover every settlement value exactly once"
                            + (step == null ? "" : ", taking " + STEP_TAKEN) + ": "
                            + s.get("side").asText() + " on every market pays "
                            + s.get("guaranteed_payout").asInt()));
                    }
                }
            }
        }
        ObjectNode x = MarketBaskets.exclusiveLock(e, live.feeRates);
        if (x != null) {
            JsonNode s = x.get("buy_all_no");
            ObjectNode b = within("exclusive_set", e, row.at, s.get("legs"),
                s.get("cost").asDouble(), s.get("floor_profit").asDouble(),
                x.get("basis").asText());
            b.set("yes_bids_sum", x.get("yes_bids_sum"));
            row.baskets.add(b);
        }
        return row;
    }

    // ─── One pair across venues ────────────────────────────────────────────────

    /** The lowest and highest threshold among the legs. */
    private static double[] span(List<MarketPricing.Leg> legs) {
        double lo = Double.POSITIVE_INFINITY;
        double hi = Double.NEGATIVE_INFINITY;
        for (MarketPricing.Leg l : legs) {
            lo = Math.min(lo, l.condition.low);
            hi = Math.max(hi, l.condition.high);
        }
        return new double[]{lo, hi};
    }

    private static String labels(PredictionMarkets.Event e) {
        List<String> shown = new ArrayList<>();
        for (PredictionMarkets.Market m : e.legs) {
            if (shown.size() < 3) {
                shown.add(m.label != null ? m.label : m.title);
            }
        }
        return shown.toString();
    }

    /**
     * Prices one Kalshi and Polymarket pair.
     *
     * @param found the baskets that lose at no outcome are added here; each carries
     *     {@code same_quantity}, {@code verified} only when both events resolve to one series
     * @param info filled when the pair is priced
     * @return why the pair was not priced, or null when it was
     */
    private static String pair(String basket, Row k, Row p, int search, Double minFloor,
            List<ObjectNode> found, Paired info) {
        PredictionMarkets.Event a = k.live.event;
        PredictionMarkets.Event b = p.live.event;
        MarketForecasts.Quantity qa = MarketForecasts.quantityOf(a);
        MarketForecasts.Quantity qb = MarketForecasts.quantityOf(b);
        boolean convert = MarketForecasts.convertible(qa, qb);
        if (!convert && qa.key != null && qb.key != null && !qa.key.equals(qb.key)) {
            return "the two events settle on different series: " + qa.key + " and " + qb.key;
        }
        if (!convert && qa.series != null && qb.series != null
                && !qa.series.equals(qb.series)) {
            return "the two events settle on different series: " + qa.series + " and "
                + qb.series;
        }
        if ((k.extreme == null) != (p.extreme == null)) {
            Row x = k.extreme != null ? k : p;
            return "the " + x.live.event.source + " event is one day's " + x.extreme.kind
                + " temperature and the " + (x == k ? p : k).live.event.source
                + " event is not: they are not one quantity";
        }
        if (k.extreme != null) {
            String apart = apart(k, p);
            if (apart != null) {
                return apart;
            }
        }
        ObjectNode rules = MarketRules.compare(a, b);
        List<String> differing = new ArrayList<>();
        for (String d : QUANTITY) {
            // A converted pair differs in series and transform by what it is.
            if (MarketRules.DIFFER.equals(rules.get("dimensions").get(d).get("status")
                    .asText()) && (!convert || "settlement_period".equals(d))) {
                differing.add(d);
            }
        }
        if (!differing.isEmpty()) {
            return "the rules name different quantities: " + differing + " differ";
        }
        if ((k.decision == null) != (p.decision == null)) {
            Row d = k.decision != null ? k : p;
            return "the " + d.live.event.source + " event is the change the FOMC makes at one "
                + "meeting and the " + (d == k ? p : k).live.event.source + " event is not: "
                + "they are not one quantity";
        }
        if (k.decision != null && !k.decision.meeting.equals(p.decision.meeting)) {
            return "the two events are the decisions of different FOMC meetings: "
                + k.decision.meeting + " and " + p.decision.meeting;
        }
        Double step = k.decision != null ? Double.valueOf(MarketPricing.Decision.STEP)
            : k.extreme != null ? Double.valueOf(MarketPricing.DailyExtreme.STEP) : null;
        List<MarketPricing.Leg> legs = new ArrayList<>();
        List<List<MarketPricing.Leg>> sides = new ArrayList<>();
        for (Row r : new Row[]{k, p}) {
            List<MarketPricing.Leg> own = MarketPricing.priceEvent(r.live, null, r.conditions,
                0, null, true, COLUMN).legs;
            if (own.isEmpty()) {
                return "no market of the " + r.live.event.source + " event states a number "
                    + "the engine can read, e.g. " + labels(r.live.event);
            }
            if (k.extreme != null) {
                for (MarketPricing.Leg l : own) {
                    if (l.condition.low % step != 0 || l.condition.high % step != 0) {
                        return "a strike of the " + r.live.event.source + " event is not a "
                            + "whole number of degrees";
                    }
                }
            }
            sides.add(own);
            legs.addAll(own);
        }
        double[] ka = span(sides.get(0));
        double[] pa = span(sides.get(1));
        if (!convert && (ka[1] < pa[0] || pa[1] < ka[0])) {
            return "the two events' strikes do not overlap, so they are not one quantity in "
                + "one unit";
        }
        int depth = search;
        while (depth > 2
                && MarketPricing.subsetCount(legs.size(), depth) > MarketPricing.MAX_SUBSETS) {
            depth--;
        }
        if (MarketPricing.subsetCount(legs.size(), depth) > MarketPricing.MAX_SUBSETS) {
            return "the pair has too many quoted legs to search";
        }
        Map<String, MarketPricing.Leg> byKey = new LinkedHashMap<>();
        for (MarketPricing.Leg l : legs) {
            byKey.put(l.source + ":" + l.key(), l);
        }
        Instant at = k.at.isBefore(p.at) ? k.at : p.at;
        info.legs = legs;
        info.byKey = byKey;
        info.depth = depth;
        info.rules = rules;
        info.at = at;
        if (convert) {
            boolean kalshiMom = qa.transform == MarketForecasts.Transform.MOM;
            info.momRow = kalshiMom ? k : p;
            info.mom = kalshiMom ? qa : qb;
            info.yoy = kalshiMom ? qb : qa;
            info.settlesOn = info.mom.key + " and " + info.yoy.key;
            return null;
        }
        info.settlesOn = k.decision != null
            ? "the change of the FOMC target rate at the " + k.decision.meeting
                + " meeting, basis points"
            : qa.key != null && qb.key != null ? qa.key : null;
        ObjectNode score = MarketPricing.scoreBasket(legs, MarketPricing.grid(legs, step), true,
            depth, true, minFloor, PER_PAIR, 2);
        for (JsonNode best : score.get("search").get("best")) {
            ObjectNode o = entry("cross_venue", basket, best, a, b, info);
            o.set("floor_profit", best.get("worst"));
            o.set("floor", best.get("floor"));
            o.set("best_profit", best.get("best"));
            if (info.settlesOn != null) {
                o.put("same_quantity", VERIFIED);
                o.put("settles_on", info.settlesOn);
            } else if (k.extreme != null) {
                o.put("same_quantity", TWO_MEASUREMENTS);
                o.put("same_quantity_reason", "both events are the " + k.extreme.kind
                    + " temperature at " + k.extreme.station + " on " + k.extreme.day
                    + ", but " + a.source + " settles on " + k.extreme.measured + " and "
                    + b.source + " on " + p.extreme.measured + ": the two records can differ, "
                    + "and where they do both legs can lose");
            } else {
                o.put("same_quantity", "unverified");
                o.put("same_quantity_reason", "the settlement series of the "
                    + (qa.key == null ? a.source : b.source) + " event is not resolved: "
                    + (qa.key == null ? qa.reason : qb.reason));
            }
            o.put("basis", "loses at no outcome the two events' conditions can tell apart, "
                + "taking both to settle on one number"
                + (step == null ? "" : " and "
                    + (k.extreme != null ? DEGREE_TAKEN : STEP_TAKEN)));
            o.put("quotes_read_at", at.toString());
            found.add(o);
        }
        return null;
    }

    /**
     * Why two daily temperature extremes are not one station's temperature on one day in one
     * unit, or null when they are.
     */
    private static String apart(Row k, Row p) {
        MarketPricing.DailyExtreme x = k.extreme;
        MarketPricing.DailyExtreme y = p.extreme;
        if (!x.kind.equals(y.kind)) {
            return "one event is the highest temperature of the day and the other the lowest";
        }
        if (!x.day.equals(y.day)) {
            return "the two events are the temperature on different days: " + x.day + " and "
                + y.day;
        }
        for (Row r : new Row[]{k, p}) {
            if (r.extreme.station == null) {
                return "the rules of the " + r.live.event.source + " temperature event name "
                    + "no station the engine can read";
            }
        }
        if (!x.station.equals(y.station)) {
            return "the two events are the temperature at different stations: " + x.station
                + " and " + y.station;
        }
        for (Row r : new Row[]{k, p}) {
            if (r.extreme.unit == null) {
                return "the rules of the " + r.live.event.source + " temperature event do "
                    + "not say Fahrenheit or Celsius";
            }
        }
        if (!x.unit.equals(y.unit)) {
            return "the two events are in different units: " + x.unit + " and " + y.unit;
        }
        return null;
    }

    /** What a lock and a near-lock of one pair say alike: events, legs, cost and rules. */
    private static ObjectNode entry(String type, String basket, JsonNode best,
            PredictionMarkets.Event a, PredictionMarkets.Event b, Paired info) {
        ObjectNode o = MAPPER.createObjectNode();
        o.put("type", type);
        o.put("basket", basket);
        o.set("venues", best.get("venues"));
        o.putArray("events").add(ref(a)).add(ref(b));
        ArrayNode out = o.putArray("legs");
        for (JsonNode l : best.get("legs")) {
            ObjectNode leg = out.addObject();
            leg.put("market_id", l.get("id").asText());
            leg.set("title", l.get("title"));
            leg.set("source", l.get("source"));
            leg.set("side", l.get("side"));
            leg.set("price", l.get("price"));
            leg.set("fee", l.get("fee"));
            if (l.has("forecast_p_win")) {
                leg.set("forecast_p_win", l.get("forecast_p_win"));
            }
            MarketPricing.Condition c = info.byKey.get(l.get("source").asText() + ":"
                + l.get("id").asText() + ":" + l.get("side").asText()).condition;
            leg.set("condition", c.toJson());
            if (c.lowUnsure || c.highUnsure) {
                // The label shares this end with the next range: scored as a loss there.
                ArrayNode ends = leg.putArray("counted_as_a_loss_at");
                if (c.lowUnsure) {
                    ends.add(c.low);
                }
                if (c.highUnsure) {
                    ends.add(c.high);
                }
            }
        }
        o.set("cost", best.get("cost"));
        o.put("legs_searched", info.depth);
        o.set("rules_match", info.rules.get("rules_match"));
        o.set("rules_differing", info.rules.get("differing"));
        o.set("rules_unknown", info.rules.get("unknown"));
        return o;
    }

    // ─── Near-locks ────────────────────────────────────────────────────────────

    /** Builds, once, the forecast of the quantity an event settles on. */
    private void forecast(Row r) {
        r.forecastTried = true;
        PredictionMarkets.Event ev = r.live.event;
        MarketForecasts.Result built;
        try {
            built = builder.forecast(ev.eventTitle, ev.rules, ev.driver, ev.closeTime,
                new MarketForecasts.Request());
        } catch (IllegalArgumentException e) {
            r.forecastReason = "the forecast of the pair's quantity was not resolved: "
                + brief(e.getMessage());
            return;
        } catch (Exception e) {
            // A lock needs no forecast: the scan still reports them, and says this.
            r.forecastReason = "the forecast of the pair's quantity could not be built: "
                + brief(e.getMessage());
            return;
        }
        if (built.forecast == null) {
            r.forecastReason = "the pair's series is not in the catalog: "
                + built.json.path("series_named").asText("not named");
            return;
        }
        r.forecast = built.forecast;
        r.unrounded = built.unrounded;
    }

    /** What a near-lock says of its loss and its odds, whichever way it was scored. */
    private static void odds(ObjectNode o, JsonNode best) {
        o.set("worst_profit", best.get("worst"));
        o.set("worst", best.get("floor"));
        o.set("best_profit", best.get("best"));
        o.set("loses_between", best.get("loses_between"));
        o.set("p_loss", best.get("p_loss"));
        o.set("market_p_loss", best.get("market_p_loss"));
        o.set("p_profit", best.get("p_profit"));
        o.set("expected_profit", best.get("expected"));
        o.set("expected_yield", best.get("yield"));
        o.set("max_quote_gap", best.get("max_quote_gap"));
        o.set("max_quote_ratio", best.get("max_quote_ratio"));
    }

    private static String noneOf(MarketPricing.NearLocks scored) {
        if (scored.bandUnpriced > 0) {
            return "the forecast favours a basket of the pair, but neither venue's quotes "
                + "price its losing band";
        }
        if (scored.bandLikely > 0) {
            return "the forecast favours a basket of the pair, but the quotes put more than "
                + "max_loss_probability on its losing band: a bet on the forecast against "
                + "the market";
        }
        if (scored.overGap > 0) {
            return "the forecast favours a basket of the pair, but puts a leg's chance of "
                + "winning more than " + MAX_QUOTE_GAP + " from its price, or its winning "
                + "or losing at over " + MAX_QUOTE_RATIO + " times what its price implies: a "
                + "bet on the forecast against the market";
        }
        return "no basket of the pair profits in both tails with a positive expected "
            + "profit and p_loss <= max_loss_probability";
    }

    /**
     * The near-locks of a pair that settles on two numbers: one month's month-over-month
     * change and the same index's year-over-year change. Scored on the forecast of the
     * monthly change with the conversion's error drawn from its history.
     */
    private static List<ObjectNode> converted(String basket, Row k, Row p, Paired info,
            MarketForecasts.Conversion c, double maxLoss, String[] why) {
        Row m = info.momRow;
        MarketPricing.Joint joint = new MarketPricing.Joint(m.live.event.source,
            info.mom.key, info.yoy.key, m.unrounded.values, c.errors, c.intercept(),
            c.slope(), info.mom.round, info.yoy.round);
        MarketPricing.NearLocks scored = MarketPricing.nearLocks(info.legs, joint,
            info.depth, 2, maxLoss, MAX_QUOTE_GAP, MAX_QUOTE_RATIO, PER_PAIR);
        why[0] = noneOf(scored);
        List<ObjectNode> out = new ArrayList<>();
        for (JsonNode best : scored.kept) {
            ObjectNode o = entry("near_lock", basket, best, k.live.event, p.live.event, info);
            odds(o, best);
            o.set("forecast", m.forecast.toJson());
            o.put("same_quantity", CONVERTED);
            o.put("settles_on", info.settlesOn + ", " + c.month);
            o.set("conversion", c.toJson());
            o.put("basis", "the two events settle on two numbers: the month's one-month "
                + "change fixes its twelve-month change up to the conversion's wedge_error "
                + "and each number's rounding. Loses only where both numbers fall inside "
                + "loses_between; that band and worst_profit take the error at its least, "
                + "at zero and at its most in wedge_error's months, which does not bound the "
                + "next one. p_loss, p_profit and expected_profit are the engine's forecast "
                + "of the monthly change with the error drawn from those months, not the "
                + "quotes; market_p_loss is the larger of what the two venues' quotes put on the "
                + "loss, each venue's price of a range of its own number times the share of "
                + "the forecast's draws in that range that lose");
            o.put("quotes_read_at", info.at.toString());
            out.add(o);
        }
        return out;
    }

    /**
     * The baskets of a verified pair that lose only inside a band the forecast gives at most
     * {@code maxLoss}, with a positive expected profit.
     */
    private static List<ObjectNode> nearLocks(String basket, Row k, Row p, Paired info,
            double maxLoss, String[] why) {
        List<ObjectNode> out = new ArrayList<>();
        MarketPricing.NearLocks scored = MarketPricing.nearLocks(info.legs, k.forecast,
            info.depth, 2, maxLoss, MAX_QUOTE_GAP, MAX_QUOTE_RATIO, PER_PAIR);
        why[0] = noneOf(scored);
        for (JsonNode best : scored.kept) {
            ObjectNode o = entry("near_lock", basket, best, k.live.event, p.live.event, info);
            odds(o, best);
            o.set("forecast", k.forecast.toJson());
            o.put("same_quantity", VERIFIED);
            o.put("settles_on", info.settlesOn);
            o.put("basis", "profits at every outcome outside loses_between, taking both "
                + "events to settle on one number; p_loss, p_profit and expected_profit are "
                + "the engine's forecast of that number, not the quotes");
            o.put("quotes_read_at", info.at.toString());
            out.add(o);
        }
        return out;
    }

    // ─── Size and time ─────────────────────────────────────────────────────────

    /**
     * Adds what a basket is worth in dollars: the time to its last event's close with the
     * floor annualized over it, and {@code size}, the sets its legs' order books fill while
     * each further set still pays more than it costs.
     *
     * <p>A set is one contract of every leg. The books are walked together, best price
     * first; a set's cost is each leg's price at its current level plus the taker fee at
     * that price, and the walk stops at the first set that costs what it pays or more, or
     * when a leg's book has no more orders.
     *
     * <p>A near-lock has no floor to pay: its sets are walked against the payout the forecast
     * expects of one set, and its rate is the expected yield.
     */
    private void size(ObjectNode basket, Instant now) {
        boolean lock = basket.has("floor_profit");
        String rateOf = lock ? "floor" : "expected_yield";
        String annualized = lock ? "annualized_floor_simple_365d"
            : "annualized_expected_simple_365d";
        String note = lock ? "annualized_floor_note" : "annualized_expected_note";
        Instant close = null;
        for (JsonNode e : basket.get("events")) {
            if (!e.hasNonNull("close_time")) {
                close = null;
                break;
            }
            Instant c = PredictionMarkets.closeInstant(e.get("close_time").asText());
            close = close == null || c.isAfter(close) ? c : close;
        }
        if (close == null) {
            basket.putNull("days_to_settlement");
            basket.put(note, "the venue gave no close time for an event");
        } else {
            double days = Duration.between(now, close).toMillis() / 86400000.0;
            basket.put("days_to_settlement", PredictionMarkets.round(days, 3));
            if (days < 1) {
                basket.put(note, "settlement is under one day away (or past): not annualized");
            } else {
                basket.put(annualized, PredictionMarkets.round(
                    basket.get(rateOf).asDouble() * 365 / days, 4));
            }
        }

        JsonNode legs = basket.get("legs");
        int n = legs.size();
        // The least one set pays: a whole number of winning contracts.
        double least = Math.rint(basket.get("cost").asDouble()
            + basket.get(lock ? "floor_profit" : "worst_profit").asDouble());
        // What a set is bought against: its floor, or the payout the forecast expects.
        double payout = lock ? least : basket.get("cost").asDouble()
            + basket.get("expected_profit").asDouble();
        List<List<MarketHistory.Level>> books = new ArrayList<>();
        double[] rate = new double[n];
        for (int i = 0; i < n; i++) {
            JsonNode l = legs.get(i);
            String source = l.get("source").asText();
            String id = l.get("market_id").asText();
            List<MarketHistory.Level> levels;
            try {
                MarketHistory.MarketRef ref;
                if ("kalshi".equals(source)) {
                    ref = new MarketHistory.MarketRef();
                    ref.source = source;
                    ref.marketId = id;
                } else {
                    ref = MarketHistory.resolve(fetcher, source, id, null);
                    if (!ref.open) {
                        basket.putNull("size");
                        basket.put("size_note", "leg " + id + " is not taking orders: "
                            + ref.status);
                        return;
                    }
                }
                levels = MarketHistory.orderBook(fetcher, ref).levels(
                    "yes".equals(l.get("side").asText()) ? MarketHistory.Side.BUY_YES
                        : MarketHistory.Side.BUY_NO);
            } catch (IOException e) {
                basket.putNull("size");
                basket.put("size_note", "the order book of leg " + id + " was not read: "
                    + brief(e.getMessage()));
                return;
            }
            if (levels.isEmpty()) {
                basket.putNull("size");
                basket.put("size_note", "leg " + id + " has no resting order to buy from");
                return;
            }
            books.add(levels);
            double p = l.get("price").asDouble();
            rate[i] = p > 0 && p < 1 ? l.get("fee").asDouble() / (p * (1 - p)) : 0;
        }

        int[] at = new int[n];
        double[] left = new double[n];
        for (int i = 0; i < n; i++) {
            left[i] = books.get(i).get(0).size;
        }
        double sets = 0;
        double capital = 0;
        double profit = 0;
        double atBest = 0;
        double firstCost = Double.NaN;
        String binding = null;
        String stops;
        while (true) {
            double marginal = 0;
            int tight = 0;
            for (int i = 0; i < n; i++) {
                double q = books.get(i).get(at[i]).price;
                marginal += q + rate[i] * q * (1 - q);
                tight = left[i] < left[tight] ? i : tight;
            }
            if (Double.isNaN(firstCost)) {
                firstCost = marginal;
                binding = legs.get(tight).get("market_id").asText();
            }
            if (marginal >= payout - 1e-9) {
                stops = "the next set costs " + PredictionMarkets.round(marginal, 4)
                    + (lock ? " and pays " + (int) payout
                        : " and is expected to pay " + PredictionMarkets.round(payout, 4));
                break;
            }
            double take = left[tight];
            atBest = sets == 0 ? take : atBest;
            sets += take;
            capital += take * marginal;
            profit += take * (payout - marginal);
            String empty = null;
            for (int i = 0; i < n; i++) {
                left[i] -= take;
                if (left[i] <= 1e-9) {
                    at[i]++;
                    if (at[i] >= books.get(i).size()) {
                        empty = legs.get(i).get("market_id").asText();
                    } else {
                        left[i] = books.get(i).get(at[i]).size;
                    }
                }
            }
            if (empty != null) {
                stops = "the book of leg " + empty + " has no more orders";
                break;
            }
        }
        ObjectNode size = basket.putObject("size");
        size.put("books_read_at", clock.get().toString());
        size.put("first_set_cost", PredictionMarkets.round(firstCost, 5));
        size.put("sets_at_best_price", PredictionMarkets.round(atBest, 2));
        size.put("binding_leg", binding);
        size.put(lock ? "sets_with_a_positive_floor" : "sets_with_a_positive_expected_profit",
            PredictionMarkets.round(sets, 2));
        size.put("capital", PredictionMarkets.round(capital, 2));
        size.put(lock ? "floor_profit" : "expected_profit", PredictionMarkets.round(profit, 2));
        if (!lock) {
            size.put("worst_profit", PredictionMarkets.round(sets * least - capital, 2));
        }
        if (sets > 0) {
            size.put(rateOf, PredictionMarkets.round(profit / capital, 4));
            double days = basket.path("days_to_settlement").asDouble(0);
            if (days >= 1) {
                // Lower than the basket's own: later sets fill at worse prices.
                size.put(annualized, PredictionMarkets.round(profit / capital * 365 / days, 4));
            }
        } else {
            size.put("note", "no set fills at a positive " + (lock ? "floor" : "expected profit")
                + " at the books read: the quotes moved since quotes_read_at");
        }
        size.put("stops_because", stops);
        ObjectNode flows = size.putObject("cashflows");
        flows.put("paid_at_purchase", PredictionMarkets.round(capital, 2));
        flows.put("received_at_settlement_at_least", PredictionMarkets.round(sets * least, 2));
        if (!lock) {
            flows.put("received_at_settlement_expected",
                PredictionMarkets.round(capital + profit, 2));
        }
        flows.put("last_event_closes", close == null ? null : close.toString());
        flows.put("basis", "every leg is paid for in full when bought, fee included; each "
            + "winning contract pays 1 when its event settles, on or after its close");
    }

    // ─── The scan ──────────────────────────────────────────────────────────────

    private static void count(Map<String, Integer> why, Map<String, List<String>> examples,
            String reason, String example) {
        why.merge(reason, 1, Integer::sum);
        List<String> ex = examples.computeIfAbsent(reason, x -> new ArrayList<String>());
        if (ex.size() < EXAMPLES_SHOWN) {
            ex.add(example);
        }
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
        int within = intArg(args, "within", 180);
        int minDays = intArg(args, "min_days", 1);
        double minVolume = doubleArg(args, "min_volume", 1000);
        double maxSpread = doubleArg(args, "max_spread", 0.10);
        double minFloor = doubleArg(args, "min_floor", 0);
        if (minFloor < 0) {
            throw new IllegalArgumentException("min_floor must be 0 or more, got " + minFloor);
        }
        double maxLoss = doubleArg(args, "max_loss_probability", 0.10);
        if (maxLoss < 0 || maxLoss >= 0.5) {
            throw new IllegalArgumentException("max_loss_probability must be at least 0 and "
                + "under 0.5, got " + maxLoss);
        }
        int search = intArg(args, "search", 3);
        if (search < 2 || search > 4) {
            throw new IllegalArgumentException("search must be 2 to 4, got " + search);
        }
        int limit = intArg(args, "limit", 5);
        int maxEvents = intArg(args, "max_events", 60);
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
        for (PredictionMarkets.Event e : PredictionMarkets.screen(listing.rows, now, within,
                minDays, minVolume)) {
            if (driver == null || driver.equals(e.driver.name)) {
                matched.add(e);
            }
        }
        List<PredictionMarkets.Event> pairable =
            MarketBaskets.filter(matched, BASES, maxSpread);
        List<MarketBaskets.Basket> crossVenue = MarketBaskets.crossVenue(pairable);
        crossVenue.sort(Comparator.comparingLong(MarketBaskets.Basket::volume24h).reversed());
        Map<String, Integer> otherRecipes = new TreeMap<>();
        for (MarketBaskets.Basket b : MarketBaskets.propose(pairable, null, null)) {
            String recipe = b.name.substring(0, b.name.indexOf(':'));
            if (!"cross_venue".equals(recipe)) {
                otherRecipes.merge(recipe, 1, Integer::sum);
            }
        }

        // Pairs first, then events whose listed quotes already contradict each other, then
        // events the venue states exclusive, then the rest; most traded first within each.
        Map<String, PredictionMarkets.Event> chosen = new LinkedHashMap<>();
        for (MarketBaskets.Basket b : crossVenue) {
            for (PredictionMarkets.Event e : b.listed(0)) {
                chosen.putIfAbsent(key(e), e);
            }
        }
        List<PredictionMarkets.Event> rest = new ArrayList<>(matched);
        rest.sort(Comparator.comparing((PredictionMarkets.Event e) -> e.locks.isEmpty())
            .thenComparing(e -> !e.exclusive)
            .thenComparing(Comparator.comparingLong(
                (PredictionMarkets.Event e) -> e.volume24h).reversed()));
        for (PredictionMarkets.Event e : rest) {
            chosen.putIfAbsent(key(e), e);
        }
        List<PredictionMarkets.Event> toRead = new ArrayList<>(chosen.values());
        if (toRead.size() > maxEvents) {
            toRead = toRead.subList(0, maxEvents);
        }

        long started = System.nanoTime();
        int done = 0;
        int fresh = 0;
        boolean outOfTime = false;
        for (PredictionMarkets.Event e : toRead) {
            Row have = rows.get(key(e));
            if (have != null && Duration.between(have.at, now).compareTo(ttl) < 0) {
                done++;
                continue;
            }
            // At least one event per call, so a scan always advances.
            if (fresh > 0 && (System.nanoTime() - started) / 1_000_000L > budgetMillis) {
                outOfTime = true;
                break;
            }
            rows.put(key(e), read(e));
            done++;
            fresh++;
        }

        Map<String, Integer> why = new TreeMap<>();
        Map<String, List<String>> examples = new TreeMap<>();
        Set<String> seen = new HashSet<>();
        List<ObjectNode> locks = new ArrayList<>();
        List<ObjectNode> unverified = new ArrayList<>();
        List<ObjectNode> closest = new ArrayList<>();
        List<ObjectNode> nears = new ArrayList<>();
        Map<String, Integer> nearWhy = new TreeMap<>();
        Map<String, List<String>> nearExamples = new TreeMap<>();
        int forecasts = 0;
        int forecastsLeft = 0;
        int read = 0;
        int exclusive = 0;
        int labelled = 0;
        int lockedWithin = 0;
        for (PredictionMarkets.Event e : toRead) {
            Row r = rows.get(key(e));
            if (r == null) {
                continue;
            }
            if (r.live == null) {
                count(why, examples, "event not read: " + r.reason, key(e));
                continue;
            }
            read++;
            exclusive += r.live.event.exclusive ? 1 : 0;
            labelled += r.conditions.isEmpty() ? 0 : 1;
            for (ObjectNode b : r.baskets) {
                Set<String> ids = new TreeSet<>();
                for (JsonNode l : b.get("legs")) {
                    ids.add(l.get("market_id").asText() + ":" + l.get("side").asText());
                }
                if (!seen.add(e.source + ids)) {
                    continue;
                }
                if (b.get("floor_profit").asDouble() > 0) {
                    lockedWithin++;
                    if (b.get("floor").asDouble() >= minFloor) {
                        locks.add(b);
                    }
                } else if ("exclusive_set".equals(b.get("type").asText())) {
                    closest.add(b);
                }
            }
        }
        int pairs = 0;
        int priced = 0;
        int lockedAcross = 0;
        int converted = 0;
        int twoMeasurements = 0;
        // A Conversion, or why it was not built, per pair of series and month.
        Map<String, Object> conversions = new LinkedHashMap<>();
        for (MarketBaskets.Basket b : crossVenue) {
            for (PredictionMarkets.Event k : b.events) {
                for (PredictionMarkets.Event p : b.events) {
                    if (!"kalshi".equals(k.source) || !"polymarket".equals(p.source)) {
                        continue;
                    }
                    pairs++;
                    String name = key(k) + " and " + key(p);
                    Row rk = rows.get(key(k));
                    Row rp = rows.get(key(p));
                    if (rk == null || rp == null) {
                        if (!outOfTime) {
                            count(why, examples, "an event of the pair is past max_events",
                                name);
                        }
                        continue;
                    }
                    if (rk.live == null || rp.live == null) {
                        count(why, examples, "an event of the pair was not read", name);
                        continue;
                    }
                    List<ObjectNode> found = new ArrayList<>();
                    Paired info = new Paired();
                    String reason = pair(b.name, rk, rp, search, minFloor > 0 ? minFloor : null,
                        found, info);
                    if (reason != null) {
                        count(why, examples, reason, name);
                        continue;
                    }
                    priced++;
                    if (rk.extreme != null) {
                        twoMeasurements++;
                    }
                    for (ObjectNode f : found) {
                        if (VERIFIED.equals(f.get("same_quantity").asText())) {
                            lockedAcross++;
                            locks.add(f);
                        } else {
                            unverified.add(f);
                        }
                    }
                    if (info.settlesOn == null || !found.isEmpty()) {
                        continue;
                    }
                    if (rk.decision != null) {
                        count(nearWhy, nearExamples, "the engine builds no forecast of an "
                            + "FOMC decision", name);
                        continue;
                    }
                    // No lock in a pair on one number: is there a basket that loses only
                    // where the forecast of that number seldom lands?
                    // A converted pair is scored on its monthly change.
                    Row rf = info.momRow != null ? info.momRow : rk;
                    if (info.momRow != null) {
                        converted++;
                    }
                    if (!rf.forecastTried) {
                        if (outOfTime || fresh + forecasts > 0
                                && (System.nanoTime() - started) / 1_000_000L > budgetMillis) {
                            outOfTime = true;
                            forecastsLeft++;
                            continue;
                        }
                        forecast(rf);
                        forecasts++;
                    }
                    if (rf.forecast == null) {
                        count(nearWhy, nearExamples, rf.forecastReason, name);
                        continue;
                    }
                    String[] none = new String[1];
                    List<ObjectNode> near;
                    if (info.momRow == null) {
                        near = nearLocks(b.name, rk, rp, info, maxLoss, none);
                    } else {
                        String unrounded = info.mom.round == null ? info.mom.key
                            : info.yoy.round == null ? info.yoy.key : null;
                        if (unrounded != null) {
                            count(nearWhy, nearExamples, "the rules of the " + unrounded
                                + " event do not say how many decimals it is published to, "
                                + "so its number cannot be placed against the other's", name);
                            continue;
                        }
                        String key = info.settlesOn + " " + info.mom.month + " "
                            + info.yoy.month;
                        if (!conversions.containsKey(key)) {
                            try {
                                conversions.put(key, builder.conversion(info.mom, info.yoy));
                            } catch (IllegalStateException | IllegalArgumentException e) {
                                conversions.put(key, "the month-over-month and year-over-year "
                                    + "events were not converted: " + brief(e.getMessage()));
                            }
                        }
                        Object conversion = conversions.get(key);
                        if (conversion instanceof String) {
                            count(nearWhy, nearExamples, (String) conversion, name);
                            continue;
                        }
                        near = converted(b.name, rk, rp, info,
                            (MarketForecasts.Conversion) conversion, maxLoss, none);
                    }
                    if (near.isEmpty()) {
                        count(nearWhy, nearExamples, none[0], name);
                    }
                    nears.addAll(near);
                }
            }
        }
        Comparator<ObjectNode> byFloor = Comparator.comparingDouble(
            (ObjectNode b) -> b.get("floor").asDouble()).reversed();
        locks.sort(byFloor);
        unverified.sort(byFloor);
        closest.sort(Comparator.comparingDouble(
            (ObjectNode b) -> b.get("floor_profit").asDouble()).reversed());
        nears.sort(Comparator.comparingDouble(
            (ObjectNode b) -> b.get("expected_yield").asDouble()).reversed());

        ObjectNode out = MAPPER.createObjectNode();
        out.put("status", outOfTime ? "scanning" : COMPLETE);
        out.put("listing_read_at", listing.fetchedAt.toString());
        out.put("min_floor", minFloor);
        out.put("max_loss_probability", maxLoss);
        ObjectNode funnel = out.putObject("funnel");
        funnel.put("events_matched", matched.size());
        funnel.put("events_to_read", toRead.size());
        funnel.put("events_read", read);
        funnel.put("events_the_venue_states_exclusive", exclusive);
        funnel.put("events_with_conditions_read_from_labels", labelled);
        funnel.put("events_with_a_lock_inside", lockedWithin);
        funnel.put("cross_venue_baskets", crossVenue.size());
        funnel.put("cross_venue_pairs", pairs);
        funnel.put("cross_venue_pairs_priced", priced);
        funnel.put("cross_venue_pairs_with_a_lock", lockedAcross);
        funnel.put("cross_venue_pairs_on_two_measurements", twoMeasurements);
        funnel.put("cross_venue_pairs_unverified_with_a_gap", unverified.size());
        funnel.put("locks_found", locks.size());
        funnel.put("cross_venue_pairs_converted", converted);
        funnel.put("cross_venue_pairs_forecast", forecasts);
        funnel.put("near_locks_found", nears.size());
        ObjectNode other = funnel.putObject("baskets_of_other_recipes_not_priced");
        for (Map.Entry<String, Integer> e : otherRecipes.entrySet()) {
            other.put(e.getKey(), e.getValue());
        }
        ArrayNode baskets = out.putArray("baskets");
        for (int i = 0; i < locks.size() && i < limit; i++) {
            if (!outOfTime) {
                size(locks.get(i), now);
            }
            baskets.add(locks.get(i));
        }
        ArrayNode maybe = out.putArray("unverified");
        for (int i = 0; i < unverified.size() && i < limit; i++) {
            if (!outOfTime) {
                size(unverified.get(i), now);
            }
            maybe.add(unverified.get(i));
        }
        out.put("unverified_is", "cross-venue pairs whose quotes leave a gap but where the "
            + "engine could not establish that both events settle on one series. A gap "
            + "between two different quantities is not a lock. same_quantity '"
            + TWO_MEASUREMENTS + "' is a pair on one station's highest or lowest temperature "
            + "on one day that each venue reads from a different record (same_quantity_reason "
            + "names both): its floor holds only where the two records give the same whole "
            + "degree, and the engine has no history of how often they do.");
        ArrayNode arbLike = out.putArray("near_locks");
        for (int i = 0; i < nears.size() && i < limit; i++) {
            if (!outOfTime) {
                size(nears.get(i), now);
            }
            arbLike.add(nears.get(i));
        }
        out.put("near_lock_is", "a basket of a verified or converted pair that is not a lock: "
            + "it profits "
            + "at every outcome outside loses_between and loses worst_profit at worst. p_loss, "
            + "p_profit and expected_profit come from the engine's forecast of the number the "
            + "pair settles on, so they are a judgement and not a property of the quotes. "
            + "The quotes agree the loss is unlikely: market_p_loss, what a venue's quotes "
            + "put on the losing band, is at most max_loss_probability too, and the forecast "
            + "puts each leg's chance of winning (forecast_p_win) within " + MAX_QUOTE_GAP
            + " of its price, and neither its winning nor its losing at over "
            + MAX_QUOTE_RATIO + " times what the price implies (max_quote_ratio). A basket "
            + "only the forecast favours is a bet on the forecast "
            + "against the market and is counted in near_locks_not_scored. "
            + "same_quantity '" + CONVERTED + "' is a pair on two numbers, one month's "
            + "month-over-month change and the same index's year-over-year change: its "
            + "conversion, the error of that conversion and what the odds then mean are in "
            + "conversion and basis. "
            + "size walks the books while a further set still costs less than the forecast "
            + "expects it to pay.");
        ArrayNode nearNot = out.putArray("near_locks_not_scored");
        for (Map.Entry<String, Integer> e : nearWhy.entrySet()) {
            ObjectNode n = nearNot.addObject();
            n.put("why", e.getKey());
            n.put("count", e.getValue());
            ArrayNode ex = n.putArray("examples");
            for (String id : nearExamples.get(e.getKey())) {
                ex.add(id);
            }
        }
        ArrayNode near = out.putArray("closest");
        for (int i = 0; i < closest.size() && i < CLOSEST_SHOWN; i++) {
            ObjectNode b = closest.get(i);
            ObjectNode c = near.addObject();
            c.set("type", b.get("type"));
            c.set("event", b.get("events").get(0));
            c.put("markets", b.get("legs").size());
            c.set("yes_bids_sum", b.get("yes_bids_sum"));
            c.set("floor_profit", b.get("floor_profit"));
        }
        ArrayNode not = out.putArray("not_priced");
        for (Map.Entry<String, Integer> e : why.entrySet()) {
            ObjectNode n = not.addObject();
            n.put("why", e.getKey());
            n.put("count", e.getValue());
            ArrayNode ex = n.putArray("examples");
            for (String id : examples.get(e.getKey())) {
                ex.add(id);
            }
        }
        out.put("lock_is", "one contract per leg bought at the quote read at quotes_read_at, "
            + "net of the venue's taker fee; floor_profit is the worst case and floor is it "
            + "per unit of cost. The whole cost is paid at purchase. size is what the legs' "
            + "order books fill: capital and floor_profit there are dollars.");
        if (outOfTime) {
            out.put("next", "The scan is unfinished: " + done + " of " + toRead.size()
                + " events read" + (forecastsLeft > 0 ? ", " + forecastsLeft + " pairs still "
                    + "to forecast" : "") + ". You MUST call " + TOOL + " again with the same arguments "
                + "before answering.");
        } else if (locks.isEmpty()) {
            out.put("next", "No basket locks a profit after fees"
                + (minFloor > 0 ? " with floor >= " + minFloor : "") + ". Say none was found "
                + "and report funnel, not_priced and closest."
                + (unverified.isEmpty() ? "" : " You MUST NOT report an unverified entry as "
                    + "a lock: read both events' rules with compare_settlement_rules and "
                    + "state same_quantity_reason.")
                + (nears.isEmpty() ? "" : NEAR_RULE)
                + " scan_market_opportunities prices single events against a forecast.");
        } else {
            out.put("next", "You MUST report a cross_venue basket as not a lock unless its "
                + "rules_match is 'match', and state rules_differing and rules_unknown. You "
                + "MUST state cost, fees, floor and quotes_read_at for each basket reported, "
                + "and from size the sets that fill, the capital and the dollar profit: the "
                + "annualized floor applies to that capital only. price_market_event(source, event_id, "
                + "build_forecast=true) gives the forecast's probability of each outcome of "
                + "a basket's event." + (nears.isEmpty() ? "" : NEAR_RULE));
        }
        return MAPPER.writeValueAsString(out);
    }
}
