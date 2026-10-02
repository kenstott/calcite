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
 * Prices every basket the quotes alone can settle, in one pass and with no forecast.
 *
 * <p>Two tiers. Within one event: a ladder whose strikes are priced out of order, a bucket
 * partition the strikes prove, and a set of markets the venue states has at most one winner.
 * Across venues: each Kalshi and Polymarket pair of one cross-venue basket, scored over every
 * outcome their conditions can tell apart, keeping subsets with a leg on each venue that lose
 * at none. A pair is a lock only when the forecast builder reads the same series and transform
 * from both events; a pair it reads differently is not priced, and a pair it cannot read on
 * one side is priced and listed apart as unverified.
 *
 * <p>An event is read once and kept for the listing's lifetime, so a scan that runs out of its
 * time budget resumes where it stopped on the next call.
 */
final class MarketBasketScan {
    static final String TOOL = "scan_market_baskets";
    static final Set<String> KEYS = new TreeSet<>(Arrays.asList("driver", "within", "min_days",
        "min_volume", "max_spread", "min_floor", "search", "limit", "max_events", "refresh"));
    /** The status of a scan that read every event it chose. */
    static final String COMPLETE = "complete";
    /** Rule dimensions that say what number an event settles on. */
    static final List<String> QUANTITY = Arrays.asList("series", "settlement_period",
        "transform");
    private static final Set<String> BASES =
        new TreeSet<>(Arrays.asList("release", "climatology", "policy"));
    private static final String COLUMN = "v";
    private static final String VERIFIED = "verified";
    private static final int EXAMPLES_SHOWN = 5;
    private static final int CLOSEST_SHOWN = 3;
    private static final int PER_PAIR = 1;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PredictionMarkets.Fetcher fetcher;
    private final PredictionMarkets.ListingCache cache;
    private final Supplier<Instant> clock;
    private final long listingWaitMillis;
    private final long budgetMillis;
    private final Duration ttl;
    /** One read per event, keyed source:event_id. */
    private final Map<String, Row> rows = new LinkedHashMap<>();

    MarketBasketScan(PredictionMarkets.Fetcher fetcher, PredictionMarkets.ListingCache cache,
            Supplier<Instant> clock, long listingWaitMillis, long budgetMillis, Duration ttl) {
        this.fetcher = fetcher;
        this.cache = cache;
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
        /** Baskets inside the event, locking or not. */
        List<ObjectNode> baskets = new ArrayList<>();
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
        "Find baskets of Kalshi and Polymarket contracts that lock a profit after fees, "
        + "with no forecast. Within one event: strikes priced out of order, a bucket partition the strikes prove, and NO on every "
        + "market of an event the venue states has at most one winner. Across venues: every "
        + "Kalshi and Polymarket pair find_market_baskets lists as cross_venue, scored over "
        + "every outcome the two events' conditions can tell apart, keeping baskets with a "
        + "leg on each venue that lose at none. A pair is "
        + "in baskets only when the engine resolves both events to one settlement series "
        + "and transform (settles_on); a pair it resolves to different series is in "
        + "not_priced; a pair it cannot resolve on one side is in unverified with "
        + "same_quantity_reason. Each "
        + "basket gives its legs with side, price and fee, cost, floor_profit, floor (worst "
        + "case per unit of cost) and, across venues, rules_match with the differing and "
        + "unknown rule dimensions. funnel counts what was read and priced; not_priced "
        + "says why a pair was left out; closest lists the exclusive events nearest a "
        + "lock. floor is for one contract per leg at the quote; size reads the "
        + "legs' order books: sets_at_best_price, the sets that fill at a positive floor, "
        + "their capital and profit in dollars. Cost is paid in full at purchase; "
        + "annualized_floor_simple_365d is floor over days_to_settlement. Baskets of other "
        + "recipes (series_run, calendar, linked_drivers, "
        + "same_place) settle on different quantities and are counted, not priced: "
        + "price_market_basket takes them with scenarios_sql. You MUST call again with "
        + "the same arguments while status is 'loading' or 'scanning'. You MUST use this tool first when asked to find a basket "
        + "that locks a yield or an arbitrage. You MUST report a cross_venue basket as not a "
        + "lock unless its rules_match is 'match', and state the differing and unknown "
        + "dimensions. You MUST state cost, fees, floor, size and quotes_read_at for every "
        + "basket reported. You MUST NOT report an unverified entry as a lock. When baskets is "
        + "empty you MUST say none was found and report funnel and not_priced.";

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
        row.conditions = MarketPricing.labelConditions(e);
        for (JsonNode n : MarketPricing.structuralLocks(live, null, null, row.conditions)) {
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
                            "the strikes cover every settlement value exactly once: "
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
     * @return why the pair was not priced, or null when it was
     */
    private static String pair(String basket, Row k, Row p, int search, Double minFloor,
            List<ObjectNode> found) {
        PredictionMarkets.Event a = k.live.event;
        PredictionMarkets.Event b = p.live.event;
        MarketForecasts.Quantity qa = MarketForecasts.quantityOf(a);
        MarketForecasts.Quantity qb = MarketForecasts.quantityOf(b);
        if (qa.key != null && qb.key != null && !qa.key.equals(qb.key)) {
            return "the two events settle on different series: " + qa.key + " and " + qb.key;
        }
        if (qa.series != null && qb.series != null && !qa.series.equals(qb.series)) {
            return "the two events settle on different series: " + qa.series + " and "
                + qb.series;
        }
        ObjectNode rules = MarketRules.compare(a, b);
        List<String> differing = new ArrayList<>();
        for (String d : QUANTITY) {
            if (MarketRules.DIFFER.equals(rules.get("dimensions").get(d).get("status")
                    .asText())) {
                differing.add(d);
            }
        }
        if (!differing.isEmpty()) {
            return "the rules name different quantities: " + differing + " differ";
        }
        List<MarketPricing.Leg> legs = new ArrayList<>();
        List<List<MarketPricing.Leg>> sides = new ArrayList<>();
        for (Row r : new Row[]{k, p}) {
            List<MarketPricing.Leg> own = MarketPricing.priceEvent(r.live, null, r.conditions,
                0, null, true, COLUMN).legs;
            if (own.isEmpty()) {
                return "no market of the " + r.live.event.source + " event states a number "
                    + "the engine can read, e.g. " + labels(r.live.event);
            }
            sides.add(own);
            legs.addAll(own);
        }
        double[] ka = span(sides.get(0));
        double[] pa = span(sides.get(1));
        if (ka[1] < pa[0] || pa[1] < ka[0]) {
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
        ObjectNode score = MarketPricing.scoreBasket(legs, MarketPricing.grid(legs), true,
            depth, true, minFloor, PER_PAIR, 2);
        Instant at = k.at.isBefore(p.at) ? k.at : p.at;
        for (JsonNode best : score.get("search").get("best")) {
            ObjectNode o = MAPPER.createObjectNode();
            o.put("type", "cross_venue");
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
                MarketPricing.Condition c = byKey.get(l.get("source").asText() + ":"
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
            o.set("floor_profit", best.get("worst"));
            o.set("floor", best.get("floor"));
            o.set("best_profit", best.get("best"));
            o.put("legs_searched", depth);
            o.set("rules_match", rules.get("rules_match"));
            o.set("rules_differing", rules.get("differing"));
            o.set("rules_unknown", rules.get("unknown"));
            if (qa.key != null && qb.key != null) {
                o.put("same_quantity", VERIFIED);
                o.put("settles_on", qa.key);
            } else {
                o.put("same_quantity", "unverified");
                o.put("same_quantity_reason", "the settlement series of the "
                    + (qa.key == null ? a.source : b.source) + " event is not resolved: "
                    + (qa.key == null ? qa.reason : qb.reason));
            }
            o.put("basis", "loses at no outcome the two events' conditions can tell apart, "
                + "taking both to settle on one number");
            o.put("quotes_read_at", at.toString());
            found.add(o);
        }
        return null;
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
     */
    private void size(ObjectNode basket, Instant now) {
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
            basket.put("annualized_floor_note", "the venue gave no close time for an event");
        } else {
            double days = Duration.between(now, close).toMillis() / 86400000.0;
            basket.put("days_to_settlement", PredictionMarkets.round(days, 3));
            if (days < 1) {
                basket.put("annualized_floor_note",
                    "settlement is under one day away (or past): not annualized");
            } else {
                basket.put("annualized_floor_simple_365d", PredictionMarkets.round(
                    basket.get("floor").asDouble() * 365 / days, 4));
            }
        }

        JsonNode legs = basket.get("legs");
        int n = legs.size();
        // The floor payout of one set: a whole number of winning contracts.
        double payout = Math.rint(basket.get("cost").asDouble()
            + basket.get("floor_profit").asDouble());
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
                    + " and pays " + (int) payout;
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
        size.put("sets_with_a_positive_floor", PredictionMarkets.round(sets, 2));
        size.put("capital", PredictionMarkets.round(capital, 2));
        size.put("floor_profit", PredictionMarkets.round(profit, 2));
        if (sets > 0) {
            size.put("floor", PredictionMarkets.round(profit / capital, 4));
            double days = basket.path("days_to_settlement").asDouble(0);
            if (days >= 1) {
                // Lower than the basket's own: later sets fill at worse prices.
                size.put("annualized_floor_simple_365d", PredictionMarkets.round(
                    profit / capital * 365 / days, 4));
            }
        } else {
            size.put("note", "no set fills at a positive floor at the books read: the quotes "
                + "moved since quotes_read_at");
        }
        size.put("stops_because", stops);
        ObjectNode flows = size.putObject("cashflows");
        flows.put("paid_at_purchase", PredictionMarkets.round(capital, 2));
        flows.put("received_at_settlement_at_least", PredictionMarkets.round(sets * payout, 2));
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
                    String reason = pair(b.name, rk, rp, search, minFloor > 0 ? minFloor : null,
                        found);
                    if (reason != null) {
                        count(why, examples, reason, name);
                        continue;
                    }
                    priced++;
                    for (ObjectNode f : found) {
                        if (VERIFIED.equals(f.get("same_quantity").asText())) {
                            lockedAcross++;
                            locks.add(f);
                        } else {
                            unverified.add(f);
                        }
                    }
                }
            }
        }
        Comparator<ObjectNode> byFloor = Comparator.comparingDouble(
            (ObjectNode b) -> b.get("floor").asDouble()).reversed();
        locks.sort(byFloor);
        unverified.sort(byFloor);
        closest.sort(Comparator.comparingDouble(
            (ObjectNode b) -> b.get("floor_profit").asDouble()).reversed());

        ObjectNode out = MAPPER.createObjectNode();
        out.put("status", outOfTime ? "scanning" : COMPLETE);
        out.put("listing_read_at", listing.fetchedAt.toString());
        out.put("min_floor", minFloor);
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
        funnel.put("cross_venue_pairs_unverified_with_a_gap", unverified.size());
        funnel.put("locks_found", locks.size());
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
            + "between two different quantities is not a lock.");
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
                + " events read. You MUST call " + TOOL + " again with the same arguments "
                + "before answering.");
        } else if (locks.isEmpty()) {
            out.put("next", "No basket locks a profit after fees"
                + (minFloor > 0 ? " with floor >= " + minFloor : "") + ". Say none was found "
                + "and report funnel, not_priced and closest."
                + (unverified.isEmpty() ? "" : " You MUST NOT report an unverified entry as "
                    + "a lock: read both events' rules with compare_settlement_rules and "
                    + "state same_quantity_reason.")
                + " scan_market_opportunities prices single events against a forecast.");
        } else {
            out.put("next", "You MUST report a cross_venue basket as not a lock unless its "
                + "rules_match is 'match', and state rules_differing and rules_unknown. You "
                + "MUST state cost, fees, floor and quotes_read_at for each basket reported, "
                + "and from size the sets that fill, the capital and the dollar profit: the "
                + "annualized floor applies to that capital only. price_market_event(source, event_id, "
                + "build_forecast=true) gives the forecast's probability of each outcome of "
                + "a basket's event.");
        }
        return MAPPER.writeValueAsString(out);
    }
}
