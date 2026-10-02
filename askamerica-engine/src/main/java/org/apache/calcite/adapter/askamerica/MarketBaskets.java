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

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Proposes baskets of correlated prediction-market events. An arbitrage here is a basket:
 * events whose outcomes move together, bought at a price that improves the odds of a positive
 * outcome. Each recipe is a reason events might move together — a hypothesis to price, not a
 * finding.
 */
final class MarketBaskets {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    /** Two venues are quoting the same release when their events close this close together. */
    static final int CROSS_VENUE_DAYS = 3;

    static final List<String> RECIPES = Collections.unmodifiableList(Arrays.asList(
        "cross_venue", "same_place", "linked_drivers", "series_run", "range", "calendar"));

    private MarketBaskets() { }

    /** Drivers joined by a causal chain. */
    static final class Link {
        final String name;
        final String why;
        final List<String> drivers;

        Link(String name, String why, String... drivers) {
            this.name = name;
            this.why = why;
            this.drivers = Arrays.asList(drivers);
        }
    }

    static final List<Link> LINKS = Collections.unmodifiableList(Arrays.asList(
        new Link("agriculture",
            "weather and drought drive crop yield; yield and tariffs drive crop prices; "
            + "crop and livestock prices feed food inflation",
            "temperature", "precipitation", "drought", "crop_yield", "livestock", "tariffs",
            "food_prices", "inflation"),
        new Link("prices_and_policy",
            "energy, food and tariffs feed CPI and PCE; inflation drives the policy rate, "
            + "and the policy rate drives yields and mortgage rates",
            "energy_price", "food_prices", "tariffs", "inflation", "pce", "policy_rate",
            "treasury_yield", "mortgage_rate"),
        new Link("labor_and_growth",
            "payrolls, unemployment and claims are one labor market; it moves output "
            + "and the policy rate",
            "payrolls", "unemployment", "jobless_claims", "output", "policy_rate"),
        new Link("rates_and_housing",
            "the policy rate and Treasury yields set mortgage rates; mortgage rates move "
            + "starts, permits and sales",
            "policy_rate", "treasury_yield", "mortgage_rate", "housing"),
        new Link("energy_and_weather",
            "temperature drives gas demand and storage; storms cut Gulf supply; "
            + "energy prices feed inflation",
            "temperature", "storms", "gas_storage", "energy_price", "inflation")));

    private static final List<String> STATES = Arrays.asList(
        "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut",
        "delaware", "florida", "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa",
        "kansas", "kentucky", "louisiana", "maine", "maryland", "massachusetts", "michigan",
        "minnesota", "mississippi", "missouri", "montana", "nebraska", "nevada",
        "new hampshire", "new jersey", "new mexico", "new york", "north carolina",
        "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania", "rhode island",
        "south carolina", "south dakota", "tennessee", "texas", "utah", "vermont", "virginia",
        "washington", "west virginia", "wisconsin", "wyoming");
    private static final Map<String, String> CITIES = new LinkedHashMap<>();
    private static final Pattern PLACE;

    static {
        String[][] cities = {
            {"nyc", "new york"}, {"chicago", "illinois"}, {"miami", "florida"},
            {"austin", "texas"}, {"dallas", "texas"}, {"houston", "texas"},
            {"denver", "colorado"}, {"los angeles", "california"},
            {"san francisco", "california"}, {"philadelphia", "pennsylvania"},
            {"atlanta", "georgia"}, {"seattle", "washington"}, {"boston", "massachusetts"},
            {"phoenix", "arizona"}, {"las vegas", "nevada"}, {"minneapolis", "minnesota"},
            {"detroit", "michigan"}, {"new orleans", "louisiana"}, {"nashville", "tennessee"}};
        for (String[] c : cities) {
            CITIES.put(c[0], c[1]);
        }
        List<String> names = new ArrayList<>(STATES);
        names.addAll(CITIES.keySet());
        // Longest names first, so "west virginia" is not read as "virginia".
        names.sort(Comparator.comparingInt(String::length).reversed());
        PLACE = Pattern.compile("\\b(" + String.join("|", names) + ")\\b");
    }

    /** A proposed basket. */
    static final class Basket {
        final String name;
        final String why;
        final List<PredictionMarkets.Event> events;
        /** For a linked_drivers basket, the drivers present in causal order, upstream first;
         *  null for every other recipe. */
        List<String> direction;

        Basket(String name, String why, List<PredictionMarkets.Event> events) {
            this.name = name;
            this.why = why;
            this.events = events;
        }

        Set<String> drivers() {
            Set<String> d = new TreeSet<>();
            for (PredictionMarkets.Event e : events) {
                d.add(e.driver.name);
            }
            return d;
        }

        Set<String> venues() {
            Set<String> v = new TreeSet<>();
            for (PredictionMarkets.Event e : events) {
                v.add(e.source);
            }
            return v;
        }

        long volume24h() {
            long v = 0;
            for (PredictionMarkets.Event e : events) {
                v += e.volume24h;
            }
            return v;
        }

        /** At most {@code maxEvents} of the events (0 = all), by driver then 24-hour volume,
         *  both venues represented. */
        List<PredictionMarkets.Event> listed(int maxEvents) {
            List<PredictionMarkets.Event> sorted = new ArrayList<>(events);
            sorted.sort(Comparator.comparing((PredictionMarkets.Event e) -> e.driver.name)
                .thenComparing(Comparator.comparingLong(
                    (PredictionMarkets.Event e) -> e.volume24h).reversed()));
            return maxEvents > 0 ? MarketTools.balanced(sorted, maxEvents) : sorted;
        }

        /** The basket with at most {@code maxEvents} of its events listed (0 = all), both
         *  venues represented. */
        ObjectNode toJson(int maxEvents) {
            ObjectNode o = MAPPER.createObjectNode();
            o.put("basket", name);
            o.put("why", why);
            if (direction != null) {
                ArrayNode dir = o.putArray("link_direction");
                for (String s : direction) {
                    dir.add(s);
                }
            }
            ArrayNode d = o.putArray("drivers");
            Set<String> tables = new TreeSet<>();
            for (String s : drivers()) {
                d.add(s);
                tables.addAll(PredictionMarkets.driverNamed(s).tables);
            }
            ArrayNode v = o.putArray("venues");
            for (String s : venues()) {
                v.add(s);
            }
            o.put("events", events.size());
            int contested = 0;
            int locks = 0;
            String first = null;
            String last = null;
            for (PredictionMarkets.Event e : events) {
                contested += e.contested;
                locks += e.locks.size();
                String day = e.closeTime.substring(0, 10);
                first = first == null || day.compareTo(first) < 0 ? day : first;
                last = last == null || day.compareTo(last) > 0 ? day : last;
            }
            o.put("contested", contested);
            o.put("locks", locks);
            o.put("volume_24h", volume24h());
            o.putArray("closes").add(first).add(last);
            ArrayNode t = o.putArray("govdata_tables");
            for (String s : tables) {
                t.add(s);
            }
            List<PredictionMarkets.Event> shown = listed(maxEvents);
            o.put("events_not_listed", events.size() - shown.size());
            ArrayNode list = o.putArray("event_list");
            for (PredictionMarkets.Event e : shown) {
                list.add(e.toSummaryJson());
            }
            return o;
        }
    }

    private static LocalDate day(PredictionMarkets.Event e) {
        return LocalDate.parse(e.closeTime.substring(0, 10));
    }

    /**
     * One driver quoted on both venues, closing within days of each other: two prices for
     * what is very nearly one outcome. The simplest arbitrage — it needs no forecast, only
     * both rule texts read and both sets of quotes.
     */
    static List<Basket> crossVenue(List<PredictionMarkets.Event> events) {
        Map<String, List<PredictionMarkets.Event>> byDriver = new TreeMap<>();
        for (PredictionMarkets.Event e : events) {
            byDriver.computeIfAbsent(e.driver.name, k -> new ArrayList<>()).add(e);
        }
        List<Basket> out = new ArrayList<>();
        for (Map.Entry<String, List<PredictionMarkets.Event>> en : byDriver.entrySet()) {
            List<PredictionMarkets.Event> sorted = new ArrayList<>(en.getValue());
            sorted.sort(Comparator.comparing(MarketBaskets::day));
            List<List<PredictionMarkets.Event>> runs = new ArrayList<>();
            List<PredictionMarkets.Event> run = new ArrayList<>();
            for (PredictionMarkets.Event e : sorted) {
                if (!run.isEmpty() && ChronoUnit.DAYS.between(day(run.get(run.size() - 1)),
                        day(e)) > CROSS_VENUE_DAYS) {
                    runs.add(run);
                    run = new ArrayList<>();
                }
                run.add(e);
            }
            runs.add(run);
            for (List<PredictionMarkets.Event> r : runs) {
                Basket b = new Basket("cross_venue:" + en.getKey() + ":" + day(r.get(0)),
                    "Kalshi and Polymarket both price this quantity", r);
                if (b.venues().size() > 1) {
                    out.add(b);
                }
            }
        }
        return out;
    }

    /** Different drivers settling on the same state: one local shock moves all of them. */
    static List<Basket> samePlace(List<PredictionMarkets.Event> events) {
        Map<String, List<PredictionMarkets.Event>> byPlace = new TreeMap<>();
        for (PredictionMarkets.Event e : events) {
            Matcher m = PLACE.matcher(e.eventTitle.toLowerCase(Locale.ROOT));
            if (m.find()) {
                String place = CITIES.containsKey(m.group(1)) ? CITIES.get(m.group(1))
                    : m.group(1);
                byPlace.computeIfAbsent(place, k -> new ArrayList<>()).add(e);
            }
        }
        List<Basket> out = new ArrayList<>();
        for (Map.Entry<String, List<PredictionMarkets.Event>> en : byPlace.entrySet()) {
            Basket b = new Basket("same_place:" + en.getKey().replace(' ', '_'),
                "different quantities measured in one state", en.getValue());
            if (b.drivers().size() > 1) {
                out.add(b);
            }
        }
        return out;
    }

    /** Drivers joined by a causal chain in {@link #LINKS}. */
    static List<Basket> linkedDrivers(List<PredictionMarkets.Event> events) {
        List<Basket> out = new ArrayList<>();
        for (Link link : LINKS) {
            List<PredictionMarkets.Event> in = new ArrayList<>();
            for (PredictionMarkets.Event e : events) {
                if (link.drivers.contains(e.driver.name)) {
                    in.add(e);
                }
            }
            Basket b = new Basket("linked_drivers:" + link.name, link.why, in);
            if (b.drivers().size() > 1) {
                // The order of Link.drivers is the chain, cause first.
                List<String> present = new ArrayList<>();
                for (String d : link.drivers) {
                    if (b.drivers().contains(d)) {
                        present.add(d);
                    }
                }
                Basket directed = new Basket(b.name, link.why + "; direction of the link, cause "
                    + "to effect: " + String.join(" -> ", present) + ". The order is the causal "
                    + "chain; the sign of each link is not asserted", in);
                directed.direction = present;
                out.add(directed);
            }
        }
        return out;
    }

    /** One statistic over consecutive periods: a surprise in one carries into the next. */
    static List<Basket> seriesRun(List<PredictionMarkets.Event> events) {
        Map<String, List<PredictionMarkets.Event>> bySeries = new TreeMap<>();
        for (PredictionMarkets.Event e : events) {
            if (e.series != null && !e.series.isEmpty()) {
                bySeries.computeIfAbsent(e.source + ":" + e.series, k -> new ArrayList<>())
                    .add(e);
            }
        }
        List<Basket> out = new ArrayList<>();
        for (Map.Entry<String, List<PredictionMarkets.Event>> en : bySeries.entrySet()) {
            if (en.getValue().size() > 1) {
                out.add(new Basket("series_run:" + en.getKey(),
                    "one series over consecutive periods", en.getValue()));
            }
        }
        return out;
    }

    /**
     * The collar analogue: within one event's ladder of "above" or "at least" strikes, buy YES
     * on the lowest strike and NO on the highest. The pair pays 2 when the settlement lands
     * between the strikes and 1 outside them, so the basket pays most inside the range.
     */
    static List<Basket> range(List<PredictionMarkets.Event> events) {
        List<Basket> out = new ArrayList<>();
        for (PredictionMarkets.Event e : events) {
            PredictionMarkets.Market low = null;
            PredictionMarkets.Market high = null;
            for (PredictionMarkets.Market m : e.legs) {
                MarketPricing.Condition c = MarketPricing.Condition.ofStrike(m);
                if (c == null || !("above".equals(c.kind) || "at_least".equals(c.kind))) {
                    continue;
                }
                if (low == null || m.floorStrike < low.floorStrike) {
                    low = m;
                }
                if (high == null || m.floorStrike > high.floorStrike) {
                    high = m;
                }
            }
            if (low != null && low.floorStrike < high.floorStrike) {
                out.add(new Basket("range:" + e.source + ":" + e.eventId,
                    "within one event's ladder, buy YES on " + low.marketId + " (strike "
                    + low.floorStrike + ") and NO on " + high.marketId + " (strike "
                    + high.floorStrike + "): the pair pays 2 when the settlement lands between "
                    + "the strikes and 1 outside them", Collections.singletonList(e)));
            }
        }
        return out;
    }

    /**
     * The same quantity at adjacent periods on one venue. Unlike {@link #seriesRun}, which
     * holds every period of one venue series ticker in a single basket, a calendar basket
     * holds exactly two adjacent close dates of one driver, whatever their series tickers,
     * so the spread between the two periods can be priced as a pair.
     */
    static List<Basket> calendar(List<PredictionMarkets.Event> events) {
        Map<String, Map<String, List<PredictionMarkets.Event>>> byKey = new TreeMap<>();
        for (PredictionMarkets.Event e : events) {
            byKey.computeIfAbsent(e.source + ":" + e.driver.name, k -> new TreeMap<>())
                .computeIfAbsent(e.closeTime.substring(0, 10), k -> new ArrayList<>()).add(e);
        }
        List<Basket> out = new ArrayList<>();
        for (Map.Entry<String, Map<String, List<PredictionMarkets.Event>>> en
                : byKey.entrySet()) {
            List<String> days = new ArrayList<>(en.getValue().keySet());
            for (int i = 0; i + 1 < days.size(); i++) {
                List<PredictionMarkets.Event> pair =
                    new ArrayList<>(en.getValue().get(days.get(i)));
                pair.addAll(en.getValue().get(days.get(i + 1)));
                out.add(new Basket("calendar:" + en.getKey() + ":" + days.get(i) + "/"
                    + days.get(i + 1),
                    "one driver on one venue at two adjacent periods (series_run instead holds "
                    + "every period of one series ticker)", pair));
            }
        }
        return out;
    }

    private static List<Basket> run(String recipe, List<PredictionMarkets.Event> events) {
        switch (recipe) {
            case "cross_venue":    return crossVenue(events);
            case "same_place":     return samePlace(events);
            case "linked_drivers": return linkedDrivers(events);
            case "series_run":     return seriesRun(events);
            case "range":          return range(events);
            case "calendar":       return calendar(events);
            default:
                throw new IllegalArgumentException("recipe must be one of " + RECIPES
                    + ", got '" + recipe + "'");
        }
    }

    /** The events a basket may hold: an allowed basis and a two-sided quote no wider than
     *  {@code maxSpread}. */
    static List<PredictionMarkets.Event> filter(List<PredictionMarkets.Event> events,
            Set<String> bases, double maxSpread) {
        List<PredictionMarkets.Event> out = new ArrayList<>();
        for (PredictionMarkets.Event e : events) {
            if (bases.contains(e.driver.basis) && e.medianSpread != null
                    && e.medianSpread <= maxSpread) {
                out.add(e);
            }
        }
        return out;
    }

    /**
     * Runs {@code recipe} (every recipe when null) over {@code events}. Recipes in the order
     * of {@link #RECIPES}; within one, baskets with more distinct drivers first, then 24-hour
     * volume. {@code match} keeps baskets whose name contains it.
     */
    static List<Basket> propose(List<PredictionMarkets.Event> events, String recipe,
            String match) {
        List<Basket> out = new ArrayList<>();
        for (String r : recipe == null ? RECIPES : Collections.singletonList(recipe)) {
            List<Basket> found = run(r, events);
            found.sort(Comparator.comparingInt((Basket b) -> b.drivers().size())
                .thenComparingLong(Basket::volume24h).reversed());
            for (Basket b : found) {
                if (match == null || b.name.contains(match.toLowerCase(Locale.ROOT))) {
                    out.add(b);
                }
            }
        }
        return out;
    }

    // ─── Structural locks within one event ─────────────────────────────────────

    private static final String FEE_BASIS = "fee per contract = rate * price * (1 - price), a "
        + "taker order at the quote, with the rate from the event's taker_fee_rates; Kalshi's "
        + "per-order rounding up to the cent and the depth behind the quote are not modelled";

    private static double feeRate(Map<String, Double> feeRates, String marketId) {
        Double r = feeRates.get(marketId);
        if (r == null) {
            throw new IllegalArgumentException("no taker fee rate for market " + marketId);
        }
        return r;
    }

    private static boolean quoted(Double p) {
        return p != null && p > 0 && p < 1;
    }

    /** The settlement values a condition covers, as an interval with open or closed ends. */
    private static final class Span {
        final PredictionMarkets.Market market;
        final double lo;
        final boolean loIncl;
        final double hi;
        final boolean hiIncl;

        Span(PredictionMarkets.Market market, MarketPricing.Condition c) {
            this.market = market;
            switch (c.kind) {
                case "above":    lo = c.low; loIncl = false; hi = Double.POSITIVE_INFINITY;
                                 hiIncl = false; break;
                case "at_least": lo = c.low; loIncl = true; hi = Double.POSITIVE_INFINITY;
                                 hiIncl = false; break;
                case "below":    lo = Double.NEGATIVE_INFINITY; loIncl = false; hi = c.low;
                                 hiIncl = false; break;
                case "at_most":  lo = Double.NEGATIVE_INFINITY; loIncl = false; hi = c.low;
                                 hiIncl = true; break;
                case "between":  lo = c.low; loIncl = true; hi = c.high; hiIncl = true; break;
                default:
                    throw new IllegalStateException("unknown condition kind " + c.kind);
            }
        }
    }

    private static ObjectNode legNode(PredictionMarkets.Market m, String side, double price,
            double fee) {
        ObjectNode l = MAPPER.createObjectNode();
        l.put("market_id", m.marketId);
        l.put("title", m.title);
        l.put("side", side);
        l.put("price", PredictionMarkets.round(price, 4));
        l.put("fee", PredictionMarkets.round(fee, 5));
        return l;
    }

    /** One side bought across {@code markets}: null when a market has no quote for it. */
    private static ObjectNode sideBasket(List<PredictionMarkets.Market> markets, String side,
            int guaranteedPayout, Map<String, Double> feeRates) {
        ObjectNode b = MAPPER.createObjectNode();
        ArrayNode legs = b.putArray("legs");
        double cost = 0;
        for (PredictionMarkets.Market m : markets) {
            Double quote = "yes".equals(side) ? m.yesAsk : m.yesBid;
            if (!quoted(quote)) {
                return null;
            }
            double price = "yes".equals(side) ? quote : 1 - quote;
            double fee = PredictionMarkets.takerFee(feeRate(feeRates, m.marketId), price);
            legs.add(legNode(m, side, price, fee));
            cost += price + fee;
        }
        b.put("side", side);
        b.put("cost", PredictionMarkets.round(cost, 4));
        b.put("guaranteed_payout", guaranteedPayout);
        b.put("floor_profit", PredictionMarkets.round(guaranteedPayout - cost, 4));
        b.put("floor_profit_per_cost", PredictionMarkets.round((guaranteedPayout - cost) / cost,
            4));
        b.put("lock", guaranteedPayout - cost > 0);
        return b;
    }

    /**
     * Bucket partitions within one event: "between" markets, with the open-ended tails the
     * event lists, that are mutually exclusive and exhaustive. Then one contract on each
     * market's YES pays exactly 1, and one on each NO pays exactly n - 1, so a basket costing
     * less than that after fees is a lock needing no forecast.
     *
     * <p>Exhaustiveness is claimed only when the strikes show it: the spans, sorted, run from
     * minus infinity to plus infinity with no gap. {@code step} is the grid settlement values
     * fall on (0.1 for a rate quoted to a tenth), or null when unknown; a gap no wider than
     * it holds no value. Where the strikes do not show it, the node says "not_established"
     * and {@code lock} is false whatever the prices. Returns nothing for an event with fewer
     * than two "between" markets. Every market's fee rate must be in {@code feeRates}.
     */
    static List<ObjectNode> partitionLocks(PredictionMarkets.Event event,
            Map<String, Double> feeRates, Double step) {
        return partitionLocks(event, feeRates, step, Collections.emptyMap());
    }

    /** The condition stated for a market, else the one its venue strike gives, else null. */
    private static MarketPricing.Condition condition(PredictionMarkets.Market m,
            Map<String, MarketPricing.Condition> given) {
        MarketPricing.Condition c = given.get(m.marketId);
        return c != null ? c : MarketPricing.Condition.ofStrike(m);
    }

    /** {@link #partitionLocks(PredictionMarkets.Event, Map, Double)} with the conditions the
     *  caller stated for markets whose venue gives no strike, keyed by market id. */
    static List<ObjectNode> partitionLocks(PredictionMarkets.Event event,
            Map<String, Double> feeRates, Double step,
            Map<String, MarketPricing.Condition> given) {
        List<Span> spans = new ArrayList<>();
        int betweens = 0;
        for (PredictionMarkets.Market m : event.legs) {
            MarketPricing.Condition c = condition(m, given);
            if (c != null) {
                spans.add(new Span(m, c));
                if ("between".equals(c.kind)) {
                    betweens++;
                }
            }
        }
        List<ObjectNode> out = new ArrayList<>();
        if (betweens < 2) {
            return out;
        }
        spans.sort(Comparator.comparingDouble((Span x) -> x.lo)
            .thenComparingDouble(x -> x.hi));
        List<String> reasons = new ArrayList<>();
        boolean exclusive = true;
        double maxHi = spans.get(0).hi;
        if (spans.get(0).lo != Double.NEGATIVE_INFINITY) {
            reasons.add("no market covers settlements below " + spans.get(0).lo);
        }
        for (int i = 0; i + 1 < spans.size(); i++) {
            Span a = spans.get(i);
            Span b = spans.get(i + 1);
            maxHi = Math.max(maxHi, a.hi);
            double d = a.hi == Double.POSITIVE_INFINITY ? Double.NEGATIVE_INFINITY : b.lo - a.hi;
            if (d < 0 || d == 0 && a.hiIncl && b.loIncl) {
                exclusive = false;
                reasons.add(a.market.marketId + " and " + b.market.marketId + " overlap");
            } else if (d == 0 && !a.hiIncl && !b.loIncl) {
                reasons.add("the point " + a.hi + " belongs to neither " + a.market.marketId
                    + " nor " + b.market.marketId);
            } else if (d > 0 && (step == null || d > step + 1e-9)) {
                reasons.add("settlements between " + a.hi + " and " + b.lo + " fall in no market"
                    + (step == null ? " (the settlement grid is not known)" : ""));
            }
        }
        maxHi = Math.max(maxHi, spans.get(spans.size() - 1).hi);
        if (maxHi != Double.POSITIVE_INFINITY) {
            reasons.add("no market covers settlements above " + maxHi);
        }
        boolean established = reasons.isEmpty();
        List<PredictionMarkets.Market> markets = new ArrayList<>();
        for (Span x : spans) {
            markets.add(x.market);
        }
        ObjectNode node = MAPPER.createObjectNode();
        node.put("type", "bucket_partition");
        node.put("source", event.source);
        node.put("event_id", event.eventId);
        node.put("markets", markets.size());
        node.put("exclusive", exclusive);
        node.put("exhaustive", established ? "established" : "not_established");
        ArrayNode why = node.putArray("exhaustive_reasons");
        for (String r : reasons) {
            why.add(r);
        }
        node.put("fee_basis", FEE_BASIS);
        ObjectNode yes = sideBasket(markets, "yes", 1, feeRates);
        ObjectNode no = sideBasket(markets, "no", markets.size() - 1, feeRates);
        node.set("buy_all_yes", yes);
        node.set("buy_all_no", no);
        boolean lock = established && exclusive
            && (yes != null && yes.get("lock").asBoolean()
                || no != null && no.get("lock").asBoolean());
        node.put("lock", lock);
        out.add(node);
        return out;
    }

    /**
     * Non-monotone ladders, fee-inclusive. Within a family of "above or at least" strikes (or
     * of "below or at most" strikes), the market covering fewer settlement values can never be
     * worth more than the one covering more. Buying YES on the wider market and NO on the
     * narrower pays at least 1 (2 between the strikes), so it locks when its cost after fees
     * is under 1. A pair is returned when it locks before fees; {@code lock_after_fees} says
     * whether the fees leave it standing. {@link PredictionMarkets#locks} is the fee-blind
     * version of this check. Every market's fee rate must be in {@code feeRates}.
     */
    static List<ObjectNode> ladderLocks(PredictionMarkets.Event event,
            Map<String, Double> feeRates) {
        return ladderLocks(event, feeRates, Collections.emptyMap());
    }

    /** {@link #ladderLocks(PredictionMarkets.Event, Map)} with the conditions the caller
     *  stated for markets whose venue gives no strike, keyed by market id. */
    static List<ObjectNode> ladderLocks(PredictionMarkets.Event event,
            Map<String, Double> feeRates, Map<String, MarketPricing.Condition> given) {
        List<PredictionMarkets.Market> up = new ArrayList<>();
        List<PredictionMarkets.Market> down = new ArrayList<>();
        // The strike of each ladder market: a one-sided condition holds it in low.
        Map<String, Double> strike = new HashMap<>();
        for (PredictionMarkets.Market m : event.legs) {
            MarketPricing.Condition c = condition(m, given);
            if (c == null || !quoted(m.yesAsk) || !quoted(m.yesBid)) {
                continue;
            }
            if ("above".equals(c.kind) || "at_least".equals(c.kind)) {
                up.add(m);
                strike.put(m.marketId, c.low);
            } else if ("below".equals(c.kind) || "at_most".equals(c.kind)) {
                down.add(m);
                strike.put(m.marketId, c.low);
            }
        }
        up.sort(Comparator.comparingDouble((PredictionMarkets.Market m)
            -> strike.get(m.marketId)));
        down.sort(Comparator.comparingDouble((PredictionMarkets.Market m)
            -> strike.get(m.marketId)).reversed());
        List<ObjectNode> out = new ArrayList<>();
        for (List<PredictionMarkets.Market> ladder : Arrays.asList(up, down)) {
            for (int i = 0; i < ladder.size(); i++) {
                for (int j = i + 1; j < ladder.size(); j++) {
                    PredictionMarkets.Market wide = ladder.get(i);
                    PredictionMarkets.Market narrow = ladder.get(j);
                    double sw = strike.get(wide.marketId);
                    double sn = strike.get(narrow.marketId);
                    if (sw == sn || !(narrow.yesBid > wide.yesAsk)) {
                        continue;
                    }
                    double yesPrice = wide.yesAsk;
                    double noPrice = 1 - narrow.yesBid;
                    double yesFee = PredictionMarkets.takerFee(
                        feeRate(feeRates, wide.marketId), yesPrice);
                    double noFee = PredictionMarkets.takerFee(
                        feeRate(feeRates, narrow.marketId), noPrice);
                    double cost = yesPrice + yesFee + noPrice + noFee;
                    ObjectNode n = MAPPER.createObjectNode();
                    n.put("type", "non_monotone_ladder");
                    n.put("source", event.source);
                    n.put("event_id", event.eventId);
                    ArrayNode legs = n.putArray("legs");
                    legs.add(legNode(wide, "yes", yesPrice, yesFee));
                    legs.add(legNode(narrow, "no", noPrice, noFee));
                    n.put("gross_gap", PredictionMarkets.round(narrow.yesBid - wide.yesAsk, 4));
                    n.put("cost", PredictionMarkets.round(cost, 4));
                    n.put("guaranteed_payout", 1);
                    n.put("max_payout", 2);
                    n.put("floor_profit", PredictionMarkets.round(1 - cost, 4));
                    n.put("lock_before_fees", true);
                    n.put("lock_after_fees", 1 - cost > 0);
                    n.put("fee_basis", FEE_BASIS);
                    out.add(n);
                }
            }
        }
        return out;
    }

    // ─── Payoff curve ──────────────────────────────────────────────────────────

    /**
     * Profit per unit of basket cost by settlement value, one contract per leg. A leg that
     * holds pays 1; its price and fee are paid up front. All legs must settle on one quantity
     * (the same {@code column}); otherwise this throws naming them. {@code grid} lists the
     * values reported. The floor, the maximum and the break-evens come from the strikes, not
     * the grid: profit is constant between strikes, so each strike and the stretch on either
     * side of it is evaluated. A break-even is a strike where profit crosses zero, with the
     * profit on each side; at the strike itself, "above" and "below" legs do not hold and
     * "at_least" and "at_most" legs do.
     */
    static ObjectNode payoffCurve(List<MarketPricing.Leg> legs, double[] grid) {
        if (legs.isEmpty()) {
            throw new IllegalArgumentException("a payoff curve needs at least one leg");
        }
        if (grid.length == 0) {
            throw new IllegalArgumentException("a payoff curve needs at least one value");
        }
        Set<String> columns = new TreeSet<>();
        List<String> names = new ArrayList<>();
        for (MarketPricing.Leg l : legs) {
            columns.add(String.valueOf(l.column));
            names.add(l.key() + " (settles on " + l.column + ")");
        }
        if (legs.get(0).column == null) {
            throw new IllegalArgumentException("legs name no settlement quantity: "
                + String.join("; ", names));
        }
        if (columns.size() > 1) {
            throw new IllegalArgumentException("legs settle on different quantities and share "
                + "no settlement axis: " + String.join("; ", names));
        }
        double cost = 0;
        for (MarketPricing.Leg l : legs) {
            cost += l.price + l.fee;
        }
        if (!(cost > 0)) {
            throw new IllegalArgumentException("basket cost must be positive, got " + cost);
        }
        ObjectNode out = MAPPER.createObjectNode();
        out.put("column", legs.get(0).column);
        out.put("cost", PredictionMarkets.round(cost, 4));
        out.put("tie_convention", "above and below are strict; at_least, at_most and "
            + "between include their strike");
        ArrayNode curve = out.putArray("curve");
        for (double v : grid) {
            ObjectNode p = curve.addObject();
            p.put("value", v);
            p.put("payout", payout(legs, v));
            p.put("profit_per_cost", PredictionMarkets.round((payout(legs, v) - cost) / cost,
                4));
        }
        TreeSet<Double> strikes = new TreeSet<>();
        for (MarketPricing.Leg l : legs) {
            strikes.add(l.condition.low);
            if ("between".equals(l.condition.kind)) {
                strikes.add(l.condition.high);
            }
        }
        // Evaluation points in order: the stretch below the first strike, each strike, the
        // stretch after it.
        List<Double> values = new ArrayList<>();
        List<String> where = new ArrayList<>();
        Double prev = null;
        for (double k : strikes) {
            values.add(prev == null ? k - 1 : (prev + k) / 2);
            where.add(prev == null ? "below " + k : "between " + prev + " and " + k);
            values.add(k);
            where.add("at " + k);
            prev = k;
        }
        values.add(prev + 1);
        where.add("above " + prev);
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        String minAt = null;
        String maxAt = null;
        ArrayNode breakEvens = out.putArray("break_even_values");
        for (int i = 0; i < values.size(); i++) {
            double profit = (payout(legs, values.get(i)) - cost) / cost;
            if (profit < min) {
                min = profit;
                minAt = where.get(i);
            }
            if (profit > max) {
                max = profit;
                maxAt = where.get(i);
            }
            // Profit changes only at strikes; the stretch before a strike is values[i - 1].
            if (i % 2 == 1 && i > 0) {
                double before = (payout(legs, values.get(i - 1)) - cost) / cost;
                double after = (payout(legs, values.get(i + 1)) - cost) / cost;
                if ((before >= 0) != (after >= 0) || (before >= 0) != (profit >= 0)) {
                    ObjectNode be = breakEvens.addObject();
                    be.put("value", values.get(i));
                    be.put("profit_below", PredictionMarkets.round(before, 4));
                    be.put("profit_at", PredictionMarkets.round(profit, 4));
                    be.put("profit_above", PredictionMarkets.round(after, 4));
                }
            }
        }
        out.put("floor_profit_per_cost", PredictionMarkets.round(min, 4));
        out.put("floor_where", minAt);
        out.put("max_profit_per_cost", PredictionMarkets.round(max, 4));
        out.put("max_where", maxAt);
        out.put("floor_is_fee_inclusive", true);
        return out;
    }

    private static int payout(List<MarketPricing.Leg> legs, double v) {
        int n = 0;
        for (MarketPricing.Leg l : legs) {
            if (l.condition.holds(v) == "yes".equals(l.side)) {
                n++;
            }
        }
        return n;
    }
}
