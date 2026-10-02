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
        "cross_venue", "same_place", "linked_drivers", "series_run"));

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

        /** The basket with at most {@code maxEvents} of its events listed (0 = all), both
         *  venues represented. */
        ObjectNode toJson(int maxEvents) {
            ObjectNode o = MAPPER.createObjectNode();
            o.put("basket", name);
            o.put("why", why);
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
            List<PredictionMarkets.Event> sorted = new ArrayList<>(events);
            sorted.sort(Comparator.comparing((PredictionMarkets.Event e) -> e.driver.name)
                .thenComparing(Comparator.comparingLong(
                    (PredictionMarkets.Event e) -> e.volume24h).reversed()));
            List<PredictionMarkets.Event> shown = maxEvents > 0
                ? MarketTools.balanced(sorted, maxEvents) : sorted;
            o.put("events_not_listed", sorted.size() - shown.size());
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
                out.add(b);
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

    private static List<Basket> run(String recipe, List<PredictionMarkets.Event> events) {
        switch (recipe) {
            case "cross_venue":    return crossVenue(events);
            case "same_place":     return samePlace(events);
            case "linked_drivers": return linkedDrivers(events);
            case "series_run":     return seriesRun(events);
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
}
