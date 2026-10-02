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

import org.apache.commons.math3.distribution.NormalDistribution;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Prices prediction-market contracts against a forecast, and baskets of them against joint
 * scenarios.
 *
 * <p>The fair value of a market is the share of the forecast that meets its settlement
 * condition. The edge of a side is fair value minus what the side costs: YES costs the ask, NO
 * costs one minus the bid, plus the venue's taker fee. A basket holds one contract per leg; a
 * leg that wins pays 1.
 */
final class MarketPricing {

    /** Points used to stand in for a parametric forecast. */
    static final int NORMAL_POINTS = 20001;
    /** A subset search larger than this is refused. */
    static final int MAX_SUBSETS = 200000;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<String> KINDS =
        Arrays.asList("above", "at_least", "below", "at_most", "between");
    private static final double EPS = 1e-9;

    private MarketPricing() { }

    // ─── Conditions ────────────────────────────────────────────────────────────

    /** What the settlement value must satisfy for a market to resolve YES. */
    static final class Condition {
        final String kind;
        final double low;
        /** Used by {@code between} only. */
        final double high;

        Condition(String kind, double low, double high) {
            this.kind = kind;
            this.low = low;
            this.high = high;
        }

        /** Parses {@code {"above": 3.6}} or {@code {"between": [3.5, 3.6]}}. */
        static Condition parse(JsonNode node) {
            List<String> found = new ArrayList<>();
            if (node != null && node.isObject()) {
                for (String k : KINDS) {
                    if (node.has(k)) {
                        found.add(k);
                    }
                }
            }
            if (found.size() != 1) {
                throw new IllegalArgumentException("a condition needs exactly one of " + KINDS
                    + ": " + node);
            }
            String kind = found.get(0);
            JsonNode v = node.get(kind);
            if ("between".equals(kind)) {
                if (!v.isArray() || v.size() != 2 || !v.get(0).isNumber()
                        || !v.get(1).isNumber()) {
                    throw new IllegalArgumentException(
                        "between needs [low, high] as two numbers: " + node);
                }
                return new Condition(kind, v.get(0).asDouble(), v.get(1).asDouble());
            }
            if (!v.isNumber()) {
                throw new IllegalArgumentException(kind + " needs a number: " + node);
            }
            return new Condition(kind, v.asDouble(), v.asDouble());
        }

        /** The condition a Kalshi strike states, or null when the venue gives no strike. */
        static Condition ofStrike(PredictionMarkets.Market m) {
            String t = m.strikeType;
            if ("greater".equals(t) && m.floorStrike != null) {
                return new Condition("above", m.floorStrike, m.floorStrike);
            }
            if ("greater_or_equal".equals(t) && m.floorStrike != null) {
                return new Condition("at_least", m.floorStrike, m.floorStrike);
            }
            if ("less".equals(t) && m.capStrike != null) {
                return new Condition("below", m.capStrike, m.capStrike);
            }
            if ("less_or_equal".equals(t) && m.capStrike != null) {
                return new Condition("at_most", m.capStrike, m.capStrike);
            }
            if ("between".equals(t) && m.floorStrike != null && m.capStrike != null) {
                return new Condition("between", m.floorStrike, m.capStrike);
            }
            return null;
        }

        boolean holds(double v) {
            switch (kind) {
                case "above":    return v > low;
                case "at_least": return v >= low;
                case "below":    return v < low;
                case "at_most":  return v <= low;
                case "between":  return low <= v && v <= high;
                default:
                    throw new IllegalStateException("unknown condition kind " + kind);
            }
        }

        void writeTo(ObjectNode o) {
            if ("between".equals(kind)) {
                o.putArray(kind).add(low).add(high);
            } else {
                o.put(kind, low);
            }
        }

        ObjectNode toJson() {
            ObjectNode o = MAPPER.createObjectNode();
            writeTo(o);
            return o;
        }
    }

    // ─── Forecasts ─────────────────────────────────────────────────────────────

    /** A distribution of the value an event settles on, as a list of equally likely values. */
    static final class Forecast {
        final double[] values;
        /** True for samples, whose fair values carry sampling error. */
        final boolean sampled;
        final String form;

        Forecast(double[] values, boolean sampled, String form) {
            this.values = values;
            this.sampled = sampled;
            this.form = form;
        }

        /** The forecast rounded to {@code places} decimals, as a settlement source publishes
         *  it. Rounds the exact binary value half-even. */
        Forecast rounded(int places) {
            double[] out = new double[values.length];
            for (int i = 0; i < values.length; i++) {
                out[i] = new BigDecimal(values[i]).setScale(places, RoundingMode.HALF_EVEN)
                    .doubleValue();
            }
            return new Forecast(out, sampled, form + ", rounded to " + places + " decimals");
        }

        ObjectNode toJson() {
            double[] ordered = values.clone();
            Arrays.sort(ordered);
            int n = ordered.length;
            ObjectNode o = MAPPER.createObjectNode();
            o.put("form", form);
            o.put("n", n);
            o.put("median", n % 2 == 1 ? ordered[n / 2]
                : (ordered[n / 2 - 1] + ordered[n / 2]) / 2);
            o.put("p05", ordered[(int) (0.05 * n)]);
            o.put("p95", ordered[(int) (0.95 * n)]);
            return o;
        }
    }

    static Forecast samples(double[] values) {
        if (values.length < 2) {
            throw new IllegalArgumentException("a sampled forecast needs at least 2 values, got "
                + values.length);
        }
        return new Forecast(values, true, "samples");
    }

    private static double[] standardQuantiles() {
        NormalDistribution std = new NormalDistribution();
        double[] z = new double[NORMAL_POINTS];
        for (int i = 0; i < NORMAL_POINTS; i++) {
            z[i] = std.inverseCumulativeProbability((i + 0.5) / NORMAL_POINTS);
        }
        return z;
    }

    static Forecast normal(double mean, double sd) {
        if (!(sd > 0)) {
            throw new IllegalArgumentException("sd must be positive, got " + sd);
        }
        double[] z = standardQuantiles();
        for (int i = 0; i < z.length; i++) {
            z[i] = mean + sd * z[i];
        }
        return new Forecast(z, false, "normal(mean=" + mean + ", sd=" + sd + ")");
    }

    /** A normal forecast from a point forecast and its two-sided interval at {@code level},
     *  the shape arima_forecast reports. */
    static Forecast normalFromInterval(double mean, double lower, double upper, double level) {
        if (!(level > 0 && level < 1)) {
            throw new IllegalArgumentException("level must be strictly between 0 and 1, got "
                + level);
        }
        if (!(upper > lower)) {
            throw new IllegalArgumentException("upper must be above lower, got " + lower
                + " and " + upper);
        }
        double z = new NormalDistribution().inverseCumulativeProbability(0.5 + level / 2);
        return normal(mean, (upper - lower) / (2 * z));
    }

    /** A lognormal forecast of a price: the median and the cumulative volatility in percent
     *  over the horizon, the shape volatility_forecast's price_band reports. */
    static Forecast lognormal(double median, double cumulativeVolPct) {
        if (!(median > 0)) {
            throw new IllegalArgumentException("price_median must be positive, got " + median);
        }
        if (!(cumulativeVolPct > 0)) {
            throw new IllegalArgumentException("cumulative_vol_pct must be positive, got "
                + cumulativeVolPct);
        }
        double[] z = standardQuantiles();
        for (int i = 0; i < z.length; i++) {
            z[i] = median * Math.exp(cumulativeVolPct / 100.0 * z[i]);
        }
        return new Forecast(z, false, "lognormal(median=" + median + ", cumulative_vol_pct="
            + cumulativeVolPct + ")");
    }

    // ─── One event ─────────────────────────────────────────────────────────────

    /** One side of one market, bought at the quote. */
    static final class Leg {
        String id;
        String source;
        String eventId;
        String title;
        String driver;
        /** The scenario column holding the value this leg's event settles on. */
        String column;
        String side;
        double price;
        double fee;
        Double fair;
        Double edge;
        Double se;
        Condition condition;

        String key() {
            return id + ":" + side;
        }

        ObjectNode toJson() {
            ObjectNode o = MAPPER.createObjectNode();
            o.put("id", id);
            o.put("source", source);
            o.put("event_id", eventId);
            o.put("title", title);
            o.put("driver", driver);
            o.put("column", column);
            o.put("side", side);
            o.put("price", price);
            o.put("fee", fee);
            PredictionMarkets.putNumber(o, "fair", fair);
            PredictionMarkets.putNumber(o, "edge", edge);
            PredictionMarkets.putNumber(o, "se", se);
            condition.writeTo(o);
            return o;
        }
    }

    /** An event priced: the report, and the legs a basket can be built from. */
    static final class Priced {
        ObjectNode json;
        List<Leg> legs = new ArrayList<>();
    }

    private static Double rounded(Double v, int places) {
        return v == null ? null : PredictionMarkets.round(v, places);
    }

    /**
     * Prices every market of {@code live} that has a settlement condition.
     *
     * @param forecast the forecast, or null to report quotes and fees only
     * @param given conditions by market_id; one given here replaces the venue's strike
     * @param minEdge the smallest edge, after the fee, reported as mispriced
     * @param feeRate a rate for every market in place of the venue's, or null
     * @param bothSides keep every quoted side as a leg whatever its edge (a lock search)
     * @param column the scenario column of this event's settlement value, or null for its
     *     event_id
     */
    static Priced priceEvent(PredictionMarkets.LiveEvent live, Forecast forecast,
            Map<String, Condition> given, double minEdge, Double feeRate, boolean bothSides,
            String column) {
        PredictionMarkets.Event event = live.event;
        Set<String> ids = new HashSet<>();
        for (PredictionMarkets.Market m : event.legs) {
            ids.add(m.marketId);
        }
        for (String id : given.keySet()) {
            if (!ids.contains(id)) {
                throw new IllegalArgumentException("conditions names market_id '" + id
                    + "', which is not an open market of " + event.source + " event "
                    + event.eventId + "; its markets are " + new TreeSet<>(ids));
            }
        }
        Priced out = new Priced();
        ObjectNode json = event.toJson();
        json.remove("legs");
        if (forecast == null) {
            json.putNull("forecast");
        } else {
            json.set("forecast", forecast.toJson());
        }
        Set<Double> rates = new TreeSet<>();
        ArrayNode markets = json.putArray("priced_markets");
        ArrayNode unpriced = MAPPER.createArrayNode();
        List<PredictionMarkets.Market> ordered = new ArrayList<>(event.legs);
        ordered.sort(Comparator.comparing((PredictionMarkets.Market m) -> m.marketId));
        int mispricedCount = 0;
        for (PredictionMarkets.Market m : ordered) {
            Condition condition = given.containsKey(m.marketId) ? given.get(m.marketId)
                : Condition.ofStrike(m);
            if (condition == null) {
                ObjectNode u = MAPPER.createObjectNode();
                u.put("market_id", m.marketId);
                u.put("title", m.title);
                PredictionMarkets.putNumber(u, "yes_bid", m.yesBid);
                PredictionMarkets.putNumber(u, "yes_ask", m.yesAsk);
                unpriced.add(u);
                continue;
            }
            double rate;
            if (feeRate != null) {
                rate = feeRate;
            } else {
                Double venueRate = live.feeRates.get(m.marketId);
                if (venueRate == null) {
                    throw new IllegalStateException("no fee rate was read for market "
                        + m.marketId);
                }
                rate = venueRate;
            }
            rates.add(rate);
            Double fair = null;
            Double se = null;
            if (forecast != null) {
                int hits = 0;
                for (double v : forecast.values) {
                    if (condition.holds(v)) {
                        hits++;
                    }
                }
                fair = (double) hits / forecast.values.length;
                if (forecast.sampled) {
                    se = Math.sqrt(fair * (1 - fair) / forecast.values.length);
                }
            }
            // A side can be bought only when someone is quoting the other side of it.
            List<Leg> offers = new ArrayList<>();
            if (m.yesAsk != null && m.yesAsk > 0 && m.yesAsk < 1) {
                offers.add(offer(event, m, condition, column, "yes", m.yesAsk,
                    PredictionMarkets.takerFee(rate, m.yesAsk), fair, se));
            }
            if (m.yesBid != null && m.yesBid > 0 && m.yesBid < 1) {
                offers.add(offer(event, m, condition, column, "no", 1 - m.yesBid,
                    PredictionMarkets.takerFee(rate, 1 - m.yesBid), fair, se));
            }
            Leg best = null;
            if (fair != null) {
                for (Leg o : offers) {
                    if (best == null || o.edge > best.edge) {
                        best = o;
                    }
                }
            }
            boolean mispriced = best != null && best.edge >= minEdge;
            ObjectNode row = MAPPER.createObjectNode();
            row.put("market_id", m.marketId);
            row.put("title", m.title);
            row.set("condition", condition.toJson());
            row.put("fee_rate", rate);
            PredictionMarkets.putNumber(row, "fair", rounded(fair, 4));
            PredictionMarkets.putNumber(row, "se", rounded(se, 4));
            PredictionMarkets.putNumber(row, "yes_bid", m.yesBid);
            PredictionMarkets.putNumber(row, "yes_ask", m.yesAsk);
            PredictionMarkets.putNumber(row, "volume", m.volume);
            PredictionMarkets.putNumber(row, "open_interest", m.openInterest);
            if (best == null) {
                row.putNull("side");
            } else {
                row.put("side", best.side);
                row.put("price", PredictionMarkets.round(best.price, 4));
                row.put("fee", PredictionMarkets.round(best.fee, 5));
                row.put("edge", PredictionMarkets.round(best.edge, 4));
                row.put("return_on_cost",
                    PredictionMarkets.round(best.edge / (best.price + best.fee), 4));
            }
            String verdict;
            if (best == null) {
                verdict = fair == null ? "not_forecast" : "no_quote";
            } else if (!mispriced) {
                verdict = "within_min_edge";
            } else if (se != null && best.edge < 2 * se) {
                verdict = "within_sampling_error";
            } else {
                verdict = "mispriced";
                mispricedCount++;
            }
            row.put("verdict", verdict);
            markets.add(row);
            for (Leg o : offers) {
                if (bothSides || (mispriced && o == best)) {
                    o.price = PredictionMarkets.round(o.price, 4);
                    o.fee = PredictionMarkets.round(o.fee, 5);
                    o.fair = rounded(o.fair, 4);
                    o.edge = rounded(o.edge, 4);
                    o.se = rounded(o.se, 4);
                    out.legs.add(o);
                }
            }
        }
        json.set("markets_without_condition", unpriced);
        ArrayNode rateArr = json.putArray("taker_fee_rates");
        for (Double r : rates) {
            rateArr.add(r);
        }
        json.put("fee_basis", (feeRate == null ? "read from " + event.source
            : "fee_rate argument") + "; fee per contract = rate * price * (1 - price), a taker "
            + "order at the quote. Not modelled: Kalshi's rounding of each order's fee up to "
            + "the cent, Kalshi event-level fee overrides, the depth behind the quote.");
        json.put("min_edge", minEdge);
        json.put("mispriced_markets", mispricedCount);
        ArrayNode legArr = json.putArray("legs");
        for (Leg l : out.legs) {
            legArr.add(l.toJson());
        }
        json.put("legs_basis", bothSides ? "every quoted side"
            : "the better side of each market with edge >= " + minEdge);
        out.json = json;
        return out;
    }

    private static Leg offer(PredictionMarkets.Event event, PredictionMarkets.Market m,
            Condition condition, String column, String side, double price, double fee,
            Double fair, Double se) {
        Leg l = new Leg();
        l.id = m.marketId;
        l.source = event.source;
        l.eventId = event.eventId;
        l.title = m.title;
        l.driver = event.driver == null ? null : event.driver.name;
        l.column = column == null ? event.eventId : column;
        l.side = side;
        l.price = price;
        l.fee = fee;
        l.fair = fair;
        l.se = se;
        l.condition = condition;
        if (fair != null) {
            // YES wins with probability fair and costs the ask; NO wins with 1 - fair and
            // costs 1 - bid, which is bid - fair.
            l.edge = "yes".equals(side) ? fair - price - fee : (1 - price) - fair - fee;
        }
        return l;
    }

    // ─── Baskets ───────────────────────────────────────────────────────────────

    /** Joint scenarios: per row, the value each column settles on, and the row's weight. */
    static final class Scenarios {
        final List<Map<String, Double>> rows;
        final double[] weights;
        final String basis;

        Scenarios(List<Map<String, Double>> rows, double[] weights, String basis) {
            this.rows = rows;
            this.weights = weights;
            this.basis = basis;
        }

        /** Equally weighted rows, or the weights of an optional column {@code p}. */
        static Scenarios of(List<Map<String, Double>> rows, String basis) {
            if (rows.isEmpty()) {
                throw new IllegalArgumentException("no scenarios were given");
            }
            double[] w = new double[rows.size()];
            boolean weighted = rows.get(0).containsKey("p");
            double total = 0;
            for (int i = 0; i < w.length; i++) {
                if (rows.get(i).containsKey("p") != weighted) {
                    throw new IllegalArgumentException(
                        "the weight column p is present in some scenarios and not others");
                }
                w[i] = weighted ? rows.get(i).get("p") : 1.0 / w.length;
                total += w[i];
            }
            if (Math.abs(total - 1) > 1e-6) {
                throw new IllegalArgumentException("scenario weights p sum to " + total
                    + ", not 1");
            }
            return new Scenarios(rows, w, basis);
        }
    }

    /**
     * Every outcome the legs' conditions can tell apart, for legs that all settle on one
     * quantity: each threshold itself and one value inside each interval the thresholds cut,
     * including below the lowest and above the highest. A basket that loses at none of these
     * loses at no value at all.
     */
    static Scenarios grid(List<Leg> legs) {
        Set<String> columns = new TreeSet<>();
        TreeSet<Double> cuts = new TreeSet<>();
        for (Leg l : legs) {
            columns.add(l.column);
            cuts.add(l.condition.low);
            cuts.add(l.condition.high);
        }
        if (columns.size() != 1) {
            throw new IllegalArgumentException("scenario_grid needs every leg to settle on one "
                + "quantity: give every event the same column and state each market's "
                + "condition in that quantity's units. Columns found: " + columns);
        }
        String column = columns.iterator().next();
        List<Double> points = new ArrayList<>();
        points.add(cuts.first() - 1);
        Double previous = null;
        for (Double c : cuts) {
            if (previous != null) {
                points.add((previous + c) / 2);
            }
            points.add(c);
            previous = c;
        }
        points.add(cuts.last() + 1);
        List<Map<String, Double>> rows = new ArrayList<>();
        for (Double p : points) {
            Map<String, Double> row = new LinkedHashMap<>();
            row.put(column, p);
            rows.add(row);
        }
        double[] w = new double[rows.size()];
        Arrays.fill(w, 1.0 / w.length);
        return new Scenarios(rows, w, "outcome grid over " + cuts.size() + " thresholds of '"
            + column + "': every threshold and one value in each interval between, below and "
            + "above them. Rows are outcomes, not probabilities — only floor, worst and best "
            + "mean anything.");
    }

    private static final class Score {
        double cost;
        double pProfit;
        double pLoss;
        double expected;
        double worst;
        double best;

        double yield() {
            return expected / cost;
        }

        double floor() {
            return worst / cost;
        }
    }

    private static Score score(int[] idx, List<Leg> legs, boolean[][] wins, double[] weights) {
        Score s = new Score();
        for (int i : idx) {
            s.cost += legs.get(i).price + legs.get(i).fee;
        }
        s.worst = Double.POSITIVE_INFINITY;
        s.best = Double.NEGATIVE_INFINITY;
        for (int sc = 0; sc < weights.length; sc++) {
            int won = 0;
            for (int i : idx) {
                if (wins[i][sc]) {
                    won++;
                }
            }
            double pnl = won - s.cost;
            if (pnl > EPS) {
                s.pProfit += weights[sc];
            } else if (pnl < -EPS) {
                s.pLoss += weights[sc];
            }
            s.expected += weights[sc] * pnl;
            s.worst = Math.min(s.worst, pnl);
            s.best = Math.max(s.best, pnl);
        }
        return s;
    }

    private static ObjectNode scoreJson(int[] idx, List<Leg> legs, Score s, boolean outcomes) {
        ObjectNode o = MAPPER.createObjectNode();
        ArrayNode ids = o.putArray("legs");
        Set<String> events = new TreeSet<>();
        Set<String> venues = new TreeSet<>();
        for (int i : idx) {
            Leg l = legs.get(i);
            ObjectNode li = ids.addObject();
            li.put("id", l.id);
            li.put("source", l.source);
            li.put("side", l.side);
            li.put("price", l.price);
            li.put("fee", l.fee);
            events.add(l.source + ":" + l.eventId);
            venues.add(l.source);
        }
        o.put("events", events.size());
        ArrayNode v = o.putArray("venues");
        for (String s2 : venues) {
            v.add(s2);
        }
        o.put("cost", PredictionMarkets.round(s.cost, 5));
        o.put("floor", PredictionMarkets.round(s.floor(), 4));
        o.put("worst", PredictionMarkets.round(s.worst, 5));
        o.put("best", PredictionMarkets.round(s.best, 5));
        if (!outcomes) {
            o.put("p_profit", PredictionMarkets.round(s.pProfit, 4));
            o.put("p_loss", PredictionMarkets.round(s.pLoss, 4));
            o.put("expected", PredictionMarkets.round(s.expected, 5));
            o.put("yield", PredictionMarkets.round(s.yield(), 4));
        }
        return o;
    }

    /** The number of subsets of 2..k of n legs. */
    static long subsetCount(int n, int k) {
        long total = 0;
        long c = n;                       // C(n, 1)
        for (int size = 2; size <= Math.min(k, n); size++) {
            c = c * (n - size + 1) / size;
            total += c;
            if (total > Long.MAX_VALUE / 4) {
                return total;
            }
        }
        return total;
    }

    /**
     * Scores a basket of legs against joint scenarios: each leg alone, all of them together,
     * and with {@code search} at least 2 every subset of 2..search legs.
     *
     * @param outcomes true when the scenarios are an outcome grid, whose rows carry no
     *     probability: only cost, floor, worst and best are reported, and {@code lock} must be
     *     set
     * @param lock keep only subsets that lose in no scenario, ranked by floor; legs of one
     *     event may then combine
     * @param minYield smallest yield kept (floor under lock), or null
     */
    static ObjectNode scoreBasket(List<Leg> legs, Scenarios scenarios, boolean outcomes,
            int search, boolean lock, Double minYield, int top) {
        if (legs.isEmpty()) {
            throw new IllegalArgumentException("the basket has no legs: no market of the given "
                + "events had a side to buy");
        }
        if (outcomes && !lock) {
            throw new IllegalArgumentException("scenario_grid rows are outcomes without "
                + "probabilities, so it can only answer a lock search: set lock=true");
        }
        Set<String> seen = new HashSet<>();
        for (Leg l : legs) {
            if (!seen.add(l.source + ":" + l.key())) {
                throw new IllegalArgumentException("leg " + l.key() + " appears twice");
            }
        }
        int n = legs.size();
        int rows = scenarios.rows.size();
        boolean[][] wins = new boolean[n][rows];
        for (int i = 0; i < n; i++) {
            Leg l = legs.get(i);
            for (int sc = 0; sc < rows; sc++) {
                Double v = scenarios.rows.get(sc).get(l.column);
                if (v == null) {
                    throw new IllegalArgumentException("scenario " + (sc + 1)
                        + " has no value for column '" + l.column + "' (leg " + l.id
                        + "); columns there: " + scenarios.rows.get(sc).keySet());
                }
                boolean yes = l.condition.holds(v);
                wins[i][sc] = "yes".equals(l.side) ? yes : !yes;
            }
        }
        ObjectNode out = MAPPER.createObjectNode();
        out.put("scenarios", rows);
        out.put("scenario_basis", scenarios.basis);
        out.put("payout", "one contract per leg; a winning leg pays 1; cost = price + taker "
            + "fee per leg");
        ArrayNode alone = out.putArray("legs_alone");
        for (int i = 0; i < n; i++) {
            int[] idx = {i};
            ObjectNode o = scoreJson(idx, legs, score(idx, legs, wins, scenarios.weights),
                outcomes);
            o.put("title", legs.get(i).title);
            o.set("condition", legs.get(i).condition.toJson());
            PredictionMarkets.putNumber(o, "edge", legs.get(i).edge);
            alone.add(o);
        }
        int[] all = new int[n];
        for (int i = 0; i < n; i++) {
            all[i] = i;
        }
        out.set("all_legs", scoreJson(all, legs, score(all, legs, wins, scenarios.weights),
            outcomes));
        if (search >= 2) {
            long count = subsetCount(n, search);
            if (count > MAX_SUBSETS) {
                throw new IllegalArgumentException(count + " subsets of up to " + search
                    + " of " + n + " legs exceeds " + MAX_SUBSETS + ": lower search, raise "
                    + "min_edge, or price fewer events");
            }
            List<int[]> kept = new ArrayList<>();
            List<Score> scores = new ArrayList<>();
            for (int size = 2; size <= Math.min(search, n); size++) {
                int[] idx = new int[size];
                for (int i = 0; i < size; i++) {
                    idx[i] = i;
                }
                while (true) {
                    Set<String> events = new HashSet<>();
                    for (int i : idx) {
                        events.add(legs.get(i).source + ":" + legs.get(i).eventId);
                    }
                    if (lock || events.size() >= 2) {
                        Score s = score(idx, legs, wins, scenarios.weights);
                        boolean keep = !lock || s.worst > EPS;
                        if (keep && minYield != null) {
                            keep = (lock ? s.floor() : s.yield()) >= minYield;
                        }
                        if (keep) {
                            kept.add(idx.clone());
                            scores.add(s);
                        }
                    }
                    int pos = size - 1;
                    while (pos >= 0 && idx[pos] == n - size + pos) {
                        pos--;
                    }
                    if (pos < 0) {
                        break;
                    }
                    idx[pos]++;
                    for (int j = pos + 1; j < size; j++) {
                        idx[j] = idx[j - 1] + 1;
                    }
                }
            }
            Integer[] order = new Integer[kept.size()];
            for (int i = 0; i < order.length; i++) {
                order[i] = i;
            }
            Comparator<Integer> cmp = lock
                ? Comparator.comparingDouble((Integer i) -> scores.get(i).floor()).reversed()
                : Comparator.comparingDouble((Integer i) -> scores.get(i).pProfit)
                    .thenComparingDouble(i -> scores.get(i).yield()).reversed();
            Arrays.sort(order, cmp);
            ObjectNode found = out.putObject("search");
            found.put("subsets_scored", count);
            found.put("kept", kept.size());
            found.put("rule", lock
                ? "subsets of 2.." + search + " legs that lose in no scenario"
                    + (minYield == null ? "" : " with floor >= " + minYield) + ", by floor"
                : "subsets of 2.." + search + " legs spanning at least two events"
                    + (minYield == null ? "" : " with yield >= " + minYield)
                    + ", by P(profit) then yield");
            ArrayNode best = found.putArray("best");
            Iterator<Integer> it = Arrays.asList(order).iterator();
            for (int shown = 0; shown < top && it.hasNext(); shown++) {
                int i = it.next();
                best.add(scoreJson(kept.get(i), legs, scores.get(i), outcomes));
            }
        }
        return out;
    }
}
