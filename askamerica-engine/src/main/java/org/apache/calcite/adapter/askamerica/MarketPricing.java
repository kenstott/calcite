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
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Year;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        /** A range read from a venue label shares this end with the next range of its event,
         *  and the label does not say which of the two holds the shared value. */
        final boolean lowUnsure;
        final boolean highUnsure;

        Condition(String kind, double low, double high) {
            this(kind, low, high, false, false);
        }

        Condition(String kind, double low, double high, boolean lowUnsure,
                boolean highUnsure) {
            if (high < low) {
                throw new IllegalArgumentException(kind + " needs low <= high, got [" + low
                    + ", " + high + "]");
            }
            this.kind = kind;
            this.low = low;
            this.high = high;
            this.lowUnsure = lowUnsure;
            this.highUnsure = highUnsure;
        }

        private static final String NUM =
            "([-\u2212+]?)\\$?(\\d[\\d,]*(?:\\.\\d+)?|\\.\\d+)\\s*([kKmMbB]?)\\s*%?";
        private static final Pattern LABEL_ABOVE = Pattern.compile(
            "(?:above|over|more than|greater than|>)\\s*" + NUM, Pattern.CASE_INSENSITIVE);
        private static final Pattern LABEL_BELOW = Pattern.compile(
            "(?:below|under|less than|<)\\s*" + NUM, Pattern.CASE_INSENSITIVE);
        private static final Pattern LABEL_AT_LEAST = Pattern.compile(
            "(?:\u2265|>=|at least)\\s*" + NUM, Pattern.CASE_INSENSITIVE);
        private static final Pattern LABEL_AT_MOST = Pattern.compile(
            "(?:\u2264|<=|at most)\\s*" + NUM, Pattern.CASE_INSENSITIVE);
        private static final Pattern LABEL_RANGE = Pattern.compile(
            NUM + "\\s*(?:\u2013|\u2014|-|to)\\s*" + NUM, Pattern.CASE_INSENSITIVE);
        private static final Pattern LABEL_EXACT = Pattern.compile(NUM);

        private static double labelNumber(Matcher m, int group) {
            double v = Double.parseDouble(m.group(group + 1).replace(",", ""));
            switch (m.group(group + 2).toLowerCase(Locale.ROOT)) {
                case "k": v *= 1e3; break;
                case "m": v *= 1e6; break;
                case "b": v *= 1e9; break;
                default: break;
            }
            String sign = m.group(group);
            return "-".equals(sign) || "\u2212".equals(sign) ? -v : v;
        }

        /**
         * The condition a venue's outcome label states, or null when the label is not one of
         * the forms read: "Above 4.5%", "&lt;0.5%", "&ge;3.0%", "&le;0.0%", "0.5&ndash;1.0%"
         * (both ends included) and a bare "0.3%" (exactly that value). A label of any other
         * form ("25 bps decrease", "No change") states no number this can price;
         * {@link Decision} reads those of an FOMC decision.
         */
        static Condition ofLabel(String label) {
            if (label == null) {
                return null;
            }
            String text = label.trim();
            Matcher m = LABEL_AT_LEAST.matcher(text);
            if (m.matches()) {
                double v = labelNumber(m, 1);
                return new Condition("at_least", v, v);
            }
            m = LABEL_AT_MOST.matcher(text);
            if (m.matches()) {
                double v = labelNumber(m, 1);
                return new Condition("at_most", v, v);
            }
            m = LABEL_ABOVE.matcher(text);
            if (m.matches()) {
                double v = labelNumber(m, 1);
                return new Condition("above", v, v);
            }
            m = LABEL_BELOW.matcher(text);
            if (m.matches()) {
                double v = labelNumber(m, 1);
                return new Condition("below", v, v);
            }
            m = LABEL_EXACT.matcher(text);
            if (m.matches()) {
                double v = labelNumber(m, 1);
                return new Condition("between", v, v);
            }
            m = LABEL_RANGE.matcher(text);
            if (m.matches() && m.group(3).equalsIgnoreCase(m.group(6))) {
                double low = labelNumber(m, 1);
                double high = labelNumber(m, 4);
                return low < high ? new Condition("between", low, high) : null;
            }
            return null;
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

        /**
         * Whether a contract on {@code side} is sure to pay at {@code v}. At an end a label
         * left unsure neither side is: a lock must not rest on a value two ranges both claim.
         */
        boolean wins(String side, double v) {
            if (lowUnsure && v == low || highUnsure && v == high) {
                return false;
            }
            return "yes".equals(side) == holds(v);
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
        /** Decimals the values were rounded to, or null when they were not rounded. */
        final Integer places;

        Forecast(double[] values, boolean sampled, String form) {
            this(values, sampled, form, null);
        }

        private Forecast(double[] values, boolean sampled, String form, Integer places) {
            this.values = values;
            this.sampled = sampled;
            this.form = form;
            this.places = places;
        }

        /** The forecast rounded to {@code places} decimals, as a settlement source publishes
         *  it. Rounds the exact binary value half-even. */
        Forecast rounded(int places) {
            double[] out = new double[values.length];
            for (int i = 0; i < values.length; i++) {
                out[i] = new BigDecimal(values[i]).setScale(places, RoundingMode.HALF_EVEN)
                    .doubleValue();
            }
            return new Forecast(out, sampled, form + ", rounded to " + places + " decimals",
                places);
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
        PredictionMarkets.Event event;
        ObjectNode json;
        List<Leg> legs = new ArrayList<>();
    }

    private static Double rounded(Double v, int places) {
        return v == null ? null : PredictionMarkets.round(v, places);
    }

    /**
     * The conditions an event's outcome labels state, by market id, for markets whose venue
     * gives no strike. Two ranges that share an end ("0.5&ndash;1.0%" and "1.0&ndash;1.5%")
     * are each marked unsure at it.
     */
    static Map<String, Condition> labelConditions(PredictionMarkets.Event event) {
        Map<String, Condition> read = new LinkedHashMap<>();
        for (PredictionMarkets.Market m : event.legs) {
            if (Condition.ofStrike(m) == null) {
                Condition c = Condition.ofLabel(m.label);
                if (c != null) {
                    read.put(m.marketId, c);
                }
            }
        }
        Map<String, Condition> out = new LinkedHashMap<>();
        for (Map.Entry<String, Condition> e : read.entrySet()) {
            Condition c = e.getValue();
            boolean low = false;
            boolean high = false;
            if ("between".equals(c.kind) && c.low < c.high) {
                for (Condition o : read.values()) {
                    if (o != c && "between".equals(o.kind) && o.low < o.high) {
                        low |= o.high == c.low;
                        high |= o.low == c.high;
                    }
                }
            }
            out.put(e.getKey(), low || high
                ? new Condition(c.kind, c.low, c.high, low, high) : c);
        }
        return out;
    }

    /**
     * The decision of one FOMC meeting that an event's markets are the outcomes of. Its
     * conditions are on the change of the target rate in basis points: a cut is negative.
     */
    static final class Decision {
        /** The change is taken to be a multiple of this many basis points. */
        static final double STEP = 25;
        private static final Pattern BY = Pattern.compile(
            "\\b(hike|cut) rates by (>\\s*)?(\\d+)\\s*bps\\b", Pattern.CASE_INSENSITIVE);
        private static final Pattern MOVE = Pattern.compile(
            "(\\d+)(\\+?)\\s*bps (increase|decrease)", Pattern.CASE_INSENSITIVE);
        private static final Pattern MEETING = Pattern.compile(
            "\\b(january|february|march|april|may|june|july|august|september|october|november"
            + "|december) (\\d{4}) meeting\\b", Pattern.CASE_INSENSITIVE);

        /** Conditions by market id. */
        final Map<String, Condition> conditions;
        /** The meeting every market names, e.g. "December 2026". */
        final String meeting;

        private Decision(Map<String, Condition> conditions, String meeting) {
            this.conditions = conditions;
            this.meeting = meeting;
        }

        /**
         * The change one outcome states, or null when it states none: Kalshi's "Hike rates by
         * 25bps" and "Cut rates by &gt;25bps" in a market title, Polymarket's "No change",
         * "25 bps decrease" and "50+ bps increase" as an outcome label.
         */
        static Condition outcome(PredictionMarkets.Market m) {
            double size;
            boolean up;
            boolean open;
            boolean inclusive;
            if (m.label != null) {
                String label = m.label.trim();
                if (label.equalsIgnoreCase("no change")) {
                    return new Condition("between", 0, 0);
                }
                Matcher move = MOVE.matcher(label);
                if (!move.matches()) {
                    return null;
                }
                size = Double.parseDouble(move.group(1));
                up = move.group(3).equalsIgnoreCase("increase");
                open = !move.group(2).isEmpty();
                inclusive = true;
            } else {
                Matcher by = BY.matcher(m.title == null ? "" : m.title);
                if (!by.find()) {
                    return null;
                }
                size = Double.parseDouble(by.group(3));
                up = by.group(1).equalsIgnoreCase("hike");
                open = by.group(2) != null;
                inclusive = false;
            }
            if (size % STEP != 0) {
                return null;
            }
            double v = up || size == 0 ? size : -size;
            if (!open) {
                return new Condition("between", v, v);
            }
            if (inclusive) {
                return new Condition(up ? "at_least" : "at_most", v, v);
            }
            return new Condition(up ? "above" : "below", v, v);
        }

        /**
         * The decision {@code event} is on, or null when it is not one: its driver is the
         * policy rate, at least two of its markets state a change, and every market that
         * states one names the same meeting.
         */
        static Decision of(PredictionMarkets.Event event) {
            if (event.driver == null || !"policy_rate".equals(event.driver.name)) {
                return null;
            }
            Map<String, Condition> read = new LinkedHashMap<>();
            Set<String> meetings = new TreeSet<>();
            for (PredictionMarkets.Market m : event.legs) {
                Condition c = outcome(m);
                if (c == null) {
                    continue;
                }
                Matcher named = MEETING.matcher(m.title == null ? "" : m.title);
                if (!named.find()) {
                    return null;
                }
                String month = named.group(1).toLowerCase(Locale.ROOT);
                meetings.add(Character.toUpperCase(month.charAt(0)) + month.substring(1) + " "
                    + named.group(2));
                read.put(m.marketId, c);
            }
            if (read.size() < 2 || meetings.size() != 1) {
                return null;
            }
            return new Decision(read, meetings.iterator().next());
        }
    }

    /**
     * One day's highest or lowest temperature at one place, which an event's markets are the
     * outcomes of: Kalshi's "Highest temperature in Los Angeles on Oct 2, 2026?" and
     * Polymarket's "Highest temperature in Los Angeles on October 2?". Its conditions are in
     * whole degrees.
     */
    static final class DailyExtreme {
        /** Both venues' sources report whole degrees. */
        static final double STEP = 1;
        private static final Pattern TITLE = Pattern.compile(
            "\\b(highest|lowest) temperature in (.+?) on ([a-z]{3,9})\\.? (\\d{1,2})"
            + "(?:, (\\d{4}))?(?!\\d)", Pattern.CASE_INSENSITIVE);
        private static final List<String> MONTHS = Arrays.asList("jan", "feb", "mar", "apr",
            "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec");
        private static final Pattern CLIMATE_REPORT = Pattern.compile("\\(CLI([A-Z]{3})\\)");
        private static final Pattern TIME_SERIES = Pattern.compile(
            "weather\\.gov/\\S*[?&]site=([A-Za-z]{4})\\b");
        private static final Pattern UNDERGROUND = Pattern.compile(
            "wunderground\\.com/history/daily/\\S*/([A-Z]{4})\\b");
        private static final Pattern UNIT = Pattern.compile("\\b(fahrenheit|celsius)\\b",
            Pattern.CASE_INSENSITIVE);
        private static final String DEGREES = "(-?\\d+)\\s*°\\s*[FC]?";
        private static final Pattern OR_HIGHER = Pattern.compile(
            DEGREES + "\\s+or\\s+(?:higher|above)", Pattern.CASE_INSENSITIVE);
        private static final Pattern OR_BELOW = Pattern.compile(
            DEGREES + "\\s+or\\s+(?:below|lower)", Pattern.CASE_INSENSITIVE);
        private static final Pattern RANGE = Pattern.compile(
            "(?<![\\d-])(-?\\d+)\\s*-\\s*" + DEGREES);
        private static final Pattern EXACT = Pattern.compile("(?<![\\d-])" + DEGREES);

        /** "highest" or "lowest". */
        final String kind;
        /** The place the title names, lower case, e.g. "los angeles". */
        final String place;
        final LocalDate day;
        /** The station the rules name without a leading K, e.g. "LAX"; null when they name
         *  none. */
        final String station;
        /** What the rules read the temperature from; null when {@link #station} is. */
        final String measured;
        /** "fahrenheit" or "celsius"; null when the rules state neither. */
        final String unit;
        /** Conditions by market id, for the markets whose venue gives no strike. */
        final Map<String, Condition> conditions;

        private DailyExtreme(String kind, String place, LocalDate day, String station,
                String measured, String unit, Map<String, Condition> conditions) {
            this.kind = kind;
            this.place = place;
            this.day = day;
            this.station = station;
            this.measured = measured;
            this.unit = unit;
            this.conditions = conditions;
        }

        /**
         * The temperature one outcome states, or null when it states none: "86-87°F"
         * (both ends included), "94°F or higher", "75°F or below" and a bare
         * "23°C".
         */
        static Condition outcome(String text) {
            if (text == null) {
                return null;
            }
            Matcher m = OR_HIGHER.matcher(text);
            if (m.find()) {
                double v = Double.parseDouble(m.group(1));
                return new Condition("at_least", v, v);
            }
            m = OR_BELOW.matcher(text);
            if (m.find()) {
                double v = Double.parseDouble(m.group(1));
                return new Condition("at_most", v, v);
            }
            m = RANGE.matcher(text);
            if (m.find()) {
                double low = Double.parseDouble(m.group(1));
                double high = Double.parseDouble(m.group(2));
                return low < high ? new Condition("between", low, high) : null;
            }
            m = EXACT.matcher(text);
            if (m.find()) {
                double v = Double.parseDouble(m.group(1));
                return new Condition("between", v, v);
            }
            return null;
        }

        /**
         * The daily extreme {@code event} is on, or null when it is not one: its driver is
         * temperature and its title names the highest or lowest temperature in one place on
         * one day. A title without a year takes the year that puts the day nearest the close.
         */
        static DailyExtreme of(PredictionMarkets.Event event) {
            if (event.driver == null || !"temperature".equals(event.driver.name)
                    || event.eventTitle == null) {
                return null;
            }
            Matcher t = TITLE.matcher(event.eventTitle);
            if (!t.find() || t.group(3).length() < 3) {
                return null;
            }
            int month = MONTHS.indexOf(t.group(3).substring(0, 3).toLowerCase(Locale.ROOT)) + 1;
            int dayOfMonth = Integer.parseInt(t.group(4));
            if (month == 0) {
                return null;
            }
            LocalDate day;
            try {
                if (t.group(5) != null) {
                    day = LocalDate.of(Integer.parseInt(t.group(5)), month, dayOfMonth);
                } else {
                    LocalDate close = LocalDate.parse(event.closeTime.substring(0, 10));
                    day = null;
                    for (int y = close.getYear() - 1; y <= close.getYear() + 1; y++) {
                        if (dayOfMonth == 29 && month == 2 && !Year.isLeap(y)) {
                            continue;
                        }
                        LocalDate d = LocalDate.of(y, month, dayOfMonth);
                        if (day == null || Math.abs(ChronoUnit.DAYS.between(close, d))
                                < Math.abs(ChronoUnit.DAYS.between(close, day))) {
                            day = d;
                        }
                    }
                }
            } catch (DateTimeException e) {
                return null;
            }
            if (day == null) {
                return null;
            }
            String place = t.group(2).trim().toLowerCase(Locale.ROOT);
            if ("nyc".equals(place)) {
                place = "new york city";
            }
            StringBuilder text = new StringBuilder(event.rules == null ? "" : event.rules);
            if (event.settlementSources != null) {
                for (String s : event.settlementSources) {
                    text.append(' ').append(s);
                }
            }
            String station = null;
            String measured = null;
            Matcher report = CLIMATE_REPORT.matcher(text);
            Matcher series = TIME_SERIES.matcher(text);
            Matcher history = UNDERGROUND.matcher(text);
            if (report.find()) {
                station = report.group(1);
                measured = "the daily climate report CLI" + station
                    + (text.toString().toLowerCase(Locale.ROOT).contains("weather company")
                        ? " as The Weather Company reports it" : "");
            } else if (series.find()) {
                String id = series.group(1).toUpperCase(Locale.ROOT);
                station = id.startsWith("K") ? id.substring(1) : id;
                measured = "the " + t.group(1).toLowerCase(Locale.ROOT) + " reading of the "
                    + "weather.gov time series at " + id;
            } else if (history.find()) {
                String id = history.group(1);
                station = id.startsWith("K") ? id.substring(1) : id;
                measured = "the Weather Underground daily history at " + id;
            }
            Matcher u = UNIT.matcher(text);
            String unit = u.find() ? u.group(1).toLowerCase(Locale.ROOT) : null;
            Map<String, Condition> read = new LinkedHashMap<>();
            for (PredictionMarkets.Market m : event.legs) {
                if (Condition.ofStrike(m) != null) {
                    continue;
                }
                Condition c = outcome(m.label);
                if (c == null) {
                    c = outcome(m.title);
                }
                if (c != null) {
                    read.put(m.marketId, c);
                }
            }
            return new DailyExtreme(t.group(1).toLowerCase(Locale.ROOT), place, day, station,
                measured, unit, read);
        }
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
        return priceEvent(live, forecast, given, minEdge, feeRate, bothSides, column, null);
    }

    /**
     * As the overload without {@code size}, and with {@code size} non-null each priced row also
     * carries days to settlement, annualized return, breakeven fair values and, for a mispriced
     * row, an order ticket and its depth. Rows and legs are otherwise those of the plain call.
     */
    static Priced priceEvent(PredictionMarkets.LiveEvent live, Forecast forecast,
            Map<String, Condition> given, double minEdge, Double feeRate, boolean bothSides,
            String column, SizeOptions size) {
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
            if (size != null) {
                addSizeFields(row, m, rate, offers, best, fair, "mispriced".equals(verdict),
                    minEdge, size);
            }
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
            + "the cent, Kalshi event-level fee overrides, the depth behind the quote"
            + (size == null ? "." : " (the depth is walked only where an order book was "
            + "supplied; see each ticketed row's depth)."));
        if (size != null) {
            json.put("quote_time", size.now.toString());
            json.put("annualized_return_convention", ANNUALIZED_CONVENTION);
        }
        json.put("min_edge", minEdge);
        json.put("mispriced_markets", mispricedCount);
        ArrayNode legArr = json.putArray("legs");
        for (Leg l : out.legs) {
            legArr.add(l.toJson());
        }
        json.put("legs_basis", bothSides ? "every quoted side"
            : "the better side of each market with edge >= " + minEdge);
        out.json = json;
        out.event = event;
        return out;
    }

    /**
     * The event's forecast-free locks net of fees: ladder pairs that still lock after fees,
     * and its bucket partition. A partition that is not a lock is kept without its legs, so
     * the reason exhaustiveness was not established stays visible.
     *
     * @param feeRate the rate for every market, or null to use the rates read from the venue
     * @param step the grid settlement values fall on, or null when no rounding is known
     * @param given conditions the caller stated, by market id
     */
    static ArrayNode structuralLocks(PredictionMarkets.LiveEvent live, Double feeRate,
            Double step, Map<String, Condition> given) {
        PredictionMarkets.Event event = live.event;
        Map<String, Double> rates = new LinkedHashMap<>(live.feeRates);
        if (feeRate != null) {
            for (PredictionMarkets.Market m : event.legs) {
                rates.put(m.marketId, feeRate);
            }
        }
        ArrayNode out = MAPPER.createArrayNode();
        for (ObjectNode l : MarketBaskets.ladderLocks(event, rates, given)) {
            if (l.get("lock_after_fees").asBoolean()) {
                out.add(l);
            }
        }
        for (ObjectNode p : MarketBaskets.partitionLocks(event, rates, step, given)) {
            if (!p.get("lock").asBoolean()) {
                for (String side : new String[]{"buy_all_yes", "buy_all_no"}) {
                    if (p.hasNonNull(side)) {
                        ((ObjectNode) p.get(side)).remove("legs");
                    }
                }
            }
            out.add(p);
        }
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

    // ─── Size, tickets and re-quotes ───────────────────────────────────────────

    /** Price grid of both venues' standard markets, in dollars. */
    static final double TICK = 0.01;
    static final String ANNUALIZED_CONVENTION = "return_on_cost * 365 / days_to_settlement, "
        + "simple (not compounded); omitted when settlement is under one day away";

    /** What the size-aware pricing needs from the caller; the pricing code fetches nothing. */
    static final class SizeOptions {
        /** The time of the quotes; days to settlement and the ticket's quote time. */
        final Instant now;
        /** Order books by market_id, or null when none were read. */
        final Map<String, MarketHistory.OrderBook> books;
        /** Contracts to walk the book for, or null. */
        final Double size;

        SizeOptions(Instant now, Map<String, MarketHistory.OrderBook> books, Double size) {
            if (now == null) {
                throw new IllegalArgumentException("the quote time is required");
            }
            if (size != null && !(size > 0)) {
                throw new IllegalArgumentException("size must be above 0 contracts, got " + size);
            }
            this.now = now;
            this.books = books;
            this.size = size;
        }
    }

    /**
     * The highest price on a side's own contract at which the edge still reaches
     * {@code minEdge}, given the side's win probability. Solves
     * {@code wins - p - rate * p * (1 - p) >= minEdge} for the larger p; the left side falls as p
     * rises. Returns 0 when no price qualifies.
     */
    static double maxPrice(double wins, double minEdge, double rate) {
        double g = wins - minEdge;
        if (!(g > 0)) {
            return 0;
        }
        double disc = Math.max(0, (1 + rate) * (1 + rate) - 4 * rate * g);
        // The smaller root of rate * p^2 - (1 + rate) * p + g = 0, rationalized so that a
        // zero rate gives g.
        return 2 * g / ((1 + rate) + Math.sqrt(disc));
    }

    private static double floorToTick(double price) {
        return PredictionMarkets.round(Math.floor(price / TICK + 1e-9) * TICK, 4);
    }

    private static MarketHistory.Side buySide(String side) {
        if ("yes".equals(side)) {
            return MarketHistory.Side.BUY_YES;
        }
        if ("no".equals(side)) {
            return MarketHistory.Side.BUY_NO;
        }
        throw new IllegalArgumentException("side must be yes or no, got '" + side + "'");
    }

    /** Best price on the side's own contract, or null when the book has nothing on it. */
    private static Double bestPrice(MarketHistory.OrderBook book, String side) {
        if ("yes".equals(side)) {
            MarketHistory.Level a = book.bestAsk();
            return a == null ? null : a.price;
        }
        MarketHistory.Level b = book.bestBid();
        return b == null ? null : PredictionMarkets.round(1 - b.price, 6);
    }

    private static double winProbability(String side, double fair) {
        return "yes".equals(side) ? fair : 1 - fair;
    }

    private static void addSizeFields(ObjectNode row, PredictionMarkets.Market m, double rate,
            List<Leg> offers, Leg best, Double fair, boolean mispriced, double minEdge,
            SizeOptions opts) {
        // Days to settlement and annualized return.
        if (m.closeTime == null) {
            row.putNull("days_to_settlement");
            row.put("annualized_return_note", "the venue gave no close time for this market");
        } else {
            double days = Duration.between(opts.now, PredictionMarkets.closeInstant(m.closeTime))
                .toMillis() / 86400000.0;
            row.put("days_to_settlement", PredictionMarkets.round(days, 3));
            if (best != null) {
                if (days < 1) {
                    row.put("annualized_return_note",
                        "settlement is under one day away (or past): not annualized");
                } else {
                    row.put("annualized_return_simple_365d", PredictionMarkets.round(
                        best.edge / (best.price + best.fee) * 365 / days, 4));
                }
            }
        }
        // Breakeven fair value (the probability of YES) per side, at the quote and its fee.
        Double beYes = null;
        Double beNo = null;
        for (Leg o : offers) {
            if ("yes".equals(o.side)) {
                beYes = o.price + o.fee;
            } else {
                beNo = 1 - (o.price + o.fee);
            }
        }
        PredictionMarkets.putNumber(row, "breakeven_fair_buy_yes", rounded(beYes, 4));
        PredictionMarkets.putNumber(row, "breakeven_fair_buy_no", rounded(beNo, 4));
        if (best == null) {
            row.putNull("breakeven_fair");
        } else {
            row.put("breakeven_fair", PredictionMarkets.round(
                "yes".equals(best.side) ? beYes : beNo, 4));
        }
        if (!mispriced) {
            return;
        }
        // The order ticket.
        double wins = winProbability(best.side, fair);
        double limit = floorToTick(maxPrice(wins, minEdge, rate));
        if (!(limit > 0)) {
            row.putNull("ticket");
            row.put("ticket_note", "no price on the " + best.side + " side clears min_edge "
                + minEdge + " on the " + TICK + " tick");
            return;
        }
        ObjectNode t = row.putObject("ticket");
        t.put("source", best.source);
        t.put("event_id", best.eventId);
        t.put("market_id", best.id);
        t.put("side", best.side);
        t.put("limit_price", limit);
        t.put("tick", TICK);
        t.put("fair", fair);
        t.put("fee_rate", rate);
        t.put("min_edge", minEdge);
        t.put("edge_at_limit", PredictionMarkets.round(
            wins - limit - PredictionMarkets.takerFee(rate, limit), 4));
        if (opts.size != null) {
            t.put("size", opts.size);
        }
        t.put("quote_time", opts.now.toString());
        t.put("limit_basis", "the highest price on the tick at which edge = win probability - "
            + "price - rate * price * (1 - price) is still at least min_edge");
        ArrayNode voids = t.putArray("void_conditions");
        if (m.closeTime != null) {
            ObjectNode v = voids.addObject();
            v.put("type", "settlement_close");
            v.put("time", m.closeTime);
        }
        // The book behind the ticket.
        ObjectNode depth = row.putObject("depth");
        MarketHistory.OrderBook book = opts.books == null ? null : opts.books.get(m.marketId);
        if (book == null) {
            depth.put("book_read", false);
            depth.put("note", "no order book was supplied for this market; the quote above is "
                + "top of book and its depth is unknown");
            return;
        }
        depth.put("book_read", true);
        MarketHistory.Side bs = buySide(best.side);
        MarketHistory.Fill atLimit = book.fill(bs, 1, limit);
        double available = atLimit.availableAtLimit;
        depth.put("contracts_at_or_under_limit", available);
        depth.put("dollars_at_or_under_limit", available > 0
            ? PredictionMarkets.round(book.fill(bs, available, limit).cost, 4) : 0.0);
        PredictionMarkets.putNumber(depth, "book_best_price", bestPrice(book, best.side));
        if (opts.size == null) {
            return;
        }
        MarketHistory.Fill all = book.fill(bs, opts.size, null);
        depth.put("size_requested", opts.size);
        depth.put("size_filled_by_book", all.filled);
        depth.put("fills_completely_under_limit", book.fill(bs, opts.size, limit).complete);
        if (all.averagePrice == null) {
            depth.putNull("fill_average_price");
            depth.put("fill_note", "the book has nothing on the " + best.side + " side");
            return;
        }
        double fee = PredictionMarkets.takerFee(rate, all.averagePrice);
        depth.put("fill_average_price", PredictionMarkets.round(all.averagePrice, 4));
        depth.put("fill_fee_per_contract", PredictionMarkets.round(fee, 5));
        depth.put("fill_cost_dollars", PredictionMarkets.round(all.cost, 4));
        depth.put("fill_fee_dollars", PredictionMarkets.round(fee * all.filled, 4));
        depth.put("fill_edge", PredictionMarkets.round(wins - all.averagePrice - fee, 4));
        depth.put("fill_basis", "walks the book from the best price with no limit; the fee is "
            + "taken per contract at the average fill price");
    }

    private static JsonNode need(JsonNode ticket, String field) {
        JsonNode v = ticket == null ? null : ticket.get(field);
        if (v == null || v.isNull()) {
            throw new IllegalArgumentException("the ticket has no '" + field + "'");
        }
        return v;
    }

    /**
     * Quotes a ticket against a fresh order book: no forecast, no fetch. Status is {@code open}
     * when the ticket's size (at least one contract when it has none) is available at or under
     * its limit, {@code partly_open} when some but not all is, {@code gone} when none is.
     */
    static ObjectNode requote(JsonNode ticket, MarketHistory.OrderBook book) {
        String marketId = need(ticket, "market_id").asText();
        String side = need(ticket, "side").asText();
        double limit = need(ticket, "limit_price").asDouble();
        double fair = need(ticket, "fair").asDouble();
        double rate = need(ticket, "fee_rate").asDouble();
        if (book.marketId != null && !book.marketId.equals(marketId)) {
            throw new IllegalArgumentException("the book is for market " + book.marketId
                + ", the ticket for " + marketId);
        }
        MarketHistory.Side bs = buySide(side);
        Double size = ticket.hasNonNull("size") ? ticket.get("size").asDouble() : null;
        double available = book.fill(bs, 1, limit).availableAtLimit;
        double wanted = size == null ? 1 : size;
        String status = available >= wanted - 1e-9 ? "open" : available > 0 ? "partly_open"
            : "gone";
        ObjectNode o = MAPPER.createObjectNode();
        o.put("market_id", marketId);
        o.put("side", side);
        o.put("status", status);
        o.put("limit_price", limit);
        PredictionMarkets.putNumber(o, "ticket_size", size);
        o.put("contracts_at_or_under_limit", available);
        if ("partly_open".equals(status)) {
            o.put("contracts_left", available);
        }
        Double best = bestPrice(book, side);
        PredictionMarkets.putNumber(o, "current_best_price", best);
        if (best == null) {
            o.putNull("edge_at_current_best");
            o.put("note", "the book has nothing on the " + side + " side");
        } else {
            o.put("edge_at_current_best", PredictionMarkets.round(
                winProbability(side, fair) - best - PredictionMarkets.takerFee(rate, best), 4));
        }
        o.put("edge_basis", "the ticket's fair value, unchanged; no forecast was rerun");
        return o;
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
        return grid(legs, null);
    }

    /**
     * As {@link #grid(List)}, and with {@code step} non-null the settlement value is taken to
     * fall on a multiple of it: the outcomes are every multiple from one step below the lowest
     * threshold to one step above the highest, and no value between two of them is scored.
     */
    static Scenarios grid(List<Leg> legs, Double step) {
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
        if (step != null) {
            for (Double c : cuts) {
                if (c % step != 0) {
                    throw new IllegalArgumentException("threshold " + c + " of '" + column
                        + "' is not a multiple of the step " + step);
                }
            }
            for (double v = cuts.first() - step; v <= cuts.last() + step; v += step) {
                points.add(v);
            }
        } else {
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
        }
        List<Map<String, Double>> rows = new ArrayList<>();
        for (Double p : points) {
            Map<String, Double> row = new LinkedHashMap<>();
            row.put(column, p);
            rows.add(row);
        }
        double[] w = new double[rows.size()];
        Arrays.fill(w, 1.0 / w.length);
        return new Scenarios(rows, w, "outcome grid over " + cuts.size() + " thresholds of '"
            + column + "': " + (step != null ? "every multiple of " + step + " from one below "
            + "the lowest to one above the highest"
            : "every threshold and one value in each interval between, below and above them")
            + ". Rows are outcomes, not probabilities — only floor, worst and best mean "
            + "anything.");
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
            li.put("title", l.title);
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
     * Baskets that are not locks but lose only inside a bounded band of outcomes: subsets of
     * 2..search legs, all settling on one quantity, that profit at every outcome below the
     * lowest threshold and above the highest. Each is scored against a forecast of that
     * quantity, and kept when the forecast puts at most {@code maxLoss} on a loss and its
     * expected profit is positive, and the quotes agree: the forecast puts every leg's chance
     * of winning within {@code maxGap} of its price and neither its winning nor its losing
     * at over {@code maxRatio} times what the price implies, and a venue's own quotes put at most
     * {@code maxLoss} on the losing band. A subset the forecast alone favours is a bet on the
     * forecast against the market, not on the basket, and is counted, not kept.
     *
     * @return the kept subsets by yield, at most {@code top}; {@code worst} and {@code best}
     *     are over every outcome the conditions can tell apart, {@code loses_between} the
     *     thresholds that enclose every losing outcome (null when the worst case breaks even)
     */
    static NearLocks nearLocks(List<Leg> legs, Forecast forecast, int search, int minVenues,
            double maxLoss, double maxGap, double maxRatio, int top) {
        Scenarios grid = grid(legs);
        String column = legs.get(0).column;
        TreeSet<Double> cuts = new TreeSet<>();
        for (Leg l : legs) {
            cuts.add(l.condition.low);
            cuts.add(l.condition.high);
        }
        int n = legs.size();
        // A value between two thresholds one rounding step apart is never published.
        double step = forecast.places == null ? 0 : Math.pow(10, -forecast.places);
        List<Double> reached = new ArrayList<>();
        for (Map<String, Double> row : grid.rows) {
            double v = row.get(column);
            Double below = cuts.lower(v);
            Double above = cuts.higher(v);
            if (cuts.contains(v) || below == null || above == null
                    || above - below > step + EPS) {
                reached.add(v);
            }
        }
        int points = reached.size();
        int draws = forecast.values.length;
        double[] at = new double[points];
        for (int g = 0; g < points; g++) {
            at[g] = reached.get(g);
        }
        boolean[][] onGrid = new boolean[n][points];
        boolean[][] onForecast = new boolean[n][draws];
        for (int i = 0; i < n; i++) {
            Leg l = legs.get(i);
            for (int g = 0; g < points; g++) {
                onGrid[i][g] = l.condition.wins(l.side, at[g]);
            }
            for (int d = 0; d < draws; d++) {
                onForecast[i][d] = l.condition.wins(l.side, forecast.values[d]);
            }
        }
        return nearLocks(legs, onGrid, onForecast, null, null, (loses, first, last) -> {
            double low = cuts.floor(at[first]);
            double high = cuts.ceiling(at[last]);
            ObjectNode band = MAPPER.createObjectNode();
            band.put("low", low);
            band.put("low_included", at[first] == low);
            band.put("high", high);
            band.put("high_included", at[last] == high);
            return band;
        }, search, minVenues, maxLoss, maxGap, maxRatio, top);
    }

    /** The outcomes a basket loses at, as a near-lock reports them. */
    private interface Band {
        ObjectNode of(boolean[] loses, int first, int last);
    }

    /**
     * Two published numbers that all but fix each other: {@code second = intercept + slope *
     * (first - error)} before each is rounded, where the error is known only once both
     * print. The legs of {@code firstSource} settle on the first number, the rest on the
     * second.
     */
    static final class Joint {
        final String firstSource;
        final String first;
        final String second;
        /** Equally likely values of the first number, unrounded. */
        final double[] draws;
        /** Equally likely values of the error, taken as independent of the first number. */
        final double[] errors;
        final double intercept;
        final double slope;
        final int firstPlaces;
        final int secondPlaces;

        Joint(String firstSource, String first, String second, double[] draws,
                double[] errors, double intercept, double slope, int firstPlaces,
                int secondPlaces) {
            if (draws.length < 2 || errors.length < 1 || !(slope > 0)) {
                throw new IllegalArgumentException("a joint of two numbers needs at least 2 "
                    + "draws, 1 error and a positive slope");
            }
            this.firstSource = firstSource;
            this.first = first;
            this.second = second;
            this.draws = draws;
            this.errors = errors;
            this.intercept = intercept;
            this.slope = slope;
            this.firstPlaces = firstPlaces;
            this.secondPlaces = secondPlaces;
        }

        double second(double first, double error) {
            return intercept + slope * (first - error);
        }
    }

    private static double published(double v, int places) {
        return new BigDecimal(v).setScale(places, RoundingMode.HALF_EVEN).doubleValue();
    }

    /**
     * As {@link #nearLocks(List, Forecast, int, int, double, double, double, int)} for legs
     * that settle on two numbers a {@link Joint} ties together. An outcome is a value of the
     * first number and an error; the grid takes every pair of published numbers reachable
     * with the error at its least, at zero and at its most, and the forecast every draw of
     * the first number with every error. A basket that loses at no outcome of the grid is
     * kept too: the errors seen so far do not bound the next one, so it is not a lock.
     * {@code market_p_loss} is the most any venue's quotes put on the loss, each venue's
     * price of a value of its own number weighted by the share of the forecast's draws at
     * that value that lose.
     */
    static NearLocks nearLocks(List<Leg> legs, Joint joint, int search, int minVenues,
            double maxLoss, double maxGap, double maxRatio, int top) {
        int n = legs.size();
        double least = 0;
        double most = 0;
        for (double e : joint.errors) {
            least = Math.min(least, e);
            most = Math.max(most, e);
        }
        TreeSet<Double> levels = new TreeSet<>(Arrays.asList(least, 0.0, most));
        double lo = Double.POSITIVE_INFINITY;
        double hi = Double.NEGATIVE_INFINITY;
        for (Leg l : legs) {
            for (double c : new double[]{l.condition.low, l.condition.high}) {
                if (joint.firstSource.equals(l.source)) {
                    lo = Math.min(lo, c);
                    hi = Math.max(hi, c);
                } else {
                    lo = Math.min(lo, (c - joint.intercept) / joint.slope + least);
                    hi = Math.max(hi, (c - joint.intercept) / joint.slope + most);
                }
            }
        }
        double stepFirst = Math.pow(10, -joint.firstPlaces);
        double stepSecond = Math.pow(10, -joint.secondPlaces);
        // Beyond every threshold by more than a rounding step and any error.
        lo -= 5 * Math.max(stepFirst, stepSecond / joint.slope);
        hi += 5 * Math.max(stepFirst, stepSecond / joint.slope);
        // Where a published number changes: the first's rounding edges, and the second's
        // carried back to the first at each level of the error.
        TreeSet<Double> edges = new TreeSet<>();
        for (long j = (long) Math.floor(lo / stepFirst); (j + 0.5) * stepFirst < hi; j++) {
            if ((j + 0.5) * stepFirst > lo) {
                edges.add((j + 0.5) * stepFirst);
            }
        }
        for (double e : levels) {
            double from = joint.second(lo, e);
            double to = joint.second(hi, e);
            for (long k = (long) Math.floor(from / stepSecond); (k + 0.5) * stepSecond < to;
                    k++) {
                double edge = ((k + 0.5) * stepSecond - joint.intercept) / joint.slope + e;
                if (edge > lo && edge < hi) {
                    edges.add(edge);
                }
            }
        }
        List<Double> firsts = new ArrayList<>();
        double previous = lo;
        for (double edge : edges) {
            firsts.add((previous + edge) / 2);
            previous = edge;
        }
        firsts.add((previous + hi) / 2);
        int points = firsts.size() * levels.size();
        double[] firstAt = new double[points];
        double[] secondAt = new double[points];
        int g = 0;
        for (double first : firsts) {
            for (double e : levels) {
                firstAt[g] = published(first, joint.firstPlaces);
                secondAt[g++] = published(joint.second(first, e), joint.secondPlaces);
            }
        }
        int draws = joint.draws.length * joint.errors.length;
        boolean[][] onGrid = new boolean[n][points];
        boolean[][] onForecast = new boolean[n][draws];
        double[] firstDrawn = new double[draws];
        double[] secondDrawn = new double[draws];
        int d = 0;
        for (double v : joint.draws) {
            for (double e : joint.errors) {
                firstDrawn[d] = published(v, joint.firstPlaces);
                secondDrawn[d++] = published(joint.second(v, e), joint.secondPlaces);
            }
        }
        Map<String, double[]> own = new LinkedHashMap<>();
        Map<String, double[]> ownDrawn = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            Leg l = legs.get(i);
            boolean first = joint.firstSource.equals(l.source);
            own.put(l.source, first ? firstAt : secondAt);
            ownDrawn.put(l.source, first ? firstDrawn : secondDrawn);
            for (g = 0; g < points; g++) {
                onGrid[i][g] = l.condition.wins(l.side, first ? firstAt[g] : secondAt[g]);
            }
            for (d = 0; d < draws; d++) {
                onForecast[i][d] = l.condition.wins(l.side, first ? firstDrawn[d]
                    : secondDrawn[d]);
            }
        }
        return nearLocks(legs, onGrid, onForecast, own, ownDrawn, (loses, first, last) -> {
            double[] f = {Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
            double[] s = {Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
            for (int p = 0; p < loses.length; p++) {
                if (loses[p]) {
                    f[0] = Math.min(f[0], firstAt[p]);
                    f[1] = Math.max(f[1], firstAt[p]);
                    s[0] = Math.min(s[0], secondAt[p]);
                    s[1] = Math.max(s[1], secondAt[p]);
                }
            }
            ObjectNode band = MAPPER.createObjectNode();
            band.put("of", joint.second);
            band.put("low", s[0]);
            band.put("low_included", true);
            band.put("high", s[1]);
            band.put("high_included", true);
            ObjectNode and = band.putObject("and");
            and.put("of", joint.first);
            and.put("low", f[0]);
            and.put("low_included", true);
            and.put("high", f[1]);
            and.put("high_included", true);
            return band;
        }, search, minVenues, maxLoss, maxGap, maxRatio, top);
    }

    /**
     * The search both near-lock forms share.
     *
     * @param onGrid per leg, whether it wins at each outcome; the first outcome lies below
     *     every threshold and the last above every one
     * @param onForecast per leg, whether it wins at each equally likely draw
     * @param own null when every leg settles on one number; else per venue the number its
     *     legs settle on at each outcome, and a basket that loses at no outcome is kept
     * @param ownDrawn with {@code own}, per venue the number its legs settle on at each draw
     */
    private static NearLocks nearLocks(List<Leg> legs, boolean[][] onGrid,
            boolean[][] onForecast, Map<String, double[]> own,
            Map<String, double[]> ownDrawn, Band bandOf, int search,
            int minVenues, double maxLoss, double maxGap, double maxRatio, int top) {
        int n = legs.size();
        int points = onGrid[0].length;
        int draws = onForecast[0].length;
        double[] even = new double[points];
        Arrays.fill(even, 1.0 / points);
        double[] weights = new double[draws];
        Arrays.fill(weights, 1.0 / draws);

        double[] pWin = new double[n];
        for (int i = 0; i < n; i++) {
            int won = 0;
            for (int d = 0; d < draws; d++) {
                won += onForecast[i][d] ? 1 : 0;
            }
            pWin[i] = (double) won / draws;
        }

        // What each venue's quotes say: where YES wins on a market quoted on both sides, at
        // the middle of its two asks, and where it does not, at the rest.
        Map<String, int[]> sides = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            Leg l = legs.get(i);
            int[] pair = sides.computeIfAbsent(l.source + "\n" + l.id, k -> new int[] {-1, -1});
            pair["yes".equals(l.side) ? 0 : 1] = i;
        }
        Map<String, List<Quoted>> quoted = new LinkedHashMap<>();
        for (int[] pair : sides.values()) {
            if (pair[0] < 0 || pair[1] < 0) {
                continue;
            }
            double mid = (legs.get(pair[0]).price + 1 - legs.get(pair[1]).price) / 2;
            List<Quoted> of = quoted.computeIfAbsent(legs.get(pair[0]).source,
                k -> new ArrayList<>());
            of.add(new Quoted(onGrid[pair[0]], mid, true));
            boolean[] rest = new boolean[points];
            for (int g = 0; g < points; g++) {
                rest[g] = !onGrid[pair[0]][g];
            }
            of.add(new Quoted(rest, 1 - mid, false));
        }

        List<ObjectNode> kept = new ArrayList<>();
        int overGap = 0;
        int bandLikely = 0;
        int bandUnpriced = 0;
        for (int size = 2; size <= Math.min(search, n); size++) {
            int[] idx = new int[size];
            for (int i = 0; i < size; i++) {
                idx[i] = i;
            }
            while (true) {
                Set<String> venues = new HashSet<>();
                double gap = 0;
                double ratio = 0;
                for (int i : idx) {
                    double price = legs.get(i).price;
                    venues.add(legs.get(i).source);
                    gap = Math.max(gap, Math.abs(pWin[i] - price));
                    // A cheap leg the forecast calls likely is a small gap and a large ratio.
                    ratio = Math.max(ratio, Math.max(pWin[i] / price,
                        (1 - pWin[i]) / (1 - price)));
                }
                if (venues.size() >= minVenues) {
                    Score outcome = score(idx, legs, onGrid, even);
                    int first = -1;
                    int last = -1;
                    boolean[] loses = new boolean[points];
                    for (int g = 0; g < points; g++) {
                        int won = 0;
                        for (int i : idx) {
                            won += onGrid[i][g] ? 1 : 0;
                        }
                        if (won - outcome.cost < -EPS) {
                            first = first < 0 ? g : first;
                            last = g;
                            loses[g] = true;
                        }
                    }
                    // Not a lock, and a profit in both tails: index 0 lies below every
                    // threshold and the last index above every one.
                    boolean tails = first != 0 && last != points - 1
                        && onTail(idx, onGrid, 0) - outcome.cost > EPS
                        && onTail(idx, onGrid, points - 1) - outcome.cost > EPS;
                    if (tails && (own != null || outcome.worst <= EPS)) {
                        Score s = score(idx, legs, onForecast, weights);
                        boolean scored = s.pLoss <= maxLoss + EPS && s.expected > EPS;
                        Double quotedLoss = null;
                        if (scored && first < 0 && s.pLoss <= EPS) {
                            quotedLoss = 0.0;
                        } else if (scored && own == null) {
                            for (List<Quoted> venue : quoted.values()) {
                                Double q = quotedOn(venue, loses);
                                if (q != null && (quotedLoss == null || q > quotedLoss)) {
                                    quotedLoss = q;
                                }
                            }
                        } else if (scored) {
                            boolean[] lost = new boolean[draws];
                            for (int d = 0; d < draws; d++) {
                                int won = 0;
                                for (int i : idx) {
                                    won += onForecast[i][d] ? 1 : 0;
                                }
                                lost[d] = won - outcome.cost < -EPS;
                            }
                            for (Map.Entry<String, List<Quoted>> venue : quoted.entrySet()) {
                                Double q = quotedLoss(venue.getValue(), loses, lost,
                                    own.get(venue.getKey()), ownDrawn.get(venue.getKey()));
                                if (q != null && (quotedLoss == null || q > quotedLoss)) {
                                    quotedLoss = q;
                                }
                            }
                        }
                        if (!scored) {
                            // Not a near-lock on the forecast.
                        } else if (gap > maxGap + EPS || ratio > maxRatio + EPS) {
                            overGap++;
                        } else if (quotedLoss == null) {
                            bandUnpriced++;
                        } else if (quotedLoss > maxLoss + EPS) {
                            bandLikely++;
                        } else {
                            ObjectNode o = scoreJson(idx, legs, s, false);
                            o.put("market_p_loss", PredictionMarkets.round(quotedLoss, 4));
                            for (int i = 0; i < idx.length; i++) {
                                ((ObjectNode) o.get("legs").get(i)).put("forecast_p_win",
                                    PredictionMarkets.round(pWin[idx[i]], 4));
                            }
                            o.put("max_quote_gap", PredictionMarkets.round(gap, 4));
                            o.put("max_quote_ratio", PredictionMarkets.round(ratio, 4));
                            o.put("floor", PredictionMarkets.round(outcome.floor(), 4));
                            o.put("worst", PredictionMarkets.round(outcome.worst, 5));
                            o.put("best", PredictionMarkets.round(outcome.best, 5));
                            if (first < 0) {
                                o.putNull("loses_between");
                            } else {
                                o.set("loses_between", bandOf.of(loses, first, last));
                            }
                            kept.add(o);
                        }
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
        kept.sort(Comparator.comparingDouble(
            (ObjectNode o) -> o.get("yield").asDouble()).reversed());
        ArrayNode out = MAPPER.createArrayNode();
        for (int i = 0; i < kept.size() && i < top; i++) {
            out.add(kept.get(i));
        }
        return new NearLocks(out, overGap, bandLikely, bandUnpriced);
    }

    /**
     * What one venue's quotes put on a loss that takes two numbers to tell. The venue's
     * quotes split its own number into cells, the values no quote of it tells apart; for
     * each cell the loss can come with, the venue's price of the cell times the share of
     * the forecast's draws in the cell that lose. A cell the forecast never draws counts in
     * full. Null when the venue does not price one of the cells, or a draw that loses lies
     * off the grid.
     */
    private static Double quotedLoss(List<Quoted> venue, boolean[] loses, boolean[] lost,
            double[] own, double[] ownDrawn) {
        int points = own.length;
        Map<Double, String> cellOf = new HashMap<>();
        Map<String, boolean[]> cells = new LinkedHashMap<>();
        for (int g = 0; g < points; g++) {
            StringBuilder cell = new StringBuilder();
            for (Quoted q : venue) {
                cell.append(q.at[g] ? '1' : '0');
            }
            String key = cell.toString();
            cellOf.put(own[g], key);
            cells.computeIfAbsent(key, k -> new boolean[points])[g] = true;
        }
        Map<String, int[]> drawn = new HashMap<>();
        Set<String> losing = new TreeSet<>();
        for (int g = 0; g < points; g++) {
            if (loses[g]) {
                losing.add(cellOf.get(own[g]));
            }
        }
        for (int d = 0; d < lost.length; d++) {
            String cell = cellOf.get(ownDrawn[d]);
            if (cell == null) {
                if (lost[d]) {
                    return null;
                }
                continue;
            }
            int[] count = drawn.computeIfAbsent(cell, k -> new int[2]);
            count[0]++;
            if (lost[d]) {
                count[1]++;
                losing.add(cell);
            }
        }
        double sum = 0;
        for (String cell : losing) {
            Double q = quotedOn(venue, cells.get(cell));
            if (q == null) {
                return null;
            }
            int[] count = drawn.get(cell);
            sum += q * (count == null ? 1 : (double) count[1] / count[0]);
        }
        return Math.min(1, sum);
    }

    /** A set of outcomes one venue's quotes put a probability on. */
    private static final class Quoted {
        final boolean[] at;
        final double p;
        /** True where a market's YES wins, false for the rest of the outcomes. */
        final boolean yes;

        Quoted(boolean[] at, double p, boolean yes) {
            this.at = at;
            this.p = p;
            this.yes = yes;
        }
    }

    /**
     * The probability one venue's quotes put on a set of outcomes: a market that wins exactly
     * there, one such set less another inside it, or ranges that add up to it. Null when the
     * venue's markets do not single the set out.
     */
    private static Double quotedOn(List<Quoted> venue, boolean[] set) {
        int points = set.length;
        for (Quoted q : venue) {
            if (Arrays.equals(q.at, set)) {
                return Math.max(0, Math.min(1, q.p));
            }
        }
        for (Quoted outer : venue) {
            for (Quoted inner : venue) {
                boolean match = true;
                for (int g = 0; g < points && match; g++) {
                    match = (!inner.at[g] || outer.at[g])
                        && set[g] == (outer.at[g] && !inner.at[g]);
                }
                if (match) {
                    return Math.max(0, Math.min(1, outer.p - inner.p));
                }
            }
        }
        int[] covered = new int[points];
        double sum = 0;
        for (Quoted q : venue) {
            boolean inside = q.yes;
            for (int g = 0; g < points && inside; g++) {
                inside = !q.at[g] || set[g];
            }
            if (inside) {
                sum += q.p;
                for (int g = 0; g < points; g++) {
                    covered[g] += q.at[g] ? 1 : 0;
                }
            }
        }
        for (int g = 0; g < points; g++) {
            if (covered[g] != (set[g] ? 1 : 0)) {
                return null;
            }
        }
        return Math.max(0, Math.min(1, sum));
    }

    /** The near-locks kept, and how many more qualified on the forecast but not the quotes. */
    static final class NearLocks {
        final ArrayNode kept;
        /** Baskets with a leg the forecast puts over the gap or the ratio from its price. */
        final int overGap;
        /** Baskets whose losing band a venue's quotes put over the cap. */
        final int bandLikely;
        /** Baskets whose losing band neither venue's quotes single out. */
        final int bandUnpriced;

        NearLocks(ArrayNode kept, int overGap, int bandLikely, int bandUnpriced) {
            this.kept = kept;
            this.overGap = overGap;
            this.bandLikely = bandLikely;
            this.bandUnpriced = bandUnpriced;
        }
    }

    /** The contracts of a subset that win at one outcome. */
    private static int onTail(int[] idx, boolean[][] wins, int outcome) {
        int won = 0;
        for (int i : idx) {
            won += wins[i][outcome] ? 1 : 0;
        }
        return won;
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
        return scoreBasket(legs, scenarios, outcomes, search, lock, minYield, top, 1);
    }

    /** As the overload without {@code minVenues}, keeping only subsets whose legs are on at
     *  least that many venues. */
    static ObjectNode scoreBasket(List<Leg> legs, Scenarios scenarios, boolean outcomes,
            int search, boolean lock, Double minYield, int top, int minVenues) {
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
                wins[i][sc] = l.condition.wins(l.side, v);
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
                    Set<String> venues = new HashSet<>();
                    for (int i : idx) {
                        events.add(legs.get(i).source + ":" + legs.get(i).eventId);
                        venues.add(legs.get(i).source);
                    }
                    if ((lock || events.size() >= 2) && venues.size() >= minVenues) {
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
