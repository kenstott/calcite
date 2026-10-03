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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Does the engine's baseline forecast beat the venue's own price? For each past settled event
 * of one series it builds the forecast {@link MarketForecasts} would have built a stated
 * number of days before the event closed, reads the venue's price for each settled market at
 * the same moment, and scores both probabilities against what the market settled on.
 *
 * <p>The baseline's probability for a market is the share of the forecast's samples that
 * satisfy the market's strike, as {@link MarketPricing} computes it. The venue's price is the
 * midpoint of the closing bid and ask of the last hourly candle at or before the moment, or
 * that candle's last trade when the quote is one-sided. A market with no strike, no outcome
 * or no price at the moment is left out with the reason; nothing is filled in.
 */
final class MarketBacktest {
    static final String TOOL = "backtest_market_forecast";
    static final Set<String> KEYS = new TreeSet<>(Arrays.asList("source", "series",
        "days_before", "events", "forecast_args"));
    /** Fewest scored events a verdict may rest on. */
    static final int MIN_EVENTS = 8;
    /** A difference within this many standard errors of zero is not a verdict. */
    static final double SE_BAND = 2.0;
    static final int DEFAULT_DAYS_BEFORE = 7;
    static final int MAX_DAYS_BEFORE = 365;
    static final int DEFAULT_EVENTS = 12;
    static final int MAX_EVENTS = 40;
    /** Settled markets read per event asked for, so an event's markets are read whole. */
    private static final int MARKETS_PER_EVENT = 40;
    /** How far before the moment a candle may lie and still be the price at it. */
    private static final int PRICE_LOOKBACK_DAYS = 7;
    private static final int PLACES = 4;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static final String TIER_BACKTESTED = "backtested";
    static final String TIER_WEAK = "weak";
    private static final String[] RECORD_FIELDS = {"source", "series", "verdict",
        "verdict_reason", "events_scored", "days_before", "brier_baseline", "brier_price",
        "brier_difference", "brier_difference_se"};

    private final PredictionMarkets.Fetcher fetcher;
    private final MarketForecasts builder;
    private final Supplier<Instant> clock;
    /** The last backtest of the engine's own forecast per series, keyed source:series. */
    private final Map<String, ObjectNode> records = new ConcurrentHashMap<>();

    MarketBacktest(PredictionMarkets.Fetcher fetcher, MarketTools.SqlRunner sql,
            Supplier<Instant> clock) {
        this.fetcher = fetcher;
        this.clock = clock;
        this.builder = new MarketForecasts(fetcher, sql, clock);
    }

    /** The last backtest of {@code series} with the engine's own forecast, or null when none
     *  has run. A backtest given forecast_args scores another forecast and leaves no record. */
    ObjectNode record(String source, String series) {
        if (source == null || series == null) {
            return null;
        }
        ObjectNode r = records.get(source + ":" + series);
        return r == null ? null : r.deepCopy();
    }

    /** Keeps the summary of a backtest report as its series' record. */
    void remember(JsonNode report) {
        ObjectNode record = MAPPER.createObjectNode();
        for (String f : RECORD_FIELDS) {
            if (!report.has(f)) {
                throw new IllegalArgumentException("a backtest report carries " + f);
            }
            record.set(f, report.get(f));
        }
        record.put("run_at", clock.get().toString());
        records.put(report.get("source").asText() + ":" + report.get("series").asText(),
            record);
    }

    /**
     * How far a forecast edge can be trusted. {@code backtested}: the engine built the
     * forecast, it carries no blocking flag, and the baseline beat this series' price over
     * settled events. {@code weak}: anything else, with the reasons.
     *
     * @param flags the forecast's flags, as codes or as objects carrying a code
     * @param built whether the engine built the forecast
     */
    static ObjectNode confidence(ObjectNode record, JsonNode flags, boolean built,
            String source, String series) {
        ObjectNode c = MAPPER.createObjectNode();
        ArrayNode reasons = MAPPER.createArrayNode();
        String must = null;
        if (!built) {
            reasons.add("the forecast was given by the caller: the engine has no backtest "
                + "of it");
        } else if (record == null) {
            if ("kalshi".equals(source) && series != null) {
                reasons.add("no backtest of series " + series + " has run");
                must = "You MUST call " + TOOL + "(source='kalshi', series='" + series
                    + "') and state its verdict with this edge.";
            } else {
                reasons.add("backtests read Kalshi series only; this event has none");
            }
        } else if (!"baseline_beats_market".equals(record.path("verdict").asText())) {
            reasons.add("backtest verdict " + record.path("verdict").asText() + ": "
                + record.path("verdict_reason").asText());
        }
        if (flags != null) {
            for (JsonNode f : flags) {
                String code = f.isTextual() ? f.asText() : f.path("code").asText();
                if (MarketScan.BLOCKING_FLAGS.contains(code)) {
                    reasons.add("forecast flag " + code);
                }
            }
        }
        c.put("tier", reasons.isEmpty() ? TIER_BACKTESTED : TIER_WEAK);
        c.set("reasons", reasons);
        c.set("backtest", record);
        if (must != null) {
            c.put("must", must);
        } else if (reasons.size() > 0) {
            c.put("must", "You MUST report this edge as weak and give the reasons.");
        }
        return c;
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
        props.set("source", prop("string", "kalshi. Polymarket markets carry no strike the "
            + "engine can read, so they cannot be backtested."));
        props.set("series", prop("string", "Kalshi series ticker, e.g. KXCPIYOY, KXCPI, "
            + "KXPAYROLLS."));
        props.set("days_before", prop("integer", "Days before each event's close at which the "
            + "forecast and the price are taken (default 7, at most 365)."));
        props.set("events", prop("integer", "Most recent settled events to score (default 12, "
            + "at most 40)."));
        props.set("forecast_args", prop("object", "Arguments passed to every forecast, as "
            + "forecast_market_event takes them (transform, series, table, round, "
            + "frequency...), when the rules do not resolve the spec. Not source, event_id or "
            + "as_of."));
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("type", "object");
        schema.set("properties", props);
        ArrayNode required = schema.putArray("required");
        required.add("source");
        required.add("series");
        ObjectNode out = MAPPER.createObjectNode();
        out.put("name", TOOL);
        out.put("description", DESCRIPTION);
        out.set("inputSchema", schema);
        return out;
    }

    static final String DESCRIPTION =
        "Test whether the engine's baseline forecast beats the venue's own price for one "
        + "Kalshi series. For each past settled event of the series it rebuilds the forecast "
        + "as of days_before days before the event closed, takes the venue's price for each "
        + "settled market at that moment, and scores both against the outcome. Returns, per "
        + "series: events and markets scored, Brier score of the baseline and of the price "
        + "(mean over events; lower is better), hit rate of each (probability on the right "
        + "side of 0.5; exactly 0.5 is a miss), the share of markets where the baseline was "
        + "closer to the outcome, the Brier difference (baseline minus price) with its "
        + "standard error across events, and verdict: baseline_beats_market, "
        + "market_beats_baseline or inconclusive (difference within two standard errors, or "
        + "fewer than 8 events scored). events lists each event scored (event id, close "
        + "time, as_of, forecast median, settlement value, both Brier scores, flags); "
        + "skipped lists every event left out with the reason. Catalog rows are the current "
        + "revised values, not the values published at the time: as_of removes later "
        + "periods, not later revisions. A monthly row counts as known once its release has "
        + "printed. You MUST state the verdict, the number of events scored and the revision "
        + "caveat. You MUST state every skipped event's reason when "
        + "fewer than 8 events were scored. You MUST NOT call a baseline edge reliable when "
        + "the verdict is inconclusive or market_beats_baseline. You MUST NOT report a "
        + "series' skill from a different series' backtest.";

    // ─── Arguments ─────────────────────────────────────────────────────────────

    private static boolean has(JsonNode args, String name) {
        return args.has(name) && !args.get(name).isNull();
    }

    private static int intArg(JsonNode args, String name, int dflt, int max) {
        if (!has(args, name)) {
            return dflt;
        }
        if (!args.get(name).isIntegralNumber()) {
            throw new IllegalArgumentException(name + " must be an integer, got "
                + args.get(name));
        }
        int v = args.get(name).asInt();
        if (v < 1 || v > max) {
            throw new IllegalArgumentException(name + " must be 1 to " + max + ", got " + v);
        }
        return v;
    }

    private static String textArg(JsonNode args, String name) {
        if (!has(args, name)) {
            return null;
        }
        String s = args.get(name).asText().trim();
        return s.isEmpty() ? null : s;
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    // ─── Scoring ───────────────────────────────────────────────────────────────

    /** One event scored. */
    private static final class Scored {
        String eventId;
        String closeTime;
        LocalDate asOf;
        Instant moment;
        Double median;
        String settlementValue;
        String series;
        String transform;
        List<String> flags = new ArrayList<>();
        int markets;
        double brierBaseline;
        double brierPrice;
        int baselineHits;
        int priceHits;
        int baselineCloser;
        int priceCloser;
        Map<String, Integer> marketsSkipped = new LinkedHashMap<>();
    }

    private static boolean hit(double p, int y) {
        return y == 1 ? p > 0.5 : p < 0.5;
    }

    private static Double rounded(double v) {
        return PredictionMarkets.round(v, PLACES);
    }

    /** Backtests one series; returns the report as a JSON string. */
    synchronized String backtestTool(JsonNode args) throws Exception {
        Iterator<String> names = args.fieldNames();
        while (names.hasNext()) {
            String n = names.next();
            if (!KEYS.contains(n)) {
                throw new IllegalArgumentException("unknown key '" + n + "' in " + TOOL
                    + "; allowed: " + KEYS);
            }
        }
        String source = textArg(args, "source");
        String series = textArg(args, "series");
        if (source == null || series == null) {
            throw new IllegalArgumentException("source and series are required");
        }
        if (!"kalshi".equals(source)) {
            throw new IllegalArgumentException("source must be 'kalshi', got '" + source
                + "'; Polymarket markets carry no strike the engine can read");
        }
        int daysBefore = intArg(args, "days_before", DEFAULT_DAYS_BEFORE, MAX_DAYS_BEFORE);
        int wanted = intArg(args, "events", DEFAULT_EVENTS, MAX_EVENTS);
        ObjectNode forecastArgs = MAPPER.createObjectNode();
        if (has(args, "forecast_args")) {
            if (!args.get("forecast_args").isObject()) {
                throw new IllegalArgumentException("forecast_args must be an object, got "
                    + args.get("forecast_args"));
            }
            forecastArgs.setAll((ObjectNode) args.get("forecast_args"));
            for (String reserved : new String[] {"source", "event_id", "as_of"}) {
                if (forecastArgs.has(reserved)) {
                    throw new IllegalArgumentException("forecast_args must not carry '"
                        + reserved + "'; the backtest sets it");
                }
            }
        }
        // Parsed once up front so a bad override fails before any venue call.
        MarketForecasts.Request.parse(forecastArgs);

        int limit = wanted * MARKETS_PER_EVENT;
        List<MarketHistory.SettledMarket> listed =
            MarketHistory.settledMarkets(fetcher, source, series, limit);
        boolean cut = listed.size() >= limit;
        Map<String, List<MarketHistory.SettledMarket>> byEvent = new LinkedHashMap<>();
        for (MarketHistory.SettledMarket m : listed) {
            byEvent.computeIfAbsent(m.eventId, k -> new ArrayList<>()).add(m);
        }
        List<Scored> scored = new ArrayList<>();
        ArrayNode skipped = MAPPER.createArrayNode();
        int read = 0;
        int index = 0;
        for (Map.Entry<String, List<MarketHistory.SettledMarket>> e : byEvent.entrySet()) {
            if (read >= wanted) {
                break;
            }
            read++;
            index++;
            if (cut && index == byEvent.size()) {
                skip(skipped, e.getKey(), "the listing was cut off at " + limit + " markets, "
                    + "so this event's markets may be incomplete");
                continue;
            }
            String why = null;
            Scored s = new Scored();
            s.eventId = e.getKey();
            try {
                why = score(s, e.getValue(), series, daysBefore, forecastArgs);
            } catch (IOException | IllegalArgumentException | IllegalStateException ex) {
                why = ex.getClass().getSimpleName() + ": " + ex.getMessage();
            }
            if (why == null) {
                scored.add(s);
            } else {
                skip(skipped, e.getKey(), why);
            }
        }
        ObjectNode report = report(source, series, daysBefore, read, scored, skipped);
        if (forecastArgs.isEmpty()) {
            remember(report);
        }
        return MAPPER.writeValueAsString(report);
    }

    private static void skip(ArrayNode skipped, String eventId, String reason) {
        ObjectNode o = skipped.addObject();
        o.put("event_id", eventId);
        o.put("reason", reason);
    }

    /**
     * Scores one event into {@code s}; returns the reason it cannot be scored, or null when
     * it was.
     */
    private String score(Scored s, List<MarketHistory.SettledMarket> markets, String series,
            int daysBefore, ObjectNode forecastArgs) throws Exception {
        List<MarketHistory.SettledMarket> usable = new ArrayList<>();
        List<PredictionMarkets.Market> strikes = new ArrayList<>();
        List<MarketPricing.Condition> conditions = new ArrayList<>();
        String closeTime = null;
        for (MarketHistory.SettledMarket m : markets) {
            if (closeTime == null || m.closeTime.compareTo(closeTime) < 0) {
                closeTime = m.closeTime;
            }
            if (s.settlementValue == null) {
                s.settlementValue = m.settlementValue;
            }
            if (!m.scoreable) {
                s.marketsSkipped.merge(m.notScoreableReason, 1, Integer::sum);
                continue;
            }
            PredictionMarkets.Market strike = new PredictionMarkets.Market();
            strike.strikeType = m.strikeType;
            strike.floorStrike = m.floorStrike;
            strike.capStrike = m.capStrike;
            MarketPricing.Condition c = MarketPricing.Condition.ofStrike(strike);
            if (c == null) {
                s.marketsSkipped.merge("no strike the engine can read (strike_type "
                    + m.strikeType + ")", 1, Integer::sum);
                continue;
            }
            usable.add(m);
            conditions.add(c);
        }
        if (usable.isEmpty()) {
            return "no market with a yes or no outcome and a readable strike: "
                + s.marketsSkipped;
        }
        s.closeTime = closeTime;
        s.moment = PredictionMarkets.closeInstant(closeTime).minus(Duration.ofDays(daysBefore));
        s.asOf = s.moment.atZone(ZoneOffset.UTC).toLocalDate();

        JsonNode ev = fetcher.get(PredictionMarkets.KALSHI + "/events/" + enc(s.eventId));
        JsonNode title = ev.path("event").path("title");
        if (!title.isTextual()) {
            return "the venue's event response has no title";
        }
        String eventTitle = title.asText();
        MarketForecasts.Request req = MarketForecasts.Request.parse(forecastArgs);
        req.asOf = s.asOf;
        MarketForecasts.Result built = builder.forecast(eventTitle, usable.get(0).rules,
            PredictionMarkets.driverOf(eventTitle), closeTime, req);
        if (built.forecast == null) {
            return "no forecast: " + built.json.path("flags");
        }
        s.median = built.json.path("median").asDouble();
        s.series = built.json.path("series").asText();
        s.transform = built.json.path("transform").asText();
        for (JsonNode f : built.json.path("flags")) {
            s.flags.add(f.path("code").asText());
        }

        double[] values = built.forecast.values;
        double sumBaseline = 0;
        double sumPrice = 0;
        for (int i = 0; i < usable.size(); i++) {
            MarketHistory.SettledMarket m = usable.get(i);
            int hits = 0;
            for (double v : values) {
                if (conditions.get(i).holds(v)) {
                    hits++;
                }
            }
            double fair = (double) hits / values.length;
            Double price = priceAt(m, series, s.moment);
            if (price == null) {
                s.marketsSkipped.merge("no quote or trade in the " + PRICE_LOOKBACK_DAYS
                    + " days up to as_of", 1, Integer::sum);
                continue;
            }
            int y = "yes".equals(m.outcome) ? 1 : 0;
            s.markets++;
            sumBaseline += (fair - y) * (fair - y);
            sumPrice += (price - y) * (price - y);
            s.baselineHits += hit(fair, y) ? 1 : 0;
            s.priceHits += hit(price, y) ? 1 : 0;
            double eb = Math.abs(fair - y);
            double ep = Math.abs(price - y);
            s.baselineCloser += eb < ep ? 1 : 0;
            s.priceCloser += ep < eb ? 1 : 0;
        }
        if (s.markets == 0) {
            return "no market had a price at as_of: " + s.marketsSkipped;
        }
        s.brierBaseline = sumBaseline / s.markets;
        s.brierPrice = sumPrice / s.markets;
        return null;
    }

    /** The venue's price of Yes at {@code moment}, or null when nothing was quoted or traded
     *  in the lookback window. */
    private Double priceAt(MarketHistory.SettledMarket m, String series, Instant moment)
            throws IOException {
        MarketHistory.MarketRef ref = new MarketHistory.MarketRef();
        ref.source = m.source;
        ref.marketId = m.marketId;
        ref.series = series;
        ref.historical = m.historical;
        List<MarketHistory.PricePoint> points = MarketHistory.priceHistory(fetcher, ref,
            moment.minus(Duration.ofDays(PRICE_LOOKBACK_DAYS)), moment,
            MarketHistory.Interval.HOUR);
        for (int i = points.size() - 1; i >= 0; i--) {
            MarketHistory.PricePoint p = points.get(i);
            if (p.epochSecond > moment.getEpochSecond()) {
                continue;
            }
            Double price = PredictionMarkets.mid(p.yesBid, p.yesAsk, p.price);
            if (price != null) {
                return price;
            }
        }
        return null;
    }

    // ─── The report ────────────────────────────────────────────────────────────

    private ObjectNode report(String source, String series, int daysBefore, int read,
            List<Scored> scored, ArrayNode skipped) {
        ObjectNode out = MAPPER.createObjectNode();
        out.put("tool", TOOL);
        out.put("source", source);
        out.put("series", series);
        out.put("days_before", daysBefore);
        out.put("events_read", read);
        out.put("events_scored", scored.size());
        int markets = 0;
        int baselineHits = 0;
        int priceHits = 0;
        int baselineCloser = 0;
        int priceCloser = 0;
        double sumBaseline = 0;
        double sumPrice = 0;
        double[] diffs = new double[scored.size()];
        ArrayNode events = MAPPER.createArrayNode();
        for (int i = 0; i < scored.size(); i++) {
            Scored s = scored.get(i);
            markets += s.markets;
            baselineHits += s.baselineHits;
            priceHits += s.priceHits;
            baselineCloser += s.baselineCloser;
            priceCloser += s.priceCloser;
            sumBaseline += s.brierBaseline;
            sumPrice += s.brierPrice;
            diffs[i] = s.brierBaseline - s.brierPrice;
            ObjectNode o = events.addObject();
            o.put("event_id", s.eventId);
            o.put("close_time", s.closeTime);
            o.put("as_of", s.asOf.toString());
            o.put("price_taken_at", s.moment.toString());
            o.put("forecast_median", s.median);
            o.put("settlement_value", s.settlementValue);
            o.put("forecast_series", s.series);
            o.put("transform", s.transform);
            o.put("markets_scored", s.markets);
            o.put("baseline_brier", rounded(s.brierBaseline));
            o.put("price_brier", rounded(s.brierPrice));
            ArrayNode flags = o.putArray("flags");
            for (String f : s.flags) {
                flags.add(f);
            }
            ObjectNode left = o.putObject("markets_skipped");
            for (Map.Entry<String, Integer> k : s.marketsSkipped.entrySet()) {
                left.put(k.getKey(), k.getValue());
            }
        }
        out.put("markets_scored", markets);
        int n = scored.size();
        String verdict;
        Boolean beats = null;
        String reason;
        if (n == 0) {
            verdict = "inconclusive";
            reason = "no event was scored";
            out.putNull("brier_baseline");
            out.putNull("brier_price");
            out.putNull("brier_difference");
            out.putNull("brier_difference_se");
            out.putNull("hit_rate_baseline");
            out.putNull("hit_rate_price");
            out.putNull("baseline_closer_share");
            out.putNull("price_closer_share");
        } else {
            double meanDiff = (sumBaseline - sumPrice) / n;
            Double se = null;
            if (n >= 2) {
                double ss = 0;
                for (double d : diffs) {
                    ss += (d - meanDiff) * (d - meanDiff);
                }
                se = Math.sqrt(ss / (n - 1) / n);
            }
            out.put("brier_baseline", rounded(sumBaseline / n));
            out.put("brier_price", rounded(sumPrice / n));
            out.put("brier_difference", rounded(meanDiff));
            PredictionMarkets.putNumber(out, "brier_difference_se",
                se == null ? null : rounded(se));
            out.put("hit_rate_baseline", rounded((double) baselineHits / markets));
            out.put("hit_rate_price", rounded((double) priceHits / markets));
            out.put("baseline_closer_share", rounded((double) baselineCloser / markets));
            out.put("price_closer_share", rounded((double) priceCloser / markets));
            if (n < MIN_EVENTS) {
                verdict = "inconclusive";
                reason = "only " + n + " events were scored; a verdict needs " + MIN_EVENTS;
            } else if (Math.abs(meanDiff) <= SE_BAND * se) {
                verdict = "inconclusive";
                reason = "the Brier difference is within " + (int) SE_BAND
                    + " standard errors of zero";
            } else {
                beats = meanDiff < 0;
                verdict = beats ? "baseline_beats_market" : "market_beats_baseline";
                reason = "the Brier difference is more than " + (int) SE_BAND
                    + " standard errors from zero";
            }
        }
        out.put("verdict", verdict);
        if (beats == null) {
            out.putNull("baseline_beats_market");
        } else {
            out.put("baseline_beats_market", beats);
        }
        out.put("verdict_reason", reason);
        out.put("method", "Brier score is the mean over events of the mean squared error of "
            + "the probability against the outcome over that event's markets (lower is "
            + "better); the difference is baseline minus price, so negative favours the "
            + "baseline; its standard error is across events. Baseline probability: share of "
            + "forecast samples meeting the strike. Price: midpoint of the closing bid and "
            + "ask of the last hourly candle at or before the moment, the last trade when "
            + "one-sided. A hit is a probability on the right side of 0.5.");
        out.put("revision_caveat", "Catalog rows are the current revised values, not the "
            + "values published at the time: the as_of cutoff removes later periods but not "
            + "later revisions, so the baseline saw cleaner history than a trader did and "
            + "its score here is a best case.");
        out.set("events", events);
        out.set("skipped", skipped);
        out.put("next", next(verdict, n));
        return out;
    }

    private static String next(String verdict, int n) {
        switch (verdict) {
            case "baseline_beats_market":
                return "State the verdict, the Brier difference and its standard error, and "
                    + n + " events scored, with the revision caveat. The baseline has shown "
                    + "skill against this series' price; an edge from it is worth vetting, "
                    + "still against the rules of the specific event.";
            case "market_beats_baseline":
                return "State the verdict, the Brier difference and its standard error, and "
                    + n + " events scored, with the revision caveat. The price was closer to "
                    + "the outcome than the baseline: treat a baseline-versus-market "
                    + "disagreement on this series as the baseline being wrong, not as an "
                    + "edge.";
            default:
                return "State that the backtest is inconclusive and why (verdict_reason), "
                    + "list the skipped events and their reasons, and do not present a "
                    + "baseline edge on this series as reliable. Try another days_before or "
                    + "more events, a larger days_before when events were skipped as already in "
                    + "the catalog, or a forecast_args override when they were skipped as "
                    + "unresolved.";
        }
    }
}
