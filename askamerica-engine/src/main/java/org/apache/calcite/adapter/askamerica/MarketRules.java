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
import java.time.LocalDate;
import java.time.Month;
import java.time.YearMonth;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A structured diff of two events' settlement terms, read from their rules text: whether a
 * Kalshi event and a Polymarket event settle on the same number. A dimension the text does
 * not state is {@code unknown}, never assumed equal, so a pair is {@code match} only when
 * every dimension is stated on both sides and agrees.
 */
final class MarketRules {
    static final String TOOL = "compare_settlement_rules";
    static final Set<String> KEYS = new TreeSet<>(Arrays.asList("a", "b"));
    static final Set<String> REF_KEYS = new TreeSet<>(Arrays.asList("source", "event_id"));
    static final String MATCH = "match";
    static final String DIFFER = "differ";
    static final String UNKNOWN = "unknown";
    static final String UNVERIFIED = "unverified";

    /** The dimensions compared, in the order reported. */
    static final String[] DIMENSIONS = {"source_agency", "series", "settlement_period",
        "transform", "seasonal_adjustment", "rounding", "release_or_close_date",
        "revision_handling", "tie_handling"};

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Agency names and the forms they take in a rules text or a source url. */
    private static final Map<String, Pattern> AGENCIES = new LinkedHashMap<>();

    static {
        AGENCIES.put("BLS", Pattern.compile(
            "bureau of labor statistics|\\bbls\\b|bls\\.gov"));
        AGENCIES.put("BEA", Pattern.compile(
            "bureau of economic analysis|\\bbea\\b|bea\\.gov"));
        AGENCIES.put("FRED", Pattern.compile(
            "\\bfred\\b|fred\\.stlouisfed|federal reserve economic data"));
        AGENCIES.put("Federal Reserve", Pattern.compile(
            "federal reserve|federalreserve\\.gov|\\bfomc\\b"));
        AGENCIES.put("EIA", Pattern.compile(
            "energy information administration|\\beia\\b|eia\\.gov"));
        AGENCIES.put("Census", Pattern.compile("census bureau|census\\.gov"));
        AGENCIES.put("Freddie Mac", Pattern.compile("freddie mac|freddiemac\\.com"));
        AGENCIES.put("Treasury", Pattern.compile(
            "u\\.?s\\.? department of the treasury|treasury\\.gov|treasury department"));
        AGENCIES.put("NOAA", Pattern.compile("\\bnoaa\\b|national weather service|weather\\.gov"));
        AGENCIES.put("S&P", Pattern.compile("s&p|spglobal\\.com"));
    }

    /** A series id the rules name: BLS programs and FRED oil prices. */
    private static final Pattern NAMED_ID = Pattern.compile(
        "\\b(APU[0-9A-Z]{6,}|CU[SU]R[0-9A-Z]{6,}|CES[0-9]{8,}|LN[SU][0-9]{7,}|WPU[0-9A-Z]{4,}"
        + "|DCOIL[A-Z0-9]+|CPIAUCSL|CPILFESL)\\b");
    private static final Pattern FRED_NAMED = Pattern.compile(
        "\\bseries (?:id )?[\"']?([A-Z][A-Z0-9]{2,19})[\"']?");

    private static final Pattern MONTH_NAMED;

    static {
        StringBuilder names = new StringBuilder("sept");
        for (Month m : Month.values()) {
            String name = m.name().toLowerCase(Locale.ROOT);
            names.append('|').append(name).append('|').append(name, 0, 3);
        }
        MONTH_NAMED = Pattern.compile("\\b(" + names
            + ")\\b\\.?(?:\\s+\\d{1,2}(?!\\d),?)?(?:\\s+(\\d{4}))?");
    }

    private static final Pattern QUARTER = Pattern.compile(
        "\\b(?:q([1-4])|(first|second|third|fourth) quarter)\\b[ ,]*(?:of )?(\\d{4})?");
    private static final Pattern MOM_TEXT = Pattern.compile(
        "month[- ]over[- ]month|m/m|\\bmom\\b|monthly (change|rate|increase|inflation)"
        + "|(from|compared (to|with)) the (previous|prior|preceding) month"
        + "|(one|1)[- ]month percent(age)? change");
    private static final Pattern YOY_TEXT = Pattern.compile(
        "year[- ]over[- ]year|\\byoy\\b|12[- ]month|annual (rate|inflation|change)"
        + "|from a year (ago|earlier)|compared (to|with) (the )?same month"
        + "|over the (last|past) year|year[- ]ending");
    private static final Pattern LEVEL_TEXT = Pattern.compile(
        "\\bindex level\\b|\\blevel of\\b|\\bindex value\\b|\\bclosing (price|level|value)\\b"
        + "|\\bthe level\\b");
    private static final Pattern FIRST_PRINT = Pattern.compile(
        "first (print|release|published|reported|estimate)|initial (release|estimate|print|"
        + "report)|as (first|initially) (published|reported|released)"
        + "|(not|never) (be )?(subject to |affected by )?(revised|revisions?)"
        + "|no (subsequent |later )?revisions?|subsequent revisions? (will )?(not|are not)"
        + "|revisions? (will )?not (be )?(considered|counted|used)");
    private static final Pattern REVISED = Pattern.compile(
        "as (later )?revised|latest (revision|revised|data|value)|final (value|data|revision"
        + "|estimate)|revised (data|value|figure|estimate)|including revisions?"
        + "|subsequent revisions? (will|shall) be");
    private static final Pattern STRICT_ABOVE = Pattern.compile(
        "strictly (above|greater|more|higher)|\\b(greater|more|higher) than(?! or)|\\babove\\b"
        + "|\\bexceeds?\\b|\\bover\\b(?= [-+$]?\\d)");
    private static final Pattern INCLUSIVE_ABOVE = Pattern.compile(
        "at or above|(greater|more|higher) than or equal|\\bat least\\b|or (more|higher|above"
        + "|greater)\\b|no (less|lower) than|equal to or (above|greater|higher|more)"
        + "|\\bat or over\\b|\\b>=");
    private static final Pattern STRICT_BELOW = Pattern.compile(
        "strictly (below|less|lower|under)|\\b(less|lower|fewer) than(?! or)|\\bbelow\\b"
        + "|\\bunder\\b(?= [-+$]?\\d)");
    private static final Pattern INCLUSIVE_BELOW = Pattern.compile(
        "at or below|(less|lower|fewer) than or equal|\\bat most\\b|or (less|lower|below|under"
        + "|fewer)\\b|no (more|greater|higher) than|equal to or (below|less|lower)"
        + "|\\bat or under\\b|\\b<=");

    private final PredictionMarkets.Fetcher fetcher;

    MarketRules(PredictionMarkets.Fetcher fetcher) {
        this.fetcher = fetcher;
    }

    // ─── Tool ──────────────────────────────────────────────────────────────────

    private static ObjectNode refProp(String which) {
        ObjectNode ref = MAPPER.createObjectNode();
        ref.put("type", "object");
        ref.put("description", "Event " + which + ": source ('kalshi' or 'polymarket') and "
            + "event_id (Kalshi event ticker, Polymarket event id).");
        ObjectNode props = ref.putObject("properties");
        props.putObject("source").put("type", "string");
        props.putObject("event_id").put("type", "string");
        ref.putArray("required").add("source").add("event_id");
        return ref;
    }

    /** The MCP tool definition: name, description and input schema, as McpServer.tool(...)
     *  builds them. */
    static ObjectNode toolDef() {
        ObjectNode props = MAPPER.createObjectNode();
        props.set("a", refProp("a"));
        props.set("b", refProp("b"));
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("type", "object");
        schema.set("properties", props);
        schema.putArray("required").add("a").add("b");
        ObjectNode out = MAPPER.createObjectNode();
        out.put("name", TOOL);
        out.put("description", DESCRIPTION);
        out.set("inputSchema", schema);
        return out;
    }

    private static final String DESCRIPTION =
        "Compare the settlement terms of two prediction-market events, to decide whether a "
        + "cross-venue pair (Kalshi against Polymarket) settles on the same number. Reads both "
        + "events and diffs nine dimensions from their rules text: source_agency, series, "
        + "settlement_period, transform (level, month-over-month, year-over-year), "
        + "seasonal_adjustment, rounding, release_or_close_date, revision_handling (first "
        + "print or revised) and tie_handling (strictly above or at-or-above). Each dimension "
        + "gives a, b and a status of match, differ or unknown; unknown means the text does "
        + "not say, and is never treated as equal. rules_match is 'match' when every "
        + "dimension matches, 'differ' when any differs (differing lists which), and "
        + "'unverified' when none differs but some are unknown (unknown lists which). "
        + "Arguments a and b are each {source, event_id}. You MUST call this tool for every "
        + "cross-venue pair before calling it a lock. You MUST report a lock on a pair whose "
        + "rules_match is not 'match' as not a lock. You MUST state every differing and "
        + "unknown dimension. You MUST NOT assume an unknown dimension is equal.";

    // ─── Handler ───────────────────────────────────────────────────────────────

    private static PredictionMarkets.Event read(PredictionMarkets.Fetcher fetcher, JsonNode ref,
            String name) throws IOException {
        if (!ref.isObject()) {
            throw new IllegalArgumentException(name + " must be an object with source and "
                + "event_id, got " + ref);
        }
        Iterator<String> names = ref.fieldNames();
        while (names.hasNext()) {
            String n = names.next();
            if (!REF_KEYS.contains(n)) {
                throw new IllegalArgumentException("unknown key '" + n + "' in " + name
                    + "; allowed: " + REF_KEYS);
            }
        }
        String source = text(ref, "source", name);
        String eventId = text(ref, "event_id", name);
        return PredictionMarkets.fetchEvent(fetcher, source, eventId).event;
    }

    private static String text(JsonNode ref, String field, String name) {
        JsonNode v = ref.get(field);
        if (v == null || !v.isTextual() || v.asText().trim().isEmpty()) {
            throw new IllegalArgumentException(name + "." + field
                + " is required and must be a non-empty string");
        }
        return v.asText().trim();
    }

    /** The handler McpServer registers: reads both events and returns the diff as JSON. */
    String compareSettlementRules(JsonNode args) throws Exception {
        if (args == null || !args.isObject()) {
            throw new IllegalArgumentException("arguments must be an object with a and b");
        }
        Iterator<String> names = args.fieldNames();
        while (names.hasNext()) {
            String n = names.next();
            if (!KEYS.contains(n)) {
                throw new IllegalArgumentException("unknown argument '" + n + "'; allowed: "
                    + KEYS);
            }
        }
        for (String k : KEYS) {
            if (!args.has(k)) {
                throw new IllegalArgumentException("a and b are both required, " + k
                    + " is missing");
            }
        }
        PredictionMarkets.Event a = read(fetcher, args.get("a"), "a");
        PredictionMarkets.Event b = read(fetcher, args.get("b"), "b");
        return MAPPER.writeValueAsString(compare(a, b));
    }

    // ─── The diff ──────────────────────────────────────────────────────────────

    /**
     * Diffs two events' settlement terms. One entry per dimension under {@code dimensions},
     * each with {@code a}, {@code b} and {@code status}; then {@code rules_match},
     * {@code differing}, {@code unknown} and {@code next}.
     */
    static ObjectNode compare(PredictionMarkets.Event a, PredictionMarkets.Event b) {
        Map<String, String[]> terms = new LinkedHashMap<>();
        Terms ta = Terms.of(a);
        Terms tb = Terms.of(b);
        terms.put("source_agency", new String[]{ta.agency, tb.agency});
        terms.put("series", new String[]{ta.series, tb.series});
        terms.put("settlement_period", new String[]{ta.period, tb.period});
        terms.put("transform", new String[]{ta.transform, tb.transform});
        terms.put("seasonal_adjustment", new String[]{ta.seasonal, tb.seasonal});
        terms.put("rounding", new String[]{ta.rounding, tb.rounding});
        terms.put("release_or_close_date", new String[]{ta.date, tb.date});
        terms.put("revision_handling", new String[]{ta.revision, tb.revision});
        terms.put("tie_handling", new String[]{ta.tie, tb.tie});
        ObjectNode out = MAPPER.createObjectNode();
        out.set("a", ref(a));
        out.set("b", ref(b));
        ObjectNode dims = out.putObject("dimensions");
        ArrayNode differing = MAPPER.createArrayNode();
        ArrayNode unknown = MAPPER.createArrayNode();
        for (String d : DIMENSIONS) {
            String[] v = terms.get(d);
            ObjectNode o = dims.putObject(d);
            put(o, "a", v[0]);
            put(o, "b", v[1]);
            String status = v[0] == null || v[1] == null ? UNKNOWN
                : v[0].equals(v[1]) || "release_or_close_date".equals(d) && sameRelease(v[0], v[1])
                    ? MATCH : DIFFER;
            o.put("status", status);
            if (DIFFER.equals(status)) {
                differing.add(d);
            } else if (UNKNOWN.equals(status)) {
                unknown.add(d);
            }
        }
        String verdict = differing.size() > 0 ? DIFFER : unknown.size() > 0 ? UNVERIFIED : MATCH;
        out.put("rules_match", verdict);
        out.set("differing", differing);
        out.set("unknown", unknown);
        if (MATCH.equals(verdict)) {
            out.put("next", "Every dimension is stated on both sides and agrees. The pair "
                + "settles on the same number; a lock on it may be reported as a lock.");
        } else if (DIFFER.equals(verdict)) {
            out.put("next", "rules_match is 'differ'. A lock on this pair MUST be reported as "
                + "not a lock. You MUST state the differing dimensions: " + differing + ".");
        } else {
            out.put("next", "rules_match is 'unverified'. A lock on this pair MUST be reported "
                + "as not a lock: unverified, the rules text does not state every term. You "
                + "MUST state the unknown dimensions: " + unknown + ".");
        }
        return out;
    }

    /** Close dates this near are one release; the same window pairs events across venues. */
    private static boolean sameRelease(String a, String b) {
        return Math.abs(ChronoUnit.DAYS.between(LocalDate.parse(a), LocalDate.parse(b)))
            <= MarketBaskets.CROSS_VENUE_DAYS;
    }

    private static ObjectNode ref(PredictionMarkets.Event e) {
        ObjectNode o = MAPPER.createObjectNode();
        o.put("source", e.source);
        o.put("event_id", e.eventId);
        o.put("event_title", e.eventTitle);
        return o;
    }

    private static void put(ObjectNode o, String field, String value) {
        if (value == null) {
            o.putNull(field);
        } else {
            o.put(field, value);
        }
    }

    /** What one event's text states about each dimension; null where it is silent. */
    private static final class Terms {
        String agency;
        String series;
        String period;
        String transform;
        String seasonal;
        String rounding;
        String date;
        String revision;
        String tie;

        static Terms of(PredictionMarkets.Event e) {
            String title = e.eventTitle == null ? "" : e.eventTitle;
            String rules = e.rules == null ? "" : e.rules;
            String all = (title + " " + rules).toLowerCase(Locale.ROOT);
            Terms t = new Terms();
            t.agency = agency(e, all);
            t.series = series(title + " " + rules);
            t.period = period(title, rules, e.closeTime);
            t.transform = transform(title.toLowerCase(Locale.ROOT),
                rules.toLowerCase(Locale.ROOT));
            Boolean sa = MarketForecasts.rulesSeasonal(all);
            t.seasonal = sa == null ? null : sa ? "seasonally adjusted" : "not seasonally adjusted";
            Integer decimals = MarketForecasts.rulesRound(all);
            t.rounding = decimals == null ? null : decimals + " decimals";
            t.date = e.closeTime == null || e.closeTime.length() < 10 ? null
                : LocalDate.parse(e.closeTime.substring(0, 10)).toString();
            t.revision = revision(all);
            t.tie = tie(e, all);
            return t;
        }
    }

    private static String agency(PredictionMarkets.Event e, String all) {
        StringBuilder src = new StringBuilder(all);
        if (e.settlementSources != null) {
            for (String s : e.settlementSources) {
                src.append(' ').append(s.toLowerCase(Locale.ROOT));
            }
        }
        Set<String> found = new TreeSet<>();
        for (Map.Entry<String, Pattern> a : AGENCIES.entrySet()) {
            if (a.getValue().matcher(src).find()) {
                found.add(a.getKey());
            }
        }
        return found.isEmpty() ? null : String.join(" + ", found);
    }

    /** The series id the text names, or null when it names none. */
    private static String series(String text) {
        Matcher m = NAMED_ID.matcher(text);
        if (m.find()) {
            return m.group(1);
        }
        m = FRED_NAMED.matcher(text);
        return m.find() ? m.group(1) : null;
    }

    private static String period(String title, String rules, String closeTime) {
        String[] texts = {title.toLowerCase(Locale.ROOT), rules.toLowerCase(Locale.ROOT)};
        for (int i = 0; i < texts.length; i++) {
            Matcher q = QUARTER.matcher(texts[i]);
            if (q.find() && q.group(3) != null) {
                String n = q.group(1) != null ? q.group(1)
                    : String.valueOf(Arrays.asList("first", "second", "third", "fourth")
                        .indexOf(q.group(2)) + 1);
                return q.group(3) + "-Q" + n;
            }
            Matcher m = MONTH_NAMED.matcher(texts[i]);
            while (m.find()) {
                if (m.group(2) != null) {
                    return monthOf(m, m.group(2)).toString();
                }
                // No year given: the period ended on or before the event closed. Only the
                // title is trusted for this, as the rules quote other months too.
                if (i == 0 && closeTime != null && closeTime.length() >= 10) {
                    YearMonth close = YearMonth.from(LocalDate.parse(closeTime.substring(0, 10)));
                    YearMonth c = YearMonth.of(close.getYear(), monthOf(m, null).getMonthValue());
                    return (c.isAfter(close) ? c.minusYears(1) : c).toString();
                }
            }
        }
        return null;
    }

    private static YearMonth monthOf(Matcher m, String year) {
        String prefix = m.group(1).substring(0, 3).toUpperCase(Locale.ROOT);
        int month = 0;
        for (Month named : Month.values()) {
            if (named.name().startsWith(prefix)) {
                month = named.getValue();
            }
        }
        return YearMonth.of(year == null ? 2000 : Integer.parseInt(year), month);
    }

    private static String transform(String title, String rules) {
        String t = transformIn(title);
        return t != null ? t : transformIn(rules);
    }

    /** The transform a text names; null when it names none or more than one. */
    private static String transformIn(String text) {
        boolean mom = MOM_TEXT.matcher(text).find();
        boolean yoy = YOY_TEXT.matcher(text).find();
        if (mom && yoy) {
            return null;
        }
        if (mom) {
            return "month-over-month";
        }
        if (yoy) {
            return "year-over-year";
        }
        return LEVEL_TEXT.matcher(text).find() ? "level" : null;
    }

    private static String revision(String text) {
        boolean first = FIRST_PRINT.matcher(text).find();
        boolean revised = REVISED.matcher(text).find();
        if (first == revised) {
            return null;
        }
        return first ? "first print" : "revised";
    }

    /** Strict or inclusive threshold, from the rules wording and then the venue's strike
     *  type; null when neither says, or the text says both. */
    private static String tie(PredictionMarkets.Event e, String text) {
        boolean strict = STRICT_ABOVE.matcher(text).find() || STRICT_BELOW.matcher(text).find();
        boolean inclusive = INCLUSIVE_ABOVE.matcher(text).find()
            || INCLUSIVE_BELOW.matcher(text).find();
        // "at or above" also contains "above": inclusive wording takes it out of strict.
        if (inclusive) {
            String rest = INCLUSIVE_BELOW.matcher(INCLUSIVE_ABOVE.matcher(text).replaceAll(" "))
                .replaceAll(" ");
            strict = STRICT_ABOVE.matcher(rest).find() || STRICT_BELOW.matcher(rest).find();
        }
        if (strict != inclusive) {
            return inclusive ? "at or beyond threshold" : "strictly beyond threshold";
        }
        if (strict) {
            return null;
        }
        Set<String> kinds = new LinkedHashSet<>();
        for (PredictionMarkets.Market m : e.legs) {
            if ("greater".equals(m.strikeType) || "less".equals(m.strikeType)) {
                kinds.add("strictly beyond threshold");
            } else if ("greater_or_equal".equals(m.strikeType)
                    || "less_or_equal".equals(m.strikeType)) {
                kinds.add("at or beyond threshold");
            }
        }
        return kinds.size() == 1 ? kinds.iterator().next() : null;
    }
}
