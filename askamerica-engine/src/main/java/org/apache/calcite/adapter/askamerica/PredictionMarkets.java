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
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * Open Kalshi and Polymarket markets in one shape, and the screen that keeps the events
 * AskAmerica's data can price: an event settling on a quantity the warehouse carries, with a
 * market still contested and enough volume to trade.
 *
 * <p>Public market-data endpoints only — no account, no key, nothing here can trade.
 * Kalshi pages its whole listing by cursor (about 70 pages); Polymarket is read by tag, since
 * its full listing is several times larger and almost none of it settles on a measurement.
 *
 * <p>Every network read goes through a {@link Fetcher}, so the screen and the pricing built on
 * it are testable without the venues.
 */
final class PredictionMarkets {

    static final String KALSHI = "https://api.elections.kalshi.com/trade-api/v2";
    static final String POLYMARKET = "https://gamma-api.polymarket.com";
    /** Polymarket tags that carry the events worth screening. */
    static final String POLYMARKET_TAGS =
        "economy,fed,inflation,jobs,gdp,economic-policy,weather,climate,commodities";
    /** Kalshi's general taker fee: 0.07 * price * (1 - price), times the series multiplier. */
    static final double KALSHI_RATE = 0.07;
    /** A market still in play has a price inside this band. */
    static final double CONTESTED_LOW = 0.10;
    static final double CONTESTED_HIGH = 0.90;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int KALSHI_PAGE = 200;
    private static final int POLYMARKET_PAGE = 100;

    private PredictionMarkets() { }

    /** One GET returning a JSON document. */
    interface Fetcher {
        JsonNode get(String url) throws IOException;
    }

    /** An HTTP status the venue will keep returning; retrying it is pointless. */
    static final class HttpStatusException extends IOException {
        final int status;

        HttpStatusException(int status, String url) {
            super("HTTP " + status + " from " + url);
            this.status = status;
        }
    }

    /**
     * The live fetcher. Kalshi answers 429 after a burst of unauthenticated reads, so every
     * request is paced, and a 429 or a dropped connection is retried with a growing pause.
     * Any other HTTP status raises at once, and so does the last failed attempt.
     */
    static final class HttpFetcher implements Fetcher {
        private static final long PAUSE_MILLIS = 250;
        private static final int MAX_ATTEMPTS = 6;

        @Override public JsonNode get(String url) throws IOException {
            IOException last = null;
            for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
                HttpURLConnection c = (HttpURLConnection) URI.create(url).toURL()
                    .openConnection();
                try {
                    c.setConnectTimeout(15_000);
                    c.setReadTimeout(45_000);
                    c.setRequestProperty("User-Agent", "askamerica-engine");
                    c.setRequestProperty("Accept", "application/json");
                    c.setRequestProperty("Accept-Encoding", "gzip");
                    int code = c.getResponseCode();
                    if (code == 200) {
                        JsonNode body;
                        try (InputStream raw = c.getInputStream();
                             InputStream in = "gzip".equalsIgnoreCase(c.getContentEncoding())
                                 ? new GZIPInputStream(raw) : raw) {
                            body = MAPPER.readTree(in);
                        }
                        pause(PAUSE_MILLIS);
                        return body;
                    }
                    if (code != 429) {
                        throw new HttpStatusException(code, url);
                    }
                    last = new HttpStatusException(code, url);
                } catch (HttpStatusException e) {
                    throw e;
                // Not swallowed: a dropped connection is retried, and the last one is the
                // cause of the IOException thrown below when every attempt has failed.
                // fallback-guard: allow -- bounded retry, rethrown as the cause below
                } catch (IOException e) {
                    last = e;
                } finally {
                    c.disconnect();
                }
                pause(1500L * attempt);
            }
            throw new IOException("gave up after " + MAX_ATTEMPTS + " attempts: " + url, last);
        }

        private static void pause(long millis) throws IOException {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("interrupted while pacing venue requests");
            }
        }
    }

    // ─── Drivers ───────────────────────────────────────────────────────────────

    /** What AskAmerica's data can bring to the price, best first. */
    static final List<String> BASES =
        Collections.unmodifiableList(Arrays.asList(
            "release", "climatology", "policy", "market_price"));

    /** A kind of event, the pattern its title matches, and the tables that carry its data. */
    static final class Driver {
        final String name;
        final String basis;
        final Pattern pattern;
        final List<String> tables;

        Driver(String name, String basis, String pattern, String... tables) {
            this.name = name;
            this.basis = basis;
            this.pattern = Pattern.compile(pattern);
            this.tables = Collections.unmodifiableList(Arrays.asList(tables));
        }
    }

    /** Matched against the lowercased event title. First match wins, so the narrower
     *  pattern comes first. */
    static final List<Driver> DRIVERS = Collections.unmodifiableList(Arrays.asList(
        new Driver("drought", "release", "\\bdrought\\b", "weather.drought_monitor_weekly"),
        new Driver("crop_yield", "release",
            "\\b(corn|soybeans?|wheat|cotton) (yield|production)\\b",
            "ag.nass_crop_production", "weather.weather_daily_by_county",
            "weather.drought_monitor_weekly"),
        new Driver("livestock", "release", "\\bcattle (inventory|on feed)\\b",
            "ag.nass_livestock_inventory"),
        new Driver("food_prices", "release",
            "\\b(egg prices?|price of dozen eggs|food prices?|grocery inflation)\\b",
            "econ.food_cpi"),
        new Driver("tariffs", "policy", "\\b(tariff rate|effective tariff|tariff revenue)\\b",
            "econ.trade_statistics", "econ.trade_balance_summary"),
        new Driver("pce", "release", "\\bpce\\b", "econ.pce_inflation"),
        new Driver("inflation", "release", "\\b(cpi|inflation)\\b",
            "econ.inflation_metrics", "econ.food_cpi", "econ.fred_indicators"),
        new Driver("unemployment", "release", "\\b(unemployment( rate)?|u-3)\\b",
            "econ.employment_statistics", "econ.labor_market_conditions"),
        new Driver("payrolls", "release",
            "\\b(payrolls?|jobs added|nonfarm|jobs report|jobs numbers)\\b",
            "econ.employment_statistics"),
        new Driver("jobless_claims", "release", "\\b(jobless|initial) claims\\b",
            "econ.fred_indicators"),
        new Driver("output", "release", "\\b(gdp|recession)\\b",
            "econ.gdp_statistics", "econ.real_gdp_growth"),
        new Driver("mortgage_rate", "release", "\\bmortgage rates?\\b",
            "econ.housing_indicators", "econ.fred_indicators"),
        new Driver("policy_rate", "policy",
            "\\b(fed funds|federal funds|fed decision|rate cuts?|rate hikes?|fomc)\\b",
            "econ.fed_policy_dashboard", "econ.fred_indicators"),
        new Driver("treasury_yield", "market_price",
            "\\b(treasury yield|treasury spread|yield curve)\\b",
            "econ.treasury_yields", "econ.interest_rate_spreads"),
        new Driver("housing", "release",
            "\\b(housing starts|home sales|home prices?|building permits)\\b",
            "housing.house_price_index", "housing.building_permits", "econ.housing_indicators"),
        new Driver("federal_debt", "release", "\\b(national debt|federal debt)\\b",
            "econ.federal_debt"),
        new Driver("gas_storage", "release", "\\bnatural gas storage\\b",
            "energy.eia_natural_gas_storage"),
        new Driver("energy_price", "market_price",
            "\\b(crude|wti|brent|gas prices?|gasoline|natural gas)\\b",
            "econ.fred_indicators", "energy.eia_petroleum_stocks",
            "energy.eia_natural_gas_storage"),
        new Driver("temperature", "climatology", "(\\b(temperature|hottest|coldest)\\b|°)",
            "weather.ghcnd_daily", "weather.climate_normals_monthly"),
        new Driver("precipitation", "climatology",
            "\\b(rain|rainfall|snow|snowfall|precipitation)\\b",
            "weather.ghcnd_daily", "weather.climate_normals_monthly"),
        new Driver("storms", "climatology",
            "\\b(how many|number of)\\b.*\\b(hurricanes?|tornado(es)?|named storms?)\\b",
            "disasters.storm_events"),
        new Driver("campaign_finance", "release", "\\b(cash on hand|fundrais\\w+)\\b",
            "fec.committee_summaries"),
        new Driver("presidential_actions", "policy",
            "\\bhow many (presidential actions|executive orders)\\b",
            "fedregister.fr_documents")));

    /** Events about what someone says or who does something are not measurements. */
    private static final Pattern NOT_MEASURED =
        Pattern.compile("\\b(say|mention|dissents?|who will|nominee|streams)\\b");
    /** The warehouse is United States data. Tariff events name the trading partner and stay. */
    private static final Pattern FOREIGN = Pattern.compile(
        "\\b(china|chinese|mexico|russia|brazil|argentina|germany|german|japan|korea|india|"
        + "canada|uk|britain|eurozone|euro area|turkey|france|italy|spain|australia|venezuela|"
        + "south africa|montreal|calgary|toronto|vancouver|global|world)\\b");
    private static final Set<String> FOREIGN_EXEMPT =
        Collections.singleton("tariffs");
    /** Weather events are kept only for places the warehouse has stations for. */
    private static final Set<String> WEATHER_DRIVERS =
        new HashSet<>(Arrays.asList("temperature", "precipitation"));
    private static final Pattern US_PLACES = Pattern.compile(
        "\\b(new york|nyc|chicago|miami|austin|denver|los angeles|philadelphia|houston|dallas|"
        + "atlanta|seattle|san francisco|boston|phoenix|las vegas|washington|minneapolis|"
        + "detroit|new orleans|nashville|us|u\\.s\\.|united states|where will it rain)\\b");
    /** A team name or a stat line trips the driver patterns ("Hurricanes", "over 2.5"). */
    private static final String NEVER_SCREENED_CATEGORY = "Sports";

    /** Polymarket events carry many tags; the first of these found is the category. */
    private static final List<String> POLYMARKET_CATEGORIES = Arrays.asList(
        "Economy", "Fed", "Finance", "Business", "Weather", "Climate", "Health", "Science",
        "Tech", "AI", "Crypto", "Elections", "Politics", "Geopolitics", "World", "Sports",
        "Culture");

    /** The driver of an event title, or null when the title is not one AskAmerica can price. */
    static Driver driverOf(String title) {
        String text = title.toLowerCase(Locale.ROOT);
        if (NOT_MEASURED.matcher(text).find()) {
            return null;
        }
        for (Driver d : DRIVERS) {
            if (d.pattern.matcher(text).find()) {
                if (!FOREIGN_EXEMPT.contains(d.name) && FOREIGN.matcher(text).find()) {
                    return null;
                }
                if (WEATHER_DRIVERS.contains(d.name) && !US_PLACES.matcher(text).find()) {
                    return null;
                }
                return d;
            }
        }
        return null;
    }

    static Driver driverNamed(String name) {
        for (Driver d : DRIVERS) {
            if (d.name.equals(name)) {
                return d;
            }
        }
        return null;
    }

    // ─── Markets ───────────────────────────────────────────────────────────────

    /** One market of one event, in the shape both venues are normalized to. */
    static final class Market {
        String source;
        String category;
        String eventTitle;
        String title;
        Double yesPrice;
        Double yesBid;
        Double yesAsk;
        Double volume;
        Double volume24h;
        Double openInterest;
        String closeTime;
        String strikeType;
        Double floorStrike;
        Double capStrike;
        List<String> settlementSources = new ArrayList<>();
        String eventId;
        String marketId;
        String series;
        String rules;
        String url;

        ObjectNode toJson() {
            ObjectNode o = MAPPER.createObjectNode();
            o.put("market_id", marketId);
            o.put("title", title);
            putNumber(o, "yes_price", yesPrice);
            putNumber(o, "yes_bid", yesBid);
            putNumber(o, "yes_ask", yesAsk);
            o.put("strike_type", strikeType);
            putNumber(o, "floor_strike", floorStrike);
            putNumber(o, "cap_strike", capStrike);
            putNumber(o, "volume", volume);
            putNumber(o, "open_interest", openInterest);
            o.put("close_time", closeTime);
            return o;
        }
    }

    static void putNumber(ObjectNode o, String field, Double v) {
        if (v == null) {
            o.putNull(field);
        } else {
            o.put(field, v);
        }
    }

    static double round(double v, int places) {
        double scale = Math.pow(10, places);
        return Math.round(v * scale) / scale;
    }

    private static JsonNode required(JsonNode node, String field, String what) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) {
            throw new IllegalStateException(what + " has no '" + field + "'");
        }
        return v;
    }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    /** A number the venue sends as a JSON number or as a decimal string. */
    private static Double num(JsonNode v) {
        if (v == null || v.isNull()) {
            return null;
        }
        if (v.isNumber()) {
            return v.doubleValue();
        }
        String s = v.asText();
        return s.isEmpty() ? null : Double.valueOf(s);
    }

    /** Midpoint of a two-sided quote; the last trade when the book has one side or none. */
    static Double mid(Double bid, Double ask, Double last) {
        if (bid != null && ask != null && bid > 0 && ask < 1) {
            return round((bid + ask) / 2, 4);
        }
        return last;
    }

    /** The active markets of one Kalshi event document (nested markets included). */
    static List<Market> kalshiMarkets(JsonNode ev) {
        List<Market> rows = new ArrayList<>();
        String eventTitle = required(ev, "title", "Kalshi event").asText();
        String eventId = required(ev, "event_ticker", "Kalshi event").asText();
        String series = textOrNull(ev, "series_ticker");
        String category = ev.path("category").asText("");
        List<String> sources = new ArrayList<>();
        for (JsonNode s : ev.path("settlement_sources")) {
            // A settlement source is {name, url}; a few carry only the url.
            sources.add(s.hasNonNull("name") ? s.get("name").asText()
                : required(s, "url", "Kalshi settlement source").asText());
        }
        for (JsonNode m : ev.path("markets")) {
            if (!"active".equals(m.path("status").asText())) {
                continue;
            }
            Market row = new Market();
            row.source = "kalshi";
            row.category = category;
            row.eventTitle = eventTitle;
            // A market with no title of its own is one outcome of its event.
            String own = textOrNull(m, "title");
            row.title = own != null && !own.isEmpty() ? own
                : eventTitle + " — " + required(m, "yes_sub_title", "Kalshi market").asText();
            row.yesBid = num(m.get("yes_bid_dollars"));
            row.yesAsk = num(m.get("yes_ask_dollars"));
            row.yesPrice = mid(row.yesBid, row.yesAsk, num(m.get("last_price_dollars")));
            row.volume = num(m.get("volume_fp"));
            row.volume24h = num(m.get("volume_24h_fp"));
            row.openInterest = num(m.get("open_interest_fp"));
            row.closeTime = textOrNull(m, "close_time");
            row.strikeType = textOrNull(m, "strike_type");
            row.floorStrike = num(m.get("floor_strike"));
            row.capStrike = num(m.get("cap_strike"));
            row.settlementSources = sources;
            row.eventId = eventId;
            row.marketId = required(m, "ticker", "Kalshi market").asText();
            row.series = series;
            StringBuilder rules = new StringBuilder();
            for (String f : new String[]{"rules_primary", "rules_secondary"}) {
                String part = textOrNull(m, f);
                if (part != null && !part.isEmpty()) {
                    rules.append(rules.length() > 0 ? " " : "").append(part);
                }
            }
            row.rules = rules.toString();
            row.url = "https://kalshi.com/markets/"
                + (series == null ? "" : series.toLowerCase(Locale.ROOT));
            rows.add(row);
        }
        return rows;
    }

    /** The open markets of one Polymarket event document not already in {@code seen}. */
    static List<Market> polymarketMarkets(JsonNode ev, Set<String> seen) throws IOException {
        List<Market> rows = new ArrayList<>();
        String eventTitle = required(ev, "title", "Polymarket event").asText();
        String eventId = required(ev, "id", "Polymarket event").asText();
        List<String> tags = new ArrayList<>();
        for (JsonNode t : ev.path("tags")) {
            tags.add(required(t, "label", "Polymarket tag").asText());
        }
        String category = tags.isEmpty() ? "" : tags.get(0);
        for (String c : POLYMARKET_CATEGORIES) {
            if (tags.contains(c)) {
                category = c;
                break;
            }
        }
        for (JsonNode m : ev.path("markets")) {
            String id = required(m, "id", "Polymarket market").asText();
            if (m.path("closed").asBoolean(false) || !m.path("active").asBoolean(false)
                    || !seen.add(id)) {
                continue;
            }
            JsonNode outcomes = m.hasNonNull("outcomes")
                ? MAPPER.readTree(m.get("outcomes").asText()) : MAPPER.createArrayNode();
            JsonNode prices = m.hasNonNull("outcomePrices")
                ? MAPPER.readTree(m.get("outcomePrices").asText()) : MAPPER.createArrayNode();
            Market row = new Market();
            row.source = "polymarket";
            row.category = category;
            row.eventTitle = eventTitle;
            // Binary Yes/No quotes the Yes side; any other pair quotes its first outcome,
            // which is named in the title so the price is not mistaken for "Yes".
            row.title = required(m, "question", "Polymarket market").asText();
            if (outcomes.size() > 0 && !"Yes".equals(outcomes.get(0).asText())) {
                row.title += " [price is for: " + outcomes.get(0).asText() + "]";
            }
            row.yesBid = num(m.get("bestBid"));
            row.yesAsk = num(m.get("bestAsk"));
            Double last = prices.size() > 0 ? num(prices.get(0)) : num(m.get("lastTradePrice"));
            row.yesPrice = mid(row.yesBid, row.yesAsk, last);
            row.volume = num(m.get("volumeNum"));
            row.volume24h = num(m.get("volume24hr"));
            row.closeTime = textOrNull(m, "endDate");
            String src = textOrNull(m, "resolutionSource");
            if (src == null || src.isEmpty()) {
                src = textOrNull(ev, "resolutionSource");
            }
            if (src != null && !src.isEmpty()) {
                row.settlementSources.add(src);
            }
            row.eventId = eventId;
            row.marketId = id;
            row.series = textOrNull(ev, "seriesSlug");
            String description = textOrNull(m, "description");
            row.rules = description == null ? "" : description;
            row.url = "https://polymarket.com/event/"
                + required(ev, "slug", "Polymarket event").asText();
            rows.add(row);
        }
        return rows;
    }

    // ─── Listing ───────────────────────────────────────────────────────────────

    /** The markets of every open event whose title matches a driver, and what was read to
     *  find them. */
    static final class Listing {
        final List<Market> rows = new ArrayList<>();
        Instant fetchedAt;
        int kalshiMarkets;
        int kalshiPages;
        int polymarketMarkets;
        int polymarketPages;
    }

    /** True for an event the screen could ever keep; the rest are dropped as they are read,
     *  so a full listing — most of it sports — is never held in memory. */
    private static boolean screenable(String category, String eventTitle) {
        return !NEVER_SCREENED_CATEGORY.equals(category) && driverOf(eventTitle) != null;
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    static void pullKalshi(Fetcher fetcher, Listing into) throws IOException {
        String cursor = "";
        while (true) {
            String url = KALSHI + "/events?limit=" + KALSHI_PAGE
                + "&status=open&with_nested_markets=true"
                + (cursor.isEmpty() ? "" : "&cursor=" + enc(cursor));
            JsonNode doc = fetcher.get(url);
            List<Market> page = new ArrayList<>();
            int seen = 0;
            for (JsonNode ev : required(doc, "events", "Kalshi listing")) {
                List<Market> ms = kalshiMarkets(ev);
                seen += ms.size();
                if (!ms.isEmpty() && screenable(ms.get(0).category, ms.get(0).eventTitle)) {
                    page.addAll(ms);
                }
            }
            synchronized (into) {
                into.rows.addAll(page);
                into.kalshiMarkets += seen;
                into.kalshiPages++;
            }
            cursor = doc.path("cursor").asText("");
            if (cursor.isEmpty()) {
                return;
            }
        }
    }

    static void pullPolymarket(Fetcher fetcher, String tags, Listing into) throws IOException {
        // The union of the tags, each market listed once.
        Set<String> seenIds = new HashSet<>();
        for (String tag : tags.split(",")) {
            String cursor = "";
            while (true) {
                // /events refuses offsets past 2000; /events/keyset pages by cursor.
                String url = POLYMARKET + "/events/keyset?limit=" + POLYMARKET_PAGE
                    + "&closed=false&order=volume24hr&ascending=false&tag_slug="
                    + enc(tag.trim())
                    + (cursor.isEmpty() ? "" : "&after_cursor=" + enc(cursor));
                JsonNode doc = fetcher.get(url);
                JsonNode events = required(doc, "events", "Polymarket listing");
                List<Market> page = new ArrayList<>();
                int seen = 0;
                for (JsonNode ev : events) {
                    List<Market> ms = polymarketMarkets(ev, seenIds);
                    seen += ms.size();
                    if (!ms.isEmpty()
                            && screenable(ms.get(0).category, ms.get(0).eventTitle)) {
                        page.addAll(ms);
                    }
                }
                synchronized (into) {
                    into.rows.addAll(page);
                    into.polymarketMarkets += seen;
                    into.polymarketPages++;
                }
                cursor = doc.path("next_cursor").asText("");
                if (cursor.isEmpty() || events.size() == 0) {
                    break;
                }
            }
        }
    }

    /** Thrown when the listing is still being read and the caller should ask again. */
    static final class ListingPendingException extends Exception {
        final int pagesRead;

        ListingPendingException(int pagesRead) {
            super("the market listing is still loading (" + pagesRead + " pages read)");
            this.pagesRead = pagesRead;
        }
    }

    /**
     * The listing, read once and kept for {@link #ttl}. A full read is about a minute — longer
     * than an MCP host will reliably wait on one tool call — so it runs on its own thread: a
     * caller waits a bounded time and is otherwise told the read is in progress, and the next
     * call picks up the finished listing.
     */
    static final class ListingCache {
        private final Fetcher fetcher;
        private final Duration ttl;
        private final ExecutorService pool = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "market-listing");
            t.setDaemon(true);
            return t;
        });
        private Listing current;
        private Listing loadingInto;
        private Future<Listing> loading;

        ListingCache(Fetcher fetcher, Duration ttl) {
            this.fetcher = fetcher;
            this.ttl = ttl;
        }

        /**
         * The current listing, reading it first when there is none, it is older than the TTL,
         * or {@code refresh} is set.
         *
         * @throws ListingPendingException when the read has not finished within
         *     {@code waitMillis}; a failed read throws its cause and is retried by the next call
         */
        Listing get(boolean refresh, long waitMillis) throws Exception {
            Future<Listing> pending;
            Listing partial;
            synchronized (this) {
                boolean fresh = current != null
                    && Duration.between(current.fetchedAt, Instant.now()).compareTo(ttl) < 0;
                if (loading == null && fresh && !refresh) {
                    return current;
                }
                if (loading == null) {
                    final Listing target = new Listing();
                    loadingInto = target;
                    loading = pool.submit(() -> load(target));
                }
                pending = loading;
                partial = loadingInto;
            }
            try {
                Listing done = pending.get(waitMillis, TimeUnit.MILLISECONDS);
                finish(pending, done);
                return done;
            } catch (TimeoutException e) {
                int pages;
                synchronized (partial) {
                    pages = partial.kalshiPages + partial.polymarketPages;
                }
                throw new ListingPendingException(pages);
            } catch (ExecutionException e) {
                finish(pending, null);
                Throwable cause = e.getCause();
                if (cause instanceof Exception) {
                    throw (Exception) cause;
                }
                throw e;
            }
        }

        private synchronized void finish(Future<Listing> pending, Listing done) {
            if (loading == pending) {
                loading = null;
                loadingInto = null;
                if (done != null) {
                    current = done;
                }
            }
        }

        private Listing load(Listing target) throws Exception {
            Future<?> poly = pool.submit(() -> {
                pullPolymarket(fetcher, POLYMARKET_TAGS, target);
                return null;
            });
            try {
                pullKalshi(fetcher, target);
            } catch (IOException | RuntimeException e) {
                poly.cancel(true);
                throw e;
            }
            try {
                poly.get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof Exception) {
                    throw (Exception) cause;
                }
                throw e;
            }
            target.fetchedAt = Instant.now();
            return target;
        }
    }

    // ─── Events ────────────────────────────────────────────────────────────────

    /** One event: its priced markets and what the screen measured on them. */
    static final class Event {
        String source;
        String eventId;
        String eventTitle;
        String series;
        String url;
        /** Null for an event fetched by id whose title matches no driver. */
        Driver driver;
        List<String> settlementSources;
        String rules;
        String closeTime;
        int contested;
        long volume;
        long volume24h;
        Double medianSpread;
        Double impliedMedian;
        List<ObjectNode> locks = new ArrayList<>();
        /** Every priced market, most traded first. */
        List<Market> legs = new ArrayList<>();

        /** The event without its markets or rules text: one line of a list. */
        ObjectNode toSummaryJson() {
            ObjectNode o = MAPPER.createObjectNode();
            o.put("source", source);
            o.put("event_id", eventId);
            o.put("event_title", eventTitle);
            o.put("series", series);
            o.put("url", url);
            if (driver == null) {
                o.putNull("driver");
                o.putNull("basis");
            } else {
                o.put("driver", driver.name);
                o.put("basis", driver.basis);
                ArrayNode tables = o.putArray("govdata_tables");
                for (String t : driver.tables) {
                    tables.add(t);
                }
                o.put("forecast_with", forecastWith(driver.basis));
            }
            o.put("close_time", closeTime);
            o.put("markets", legs.size());
            o.put("contested", contested);
            o.put("volume", volume);
            o.put("volume_24h", volume24h);
            putNumber(o, "median_spread", medianSpread);
            putNumber(o, "implied_median", impliedMedian);
            ArrayNode lockArr = o.putArray("locks");
            for (ObjectNode l : locks) {
                lockArr.add(l);
            }
            return o;
        }

        /** The event with its settlement rules and every priced market. */
        ObjectNode toJson() {
            ObjectNode o = toSummaryJson();
            ArrayNode src = o.putArray("settlement_sources");
            for (String s : settlementSources) {
                src.add(s);
            }
            o.put("rules", rules);
            ArrayNode legArr = o.putArray("legs");
            for (Market m : legs) {
                legArr.add(m.toJson());
            }
            return o;
        }
    }

    /** Which forecasting tool of this server fits an event of each basis. */
    static String forecastWith(String basis) {
        switch (basis) {
            case "release":
                return "forecast_market_event with the series, table and transform given, "
                    + "or arima_forecast on the settlement series (forecast and "
                    + "forecast_std_error at the settlement period), or samples_sql of "
                    + "historical period-over-period changes applied to the latest level";
            case "climatology":
                return "samples_sql: the same station and calendar window in every past year";
            case "market_price":
                return "volatility_forecast on the price series (a band, not a direction)";
            case "policy":
                return "no forecasting tool applies to a decision; price it only with an "
                    + "explicit, sourced probability";
            default:
                throw new IllegalArgumentException("unknown basis " + basis);
        }
    }

    static Instant closeInstant(String closeTime) {
        return OffsetDateTime.parse(closeTime).toInstant();
    }

    /** Where a 'greater than' strike ladder crosses 50%: the market's median for the series. */
    static Double impliedMedian(List<Market> legs) {
        List<Market> ladder = new ArrayList<>();
        for (Market m : legs) {
            if (("greater".equals(m.strikeType) || "greater_or_equal".equals(m.strikeType))
                    && m.floorStrike != null && m.yesPrice != null) {
                ladder.add(m);
            }
        }
        ladder.sort(Comparator.comparingDouble((Market m) -> m.floorStrike)
            .thenComparingDouble(m -> m.yesPrice));
        for (int i = 0; i + 1 < ladder.size(); i++) {
            double k1 = ladder.get(i).floorStrike;
            double p1 = ladder.get(i).yesPrice;
            double k2 = ladder.get(i + 1).floorStrike;
            double p2 = ladder.get(i + 1).yesPrice;
            if (p1 >= 0.5 && 0.5 > p2) {
                return round(k1 + (k2 - k1) * (p1 - 0.5) / (p1 - p2), 3);
            }
        }
        return null;
    }

    /**
     * Ladder pairs priced inconsistently: YES on the lower strike is offered for less than YES
     * on the higher strike is bid. Buying the first and selling the second cannot lose.
     */
    static List<ObjectNode> locks(List<Market> legs) {
        List<Market> ladder = new ArrayList<>();
        for (Market m : legs) {
            if ("greater".equals(m.strikeType) && m.floorStrike != null && m.yesAsk != null
                    && m.yesBid != null) {
                ladder.add(m);
            }
        }
        ladder.sort(Comparator.comparingDouble((Market m) -> m.floorStrike));
        List<ObjectNode> out = new ArrayList<>();
        for (int i = 0; i < ladder.size(); i++) {
            Market low = ladder.get(i);
            for (int j = i + 1; j < ladder.size(); j++) {
                Market high = ladder.get(j);
                if (high.floorStrike > low.floorStrike && high.yesBid > low.yesAsk
                        && low.yesAsk > 0) {
                    ObjectNode l = MAPPER.createObjectNode();
                    l.put("buy_yes", low.marketId);
                    l.put("sell_yes", high.marketId);
                    l.put("gap", round(high.yesBid - low.yesAsk, 4));
                    out.add(l);
                }
            }
        }
        return out;
    }

    /**
     * Builds an event from its markets, or returns null when none of them is priced.
     * {@code driver} may be null.
     */
    static Event eventOf(List<Market> markets, Driver driver) {
        List<Market> priced = new ArrayList<>();
        for (Market m : markets) {
            if (m.yesPrice != null) {
                priced.add(m);
            }
        }
        if (priced.isEmpty()) {
            return null;
        }
        Market first = markets.get(0);
        Event ev = new Event();
        ev.source = first.source;
        ev.eventId = first.eventId;
        ev.eventTitle = first.eventTitle;
        ev.series = first.series;
        ev.url = first.url;
        ev.driver = driver;
        ev.settlementSources = first.settlementSources;
        ev.rules = first.rules;
        double volume = 0;
        double volume24h = 0;
        List<Double> spreads = new ArrayList<>();
        for (Market m : priced) {
            if (m.closeTime != null && (ev.closeTime == null
                    || m.closeTime.compareTo(ev.closeTime) < 0)) {
                ev.closeTime = m.closeTime;
            }
            volume += m.volume == null ? 0 : m.volume;
            volume24h += m.volume24h == null ? 0 : m.volume24h;
            if (m.yesPrice >= CONTESTED_LOW && m.yesPrice <= CONTESTED_HIGH) {
                ev.contested++;
                if (m.yesAsk != null && m.yesBid != null) {
                    spreads.add(m.yesAsk - m.yesBid);
                }
            }
        }
        ev.volume = Math.round(volume);
        ev.volume24h = Math.round(volume24h);
        if (!spreads.isEmpty()) {
            Collections.sort(spreads);
            int n = spreads.size();
            double median = n % 2 == 1 ? spreads.get(n / 2)
                : (spreads.get(n / 2 - 1) + spreads.get(n / 2)) / 2;
            ev.medianSpread = round(median, 4);
        }
        ev.impliedMedian = impliedMedian(priced);
        ev.locks = locks(priced);
        priced.sort(Comparator.comparingDouble(
            (Market m) -> m.volume == null ? 0 : m.volume).reversed());
        ev.legs = priced;
        return ev;
    }

    /**
     * Candidate events: closing between {@code minDays} and {@code within} days from
     * {@code now}, with a contested market and at least {@code minVolume} traded. Best basis
     * first, then most traded in the last 24 hours.
     */
    static List<Event> screen(List<Market> rows, Instant now, int within, int minDays,
            double minVolume) {
        Instant first = now.plus(Duration.ofDays(minDays));
        Instant last = now.plus(Duration.ofDays(within));
        Map<String, List<Market>> byEvent = new LinkedHashMap<>();
        Map<String, Driver> drivers = new LinkedHashMap<>();
        for (Market row : rows) {
            if (NEVER_SCREENED_CATEGORY.equals(row.category) || row.closeTime == null
                    || row.closeTime.isEmpty()) {
                continue;
            }
            Instant close = closeInstant(row.closeTime);
            if (close.isBefore(first) || close.isAfter(last)) {
                continue;
            }
            Driver hit = driverOf(row.eventTitle);
            if (hit == null) {
                continue;
            }
            String key = row.source + "\u0000" + row.eventId;
            byEvent.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            drivers.putIfAbsent(key, hit);
        }
        List<Event> out = new ArrayList<>();
        for (Map.Entry<String, List<Market>> e : byEvent.entrySet()) {
            Event ev = eventOf(e.getValue(), drivers.get(e.getKey()));
            if (ev == null || ev.contested == 0 || ev.volume < minVolume) {
                continue;
            }
            out.add(ev);
        }
        out.sort(Comparator.comparingInt((Event e) -> BASES.indexOf(e.driver.basis))
            .thenComparing(Comparator.comparingLong((Event e) -> e.volume24h).reversed()));
        return out;
    }

    // ─── One event, live ───────────────────────────────────────────────────────

    /** An event read from its venue just now, with the taker fee rate of each market. */
    static final class LiveEvent {
        Event event;
        /** market_id to rate, where fee per contract = rate * price * (1 - price). */
        Map<String, Double> feeRates = new LinkedHashMap<>();
    }

    /**
     * Reads one event and its fee terms from its venue.
     *
     * <p>Kalshi: rate = 0.07 * the series' fee_multiplier. Polymarket: the market's
     * feeSchedule.rate, 0 where feesEnabled is false. A fee type this does not model — a
     * Kalshi fee_type that is not quadratic, a Polymarket exponent other than 1 — raises
     * rather than being priced as if it were.
     */
    static LiveEvent fetchEvent(Fetcher fetcher, String source, String eventId)
            throws IOException {
        LiveEvent live = new LiveEvent();
        List<Market> markets;
        if ("kalshi".equals(source)) {
            JsonNode doc = fetcher.get(KALSHI + "/events/" + enc(eventId)
                + "?with_nested_markets=true");
            markets = kalshiMarkets(required(doc, "event", "Kalshi event response"));
            if (!markets.isEmpty()) {
                String series = markets.get(0).series;
                if (series == null || series.isEmpty()) {
                    throw new IllegalStateException("Kalshi event " + eventId
                        + " names no series, so its fee terms cannot be read");
                }
                JsonNode s = required(fetcher.get(KALSHI + "/series/" + enc(series)),
                    "series", "Kalshi series response");
                String feeType = required(s, "fee_type", "Kalshi series").asText();
                if (!feeType.startsWith("quadratic")) {
                    throw new IllegalStateException("Kalshi series " + series
                        + " has fee_type " + feeType + "; only quadratic is modelled");
                }
                double rate = KALSHI_RATE
                    * required(s, "fee_multiplier", "Kalshi series").asDouble();
                for (Market m : markets) {
                    live.feeRates.put(m.marketId, rate);
                }
            }
        } else if ("polymarket".equals(source)) {
            JsonNode ev = fetcher.get(POLYMARKET + "/events/" + enc(eventId));
            markets = polymarketMarkets(ev, new HashSet<>());
            for (JsonNode m : ev.path("markets")) {
                String id = required(m, "id", "Polymarket market").asText();
                if (!required(m, "feesEnabled", "Polymarket market " + id).asBoolean()) {
                    live.feeRates.put(id, 0.0);
                    continue;
                }
                JsonNode schedule = required(m, "feeSchedule", "Polymarket market " + id);
                double exponent = required(schedule, "exponent", "Polymarket fee schedule")
                    .asDouble();
                if (exponent != 1) {
                    throw new IllegalStateException("Polymarket market " + id
                        + " has fee exponent " + exponent + "; only 1 is modelled");
                }
                live.feeRates.put(id,
                    required(schedule, "rate", "Polymarket fee schedule").asDouble());
            }
        } else {
            throw new IllegalArgumentException(
                "source must be 'kalshi' or 'polymarket', got '" + source + "'");
        }
        Event ev = markets.isEmpty() ? null : eventOf(markets,
            driverOf(markets.get(0).eventTitle));
        if (ev == null) {
            throw new IllegalStateException(source + " event " + eventId
                + " has no open market with a price");
        }
        live.event = ev;
        return live;
    }

    /** Taker fee per contract bought at {@code price}. */
    static double takerFee(double rate, double price) {
        return rate * price * (1 - price);
    }
}
