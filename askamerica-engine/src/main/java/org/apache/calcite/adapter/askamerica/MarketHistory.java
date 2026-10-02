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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * What Kalshi and Polymarket have already done: the markets a series settled, the price a
 * market traded at over time, and the order book it shows now — each in one venue-neutral
 * shape.
 *
 * <p>Three capabilities, each for both venues. {@link #settledMarkets} lists settled markets
 * with their outcome and settlement value; {@link #priceHistory} reads a market's price
 * series; {@link #orderBook} reads its book, normalised so both venues read the same way,
 * and {@link OrderBook#fill} prices a sized order against it. {@link #priceHistoryTool} is
 * the {@code market_price_history} tool built on them.
 *
 * <p>Public market-data endpoints only. Every read goes through a
 * {@link PredictionMarkets.Fetcher}. A field the venue should send and does not raises an
 * error naming it; nothing here substitutes a value for a missing one.
 */
final class MarketHistory {

  /** Polymarket's order-book and price-history host; its listing host is gamma. */
  static final String CLOB = "https://clob.polymarket.com";

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int KALSHI_PAGE = 200;
  private static final int POLYMARKET_PAGE = 100;
  /** Kalshi answers at most this many candlesticks to one request. */
  private static final int KALSHI_CANDLE_CAP = 5000;
  private static final double EPS = 1e-9;
  private static final int MAX_WINDOW_DAYS = 365;
  private static final int DEFAULT_WINDOW_DAYS = 30;
  private static final Set<String> TOOL_ARGS = new HashSet<>(Arrays.asList(
      "source", "market_id", "event_id", "series", "window_days", "interval"));

  private final PredictionMarkets.Fetcher fetcher;
  private final Supplier<Instant> clock;

  MarketHistory(PredictionMarkets.Fetcher fetcher, Supplier<Instant> clock) {
    this.fetcher = fetcher;
    this.clock = clock;
  }

  // ─── Shapes ────────────────────────────────────────────────────────────────

  /** Sampling step of a price series. */
  enum Interval {
    HOUR("hour", 60), DAY("day", 1440);

    final String label;
    final int minutes;

    Interval(String label, int minutes) {
      this.label = label;
      this.minutes = minutes;
    }

    static Interval of(String label) {
      for (Interval i : values()) {
        if (i.label.equals(label)) {
          return i;
        }
      }
      throw new IllegalArgumentException("interval must be 'hour' or 'day', got '"
          + label + "'");
    }
  }

  /** One settled market, in the shape both venues are normalized to. */
  static final class SettledMarket {
    String source;
    /** Kalshi: listed by the historical tier, so its candles are read from there too. */
    boolean historical;
    String eventId;
    /** Null on Kalshi, whose market payload carries no event title. */
    String eventTitle;
    String series;
    String marketId;
    String title;
    /** Kalshi: its strike type and strikes. Polymarket: the market's label in its event. */
    String strikeType;
    Double floorStrike;
    Double capStrike;
    String condition;
    /**
     * "yes", "no", or on Polymarket the winning outcome's name; "unresolved" if none won. On
     * Kalshi a result that is neither yes nor no (void, scalar, empty) is kept as sent, with
     * {@link #scoreable} false.
     */
    String outcome;
    /** False when the market has no yes or no outcome to score a probability against. */
    boolean scoreable = true;
    /** Why {@link #scoreable} is false; null otherwise. */
    String notScoreableReason;
    /** The market's rules text where the venue sends it (Kalshi rules_primary and
     *  rules_secondary, Polymarket description); null when it sends none. */
    String rules;
    /** The value the market settled on where the venue reports one (Kalshi); else null. */
    String settlementValue;
    String closeTime;
    /** Null on Polymarket, which reports one close time only. */
    String settleTime;
    /** Last traded price of Yes, in dollars. */
    Double lastPrice;

    ObjectNode toJson() {
      ObjectNode o = MAPPER.createObjectNode();
      o.put("source", source);
      o.put("event_id", eventId);
      o.put("event_title", eventTitle);
      o.put("series", series);
      o.put("market_id", marketId);
      o.put("title", title);
      o.put("strike_type", strikeType);
      PredictionMarkets.putNumber(o, "floor_strike", floorStrike);
      PredictionMarkets.putNumber(o, "cap_strike", capStrike);
      o.put("condition", condition);
      o.put("outcome", outcome);
      o.put("scoreable", scoreable);
      o.put("not_scoreable_reason", notScoreableReason);
      o.put("settlement_value", settlementValue);
      o.put("close_time", closeTime);
      o.put("settle_time", settleTime);
      PredictionMarkets.putNumber(o, "last_price", lastPrice);
      return o;
    }
  }

  /** A market resolved to what each venue's history and book endpoints are keyed on. */
  static final class MarketRef {
    String source;
    String marketId;
    String title;
    /** Kalshi series ticker; null on Polymarket. */
    String series;
    /** Polymarket CLOB token id of the Yes outcome; null on Kalshi. */
    String yesTokenId;
    /** Kalshi: the market is past the venue's historical cutoff, so its candles are read
     *  from the historical tier. */
    boolean historical;
    /** True when the venue is still taking orders, so a book can be read. */
    boolean open;
    /** The venue's own status text, for the report. */
    String status;
  }

  /** One point of a price series. Fields the venue gives no value for are null. */
  static final class PricePoint {
    long epochSecond;
    /** Last traded price of Yes in dollars; null in a Kalshi period with no trade. */
    Double price;
    /**
     * First, highest and lowest traded price of the period; {@link #price} is its close.
     * Null where nothing traded and where the venue gives no finer points to take them from.
     */
    Double open;
    Double high;
    Double low;
    /** Closing Yes bid and ask; Kalshi only. */
    Double yesBid;
    Double yesAsk;
    /** Contracts traded in the period; null where the venue gives none (Polymarket). */
    Double volume;

    ObjectNode toJson() {
      ObjectNode o = MAPPER.createObjectNode();
      o.put("t", Instant.ofEpochSecond(epochSecond).toString());
      PredictionMarkets.putNumber(o, "price", price);
      if (open != null) {
        o.put("open", open);
        o.put("high", high);
        o.put("low", low);
      }
      PredictionMarkets.putNumber(o, "yes_bid", yesBid);
      PredictionMarkets.putNumber(o, "yes_ask", yesAsk);
      PredictionMarkets.putNumber(o, "volume", volume);
      return o;
    }
  }

  /** The side of the Yes contract an order takes. */
  enum Side {
    BUY_YES, SELL_YES, BUY_NO, SELL_NO;

    boolean buys() {
      return this == BUY_YES || this == BUY_NO;
    }

    static Side of(String name) {
      for (Side s : values()) {
        if (s.name().equalsIgnoreCase(name)) {
          return s;
        }
      }
      throw new IllegalArgumentException("side must be one of buy_yes, sell_yes, buy_no, "
          + "sell_no, got '" + name + "'");
    }
  }

  /** One price level of a book: dollars per contract and contracts resting there. */
  static final class Level {
    final double price;
    final double size;

    Level(double price, double size) {
      this.price = price;
      this.size = size;
    }
  }

  /** What an order of a given size fills at, against a book. */
  static final class Fill {
    double requested;
    double filled;
    /** Dollars paid (buy) or received (sell) for the filled contracts. */
    double cost;
    /** Cost per filled contract; null when nothing filled. */
    Double averagePrice;
    /** Contracts resting at or better than the limit price (the whole book if no limit). */
    double availableAtLimit;
    /** False when the book, or the limit, ran out before the requested size. */
    boolean complete;

    ObjectNode toJson() {
      ObjectNode o = MAPPER.createObjectNode();
      o.put("requested", requested);
      o.put("filled", filled);
      o.put("cost", cost);
      PredictionMarkets.putNumber(o, "average_price", averagePrice);
      o.put("available_at_limit", availableAtLimit);
      o.put("complete", complete);
      return o;
    }
  }

  /**
   * The book of the Yes contract. {@link #bids} are dollars buyers will pay, highest first;
   * {@link #asks} are dollars sellers want, lowest first. The No contract is the mirror
   * image: buying No at p is selling Yes at 1 - p, which {@link #fill} handles.
   */
  static final class OrderBook {
    String source;
    String marketId;
    List<Level> bids = new ArrayList<>();
    List<Level> asks = new ArrayList<>();

    Level bestBid() {
      return bids.isEmpty() ? null : bids.get(0);
    }

    Level bestAsk() {
      return asks.isEmpty() ? null : asks.get(0);
    }

    /**
     * Prices an order of {@code contracts} against this book, best price first.
     *
     * <p>BUY_YES takes the asks; SELL_YES the bids; BUY_NO takes No at 1 - each Yes bid;
     * SELL_NO gives No at 1 - each Yes ask. With a {@code limit} (dollars per contract of
     * the side's own contract: a buy pays at most it, a sell takes at least it) levels
     * beyond it are not touched. A partial book fills what it has and reports
     * {@code complete = false}; an empty one fills 0 with a null average price.
     *
     * @param side the order's side
     * @param contracts size wanted, above 0
     * @param limit worst acceptable price, or null for none
     */
    Fill fill(Side side, double contracts, Double limit) {
      if (!(contracts > 0)) {
        throw new IllegalArgumentException("contracts must be above 0, got " + contracts);
      }
      Fill f = new Fill();
      f.requested = contracts;
      double remaining = contracts;
      for (Level l : levels(side)) {
        if (limit != null && (side.buys() ? l.price > limit + EPS : l.price < limit - EPS)) {
          break;
        }
        f.availableAtLimit += l.size;
        double take = Math.min(remaining, l.size);
        f.filled += take;
        f.cost += take * l.price;
        remaining -= take;
      }
      f.averagePrice = f.filled > 0 ? f.cost / f.filled : null;
      f.complete = f.filled >= contracts - EPS;
      return f;
    }

    /** The levels an order of {@code side} walks, best first, in that side's own price. */
    private List<Level> levels(Side side) {
      List<Level> out = new ArrayList<>();
      switch (side) {
      case BUY_YES:
        return asks;
      case SELL_YES:
        return bids;
      case BUY_NO:
        for (Level l : bids) {
          out.add(new Level(PredictionMarkets.round(1 - l.price, 6), l.size));
        }
        return out;
      default:
        for (Level l : asks) {
          out.add(new Level(PredictionMarkets.round(1 - l.price, 6), l.size));
        }
        return out;
      }
    }

    ObjectNode topJson() {
      ObjectNode o = MAPPER.createObjectNode();
      Level b = bestBid();
      Level a = bestAsk();
      PredictionMarkets.putNumber(o, "best_bid", b == null ? null : b.price);
      PredictionMarkets.putNumber(o, "best_bid_size", b == null ? null : b.size);
      PredictionMarkets.putNumber(o, "best_ask", a == null ? null : a.price);
      PredictionMarkets.putNumber(o, "best_ask_size", a == null ? null : a.size);
      PredictionMarkets.putNumber(o, "spread",
          a == null || b == null ? null : PredictionMarkets.round(a.price - b.price, 6));
      o.put("bid_levels", bids.size());
      o.put("ask_levels", asks.size());
      return o;
    }
  }

  // ─── Settled markets ───────────────────────────────────────────────────────

  /**
   * Settled markets of one series or driver, most recent first, at most {@code limit}.
   *
   * <p>Kalshi: {@code series} is the series ticker (KXCPI); read from /markets with
   * status=settled, paged by cursor, outcome from {@code result}, settlement value from
   * {@code expiration_value}. Polymarket: {@code series} is the gamma series slug
   * (fed-interest-rates); read from /events with closed=true, paged by offset, outcome from
   * the market's {@code outcomePrices}; the venue reports no settlement value, so it is null.
   *
   * @param source "kalshi" or "polymarket"
   * @param series Kalshi series ticker or Polymarket series slug
   * @param limit most markets to return, above 0
   */
  static List<SettledMarket> settledMarkets(PredictionMarkets.Fetcher fetcher, String source,
      String series, int limit) throws IOException {
    if (limit < 1) {
      throw new IllegalArgumentException("limit must be at least 1, got " + limit);
    }
    if ("kalshi".equals(source)) {
      return kalshiSettled(fetcher, series, limit);
    } else if ("polymarket".equals(source)) {
      return polymarketSettled(fetcher, series, limit);
    }
    throw badSource(source);
  }

  /**
   * Kalshi keeps a market on /markets until its settlement passes the venue's historical
   * cutoff, then serves it only from /historical/markets. Both are most recent first and the
   * live tier is the newer, so it is read first and the historical tier makes up the rest.
   */
  private static List<SettledMarket> kalshiSettled(PredictionMarkets.Fetcher fetcher,
      String series, int limit) throws IOException {
    List<SettledMarket> out = new ArrayList<>();
    Set<String> seen = new HashSet<>();
    kalshiSettledTier(fetcher, "/markets?status=settled&series_ticker=", false, series, limit,
        out, seen);
    kalshiSettledTier(fetcher, "/historical/markets?series_ticker=", true, series, limit, out,
        seen);
    return out;
  }

  private static void kalshiSettledTier(PredictionMarkets.Fetcher fetcher, String path,
      boolean historical, String series, int limit, List<SettledMarket> out, Set<String> seen)
      throws IOException {
    String cursor = "";
    while (out.size() < limit) {
      String url = PredictionMarkets.KALSHI + path + enc(series) + "&limit="
          + Math.min(KALSHI_PAGE, limit - out.size())
          + (cursor.isEmpty() ? "" : "&cursor=" + enc(cursor));
      JsonNode doc = fetcher.get(url);
      JsonNode markets = required(doc, "markets", "Kalshi markets response");
      for (JsonNode m : markets) {
        if (out.size() < limit && seen.add(required(m, "ticker", "Kalshi market").asText())) {
          SettledMarket s = kalshiSettledMarket(m, series);
          s.historical = historical;
          out.add(s);
        }
      }
      cursor = required(doc, "cursor", "Kalshi markets response").asText();
      if (cursor.isEmpty() || markets.size() == 0) {
        break;
      }
    }
  }

  private static SettledMarket kalshiSettledMarket(JsonNode m, String series) {
    String id = required(m, "ticker", "Kalshi market").asText();
    String what = "Kalshi market " + id;
    SettledMarket s = new SettledMarket();
    s.source = "kalshi";
    s.series = series;
    s.marketId = id;
    s.eventId = required(m, "event_ticker", what).asText();
    s.title = required(m, "title", what).asText();
    String result = required(m, "result", what).asText();
    s.outcome = result;
    if (!"yes".equals(result) && !"no".equals(result)) {
      s.scoreable = false;
      s.notScoreableReason = "settled with result '" + result + "', not yes or no";
    }
    StringBuilder rules = new StringBuilder();
    for (String f : new String[] {"rules_primary", "rules_secondary"}) {
      String part = textOrNull(m, f);
      if (part != null && !part.isEmpty()) {
        rules.append(rules.length() > 0 ? " " : "").append(part);
      }
    }
    s.rules = rules.length() == 0 ? null : rules.toString();
    String value = required(m, "expiration_value", what).asText();
    s.settlementValue = value.isEmpty() ? null : value;
    s.closeTime = required(m, "close_time", what).asText();
    s.settleTime = required(m, "settlement_ts", what).asText();
    s.lastPrice = dollars(required(m, "last_price_dollars", what), what + " last_price_dollars");
    s.strikeType = textOrNull(m, "strike_type");
    s.floorStrike = numberOrNull(m, "floor_strike");
    s.capStrike = numberOrNull(m, "cap_strike");
    s.condition = textOrNull(m, "subtitle");
    return s;
  }

  private static List<SettledMarket> polymarketSettled(PredictionMarkets.Fetcher fetcher,
      String series, int limit) throws IOException {
    List<SettledMarket> out = new ArrayList<>();
    int offset = 0;
    while (out.size() < limit) {
      JsonNode page = fetcher.get(PredictionMarkets.POLYMARKET + "/events?closed=true"
          + "&series_slug=" + enc(series) + "&order=endDate&ascending=false&limit="
          + POLYMARKET_PAGE + "&offset=" + offset);
      if (!page.isArray()) {
        throw new IllegalStateException("Polymarket events response is not an array");
      }
      for (JsonNode ev : page) {
        String eventId = required(ev, "id", "Polymarket event").asText();
        String eventTitle = required(ev, "title", "Polymarket event " + eventId).asText();
        for (JsonNode m : required(ev, "markets", "Polymarket event " + eventId)) {
          if (out.size() < limit && required(m, "closed", "Polymarket market").asBoolean()) {
            out.add(polymarketSettledMarket(m, series, eventId, eventTitle));
          }
        }
      }
      offset += page.size();
      if (page.size() < POLYMARKET_PAGE) {
        break;
      }
    }
    return out;
  }

  private static SettledMarket polymarketSettledMarket(JsonNode m, String series,
      String eventId, String eventTitle) {
    String id = required(m, "id", "Polymarket market").asText();
    String what = "Polymarket market " + id;
    SettledMarket s = new SettledMarket();
    s.source = "polymarket";
    s.series = series;
    s.eventId = eventId;
    s.eventTitle = eventTitle;
    s.marketId = id;
    s.title = required(m, "question", what).asText();
    s.condition = textOrNull(m, "groupItemTitle");
    s.rules = textOrNull(m, "description");
    List<String> outcomes = stringList(required(m, "outcomes", what), what + " outcomes");
    List<String> prices = stringList(required(m, "outcomePrices", what),
        what + " outcomePrices");
    if (outcomes.size() != prices.size()) {
      throw new IllegalStateException(what + " has " + outcomes.size() + " outcomes but "
          + prices.size() + " outcomePrices");
    }
    s.outcome = "unresolved";
    for (int i = 0; i < prices.size(); i++) {
      if (Double.parseDouble(prices.get(i)) >= 1 - EPS) {
        String name = outcomes.get(i);
        s.outcome = "Yes".equals(name) ? "yes" : "No".equals(name) ? "no" : name;
      }
    }
    s.closeTime = required(m, "closedTime", what).asText();
    s.lastPrice = number(required(m, "lastTradePrice", what), what + " lastTradePrice");
    return s;
  }

  // ─── Resolving a market ────────────────────────────────────────────────────

  /**
   * Resolves one market to the keys its history and book endpoints take.
   *
   * <p>Kalshi: {@code marketId} is the market ticker; the series is {@code series} when
   * given, else read from the market's event. Polymarket: {@code marketId} is the gamma
   * market id; its Yes outcome's CLOB token id is read from {@code clobTokenIds}.
   */
  static MarketRef resolve(PredictionMarkets.Fetcher fetcher, String source, String marketId,
      String series) throws IOException {
    MarketRef ref = new MarketRef();
    ref.source = source;
    ref.marketId = marketId;
    if ("kalshi".equals(source)) {
      JsonNode doc;
      try {
        doc = fetcher.get(PredictionMarkets.KALSHI + "/markets/" + enc(marketId));
      } catch (PredictionMarkets.HttpStatusException e) {
        if (e.status != 404) {
          throw e;
        }
        // Not on the live tier: a market past the historical cutoff, or no market at all;
        // the historical tier's own 404 says which.
        doc = fetcher.get(PredictionMarkets.KALSHI + "/historical/markets/" + enc(marketId));
        ref.historical = true;
      }
      JsonNode m = required(doc, "market", "Kalshi market response");
      String what = "Kalshi market " + marketId;
      ref.title = required(m, "title", what).asText();
      ref.status = required(m, "status", what).asText();
      ref.open = "active".equals(ref.status);
      if (series != null) {
        ref.series = series;
      } else {
        String event = required(m, "event_ticker", what).asText();
        JsonNode ev = required(fetcher.get(PredictionMarkets.KALSHI + "/events/" + enc(event)),
            "event", "Kalshi event response");
        ref.series = required(ev, "series_ticker", "Kalshi event " + event).asText();
      }
    } else if ("polymarket".equals(source)) {
      JsonNode m = fetcher.get(PredictionMarkets.POLYMARKET + "/markets/" + enc(marketId));
      String what = "Polymarket market " + marketId;
      ref.title = required(m, "question", what).asText();
      boolean closed = required(m, "closed", what).asBoolean();
      boolean accepting = required(m, "acceptingOrders", what).asBoolean();
      ref.open = !closed && accepting;
      ref.status = closed ? "closed" : accepting ? "active" : "not_accepting_orders";
      List<String> outcomes = stringList(required(m, "outcomes", what), what + " outcomes");
      List<String> tokens = stringList(required(m, "clobTokenIds", what),
          what + " clobTokenIds");
      int yes = outcomes.indexOf("Yes");
      if (yes < 0 || outcomes.size() != 2 || tokens.size() != 2) {
        throw new IllegalStateException(what + " is not a Yes/No market with two token ids: "
            + outcomes);
      }
      ref.yesTokenId = tokens.get(yes);
    } else {
      throw badSource(source);
    }
    return ref;
  }

  // ─── Price history ─────────────────────────────────────────────────────────

  /**
   * The price series of one market between {@code start} and {@code end}, oldest first.
   *
   * <p>Kalshi: candlesticks, one per period (price = the period's last trade, null if none
   * traded; closing bid and ask; volume). At most 5000 periods per request, so a window is
   * rejected when it would exceed that. Polymarket: the CLOB prices-history of the Yes token,
   * one point per interval, no volume; the venue keeps about a month of hourly points and all
   * of the daily ones, so an hourly window older than that returns fewer points than asked.
   */
  static List<PricePoint> priceHistory(PredictionMarkets.Fetcher fetcher, MarketRef ref,
      Instant start, Instant end, Interval interval) throws IOException {
    if (!end.isAfter(start)) {
      throw new IllegalArgumentException("end must be after start");
    }
    List<PricePoint> out = new ArrayList<>();
    if ("kalshi".equals(ref.source)) {
      long periods = Duration.between(start, end).toMinutes() / interval.minutes;
      if (periods > KALSHI_CANDLE_CAP) {
        throw new IllegalArgumentException("window holds " + periods + " " + interval.label
            + " periods; Kalshi returns at most " + KALSHI_CANDLE_CAP
            + ". Shorten the window or use interval=day.");
      }
      JsonNode doc = fetcher.get(PredictionMarkets.KALSHI
          + (ref.historical ? "/historical" : "/series/" + enc(ref.series))
          + "/markets/" + enc(ref.marketId) + "/candlesticks?start_ts="
          + start.getEpochSecond() + "&end_ts=" + end.getEpochSecond() + "&period_interval="
          + interval.minutes);
      for (JsonNode c : required(doc, "candlesticks", "Kalshi candlesticks response")) {
        out.add(kalshiPoint(c, ref.marketId, ref.historical));
      }
    } else if ("polymarket".equals(ref.source)) {
      // startTs/endTs is rejected as "too long" beyond about two weeks, so the whole
      // history is read and cut to the window here.
      JsonNode doc = fetcher.get(CLOB + "/prices-history?market=" + enc(ref.yesTokenId)
          + "&interval=max&fidelity=" + interval.minutes);
      for (JsonNode p : required(doc, "history", "Polymarket prices-history response")) {
        PricePoint pt = new PricePoint();
        pt.epochSecond = required(p, "t", "Polymarket price point").asLong();
        pt.price = number(required(p, "p", "Polymarket price point"), "price point p");
        if (pt.epochSecond >= start.getEpochSecond() && pt.epochSecond <= end.getEpochSecond()) {
          out.add(pt);
        }
      }
    } else {
      throw badSource(ref.source);
    }
    out.sort(Comparator.comparingLong(p -> p.epochSecond));
    return out;
  }

  /**
   * Gives Polymarket daily points their open, high and low, taken from the venue's hourly
   * points; returns how many points got them.
   *
   * <p>Polymarket publishes one price per period and no candles. A daily point at {@code t}
   * closes the period since the daily point before it, so its candle is the hourly points in
   * that period: the first is the open, and the extremes — the close among them — are the
   * high and low. The venue keeps about thirty days of hourly points, so older daily points,
   * and any with fewer than two hourly points behind them, keep a close only.
   */
  static int addPolymarketOhlc(PredictionMarkets.Fetcher fetcher, MarketRef ref,
      List<PricePoint> daily) throws IOException {
    JsonNode doc = fetcher.get(CLOB + "/prices-history?market=" + enc(ref.yesTokenId)
        + "&interval=max&fidelity=" + Interval.HOUR.minutes);
    List<double[]> hourly = new ArrayList<>();
    for (JsonNode p : required(doc, "history", "Polymarket prices-history response")) {
      hourly.add(new double[] {required(p, "t", "Polymarket price point").asLong(),
          number(required(p, "p", "Polymarket price point"), "price point p")});
    }
    hourly.sort(Comparator.comparingDouble(h -> h[0]));
    int filled = 0;
    int at = 0;
    for (int i = 1; i < daily.size(); i++) {
      PricePoint d = daily.get(i);
      long from = daily.get(i - 1).epochSecond;
      while (at < hourly.size() && hourly.get(at)[0] <= from) {
        at++;
      }
      int count = 0;
      Double open = null;
      double high = d.price;
      double low = d.price;
      while (at < hourly.size() && hourly.get(at)[0] <= d.epochSecond) {
        double price = hourly.get(at)[1];
        open = open == null ? Double.valueOf(price) : open;
        high = Math.max(high, price);
        low = Math.min(low, price);
        count++;
        at++;
      }
      if (count >= 2) {
        d.open = open;
        d.high = high;
        d.low = low;
        filled++;
      }
    }
    return filled;
  }

  /** The historical tier names a candle's fields without the live tier's unit suffixes. */
  private static PricePoint kalshiPoint(JsonNode c, String ticker, boolean historical) {
    String what = "Kalshi candlestick of " + ticker;
    String volume = historical ? "volume" : "volume_fp";
    String close = historical ? "close" : "close_dollars";
    PricePoint p = new PricePoint();
    p.epochSecond = required(c, "end_period_ts", what).asLong();
    p.volume = number(required(c, volume, what), what + " " + volume);
    JsonNode price = required(c, "price", what);
    p.price = tradePrice(price, close, what + " price");
    if (p.price != null) {
      String suffix = historical ? "" : "_dollars";
      p.open = dollars(required(price, "open" + suffix, what + " price"), what + " price.open");
      p.high = dollars(required(price, "high" + suffix, what + " price"), what + " price.high");
      p.low = dollars(required(price, "low" + suffix, what + " price"), what + " price.low");
    }
    p.yesBid = candleClose(required(c, "yes_bid", what), close, what + " yes_bid");
    p.yesAsk = candleClose(required(c, "yes_ask", what), close, what + " yes_ask");
    return p;
  }

  /**
   * A candle's last trade. The venue sends {@code {}} or only {@code previous_dollars} for a
   * period with no trade; that is null, not a gap in the data.
   */
  private static Double tradePrice(JsonNode part, String close, String what) {
    return part.hasNonNull(close) ? candleClose(part, close, what) : null;
  }

  /** A candle's closing dollars; an empty object means nothing quoted: null. */
  private static Double candleClose(JsonNode part, String close, String what) {
    if (part.size() == 0) {
      return null;
    }
    return dollars(required(part, close, what), what + "." + close);
  }

  // ─── Order book ────────────────────────────────────────────────────────────

  /**
   * The Yes order book of one market, bids highest first and asks lowest first, in dollars
   * and contracts.
   *
   * <p>Kalshi's endpoint returns resting bids on Yes and on No only; a No bid at p is a Yes
   * ask at 1 - p, so the asks here are derived from the No ladder. Polymarket returns bids and
   * asks of the Yes token, each ordered worst first; both are re-sorted.
   */
  static OrderBook orderBook(PredictionMarkets.Fetcher fetcher, MarketRef ref)
      throws IOException {
    OrderBook book = new OrderBook();
    book.source = ref.source;
    book.marketId = ref.marketId;
    if ("kalshi".equals(ref.source)) {
      JsonNode doc = fetcher.get(PredictionMarkets.KALSHI + "/markets/" + enc(ref.marketId)
          + "/orderbook");
      JsonNode ob = required(doc, "orderbook_fp", "Kalshi orderbook response");
      book.bids = ladder(required(ob, "yes_dollars", "Kalshi orderbook_fp"), true);
      for (Level no : ladder(required(ob, "no_dollars", "Kalshi orderbook_fp"), true)) {
        book.asks.add(new Level(PredictionMarkets.round(1 - no.price, 6), no.size));
      }
      book.asks.sort(Comparator.comparingDouble(l -> l.price));
    } else if ("polymarket".equals(ref.source)) {
      JsonNode doc = fetcher.get(CLOB + "/book?token_id=" + enc(ref.yesTokenId));
      book.bids = polymarketLevels(required(doc, "bids", "Polymarket book"), true);
      book.asks = polymarketLevels(required(doc, "asks", "Polymarket book"), false);
    } else {
      throw badSource(ref.source);
    }
    return book;
  }

  /** Kalshi's [[price, size], ...] ladder, sorted by price, highest first if {@code desc}. */
  private static List<Level> ladder(JsonNode rows, boolean desc) {
    List<Level> out = new ArrayList<>();
    for (JsonNode r : rows) {
      if (!r.isArray() || r.size() != 2) {
        throw new IllegalStateException("Kalshi order book level is not [price, size]: " + r);
      }
      out.add(level(r.get(0), r.get(1)));
    }
    sort(out, desc);
    return out;
  }

  private static List<Level> polymarketLevels(JsonNode rows, boolean desc) {
    List<Level> out = new ArrayList<>();
    for (JsonNode r : rows) {
      out.add(level(required(r, "price", "Polymarket book level"),
          required(r, "size", "Polymarket book level")));
    }
    sort(out, desc);
    return out;
  }

  private static void sort(List<Level> levels, boolean desc) {
    Comparator<Level> byPrice = Comparator.comparingDouble(l -> l.price);
    levels.sort(desc ? byPrice.reversed() : byPrice);
  }

  private static Level level(JsonNode price, JsonNode size) {
    double p = dollars(price, "order book price");
    double s = number(size, "order book size");
    if (s < 0) {
      throw new IllegalStateException("order book size is negative: " + s);
    }
    return new Level(p, s);
  }

  // ─── Tool: market_price_history ────────────────────────────────────────────

  /** The {@code market_price_history} tool definition: name, description, input schema. */
  static ObjectNode toolDef() {
    ObjectNode props = MAPPER.createObjectNode();
    props.set("source", prop("string", "Venue: kalshi or polymarket."));
    props.set("market_id", prop("string",
        "The market: a Kalshi market ticker (KXCPI-26OCT-T3.0) or a Polymarket market id "
        + "(the id of a market inside an event, not the event id). Give this or event_id."));
    props.set("event_id", prop("string",
        "An event, when it has exactly one market. For an event with several, the error "
        + "lists its market ids; pick one and pass it as market_id."));
    props.set("series", prop("string",
        "Kalshi series ticker, to skip reading it from the market's event."));
    props.set("window_days", prop("integer",
        "Days of history ending now (default 30, at most 365)."));
    props.set("interval", prop("string",
        "Sampling step: day (default) or hour. hour is limited to 200 days on Kalshi."));
    ObjectNode schema = MAPPER.createObjectNode();
    schema.put("type", "object");
    schema.set("properties", props);
    schema.set("required", MAPPER.createArrayNode().add("source"));
    ObjectNode t = MAPPER.createObjectNode();
    t.put("name", "market_price_history");
    t.put("description", "Price history of one Kalshi or Polymarket market, with the order "
        + "book as it stands now. Returns the price series (Yes price in dollars, with the "
        + "period's open, high and low where the venue has them — the candles of a "
        + "render_chart candlestick; Kalshi adds closing bid, ask and volume per period), "
        + "first, last, min and max price, "
        + "and the book's top: best bid and ask with sizes and the spread. A market no "
        + "longer taking orders returns order_book null with the reason. Use it to see "
        + "whether a quoted price is new or long-standing, how it moved into a release, and "
        + "whether the book is deep enough to trade.");
    t.set("inputSchema", schema);
    return t;
  }

  /** Handles a {@code market_price_history} call; returns the result as a JSON string. */
  String priceHistoryTool(JsonNode args) throws IOException {
    Iterator<String> names = args.fieldNames();
    while (names.hasNext()) {
      String n = names.next();
      if (!TOOL_ARGS.contains(n)) {
        throw new IllegalArgumentException("unknown argument '" + n + "'; allowed: "
            + new TreeSet<>(TOOL_ARGS));
      }
    }
    String source = text(args, "source");
    if (source == null) {
      throw new IllegalArgumentException("source is required");
    }
    String series = text(args, "series");
    String marketId = text(args, "market_id");
    String eventId = text(args, "event_id");
    if ((marketId == null) == (eventId == null)) {
      throw new IllegalArgumentException("give exactly one of market_id and event_id");
    }
    int days = DEFAULT_WINDOW_DAYS;
    if (args.hasNonNull("window_days")) {
      if (!args.get("window_days").isIntegralNumber()) {
        throw new IllegalArgumentException("window_days must be an integer, got "
            + args.get("window_days"));
      }
      days = args.get("window_days").asInt();
    }
    if (days < 1 || days > MAX_WINDOW_DAYS) {
      throw new IllegalArgumentException("window_days must be 1 to " + MAX_WINDOW_DAYS
          + ", got " + days);
    }
    Interval interval = Interval.of(text(args, "interval") == null ? "day"
        : text(args, "interval"));
    if (marketId == null) {
      marketId = soleMarket(source, eventId);
    }

    MarketRef ref = resolve(fetcher, source, marketId, series);
    Instant end = clock.get();
    List<PricePoint> points = priceHistory(fetcher, ref, end.minus(Duration.ofDays(days)),
        end, interval);

    ObjectNode out = MAPPER.createObjectNode();
    out.put("source", source);
    out.put("market_id", ref.marketId);
    out.put("title", ref.title);
    out.put("status", ref.status);
    out.put("interval", interval.label);
    out.put("window_days", days);
    out.put("read_at", end.toString());
    summarize(out, points);
    if ("kalshi".equals(source)) {
      out.put("ohlc", "venue candlesticks: each traded period carries open, high and low, "
          + "and price is its close");
    } else if (interval == Interval.DAY) {
      int filled = addPolymarketOhlc(fetcher, ref, points);
      out.put("ohlc", "resampled from the venue's hourly points, which it keeps for about "
          + "30 days: " + filled + " of " + points.size() + " points carry open, high and "
          + "low; the rest have a close only");
    } else {
      out.put("ohlc", "none: Polymarket publishes one price per period, and hourly is its "
          + "finest; use interval=day for candles");
    }
    ArrayNode series2 = out.putArray("points");
    for (PricePoint p : points) {
      series2.add(p.toJson());
    }
    if (ref.open) {
      out.set("order_book", orderBook(fetcher, ref).topJson());
    } else {
      out.putNull("order_book");
      out.put("order_book_note", "market status is '" + ref.status
          + "', so it takes no orders and has no book");
    }
    return MAPPER.writeValueAsString(out);
  }

  /** First, last, min and max of the priced points; nulls and a note when none is priced. */
  private static void summarize(ObjectNode out, List<PricePoint> points) {
    PricePoint first = null;
    PricePoint last = null;
    PricePoint min = null;
    PricePoint max = null;
    int priced = 0;
    for (PricePoint p : points) {
      if (p.price == null) {
        continue;
      }
      priced++;
      first = first == null ? p : first;
      last = p;
      min = min == null || p.price < min.price ? p : min;
      max = max == null || p.price > max.price ? p : max;
    }
    out.put("points_total", points.size());
    out.put("points_priced", priced);
    for (Object[] e : new Object[][] {{"first", first}, {"last", last}, {"min", min},
        {"max", max}}) {
      PricePoint p = (PricePoint) e[1];
      if (p == null) {
        out.putNull((String) e[0]);
      } else {
        ObjectNode o = out.putObject((String) e[0]);
        o.put("t", Instant.ofEpochSecond(p.epochSecond).toString());
        o.put("price", p.price);
      }
    }
    if (priced == 0) {
      out.put("note", "no trade in the window, so there is no price to summarise");
    }
  }

  /** The one market of an event; an error naming the choices when it has several. */
  private String soleMarket(String source, String eventId) throws IOException {
    List<String> ids = new ArrayList<>();
    if ("kalshi".equals(source)) {
      JsonNode ev = required(fetcher.get(PredictionMarkets.KALSHI + "/events/" + enc(eventId)
          + "?with_nested_markets=true"), "event", "Kalshi event response");
      for (JsonNode m : required(ev, "markets", "Kalshi event " + eventId)) {
        ids.add(required(m, "ticker", "Kalshi market").asText());
      }
    } else if ("polymarket".equals(source)) {
      JsonNode ev = fetcher.get(PredictionMarkets.POLYMARKET + "/events/" + enc(eventId));
      for (JsonNode m : required(ev, "markets", "Polymarket event " + eventId)) {
        ids.add(required(m, "id", "Polymarket market").asText());
      }
    } else {
      throw badSource(source);
    }
    if (ids.size() != 1) {
      throw new IllegalArgumentException(source + " event " + eventId + " has " + ids.size()
          + " markets; pass one as market_id: "
          + ids.subList(0, Math.min(ids.size(), 30)));
    }
    return ids.get(0);
  }

  // ─── Parsing helpers ───────────────────────────────────────────────────────

  private static IllegalArgumentException badSource(String source) {
    return new IllegalArgumentException(
        "source must be 'kalshi' or 'polymarket', got '" + source + "'");
  }

  private static String enc(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }

  private static ObjectNode prop(String type, String description) {
    ObjectNode p = MAPPER.createObjectNode();
    p.put("type", type);
    p.put("description", description);
    return p;
  }

  private static String text(JsonNode args, String name) {
    if (!args.hasNonNull(name)) {
      return null;
    }
    String s = args.get(name).asText().trim();
    return s.isEmpty() ? null : s;
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

  /** An optional numeric field of the venue's payload: absent means the venue has none. */
  private static Double numberOrNull(JsonNode node, String field) {
    JsonNode v = node.get(field);
    return v == null || v.isNull() ? null : number(v, field);
  }

  /** A number the venue sends as a JSON number or as a decimal string. */
  private static double number(JsonNode v, String what) {
    if (v.isNumber()) {
      return v.asDouble();
    }
    try {
      return Double.parseDouble(v.asText());
    } catch (NumberFormatException e) {
      throw new IllegalStateException(what + " is not a number: " + v, e);
    }
  }

  private static double dollars(JsonNode v, String what) {
    double d = number(v, what);
    if (d < 0 || d > 1) {
      throw new IllegalStateException(what + " is outside 0 to 1 dollars: " + d);
    }
    return d;
  }

  /** Gamma sends arrays as JSON text inside a string: {@code "[\"Yes\", \"No\"]"}. */
  private static List<String> stringList(JsonNode v, String what) {
    try {
      JsonNode arr = v.isTextual() ? MAPPER.readTree(v.asText()) : v;
      if (!arr.isArray()) {
        throw new IllegalStateException(what + " is not an array: " + v);
      }
      List<String> out = new ArrayList<>();
      for (JsonNode e : arr) {
        out.add(e.asText());
      }
      return out;
    } catch (IOException e) {
      throw new IllegalStateException(what + " is not valid JSON: " + v, e);
    }
  }
}
