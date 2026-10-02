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
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The venue history layer against canned venue responses: no network. Each fixture is a real
 * response read from the venue on 2026-10-02, trimmed to the fields the parsers use.
 *
 * <p>Two shapes the fixtures pin down: Kalshi's order book is two bid ladders (Yes and No),
 * so the Yes ask is 1 minus the best No bid; Polymarket's book lists each side worst price
 * first.
 */
@Tag("unit")
class MarketHistoryTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.ofEpochSecond(1790949600L);
  private static final String K = PredictionMarkets.KALSHI;
  private static final String G = PredictionMarkets.POLYMARKET;
  private static final String C = MarketHistory.CLOB;
  private static final String TICKER = "KXCPI-26OCT-T1.0";
  private static final String YES_TOKEN = "100379208559626151022751801118534484742123694725746262280150222742563282755057";
  private static final String NO_TOKEN = "113732820231608904682346496304917888352004831436510840986547065248348999143469";

  /** Answers a URL from the first registered prefix it starts with, and records every URL. */
  private static final class FakeFetcher implements PredictionMarkets.Fetcher {
    final Map<String, JsonNode> byPrefix = new LinkedHashMap<>();
    final List<String> urls = new ArrayList<>();

    FakeFetcher on(String prefix, String json) {
      try {
        byPrefix.put(prefix, MAPPER.readTree(json));
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
      return this;
    }

    /** Replaces the canned answer for URLs starting with {@code prefix}, ahead of the rest. */
    FakeFetcher override(String prefix, String json) {
      Map<String, JsonNode> old = new LinkedHashMap<>(byPrefix);
      byPrefix.clear();
      on(prefix, json);
      old.remove(prefix);
      byPrefix.putAll(old);
      return this;
    }

    @Override public JsonNode get(String url) throws IOException {
      urls.add(url);
      for (Map.Entry<String, JsonNode> e : byPrefix.entrySet()) {
        if (url.startsWith(e.getKey())) {
          return e.getValue();
        }
      }
      throw new IOException("no canned response for " + url);
    }
  }

  // ─── Fixtures ──────────────────────────────────────────────────────────────

  private static String kalshiSettledMarket(String ticker, String strike, String last) {
    return """
        {"close_time":"2026-09-11T12:25:00Z","event_ticker":"KXCPI-26AUG",
         "expiration_value":"0.4","floor_strike":%s,"last_price_dollars":"%s",
         "result":"no","settlement_ts":"2026-09-11T13:28:53.706257Z","status":"finalized",
         "strike_type":"greater","subtitle":"%s%%","ticker":"%s",
         "title":"Will CPI rise more than %s%% in August 2026?"}
        """.formatted(strike, last, strike, ticker, strike);
  }

  private static final String KALSHI_SETTLED_PAGE_1 = "{\"cursor\":\"CgwIhfWJ0wYQ8OD22wES\","
      + "\"markets\":[" + kalshiSettledMarket("KXCPI-26AUG-T1.0", "1", "0.0100") + ","
      + kalshiSettledMarket("KXCPI-26AUG-T0.9", "0.9", "0.0100") + "]}";
  private static final String KALSHI_SETTLED_PAGE_2 = "{\"cursor\":\"\",\"markets\":["
      + kalshiSettledMarket("KXCPI-26AUG-T0.2", "0.2", "0.9900") + "]}";
  private static final String KALSHI_NO_MARKETS = "{\"cursor\":\"\",\"markets\":[]}";
  /** The historical tier: one market the live tier also lists, and one only it holds. */
  private static final String KALSHI_HISTORICAL_PAGE = "{\"cursor\":\"\",\"markets\":["
      + kalshiSettledMarket("KXCPI-26AUG-T0.2", "0.2", "0.9900") + ","
      + kalshiSettledMarket("KXCPI-26JUN-T0.3", "0.3", "0.0100") + "]}";
  /** A candle of the historical tier: no unit suffix on its fields, a null close when no
   *  trade printed. */
  private static final String KALSHI_HISTORICAL_CANDLES = """
      {"candlesticks":[
       {"end_period_ts":1782874800,"open_interest":"21021.29",
        "price":{"close":"0.0200","high":"0.0200","low":"0.0200","mean":"0.0200","open":"0.0200","previous":"0.0200"},
        "volume":"1.00",
        "yes_ask":{"close":"0.0200","high":"0.0200","low":"0.0100","open":"0.0200"},
        "yes_bid":{"close":"0.0000","high":"0.0000","low":"0.0000","open":"0.0000"}},
       {"end_period_ts":1783267200,"open_interest":"21020.29",
        "price":{"close":null,"high":null,"low":null,"mean":null,"open":null,"previous":"0.0100"},
        "volume":"0.00",
        "yes_ask":{"close":"0.0100","high":"0.0100","low":"0.0100","open":"0.0100"},
        "yes_bid":{"close":"0.0000","high":"0.0000","low":"0.0000","open":"0.0000"}}]}
      """;

  private static final String POLY_SETTLED = """
      [{"id":"45883","title":"Fed decision in January?","endDate":"2026-01-28T00:00:00Z",
        "closed":true,
        "markets":[
         {"id":"601697","question":"Fed decreases interest rates by 50+ bps after January 2026 meeting?",
          "outcomes":"[\\"Yes\\", \\"No\\"]","outcomePrices":"[\\"0\\", \\"1\\"]",
          "closed":true,"closedTime":"2026-01-28 22:53:04+00",
          "groupItemTitle":"50+ bps decrease","lastTradePrice":0.001},
         {"id":"601699","question":"No change in interest rates after January 2026 meeting?",
          "outcomes":"[\\"Yes\\", \\"No\\"]","outcomePrices":"[\\"1\\", \\"0\\"]",
          "closed":true,"closedTime":"2026-01-28 22:53:04+00",
          "groupItemTitle":"No change","lastTradePrice":0.999}]}]
      """;

  private static final String KALSHI_MARKET = """
      {"market":{"event_ticker":"KXCPI-26OCT","ticker":"KXCPI-26OCT-T1.0","status":"active",
       "title":"Will CPI rise more than 1.0% in October 2026?"}}
      """;
  private static final String KALSHI_EVENT = """
      {"event":{"event_ticker":"KXCPI-26OCT","series_ticker":"KXCPI","title":"CPI in October",
       "markets":[{"ticker":"KXCPI-26OCT-T1.0"},{"ticker":"KXCPI-26OCT-T0.9"}]},"markets":[]}
      """;
  private static final String KALSHI_EVENT_ONE = """
      {"event":{"event_ticker":"KXONE","series_ticker":"KXCPI","title":"One",
       "markets":[{"ticker":"KXCPI-26OCT-T1.0"}]},"markets":[]}
      """;
  private static final String KALSHI_BOOK = """
      {"orderbook_fp":{"no_dollars":[["0.0100","6800.00"],["0.0200","7480.00"],
        ["0.4700","42.00"],["0.9600","3.00"],["0.9800","1586.01"]],
       "yes_dollars":[["0.0100","2637.00"]]}}
      """;
  private static final String KALSHI_CANDLES = """
      {"candlesticks":[
       {"end_period_ts":1789790400,"open_interest_fp":"0.00","price":{},"volume_fp":"0.00",
        "yes_ask":{"close_dollars":"0.2000","high_dollars":"1.0000","low_dollars":"0.1900","open_dollars":"1.0000"},
        "yes_bid":{"close_dollars":"0.0300","high_dollars":"0.0300","low_dollars":"0.0100","open_dollars":"0.0100"}},
       {"end_period_ts":1789876800,"open_interest_fp":"10.00",
        "price":{"close_dollars":"0.1600","high_dollars":"0.1900","low_dollars":"0.1600","mean_dollars":"0.1840","open_dollars":"0.1900"},
        "volume_fp":"10.00",
        "yes_ask":{"close_dollars":"0.2000","high_dollars":"0.2000","low_dollars":"0.1400","open_dollars":"0.2000"},
        "yes_bid":{"close_dollars":"0.1600","high_dollars":"0.1600","low_dollars":"0.0300","open_dollars":"0.0300"}},
       {"end_period_ts":1789963200,"open_interest_fp":"663.00",
        "price":{"close_dollars":"0.0700","high_dollars":"0.1600","low_dollars":"0.0700","mean_dollars":"0.1138","open_dollars":"0.1600","previous_dollars":"0.1600"},
        "volume_fp":"801.00",
        "yes_ask":{"close_dollars":"0.0800","high_dollars":"0.2000","low_dollars":"0.0800","open_dollars":"0.2000"},
        "yes_bid":{"close_dollars":"0.0700","high_dollars":"0.1600","low_dollars":"0.0600","open_dollars":"0.1600"}},
       {"end_period_ts":1790136000,"open_interest_fp":"1863.00","price":{"previous_dollars":"0.0400"},
        "volume_fp":"0.00",
        "yes_ask":{"close_dollars":"0.0300","high_dollars":"0.0300","low_dollars":"0.0300","open_dollars":"0.0300"},
        "yes_bid":{"close_dollars":"0.0200","high_dollars":"0.0200","low_dollars":"0.0200","open_dollars":"0.0200"}}]}
      """;

  private static final String POLY_MARKET = """
      {"id":"609655","question":"US recession by end of 2026?","outcomes":"[\\"Yes\\", \\"No\\"]",
       "outcomePrices":"[\\"0.075\\", \\"0.925\\"]","closed":false,"acceptingOrders":true,
       "clobTokenIds":"[\\"%s\\", \\"%s\\"]","lastTradePrice":0.09}
      """.formatted(YES_TOKEN, NO_TOKEN);
  private static final String POLY_MARKET_CLOSED = """
      {"id":"601697","question":"Fed decreases 50+ bps?","outcomes":"[\\"Yes\\", \\"No\\"]",
       "closed":true,"acceptingOrders":false,
       "clobTokenIds":"[\\"%s\\", \\"%s\\"]"}
      """.formatted(YES_TOKEN, NO_TOKEN);
  private static final String POLY_EVENT = """
      {"id":"69702","markets":[{"id":"609655"},{"id":"609656"}]}
      """;
  private static final String POLY_EVENT_ONE = """
      {"id":"48802","markets":[{"id":"609655"}]}
      """;
  private static final String POLY_BOOK = """
      {"market":"0xfdc7","asset_id":"%s","timestamp":"1790949050343",
       "bids":[{"price":"0.01","size":"114239.89"},{"price":"0.02","size":"68024.92"},
               {"price":"0.07","size":"500"}],
       "asks":[{"price":"0.99","size":"1101359.69"},{"price":"0.98","size":"6457.86"},
               {"price":"0.08","size":"300"}],
       "min_order_size":"5","tick_size":"0.01","neg_risk":false,"last_trade_price":"0.080"}
      """.formatted(YES_TOKEN);
  private static final String POLY_HISTORY = """
      {"history":[{"t":1790812826,"p":0.085},{"t":1790726424,"p":0.085},
                  {"t":1790899224,"p":0.08},{"t":1790949134,"p":0.075}]}
      """;

  private static FakeFetcher kalshiFake() {
    return new FakeFetcher()
        .on(K + "/markets/" + TICKER + "/orderbook", KALSHI_BOOK)
        .on(K + "/markets/" + TICKER, KALSHI_MARKET)
        .on(K + "/series/KXCPI/markets/" + TICKER + "/candlesticks", KALSHI_CANDLES)
        .on(K + "/events/KXCPI-26OCT", KALSHI_EVENT)
        .on(K + "/events/KXONE", KALSHI_EVENT_ONE);
  }

  private static FakeFetcher polyFake() {
    return new FakeFetcher()
        .on(G + "/markets/609655", POLY_MARKET)
        .on(G + "/markets/601697", POLY_MARKET_CLOSED)
        .on(G + "/events/69702", POLY_EVENT)
        .on(G + "/events/48802", POLY_EVENT_ONE)
        .on(C + "/book?token_id=" + YES_TOKEN, POLY_BOOK)
        .on(C + "/prices-history?market=" + YES_TOKEN, POLY_HISTORY);
  }

  private static MarketHistory tool(FakeFetcher f) {
    return new MarketHistory(f, () -> NOW);
  }

  private static ObjectNode args(String... kv) {
    ObjectNode o = MAPPER.createObjectNode();
    for (int i = 0; i < kv.length; i += 2) {
      o.put(kv[i], kv[i + 1]);
    }
    return o;
  }

  // ─── Settled markets ───────────────────────────────────────────────────────

  @Test void kalshiSettledMarketsAreParsedAndPaged() throws Exception {
    FakeFetcher f = new FakeFetcher()
        .on(K + "/markets?status=settled&series_ticker=KXCPI&limit=198&cursor=",
            KALSHI_SETTLED_PAGE_2)
        .on(K + "/markets?status=settled", KALSHI_SETTLED_PAGE_1)
        .on(K + "/historical/markets?series_ticker=KXCPI&limit=197", KALSHI_NO_MARKETS);
    List<MarketHistory.SettledMarket> out =
        MarketHistory.settledMarkets(f, "kalshi", "KXCPI", 200);
    assertEquals(3, out.size());
    MarketHistory.SettledMarket m = out.get(0);
    assertEquals("kalshi", m.source);
    assertEquals("KXCPI-26AUG-T1.0", m.marketId);
    assertEquals("KXCPI-26AUG", m.eventId);
    assertEquals("no", m.outcome);
    assertEquals("0.4", m.settlementValue);
    assertEquals("greater", m.strikeType);
    assertEquals(1.0, m.floorStrike);
    assertNull(m.capStrike);
    assertEquals(0.01, m.lastPrice);
    assertEquals("2026-09-11T12:25:00Z", m.closeTime);
    assertEquals("2026-09-11T13:28:53.706257Z", m.settleTime);
    assertEquals(0.99, out.get(2).lastPrice);
    assertEquals(3, f.urls.size());
    assertTrue(f.urls.get(1).contains("cursor=CgwIhfWJ0wYQ8OD22wES"), f.urls.get(1));
  }

  @Test void kalshiSettledMarketsPastTheCutoffComeFromTheHistoricalTier() throws Exception {
    FakeFetcher f = new FakeFetcher()
        .on(K + "/markets?status=settled", KALSHI_SETTLED_PAGE_2)
        .on(K + "/historical/markets?series_ticker=KXCPI&limit=199", KALSHI_HISTORICAL_PAGE);
    List<MarketHistory.SettledMarket> out =
        MarketHistory.settledMarkets(f, "kalshi", "KXCPI", 200);
    // The market both tiers list is kept once, from the live tier.
    assertEquals(2, out.size());
    assertEquals("KXCPI-26AUG-T0.2", out.get(0).marketId);
    assertFalse(out.get(0).historical);
    assertEquals("KXCPI-26JUN-T0.3", out.get(1).marketId);
    assertTrue(out.get(1).historical);
    assertEquals("no", out.get(1).outcome);

    // A limit the live tier fills leaves the historical tier unread.
    FakeFetcher g = new FakeFetcher().on(K + "/markets?status=settled", KALSHI_SETTLED_PAGE_2);
    assertEquals(1, MarketHistory.settledMarkets(g, "kalshi", "KXCPI", 1).size());
    assertEquals(1, g.urls.size());
  }

  @Test void aKalshiMarketTheLiveTierNoLongerHoldsResolvesFromTheHistoricalTier()
      throws Exception {
    String ticker = "KXCPI-26JUN-T0.3";
    JsonNode market = MAPPER.readTree("{\"market\":{\"title\":\"Will CPI rise more than 0.3%?\","
        + "\"status\":\"finalized\",\"event_ticker\":\"KXCPI-26JUN\"}}");
    List<String> urls = new ArrayList<>();
    PredictionMarkets.Fetcher f = url -> {
      urls.add(url);
      if (url.equals(K + "/historical/markets/" + ticker)) {
        return market;
      }
      throw new PredictionMarkets.HttpStatusException(404, url);
    };
    MarketHistory.MarketRef ref = MarketHistory.resolve(f, "kalshi", ticker, "KXCPI");
    assertTrue(ref.historical);
    assertFalse(ref.open);
    assertEquals("finalized", ref.status);
    assertEquals("KXCPI", ref.series);
    assertEquals(List.of(K + "/markets/" + ticker, K + "/historical/markets/" + ticker), urls);

    // A ticker neither tier holds raises the venue's own 404.
    PredictionMarkets.HttpStatusException e = assertThrows(
        PredictionMarkets.HttpStatusException.class,
        () -> MarketHistory.resolve(f, "kalshi", "NOPE", "KXCPI"));
    assertEquals(404, e.status);
  }

  @Test void kalshiHistoricalPriceHistoryReadsTheHistoricalCandles() throws Exception {
    String ticker = "KXCPI-26JUN-T0.3";
    FakeFetcher f = new FakeFetcher()
        .on(K + "/historical/markets/" + ticker + "/candlesticks", KALSHI_HISTORICAL_CANDLES);
    MarketHistory.MarketRef ref = new MarketHistory.MarketRef();
    ref.source = "kalshi";
    ref.marketId = ticker;
    ref.series = "KXCPI";
    ref.historical = true;
    List<MarketHistory.PricePoint> points = MarketHistory.priceHistory(f, ref,
        Instant.ofEpochSecond(1782700000L), Instant.ofEpochSecond(1783267200L),
        MarketHistory.Interval.HOUR);
    assertEquals(2, points.size());
    assertEquals(0.02, points.get(0).price);
    assertEquals(1.0, points.get(0).volume);
    assertEquals(0.0, points.get(0).yesBid);
    assertEquals(0.02, points.get(0).yesAsk);
    assertNull(points.get(1).price);
    assertEquals(0.01, points.get(1).yesAsk);
    assertTrue(f.urls.get(0).startsWith(K + "/historical/markets/" + ticker
        + "/candlesticks?start_ts=1782700000&end_ts=1783267200&period_interval=60"),
        f.urls.get(0));
  }

  @Test void kalshiSettledLimitBoundsPagesAndResults() throws Exception {
    FakeFetcher f = new FakeFetcher()
        .on(K + "/markets?status=settled&series_ticker=KXCPI&limit=1&cursor=",
            KALSHI_SETTLED_PAGE_2)
        .on(K + "/markets?status=settled", KALSHI_SETTLED_PAGE_1);
    // limit 3: the second request asks only for the 1 still missing.
    assertEquals(3, MarketHistory.settledMarkets(f, "kalshi", "KXCPI", 3).size());
    assertEquals(2, f.urls.size());
    assertTrue(f.urls.get(0).contains("&limit=3"), f.urls.get(0));
    assertTrue(f.urls.get(1).contains("&limit=1&cursor="), f.urls.get(1));

    // limit 2 is met by the first page: no second request, and the page is cut to 2.
    FakeFetcher g = new FakeFetcher().on(K + "/markets?status=settled", KALSHI_SETTLED_PAGE_1);
    assertEquals(2, MarketHistory.settledMarkets(g, "kalshi", "KXCPI", 2).size());
    assertEquals(1, g.urls.size());

    // limit 1 against a page of 2 keeps only the first.
    FakeFetcher h = new FakeFetcher().on(K + "/markets?status=settled", KALSHI_SETTLED_PAGE_1);
    List<MarketHistory.SettledMarket> one = MarketHistory.settledMarkets(h, "kalshi", "KXCPI", 1);
    assertEquals(1, one.size());
    assertEquals("KXCPI-26AUG-T1.0", one.get(0).marketId);

    assertThrows(IllegalArgumentException.class,
        () -> MarketHistory.settledMarkets(h, "kalshi", "KXCPI", 0));
  }

  @Test void polymarketSettledMarketsCarryOutcomeFromPrices() throws Exception {
    FakeFetcher f = new FakeFetcher().on(G + "/events?closed=true&series_slug=fed-interest-rates",
        POLY_SETTLED);
    List<MarketHistory.SettledMarket> out =
        MarketHistory.settledMarkets(f, "polymarket", "fed-interest-rates", 10);
    assertEquals(2, out.size());
    MarketHistory.SettledMarket no = out.get(0);
    assertEquals("polymarket", no.source);
    assertEquals("45883", no.eventId);
    assertEquals("Fed decision in January?", no.eventTitle);
    assertEquals("601697", no.marketId);
    assertEquals("no", no.outcome);
    assertNull(no.settlementValue);
    assertEquals("50+ bps decrease", no.condition);
    assertEquals("2026-01-28 22:53:04+00", no.closeTime);
    assertEquals(0.001, no.lastPrice);
    assertEquals("yes", out.get(1).outcome);
    assertEquals(1, f.urls.size());
    assertTrue(f.urls.get(0).endsWith("&offset=0"), f.urls.get(0));
  }

  @Test void polymarketSettledLimitCutsMarkets() throws Exception {
    FakeFetcher f = new FakeFetcher().on(G + "/events?closed=true", POLY_SETTLED);
    assertEquals(1, MarketHistory.settledMarkets(f, "polymarket", "s", 1).size());
  }

  @Test void polymarketSettledPagesByOffsetUntilShortPage() throws Exception {
    StringBuilder full = new StringBuilder("[");
    for (int i = 0; i < 100; i++) {
      full.append(i == 0 ? "" : ",").append("{\"id\":\"e").append(i)
          .append("\",\"title\":\"t\",\"markets\":[{\"id\":\"m").append(i)
          .append("\",\"question\":\"q\",\"outcomes\":\"[\\\"Yes\\\", \\\"No\\\"]\","
              + "\"outcomePrices\":\"[\\\"1\\\", \\\"0\\\"]\",\"closed\":true,"
              + "\"closedTime\":\"2026-01-01 00:00:00+00\",\"lastTradePrice\":1}]}");
    }
    full.append("]");
    FakeFetcher f = new FakeFetcher()
        .on(G + "/events?closed=true&series_slug=s&order=endDate&ascending=false&limit=100"
            + "&offset=100", POLY_SETTLED)
        .on(G + "/events?closed=true", full.toString());
    assertEquals(102, MarketHistory.settledMarkets(f, "polymarket", "s", 500).size());
    assertEquals(2, f.urls.size());
    assertEquals(100, MarketHistory.settledMarkets(f, "polymarket", "s", 100).size());
    assertEquals(3, f.urls.size());
  }

  /** A venue whose live tier answers {@code page} and whose historical tier is empty. */
  private static FakeFetcher liveTierOnly(String page) {
    return new FakeFetcher().on(K + "/markets", page)
        .on(K + "/historical/markets?series_ticker=", KALSHI_NO_MARKETS);
  }

  @Test void missingFieldsAreNamed() {
    String noResult = KALSHI_SETTLED_PAGE_2.replace("\"result\":\"no\",", "");
    FakeFetcher f = liveTierOnly(noResult);
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> MarketHistory.settledMarkets(f, "kalshi", "KXCPI", 5));
    assertTrue(e.getMessage().contains("'result'"), e.getMessage());

    String noValue = KALSHI_SETTLED_PAGE_2.replace("\"expiration_value\":\"0.4\",", "");
    e = assertThrows(IllegalStateException.class, () -> MarketHistory.settledMarkets(
        liveTierOnly(noValue), "kalshi", "KXCPI", 5));
    assertTrue(e.getMessage().contains("'expiration_value'"), e.getMessage());

    String noPrices = POLY_SETTLED.replace("\"outcomePrices\"", "\"outcomeP\"");
    e = assertThrows(IllegalStateException.class, () -> MarketHistory.settledMarkets(
        new FakeFetcher().on(G + "/events", noPrices), "polymarket", "s", 5));
    assertTrue(e.getMessage().contains("'outcomePrices'"), e.getMessage());

    String noCursor = KALSHI_SETTLED_PAGE_2.replace("\"cursor\":\"\",", "");
    e = assertThrows(IllegalStateException.class, () -> MarketHistory.settledMarkets(
        liveTierOnly(noCursor), "kalshi", "KXCPI", 5));
    assertTrue(e.getMessage().contains("'cursor'"), e.getMessage());
  }

  @Test void aResultThatIsNotYesOrNoIsKeptAndMarkedNotScoreable() throws IOException {
    String odd = KALSHI_SETTLED_PAGE_2.replace("\"result\":\"no\"", "\"result\":\"scalar\"");
    List<MarketHistory.SettledMarket> got = MarketHistory.settledMarkets(
        liveTierOnly(odd), "kalshi", "KXCPI", 5);
    assertEquals(1, got.size());
    assertEquals("scalar", got.get(0).outcome);
    assertFalse(got.get(0).scoreable);
    assertTrue(got.get(0).notScoreableReason.contains("scalar"), got.get(0).notScoreableReason);
    assertTrue(got.get(0).toJson().get("not_scoreable_reason").asText().contains("scalar"));
    String yes = KALSHI_SETTLED_PAGE_2.replace("\"result\":\"no\"", "\"result\":\"yes\"");
    assertTrue(MarketHistory.settledMarkets(liveTierOnly(yes),
        "kalshi", "KXCPI", 5).get(0).scoreable);
  }

  @Test void theRulesTextOfASettledMarketIsReturned() throws IOException {
    String withRules = KALSHI_SETTLED_PAGE_2.replace("\"result\":\"no\"",
        "\"result\":\"no\",\"rules_primary\":\"One decimal.\",\"rules_secondary\":\"BLS.\"");
    assertEquals("One decimal. BLS.", MarketHistory.settledMarkets(
        liveTierOnly(withRules), "kalshi", "KXCPI", 5).get(0).rules);
    assertNull(MarketHistory.settledMarkets(
        liveTierOnly(KALSHI_SETTLED_PAGE_2), "kalshi", "KXCPI", 5)
        .get(0).rules);
  }

  @Test void anUnknownSourceIsRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> MarketHistory.settledMarkets(new FakeFetcher(), "manifold", "x", 5));
  }

  // ─── Price history ─────────────────────────────────────────────────────────

  @Test void kalshiPriceHistoryReadsCandlesticks() throws Exception {
    FakeFetcher f = kalshiFake();
    MarketHistory.MarketRef ref = MarketHistory.resolve(f, "kalshi", TICKER, null);
    assertEquals("KXCPI", ref.series);
    assertTrue(ref.open);
    List<MarketHistory.PricePoint> pts = MarketHistory.priceHistory(f, ref,
        NOW.minusSeconds(14 * 86400), NOW, MarketHistory.Interval.DAY);
    assertEquals(4, pts.size());
    assertNull(pts.get(3).price);
    assertEquals(0.02, pts.get(3).yesBid);
    assertNull(pts.get(0).price);
    assertEquals(0.03, pts.get(0).yesBid);
    assertEquals(0.20, pts.get(0).yesAsk);
    assertEquals(0.0, pts.get(0).volume);
    assertEquals(0.16, pts.get(1).price);
    assertEquals(10.0, pts.get(1).volume);
    assertEquals(0.07, pts.get(2).price);
    assertEquals(1789963200L, pts.get(2).epochSecond);
    String url = f.urls.get(f.urls.size() - 1);
    assertTrue(url.contains("/series/KXCPI/markets/" + TICKER + "/candlesticks?start_ts="), url);
    assertTrue(url.endsWith("&end_ts=" + NOW.getEpochSecond() + "&period_interval=1440"), url);
  }

  @Test void kalshiHourlyWindowOverTheCapIsRejected() throws Exception {
    FakeFetcher f = kalshiFake();
    MarketHistory.MarketRef ref = MarketHistory.resolve(f, "kalshi", TICKER, "KXCPI");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> MarketHistory.priceHistory(f, ref, NOW.minusSeconds(250L * 86400), NOW,
            MarketHistory.Interval.HOUR));
    assertTrue(e.getMessage().contains("5000"), e.getMessage());
  }

  @Test void kalshiCandleMissingFieldIsNamed() throws Exception {
    FakeFetcher f = kalshiFake();
    MarketHistory.MarketRef ref = MarketHistory.resolve(f, "kalshi", TICKER, "KXCPI");
    f.override(K + "/series/KXCPI/markets/" + TICKER + "/candlesticks",
        KALSHI_CANDLES.replace("\"volume_fp\":\"801.00\",", ""));
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> MarketHistory.priceHistory(f, ref, NOW.minusSeconds(86400), NOW,
            MarketHistory.Interval.DAY));
    assertTrue(e.getMessage().contains("'volume_fp'"), e.getMessage());
  }

  @Test void polymarketPriceHistoryIsSortedOldestFirst() throws Exception {
    FakeFetcher f = polyFake();
    MarketHistory.MarketRef ref = MarketHistory.resolve(f, "polymarket", "609655", null);
    assertEquals(YES_TOKEN, ref.yesTokenId);
    assertTrue(ref.open);
    List<MarketHistory.PricePoint> pts = MarketHistory.priceHistory(f, ref,
        NOW.minusSeconds(5 * 86400), NOW, MarketHistory.Interval.HOUR);
    assertEquals(4, pts.size());
    assertEquals(1790726424L, pts.get(0).epochSecond);
    assertEquals(0.075, pts.get(3).price);
    assertNull(pts.get(0).volume);
    String url = f.urls.get(f.urls.size() - 1);
    assertTrue(url.endsWith("/prices-history?market=" + YES_TOKEN
        + "&interval=max&fidelity=60"), url);
  }

  @Test void polymarketHistoryIsCutToTheWindow() throws Exception {
    FakeFetcher f = polyFake();
    MarketHistory.MarketRef ref = MarketHistory.resolve(f, "polymarket", "609655", null);
    // One day back from NOW keeps the two newest of the fixture's four points.
    List<MarketHistory.PricePoint> pts = MarketHistory.priceHistory(f, ref,
        NOW.minusSeconds(86400), NOW, MarketHistory.Interval.DAY);
    assertEquals(2, pts.size());
    assertEquals(1790899224L, pts.get(0).epochSecond);
  }

  @Test void polymarketHistoryMissingFieldIsNamed() throws Exception {
    FakeFetcher f = polyFake();
    MarketHistory.MarketRef ref = MarketHistory.resolve(f, "polymarket", "609655", null);
    f.override(C + "/prices-history", "{\"error\":\"x\"}");
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> MarketHistory.priceHistory(f, ref, NOW.minusSeconds(86400), NOW,
            MarketHistory.Interval.DAY));
    assertTrue(e.getMessage().contains("'history'"), e.getMessage());
  }

  // ─── Order book ────────────────────────────────────────────────────────────

  @Test void kalshiYesAskIsOneMinusBestNoBid() throws Exception {
    FakeFetcher f = kalshiFake();
    MarketHistory.MarketRef ref = MarketHistory.resolve(f, "kalshi", TICKER, "KXCPI");
    MarketHistory.OrderBook b = MarketHistory.orderBook(f, ref);
    assertEquals(1, b.bids.size());
    assertEquals(0.01, b.bestBid().price);
    assertEquals(2637.0, b.bestBid().size);
    // The best No bid is 0.98 (1586.01 contracts), so the best Yes ask is 0.02.
    assertEquals(5, b.asks.size());
    assertEquals(0.02, b.bestAsk().price);
    assertEquals(1586.01, b.bestAsk().size);
    assertEquals(0.04, b.asks.get(1).price);
    assertEquals(0.99, b.asks.get(4).price);
    for (int i = 1; i < b.asks.size(); i++) {
      assertTrue(b.asks.get(i).price > b.asks.get(i - 1).price);
    }
  }

  @Test void polymarketBookIsSortedBestFirst() throws Exception {
    FakeFetcher f = polyFake();
    MarketHistory.MarketRef ref = MarketHistory.resolve(f, "polymarket", "609655", null);
    MarketHistory.OrderBook b = MarketHistory.orderBook(f, ref);
    assertEquals(0.07, b.bestBid().price);
    assertEquals(500.0, b.bestBid().size);
    assertEquals(0.01, b.bids.get(2).price);
    assertEquals(0.08, b.bestAsk().price);
    assertEquals(300.0, b.bestAsk().size);
    assertEquals(0.99, b.asks.get(2).price);
    JsonNode top = b.topJson();
    assertEquals(0.01, top.get("spread").asDouble(), 1e-9);
  }

  @Test void bookMissingSideIsNamed() throws Exception {
    FakeFetcher f = polyFake();
    MarketHistory.MarketRef ref = MarketHistory.resolve(f, "polymarket", "609655", null);
    f.override(C + "/book", POLY_BOOK.replace("\"asks\"", "\"offers\""));
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> MarketHistory.orderBook(f, ref));
    assertTrue(e.getMessage().contains("'asks'"), e.getMessage());

    FakeFetcher k = kalshiFake();
    MarketHistory.MarketRef kref = MarketHistory.resolve(k, "kalshi", TICKER, "KXCPI");
    k.override(K + "/markets/" + TICKER + "/orderbook",
        "{\"orderbook_fp\":{\"yes_dollars\":[]}}");
    e = assertThrows(IllegalStateException.class, () -> MarketHistory.orderBook(k, kref));
    assertTrue(e.getMessage().contains("'no_dollars'"), e.getMessage());
  }

  // ─── Fill helper ───────────────────────────────────────────────────────────

  private static MarketHistory.OrderBook polyBook() throws Exception {
    FakeFetcher f = polyFake();
    return MarketHistory.orderBook(f, MarketHistory.resolve(f, "polymarket", "609655", null));
  }

  @Test void fillWithinOneLevelPaysThatLevel() throws Exception {
    MarketHistory.Fill fill = polyBook().fill(MarketHistory.Side.BUY_YES, 100, null);
    assertEquals(100.0, fill.filled);
    assertEquals(0.08, fill.averagePrice, 1e-12);
    assertTrue(fill.complete);
  }

  @Test void fillAcrossLevelsAveragesAndReportsPartialBook() throws Exception {
    // Asks: 300 @ 0.08, 6457.86 @ 0.98, 1101359.69 @ 0.99. Buying 400 takes 300 + 100.
    MarketHistory.Fill fill = polyBook().fill(MarketHistory.Side.BUY_YES, 400, null);
    assertEquals(400.0, fill.filled);
    assertEquals((300 * 0.08 + 100 * 0.98) / 400, fill.averagePrice, 1e-12);
    assertTrue(fill.complete);

    // A limit of 0.50 leaves only the 300 at 0.08: a partial fill, and 300 available.
    MarketHistory.Fill limited = polyBook().fill(MarketHistory.Side.BUY_YES, 400, 0.50);
    assertEquals(300.0, limited.filled);
    assertEquals(0.08, limited.averagePrice, 1e-12);
    assertEquals(300.0, limited.availableAtLimit);
    assertFalse(limited.complete);
    assertEquals(400.0, limited.requested);
  }

  @Test void fillAgainstAnExhaustedBookTakesEverythingAndFlagsIt() throws Exception {
    MarketHistory.Fill fill = polyBook().fill(MarketHistory.Side.SELL_YES, 1_000_000, null);
    // Bids: 500 @ 0.07, 68024.92 @ 0.02, 114239.89 @ 0.01.
    double total = 500 + 68024.92 + 114239.89;
    assertEquals(total, fill.filled, 1e-6);
    assertEquals(total, fill.availableAtLimit, 1e-6);
    assertEquals((500 * 0.07 + 68024.92 * 0.02 + 114239.89 * 0.01) / total,
        fill.averagePrice, 1e-9);
    assertFalse(fill.complete);
  }

  @Test void fillOnAnEmptyBookIsZeroWithNoPrice() {
    MarketHistory.OrderBook empty = new MarketHistory.OrderBook();
    MarketHistory.Fill fill = empty.fill(MarketHistory.Side.BUY_YES, 10, null);
    assertEquals(0.0, fill.filled);
    assertNull(fill.averagePrice);
    assertFalse(fill.complete);
    assertThrows(IllegalArgumentException.class,
        () -> empty.fill(MarketHistory.Side.BUY_YES, 0, null));
  }

  @Test void buyingNoWalksTheYesBidsMirrored() throws Exception {
    // Yes bids 0.07 x 500, 0.02 x 68024.92: No costs 0.93 then 0.98.
    MarketHistory.Fill fill = polyBook().fill(MarketHistory.Side.BUY_NO, 600, 0.95);
    assertEquals(500.0, fill.filled);
    assertEquals(0.93, fill.averagePrice, 1e-12);
    assertEquals(500.0, fill.availableAtLimit);
    assertFalse(fill.complete);
    MarketHistory.Fill both = polyBook().fill(MarketHistory.Side.BUY_NO, 600, null);
    assertEquals((500 * 0.93 + 100 * 0.98) / 600, both.averagePrice, 1e-12);
    // Selling No gives it at 1 minus each Yes ask, highest first: 1 - 0.08 = 0.92 x 300.
    MarketHistory.Fill sell = polyBook().fill(MarketHistory.Side.SELL_NO, 100, 0.90);
    assertEquals(0.92, sell.averagePrice, 1e-12);
    assertTrue(sell.complete);
  }

  @Test void kalshiFillUsesDerivedAsks() throws Exception {
    FakeFetcher f = kalshiFake();
    MarketHistory.OrderBook b = MarketHistory.orderBook(f,
        MarketHistory.resolve(f, "kalshi", TICKER, "KXCPI"));
    MarketHistory.Fill fill = b.fill(MarketHistory.Side.BUY_YES, 2000, 0.03);
    assertEquals(1586.01, fill.filled, 1e-9);
    assertEquals(0.02, fill.averagePrice, 1e-12);
    assertFalse(fill.complete);
  }

  // ─── Tool ──────────────────────────────────────────────────────────────────

  @Test void toolReturnsSeriesSummaryAndBookTopOnKalshi() throws Exception {
    JsonNode out = MAPPER.readTree(tool(kalshiFake()).priceHistoryTool(
        args("source", "kalshi", "market_id", TICKER)));
    assertEquals(TICKER, out.get("market_id").asText());
    assertEquals(4, out.get("points_total").asInt());
    assertEquals(2, out.get("points_priced").asInt());
    assertEquals(0.16, out.get("first").get("price").asDouble());
    assertEquals(0.07, out.get("last").get("price").asDouble());
    assertEquals(0.07, out.get("min").get("price").asDouble());
    assertEquals(0.16, out.get("max").get("price").asDouble());
    assertTrue(out.get("points").get(0).get("price").isNull());
    JsonNode top = out.get("order_book");
    assertEquals(0.01, top.get("best_bid").asDouble());
    assertEquals(0.02, top.get("best_ask").asDouble());
    assertEquals(1586.01, top.get("best_ask_size").asDouble());
    assertEquals(30, out.get("window_days").asInt());
  }

  @Test void toolAcceptsASingleMarketEventOnBothVenues() throws Exception {
    JsonNode k = MAPPER.readTree(tool(kalshiFake()).priceHistoryTool(
        args("source", "kalshi", "event_id", "KXONE")));
    assertEquals(TICKER, k.get("market_id").asText());
    JsonNode p = MAPPER.readTree(tool(polyFake()).priceHistoryTool(
        args("source", "polymarket", "event_id", "48802", "interval", "hour")));
    assertEquals("609655", p.get("market_id").asText());
    assertEquals("hour", p.get("interval").asText());
    assertEquals(0.075, p.get("last").get("price").asDouble());
    assertEquals(0.075, p.get("min").get("price").asDouble());
    assertEquals(0.085, p.get("max").get("price").asDouble());
    assertEquals(0.07, p.get("order_book").get("best_bid").asDouble());
    assertEquals(0.08, p.get("order_book").get("best_ask").asDouble());
  }

  @Test void toolNamesTheMarketsOfAMultiMarketEvent() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> tool(kalshiFake()).priceHistoryTool(
            args("source", "kalshi", "event_id", "KXCPI-26OCT")));
    assertTrue(e.getMessage().contains("KXCPI-26OCT-T0.9"), e.getMessage());
    e = assertThrows(IllegalArgumentException.class,
        () -> tool(polyFake()).priceHistoryTool(
            args("source", "polymarket", "event_id", "69702")));
    assertTrue(e.getMessage().contains("609656"), e.getMessage());
  }

  @Test void toolSkipsTheBookOfAClosedMarketAndSaysSo() throws Exception {
    FakeFetcher f = polyFake();
    JsonNode out = MAPPER.readTree(tool(f).priceHistoryTool(
        args("source", "polymarket", "market_id", "601697")));
    assertTrue(out.get("order_book").isNull());
    assertTrue(out.get("order_book_note").asText().contains("closed"));
    for (String u : f.urls) {
      assertFalse(u.contains("/book?"), u);
    }
  }

  @Test void toolReportsNoPricedPointsExplicitly() throws Exception {
    FakeFetcher f = polyFake().override(C + "/prices-history", "{\"history\":[]}");
    JsonNode out = MAPPER.readTree(tool(f).priceHistoryTool(
        args("source", "polymarket", "market_id", "609655")));
    assertEquals(0, out.get("points_total").asInt());
    assertTrue(out.get("first").isNull());
    assertTrue(out.has("note"));
  }

  @Test void toolRejectsUnknownAndBadArguments() {
    MarketHistory t = tool(kalshiFake());
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> t.priceHistoryTool(args("source", "kalshi", "market_id", TICKER, "bogus", "1")));
    assertTrue(e.getMessage().contains("unknown argument 'bogus'"), e.getMessage());
    assertThrows(IllegalArgumentException.class,
        () -> t.priceHistoryTool(args("market_id", TICKER)));
    assertThrows(IllegalArgumentException.class,
        () -> t.priceHistoryTool(args("source", "kalshi")));
    assertThrows(IllegalArgumentException.class,
        () -> t.priceHistoryTool(args("source", "kalshi", "market_id", TICKER,
            "event_id", "KXONE")));
    assertThrows(IllegalArgumentException.class,
        () -> t.priceHistoryTool(args("source", "kalshi", "market_id", TICKER,
            "interval", "week")));
    ObjectNode big = args("source", "kalshi", "market_id", TICKER);
    big.put("window_days", 400);
    assertThrows(IllegalArgumentException.class, () -> t.priceHistoryTool(big));
    ObjectNode frac = args("source", "kalshi", "market_id", TICKER);
    frac.put("window_days", 2.5);
    assertThrows(IllegalArgumentException.class, () -> t.priceHistoryTool(frac));
  }

  @Test void toolDefNamesTheToolAndItsArguments() {
    ObjectNode def = MarketHistory.toolDef();
    assertEquals("market_price_history", def.get("name").asText());
    JsonNode props = def.get("inputSchema").get("properties");
    for (String n : new String[] {"source", "market_id", "event_id", "series", "window_days",
        "interval"}) {
      assertTrue(props.has(n), n);
    }
    assertEquals("source", def.get("inputSchema").get("required").get(0).asText());
  }
}
