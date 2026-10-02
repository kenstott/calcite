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

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The venue history layer against the live Kalshi and Polymarket APIs: one current event per
 * venue read for its price history and order book, and one settled series per venue read for
 * its outcomes.
 */
@Tag("integration")
class MarketHistoryLiveIntegrationTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final PredictionMarkets.Fetcher FETCHER = new PredictionMarkets.HttpFetcher();

  @Test void kalshiSettledPriceHistoryAndBook() throws Exception {
    List<MarketHistory.SettledMarket> settled =
        MarketHistory.settledMarkets(FETCHER, "kalshi", "KXCPI", 5);
    assertFalse(settled.isEmpty());
    for (MarketHistory.SettledMarket m : settled) {
      assertTrue("yes".equals(m.outcome) || "no".equals(m.outcome), m.outcome);
      assertTrue(m.lastPrice >= 0 && m.lastPrice <= 1);
    }

    // The current CPI event: its first market, by ticker.
    JsonNode events = FETCHER.get(PredictionMarkets.KALSHI
        + "/markets?status=open&series_ticker=KXCPI&limit=1");
    String ticker = events.get("markets").get(0).get("ticker").asText();
    MarketHistory.MarketRef ref = MarketHistory.resolve(FETCHER, "kalshi", ticker, null);
    assertEquals("KXCPI", ref.series);
    Instant now = Instant.now();
    List<MarketHistory.PricePoint> pts = MarketHistory.priceHistory(FETCHER, ref,
        now.minus(Duration.ofDays(14)), now, MarketHistory.Interval.DAY);
    assertFalse(pts.isEmpty());
    MarketHistory.OrderBook book = MarketHistory.orderBook(FETCHER, ref);
    for (MarketHistory.Level l : book.asks) {
      assertTrue(l.price > 0 && l.price < 1);
    }
    for (int i = 1; i < book.bids.size(); i++) {
      assertTrue(book.bids.get(i).price < book.bids.get(i - 1).price);
    }

    MarketHistory tool = new MarketHistory(FETCHER, Instant::now);
    ObjectNode args = MAPPER.createObjectNode();
    args.put("source", "kalshi");
    args.put("market_id", ticker);
    JsonNode out = MAPPER.readTree(tool.priceHistoryTool(args));
    assertTrue(out.get("points_total").asInt() > 0, out.toString());
    assertTrue(out.get("order_book").has("best_bid"), out.toString());
  }

  /** Kalshi's historical tier: the settled listing past the live tier's cutoff, and the
   *  candles of one of its markets. */
  @Test void kalshiHistoricalSettledMarketsAndCandles() throws Exception {
    List<MarketHistory.SettledMarket> settled =
        MarketHistory.settledMarkets(FETCHER, "kalshi", "KXCPI", 400);
    MarketHistory.SettledMarket old = null;
    for (MarketHistory.SettledMarket m : settled) {
      if (m.historical) {
        old = m;
        break;
      }
    }
    assertTrue(old != null, "no KXCPI market came from the historical tier in "
        + settled.size() + " settled markets");
    assertTrue("yes".equals(old.outcome) || "no".equals(old.outcome), old.outcome);

    MarketHistory.MarketRef ref = MarketHistory.resolve(FETCHER, "kalshi", old.marketId, null);
    assertTrue(ref.historical);
    assertFalse(ref.open);
    assertEquals("KXCPI", ref.series);
    Instant close = PredictionMarkets.closeInstant(old.closeTime);
    List<MarketHistory.PricePoint> pts = MarketHistory.priceHistory(FETCHER, ref,
        close.minus(Duration.ofDays(14)), close, MarketHistory.Interval.DAY);
    assertFalse(pts.isEmpty());
    for (MarketHistory.PricePoint p : pts) {
      assertTrue(p.yesAsk == null || (p.yesAsk >= 0 && p.yesAsk <= 1), String.valueOf(p.yesAsk));
    }
  }

  @Test void polymarketSettledPriceHistoryAndBook() throws Exception {
    List<MarketHistory.SettledMarket> settled =
        MarketHistory.settledMarkets(FETCHER, "polymarket", "fed-interest-rates", 5);
    assertFalse(settled.isEmpty());
    for (MarketHistory.SettledMarket m : settled) {
      assertFalse(m.outcome.isEmpty());
    }

    // A current economy event with one market.
    JsonNode events = FETCHER.get(PredictionMarkets.POLYMARKET
        + "/events?closed=false&limit=20&tag_slug=economy");
    String marketId = null;
    for (JsonNode e : events) {
      if (e.get("markets").size() == 1 && e.get("markets").get(0).path("acceptingOrders")
          .asBoolean() && e.get("markets").get(0).hasNonNull("clobTokenIds")) {
        marketId = e.get("markets").get(0).get("id").asText();
        break;
      }
    }
    assertTrue(marketId != null, "no open single-market economy event on Polymarket");
    MarketHistory.MarketRef ref = MarketHistory.resolve(FETCHER, "polymarket", marketId, null);
    Instant now = Instant.now();
    List<MarketHistory.PricePoint> pts = MarketHistory.priceHistory(FETCHER, ref,
        now.minus(Duration.ofDays(14)), now, MarketHistory.Interval.DAY);
    assertFalse(pts.isEmpty());
    MarketHistory.OrderBook book = MarketHistory.orderBook(FETCHER, ref);
    assertFalse(book.bids.isEmpty() && book.asks.isEmpty());
    for (int i = 1; i < book.asks.size(); i++) {
      assertTrue(book.asks.get(i).price >= book.asks.get(i - 1).price);
    }

    MarketHistory tool = new MarketHistory(FETCHER, Instant::now);
    ObjectNode args = MAPPER.createObjectNode();
    args.put("source", "polymarket");
    args.put("market_id", marketId);
    JsonNode out = MAPPER.readTree(tool.priceHistoryTool(args));
    assertTrue(out.get("points_total").asInt() > 0, out.toString());
    assertTrue(out.get("order_book").has("best_ask"), out.toString());
  }
}
