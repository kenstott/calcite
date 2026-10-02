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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Size-aware pricing, order tickets and re-quotes. */
@Tag("unit")
class MarketPricingSizeTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.parse("2026-10-02T12:00:00Z");
  private static final double RATE = 0.07;

  private static PredictionMarkets.LiveEvent live(String closeTime) {
    PredictionMarkets.Market m = new PredictionMarkets.Market();
    m.source = "kalshi";
    m.eventId = "EV1";
    m.marketId = "EV1-A";
    m.title = "A above 3";
    m.yesBid = 0.40;
    m.yesAsk = 0.42;
    m.strikeType = "greater";
    m.floorStrike = 3.0;
    m.closeTime = closeTime;
    PredictionMarkets.Event e = new PredictionMarkets.Event();
    e.source = "kalshi";
    e.eventId = "EV1";
    e.settlementSources = new java.util.ArrayList<>();
    e.legs.add(m);
    PredictionMarkets.LiveEvent live = new PredictionMarkets.LiveEvent();
    live.event = e;
    live.feeRates.put("EV1-A", RATE);
    return live;
  }

  /** Fair 0.60 of YES: values above 3 are 60% of the samples. */
  private static MarketPricing.Forecast forecast(double yesShare) {
    int n = 1000;
    double[] v = new double[n];
    for (int i = 0; i < n; i++) {
      v[i] = i < yesShare * n ? 4 : 2;
    }
    return new MarketPricing.Forecast(v, false, "test");
  }

  private static MarketHistory.OrderBook book(double[][] bids, double[][] asks) {
    MarketHistory.OrderBook b = new MarketHistory.OrderBook();
    b.source = "kalshi";
    b.marketId = "EV1-A";
    for (double[] l : bids) {
      b.bids.add(new MarketHistory.Level(l[0], l[1]));
    }
    for (double[] l : asks) {
      b.asks.add(new MarketHistory.Level(l[0], l[1]));
    }
    return b;
  }

  private static JsonNode row(MarketPricing.Priced p) {
    return p.json.get("priced_markets").get(0);
  }

  private static MarketPricing.Priced price(String close, MarketPricing.Forecast f,
      MarketPricing.SizeOptions opts) {
    return MarketPricing.priceEvent(live(close), f, new HashMap<>(), 0.05, null, false, null,
        opts);
  }

  @Test void plainPricingIsUnchangedAndCarriesNoSizeFields() {
    MarketPricing.Priced p = MarketPricing.priceEvent(live("2026-11-01T12:00:00Z"),
        forecast(0.60), new HashMap<>(), 0.05, null, false, null);
    JsonNode r = row(p);
    assertEquals("mispriced", r.get("verdict").asText());
    assertFalse(r.has("days_to_settlement"));
    assertFalse(r.has("ticket"));
    assertFalse(p.json.has("quote_time"));
  }

  @Test void daysAnnualizedAndBreakevenArePerRow() {
    MarketPricing.Priced p = price("2026-11-01T12:00:00Z", forecast(0.60),
        new MarketPricing.SizeOptions(NOW, null, null));
    JsonNode r = row(p);
    assertEquals(30.0, r.get("days_to_settlement").asDouble(), 1e-9);
    double fee = RATE * 0.42 * 0.58;
    double ret = (0.60 - 0.42 - fee) / (0.42 + fee);
    assertEquals(ret * 365 / 30, r.get("annualized_return_simple_365d").asDouble(), 1e-3);
    assertEquals(0.42 + fee, r.get("breakeven_fair_buy_yes").asDouble(), 1e-4);
    assertEquals(0.42 + fee, r.get("breakeven_fair").asDouble(), 1e-4);
    double feeNo = RATE * 0.60 * 0.40;
    assertEquals(1 - 0.60 - feeNo, r.get("breakeven_fair_buy_no").asDouble(), 1e-4);
    assertTrue(p.json.get("annualized_return_convention").asText().contains("365"));
  }

  @Test void breakevenFairGivesZeroEdge() {
    // Pricing again with the forecast set to the breakeven leaves no edge.
    double be = row(price("2026-11-01T12:00:00Z", forecast(0.60),
        new MarketPricing.SizeOptions(NOW, null, null))).get("breakeven_fair").asDouble();
    MarketPricing.Priced p = MarketPricing.priceEvent(live("2026-11-01T12:00:00Z"),
        forecast(Math.round(be * 1000) / 1000.0), new HashMap<>(), 0.0, null, false, null);
    assertEquals(0.0, row(p).get("edge").asDouble(), 1e-3);
  }

  @Test void underOneDayIsNotAnnualized() {
    JsonNode r = row(price("2026-10-03T00:00:00Z", forecast(0.60),
        new MarketPricing.SizeOptions(NOW, null, null)));
    assertEquals(0.5, r.get("days_to_settlement").asDouble(), 1e-9);
    assertFalse(r.has("annualized_return_simple_365d"));
    assertTrue(r.get("annualized_return_note").asText().contains("under one day"));
  }

  @Test void limitPriceClearsMinEdgeAfterTheFeeAtThatPrice() {
    JsonNode t = row(price("2026-11-01T12:00:00Z", forecast(0.60),
        new MarketPricing.SizeOptions(NOW, null, null))).get("ticket");
    assertEquals("yes", t.get("side").asText());
    double limit = t.get("limit_price").asDouble();
    // On the tick, and edge at the limit still clears min_edge while one tick more does not.
    assertEquals(Math.round(limit * 100) / 100.0, limit, 1e-12);
    assertTrue(0.60 - limit - RATE * limit * (1 - limit) >= 0.05 - 1e-9);
    double up = limit + 0.01;
    assertTrue(0.60 - up - RATE * up * (1 - up) < 0.05);
    assertEquals(NOW.toString(), t.get("quote_time").asText());
    assertEquals("settlement_close",
        t.get("void_conditions").get(0).get("type").asText());
    assertEquals("2026-11-01T12:00:00Z", t.get("void_conditions").get(0).get("time").asText());
  }

  @Test void noSideTicketUsesTheNoContractsPrice() {
    // Fair 0.10 of YES, so NO wins 0.90 and costs 1 - 0.40 = 0.60.
    JsonNode r = row(price("2026-11-01T12:00:00Z", forecast(0.10),
        new MarketPricing.SizeOptions(NOW, null, null)));
    JsonNode t = r.get("ticket");
    assertEquals("no", t.get("side").asText());
    double limit = t.get("limit_price").asDouble();
    assertTrue(0.90 - limit - RATE * limit * (1 - limit) >= 0.05 - 1e-9);
    double up = limit + 0.01;
    assertTrue(0.90 - up - RATE * up * (1 - up) < 0.05);
    assertEquals(1 - 0.10 - 0.60 * 0.40 * RATE - 0.60 + 0.60,
        1 - 0.10 - 0.60 * 0.40 * RATE, 1e-12);
    assertEquals(1 - 0.60 - RATE * 0.60 * 0.40, r.get("breakeven_fair").asDouble(), 1e-4);
  }

  @Test void maxPriceWithZeroFeeIsFairMinusMinEdge() {
    assertEquals(0.55, MarketPricing.maxPrice(0.60, 0.05, 0), 1e-12);
    assertEquals(0.0, MarketPricing.maxPrice(0.04, 0.05, RATE), 0.0);
  }

  @Test void noTicketUnlessMispriced() {
    JsonNode r = row(price("2026-11-01T12:00:00Z", forecast(0.43),
        new MarketPricing.SizeOptions(NOW, null, null)));
    assertEquals("within_min_edge", r.get("verdict").asText());
    assertFalse(r.has("ticket"));
    assertFalse(r.has("depth"));
  }

  @Test void missingBookIsStatedNotReplacedByTheQuote() {
    JsonNode r = row(price("2026-11-01T12:00:00Z", forecast(0.60),
        new MarketPricing.SizeOptions(NOW, new HashMap<>(), 100.0)));
    JsonNode d = r.get("depth");
    assertFalse(d.get("book_read").asBoolean());
    assertFalse(d.has("contracts_at_or_under_limit"));
    assertFalse(d.has("fill_average_price"));
  }

  @Test void depthAndFillWalkTheBook() {
    // Asks: 100 @ 0.42, 200 @ 0.44, 500 @ 0.60. The limit for fair 0.60 sits near 0.50.
    Map<String, MarketHistory.OrderBook> books = new HashMap<>();
    books.put("EV1-A", book(new double[][] {{0.40, 50}},
        new double[][] {{0.42, 100}, {0.44, 200}, {0.60, 500}}));
    JsonNode r = row(price("2026-11-01T12:00:00Z", forecast(0.60),
        new MarketPricing.SizeOptions(NOW, books, 250.0)));
    JsonNode d = r.get("depth");
    assertTrue(d.get("book_read").asBoolean());
    assertEquals(300.0, d.get("contracts_at_or_under_limit").asDouble(), 1e-9);
    assertEquals(100 * 0.42 + 200 * 0.44, d.get("dollars_at_or_under_limit").asDouble(), 1e-9);
    assertEquals(0.42, d.get("book_best_price").asDouble(), 1e-9);
    assertTrue(d.get("fills_completely_under_limit").asBoolean());
    double avg = (100 * 0.42 + 150 * 0.44) / 250;
    assertEquals(avg, d.get("fill_average_price").asDouble(), 1e-4);
    double fee = RATE * avg * (1 - avg);
    assertEquals(fee * 250, d.get("fill_fee_dollars").asDouble(), 1e-3);
    assertEquals(0.60 - avg - fee, d.get("fill_edge").asDouble(), 1e-4);
  }

  @Test void sizeBeyondTheLimitIsNotCompleteUnderIt() {
    Map<String, MarketHistory.OrderBook> books = new HashMap<>();
    books.put("EV1-A", book(new double[][] {}, new double[][] {{0.42, 100}, {0.60, 500}}));
    JsonNode d = row(price("2026-11-01T12:00:00Z", forecast(0.60),
        new MarketPricing.SizeOptions(NOW, books, 200.0))).get("depth");
    assertFalse(d.get("fills_completely_under_limit").asBoolean());
    assertEquals(200.0, d.get("size_filled_by_book").asDouble(), 1e-9);
    assertEquals(100.0, d.get("contracts_at_or_under_limit").asDouble(), 1e-9);
  }

  @Test void emptySideIsStated() {
    Map<String, MarketHistory.OrderBook> books = new HashMap<>();
    books.put("EV1-A", book(new double[][] {}, new double[][] {}));
    JsonNode d = row(price("2026-11-01T12:00:00Z", forecast(0.60),
        new MarketPricing.SizeOptions(NOW, books, 10.0))).get("depth");
    assertEquals(0.0, d.get("contracts_at_or_under_limit").asDouble(), 0.0);
    assertTrue(d.get("fill_average_price").isNull());
    assertTrue(d.get("fill_note").asText().contains("nothing"));
  }

  private static JsonNode ticket(Double size) throws Exception {
    Map<String, MarketHistory.OrderBook> books = new HashMap<>();
    books.put("EV1-A", book(new double[][] {}, new double[][] {{0.42, 1000}}));
    JsonNode t = row(price("2026-11-01T12:00:00Z", forecast(0.60),
        new MarketPricing.SizeOptions(NOW, books, size))).get("ticket");
    return MAPPER.readTree(t.toString());
  }

  @Test void requoteOpenPartlyOpenAndGone() throws Exception {
    JsonNode sized = ticket(200.0);
    MarketHistory.OrderBook full = book(new double[][] {}, new double[][] {{0.43, 250}});
    JsonNode open = MarketPricing.requote(sized, full);
    assertEquals("open", open.get("status").asText());
    assertEquals(0.43, open.get("current_best_price").asDouble(), 1e-9);
    assertEquals(0.60 - 0.43 - RATE * 0.43 * 0.57, open.get("edge_at_current_best").asDouble(),
        1e-4);

    JsonNode partly = MarketPricing.requote(sized,
        book(new double[][] {}, new double[][] {{0.43, 80}, {0.90, 500}}));
    assertEquals("partly_open", partly.get("status").asText());
    assertEquals(80.0, partly.get("contracts_left").asDouble(), 1e-9);

    JsonNode gone = MarketPricing.requote(sized,
        book(new double[][] {}, new double[][] {{0.90, 500}}));
    assertEquals("gone", gone.get("status").asText());
    assertEquals(0.90, gone.get("current_best_price").asDouble(), 1e-9);
  }

  @Test void requoteWithoutSizeNeedsOneContract() throws Exception {
    JsonNode t = ticket(null);
    assertFalse(t.has("size"));
    assertEquals("open", MarketPricing.requote(t,
        book(new double[][] {}, new double[][] {{0.43, 1}})).get("status").asText());
    assertEquals("partly_open", MarketPricing.requote(t,
        book(new double[][] {}, new double[][] {{0.43, 0.5}})).get("status").asText());
  }

  @Test void requoteEmptySideAndMismatchesAreStated() throws Exception {
    JsonNode t = ticket(10.0);
    JsonNode r = MarketPricing.requote(t, book(new double[][] {}, new double[][] {}));
    assertEquals("gone", r.get("status").asText());
    assertTrue(r.get("current_best_price").isNull());
    assertTrue(r.get("edge_at_current_best").isNull());
    MarketHistory.OrderBook other = book(new double[][] {}, new double[][] {{0.4, 5}});
    other.marketId = "OTHER";
    assertThrows(IllegalArgumentException.class, () -> MarketPricing.requote(t, other));
    assertThrows(IllegalArgumentException.class,
        () -> MarketPricing.requote(MAPPER.createObjectNode(), other));
  }

  @Test void noSideTicketRequotesAgainstTheBids() throws Exception {
    Map<String, MarketHistory.OrderBook> books = new HashMap<>();
    books.put("EV1-A", book(new double[][] {{0.40, 300}}, new double[][] {{0.42, 10}}));
    JsonNode t = MAPPER.readTree(row(price("2026-11-01T12:00:00Z", forecast(0.10),
        new MarketPricing.SizeOptions(NOW, books, 100.0))).get("ticket").toString());
    JsonNode r = MarketPricing.requote(t,
        book(new double[][] {{0.40, 300}}, new double[][] {{0.42, 10}}));
    assertEquals("open", r.get("status").asText());
    assertEquals(0.60, r.get("current_best_price").asDouble(), 1e-9);
    assertNull(r.get("note"));
  }

  @Test void badSizeIsRefused() {
    assertThrows(IllegalArgumentException.class,
        () -> new MarketPricing.SizeOptions(NOW, null, 0.0));
    assertThrows(IllegalArgumentException.class,
        () -> new MarketPricing.SizeOptions(null, null, null));
  }
}
