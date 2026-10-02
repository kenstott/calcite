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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The basket scan against canned venue responses: no network, no catalog.
 *
 * <p>The fixture is October CPI on both venues. Kalshi asks 0.44 for "above 3.0"; Polymarket
 * bids whatever the test sets for its market labelled "Above 3.0%". At a 0.60 bid, YES on
 * Kalshi and NO on Polymarket cost 0.84 before fees and pay 1 at every outcome.
 */
@Tag("unit")
class MarketBasketScanTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.parse("2026-10-02T00:00:00Z");
  private static final String KALSHI_ID = "KXCPI-26OCT";
  private static final String POLY_ID = "9001";

  /** Answers a URL from the first registered prefix it starts with. */
  private static final class FakeFetcher implements PredictionMarkets.Fetcher {
    final Map<String, JsonNode> byPrefix = new LinkedHashMap<>();
    int eventReads;

    @Override public JsonNode get(String url) throws IOException {
      if (url.startsWith(PredictionMarkets.KALSHI + "/events/")
          || url.startsWith(PredictionMarkets.POLYMARKET + "/events/" + POLY_ID)) {
        eventReads++;
      }
      for (Map.Entry<String, JsonNode> e : byPrefix.entrySet()) {
        if (url.startsWith(e.getKey())) {
          return e.getValue();
        }
      }
      throw new IOException("no canned response for " + url);
    }
  }

  private static ObjectNode kalshiMarket(String ticker, double strike, String bid,
      String ask) {
    ObjectNode m = MAPPER.createObjectNode();
    m.put("ticker", ticker);
    m.put("status", "active");
    m.put("title", "Will CPI rise more than " + strike + "% in October 2026?");
    m.put("yes_bid_dollars", bid);
    m.put("yes_ask_dollars", ask);
    m.put("last_price_dollars", bid);
    m.put("volume_fp", "5000.00");
    m.put("volume_24h_fp", "800.00");
    m.put("open_interest_fp", "2000.00");
    m.put("close_time", "2026-11-12T13:30:00Z");
    m.put("strike_type", "greater");
    m.put("floor_strike", strike);
    m.put("rules_primary", "Resolves on the BLS CPI-U 12-month change, one decimal.");
    return m;
  }

  private static ObjectNode kalshiEvent() {
    ObjectNode ev = MAPPER.createObjectNode();
    ev.put("title", "CPI inflation in October 2026");
    ev.put("event_ticker", KALSHI_ID);
    ev.put("series_ticker", "KXCPI");
    ev.put("category", "Economics");
    ev.putArray("settlement_sources").addObject().put("name", "BLS");
    ArrayNode markets = ev.putArray("markets");
    markets.add(kalshiMarket(KALSHI_ID + "-T2.8", 2.8, "0.7000", "0.7400"));
    markets.add(kalshiMarket(KALSHI_ID + "-T3.0", 3.0, "0.4000", "0.4400"));
    markets.add(kalshiMarket(KALSHI_ID + "-T3.2", 3.2, "0.2000", "0.2400"));
    return ev;
  }

  private static void polymarketMarket(ArrayNode markets, String id, String label, double bid,
      double ask) {
    ObjectNode m = markets.addObject();
    m.put("id", id);
    m.put("question", "US CPI inflation in October: " + (label == null ? id : label) + "?");
    if (label != null) {
      m.put("groupItemTitle", label);
    }
    m.put("active", true);
    m.put("closed", false);
    m.put("outcomes", "[\"Yes\",\"No\"]");
    m.put("outcomePrices", "[\"" + bid + "\",\"" + (1 - bid) + "\"]");
    m.put("bestBid", bid);
    m.put("bestAsk", ask);
    m.put("volumeNum", 20000);
    m.put("volume24hr", 3000);
    m.put("endDate", "2026-11-12T12:00:00Z");
    m.put("feesEnabled", false);
    m.put("description", "Resolves on BLS CPI-U year over year, one decimal.");
  }

  private static ObjectNode polymarketEvent() {
    ObjectNode ev = MAPPER.createObjectNode();
    ev.put("id", POLY_ID);
    ev.put("title", "US CPI inflation October 2026");
    ev.put("slug", "us-cpi-october-2026");
    ev.putArray("tags").addObject().put("label", "Economy");
    ev.putArray("markets");
    return ev;
  }

  /** One Polymarket market labelled "Above 3.0%", bid as given. */
  private static ObjectNode polymarketAbove(double bid) {
    ObjectNode ev = polymarketEvent();
    polymarketMarket((ArrayNode) ev.get("markets"), "m1", "Above 3.0%", bid, bid + 0.02);
    return ev;
  }

  private static FakeFetcher venues(ObjectNode polymarket) {
    FakeFetcher f = new FakeFetcher();
    ObjectNode series = MAPPER.createObjectNode();
    series.putObject("series").put("fee_type", "quadratic").put("fee_multiplier", 1);
    f.byPrefix.put(PredictionMarkets.KALSHI + "/series/KXCPI", series);
    ObjectNode one = MAPPER.createObjectNode();
    one.set("event", kalshiEvent());
    f.byPrefix.put(PredictionMarkets.KALSHI + "/events/" + KALSHI_ID, one);
    ObjectNode kList = MAPPER.createObjectNode();
    kList.putArray("events").add(kalshiEvent());
    kList.put("cursor", "");
    f.byPrefix.put(PredictionMarkets.KALSHI + "/events?", kList);
    f.byPrefix.put(PredictionMarkets.POLYMARKET + "/events/" + POLY_ID, polymarket);
    ObjectNode pList = MAPPER.createObjectNode();
    pList.putArray("events").add(polymarket);
    pList.put("next_cursor", "");
    f.byPrefix.put(PredictionMarkets.POLYMARKET + "/events/keyset?", pList);
    return f;
  }

  private static MarketBasketScan scan(FakeFetcher f, long budgetMillis) {
    return new MarketBasketScan(f,
        new PredictionMarkets.ListingCache(f, Duration.ofMinutes(15)), () -> NOW, 10_000L,
        budgetMillis, Duration.ofMinutes(15));
  }

  private static JsonNode args(String json) throws Exception {
    return MAPPER.readTree(json.replace('\'', '"'));
  }

  private static JsonNode run(ObjectNode polymarket, String json) throws Exception {
    return MAPPER.readTree(scan(venues(polymarket), 60_000L).scan(args(json)));
  }

  @Test void aCrossVenueLockHasALegOnEachVenueAndItsFloorIsNetOfFees() throws Exception {
    JsonNode out = run(polymarketAbove(0.60), "{}");

    assertEquals("complete", out.get("status").asText());
    JsonNode funnel = out.get("funnel");
    assertEquals(2, funnel.get("events_read").asInt());
    assertEquals(1, funnel.get("cross_venue_pairs").asInt());
    assertEquals(1, funnel.get("cross_venue_pairs_priced").asInt());
    assertEquals(1, funnel.get("cross_venue_pairs_with_a_lock").asInt());
    assertEquals(1, funnel.get("events_with_conditions_read_from_labels").asInt());

    JsonNode best = out.get("baskets").get(0);
    assertEquals("cross_venue", best.get("type").asText());
    assertEquals(2, best.get("venues").size());
    assertEquals(2, best.get("events").size());
    // YES above 3.0 on Kalshi at 0.44 plus its fee 0.07 x 0.44 x 0.56, NO on Polymarket at
    // 0.40 with fees off: pays exactly 1 at every outcome.
    double cost = 0.44 + 0.07 * 0.44 * 0.56 + 0.40;
    assertEquals(cost, best.get("cost").asDouble(), 1e-3);
    assertEquals(1 - cost, best.get("floor_profit").asDouble(), 1e-3);
    assertEquals((1 - cost) / cost, best.get("floor").asDouble(), 1e-3);
    assertEquals(2, best.get("legs").size());
    for (JsonNode leg : best.get("legs")) {
      assertEquals(3.0, leg.get("condition").get("above").asDouble(), 1e-12);
      assertEquals("kalshi".equals(leg.get("source").asText()) ? "yes" : "no",
          leg.get("side").asText());
    }
    assertNotNull(best.get("rules_match"));
    assertFalse("differ".equals(best.get("rules_match").asText())
        && best.get("rules_differing").size() == 0);
    for (JsonNode b : out.get("baskets")) {
      assertEquals(2, b.get("venues").size(), "a single-venue subset is not a cross-venue lock");
    }
    assertTrue(out.get("next").asText().contains("rules_match"));
  }

  @Test void aLockNamesTheSeriesBothEventsSettleOn() throws Exception {
    JsonNode out = run(polymarketAbove(0.60), "{}");

    assertEquals(1, out.get("baskets").size(), "one basket for a pair, its best floor");
    JsonNode best = out.get("baskets").get(0);
    assertEquals("verified", best.get("same_quantity").asText());
    assertEquals("CUUR0000SA0 yoy_pct", best.get("settles_on").asText());
    assertEquals(0, out.get("unverified").size());
  }

  @Test void coreAgainstHeadlineInflationIsNotPricedHoweverWideTheGap() throws Exception {
    ObjectNode core = polymarketAbove(0.60);
    core.put("title", "US core CPI inflation October 2026");

    JsonNode out = run(core, "{}");

    assertEquals("complete", out.get("status").asText());
    assertEquals(0, out.get("baskets").size());
    assertEquals(0, out.get("unverified").size());
    assertEquals(0, out.get("funnel").get("cross_venue_pairs_priced").asInt());
    assertEquals(0, out.get("funnel").get("cross_venue_pairs_with_a_lock").asInt());
    String why = out.get("not_priced").get(0).get("why").asText();
    assertTrue(why.contains("different series"), why);
    assertTrue(why.contains("CUUR0000SA0L1E") && why.contains("CUUR0000SA0 "), why);
  }

  @Test void differentSeriesAreNotPricedEvenWhenOneTransformIsUnread() throws Exception {
    ObjectNode core = polymarketAbove(0.60);
    core.put("title", "US core CPI October 2026");
    for (JsonNode m : core.get("markets")) {
      ((ObjectNode) m).put("question", "US core CPI in October: Above 3.0%?");
      ((ObjectNode) m).put("description", "Resolves on the BLS release.");
    }

    JsonNode out = run(core, "{}");

    assertEquals(0, out.get("baskets").size());
    assertEquals(0, out.get("unverified").size());
    String why = out.get("not_priced").get(0).get("why").asText();
    assertTrue(why.contains("different series: CUUR0000SA0 and "), why);
  }

  @Test void aPairWithOneSeriesUnresolvedIsListedApartAndIsNotALock() throws Exception {
    ObjectNode shelter = polymarketAbove(0.60);
    shelter.put("title", "US shelter CPI inflation October 2026");

    JsonNode out = run(shelter, "{}");

    assertEquals(0, out.get("baskets").size());
    assertEquals(0, out.get("funnel").get("locks_found").asInt());
    assertEquals(0, out.get("funnel").get("cross_venue_pairs_with_a_lock").asInt());
    assertEquals(1, out.get("funnel").get("cross_venue_pairs_unverified_with_a_gap").asInt());
    JsonNode u = out.get("unverified").get(0);
    assertEquals("unverified", u.get("same_quantity").asText());
    assertTrue(u.get("same_quantity_reason").asText().contains("polymarket event"),
        u.get("same_quantity_reason").asText());
    assertNull(u.get("settles_on"));
    assertTrue(out.get("next").asText().contains("MUST NOT report an unverified entry"),
        out.get("next").asText());
  }

  @Test void quotesThatLeaveNoGapGiveNoBasket() throws Exception {
    // Polymarket bids 0.42: NO costs 0.58, and with Kalshi YES at 0.44 the pair costs over 1.
    JsonNode out = run(polymarketAbove(0.42), "{}");

    assertEquals("complete", out.get("status").asText());
    assertEquals(1, out.get("funnel").get("cross_venue_pairs_priced").asInt());
    assertEquals(0, out.get("funnel").get("locks_found").asInt());
    assertEquals(0, out.get("baskets").size());
    assertTrue(out.get("next").asText().startsWith("No basket locks a profit"));
  }

  @Test void minFloorLeavesOutASmallerLock() throws Exception {
    JsonNode out = run(polymarketAbove(0.60), "{'min_floor':0.5}");

    assertEquals(0, out.get("baskets").size());
    assertEquals(0.5, out.get("min_floor").asDouble(), 1e-9);
  }

  @Test void aPairWhoseLabelsStateNoNumberIsNotPricedAndSaysWhy() throws Exception {
    ObjectNode ev = polymarketEvent();
    polymarketMarket((ArrayNode) ev.get("markets"), "m1", "No change", 0.60, 0.62);
    JsonNode out = run(ev, "{}");

    assertEquals(1, out.get("funnel").get("cross_venue_pairs").asInt());
    assertEquals(0, out.get("funnel").get("cross_venue_pairs_priced").asInt());
    JsonNode not = out.get("not_priced").get(0);
    assertTrue(not.get("why").asText().contains("states a number"), not.toString());
    assertTrue(not.get("why").asText().contains("No change"), not.toString());
    assertEquals(KALSHI_ID, not.get("examples").get(0).asText().split(" and ")[0].split(":")[1]);
  }

  @Test void strikesInDifferentUnitsAreNotPriced() throws Exception {
    ObjectNode ev = polymarketEvent();
    polymarketMarket((ArrayNode) ev.get("markets"), "m1", "Above 300", 0.60, 0.62);
    JsonNode out = run(ev, "{}");

    assertEquals(0, out.get("funnel").get("cross_venue_pairs_priced").asInt());
    assertTrue(out.get("not_priced").get(0).get("why").asText().contains("do not overlap"));
  }

  @Test void anExclusiveEventLocksWhenItsBidsSumOverOne() throws Exception {
    ObjectNode ev = polymarketEvent();
    ev.put("negRisk", true);
    ArrayNode markets = (ArrayNode) ev.get("markets");
    polymarketMarket(markets, "m1", "Rise", 0.50, 0.52);
    polymarketMarket(markets, "m2", "Fall", 0.40, 0.42);
    polymarketMarket(markets, "m3", "No change", 0.20, 0.22);
    JsonNode out = run(ev, "{}");

    assertEquals(1, out.get("funnel").get("events_the_venue_states_exclusive").asInt());
    assertEquals(1, out.get("funnel").get("events_with_a_lock_inside").asInt());
    JsonNode best = out.get("baskets").get(0);
    assertEquals("exclusive_set", best.get("type").asText());
    assertEquals(3, best.get("legs").size());
    for (JsonNode leg : best.get("legs")) {
      assertEquals("no", leg.get("side").asText());
      assertEquals("polymarket", leg.get("source").asText());
    }
    // NO on three markets costs 0.50 + 0.60 + 0.80 and pays at least 2.
    assertEquals(1.90, best.get("cost").asDouble(), 1e-6);
    assertEquals(0.10, best.get("floor_profit").asDouble(), 1e-6);
    assertEquals(1.10, best.get("yes_bids_sum").asDouble(), 1e-6);
  }

  @Test void anExclusiveEventWithBidsUnderOneIsListedAsClosestNotAsALock() throws Exception {
    ObjectNode ev = polymarketEvent();
    ev.put("negRisk", true);
    ArrayNode markets = (ArrayNode) ev.get("markets");
    polymarketMarket(markets, "m1", "Rise", 0.50, 0.52);
    polymarketMarket(markets, "m2", "Fall", 0.30, 0.32);
    JsonNode out = run(ev, "{}");

    assertEquals(0, out.get("baskets").size());
    JsonNode near = out.get("closest").get(0);
    assertEquals(POLY_ID, near.get("event").get("event_id").asText());
    assertEquals(0.80, near.get("yes_bids_sum").asDouble(), 1e-6);
    assertTrue(near.get("floor_profit").asDouble() < 0);
  }

  @Test void anEventTheVenueDoesNotStateExclusiveClaimsNoExclusiveLock() throws Exception {
    ObjectNode ev = polymarketEvent();
    ArrayNode markets = (ArrayNode) ev.get("markets");
    polymarketMarket(markets, "m1", "Rise", 0.60, 0.62);
    polymarketMarket(markets, "m2", "Fall", 0.60, 0.62);
    JsonNode out = run(ev, "{}");

    assertEquals(0, out.get("funnel").get("events_the_venue_states_exclusive").asInt());
    assertEquals(0, out.get("baskets").size());
    assertEquals(0, out.get("closest").size());
  }

  @Test void aScanOutOfTimeResumesWithoutReadingAnEventTwice() throws Exception {
    FakeFetcher f = venues(polymarketAbove(0.60));
    MarketBasketScan s = scan(f, -1L);

    JsonNode first = MAPPER.readTree(s.scan(args("{}")));
    assertEquals("scanning", first.get("status").asText());
    assertEquals(1, first.get("funnel").get("events_read").asInt());
    assertTrue(first.get("next").asText().contains("call scan_market_baskets again"));
    assertEquals(0, first.get("not_priced").size(), "an unread pair is not a pair left out");
    int reads = f.eventReads;

    JsonNode second = MAPPER.readTree(s.scan(args("{}")));
    assertEquals("complete", second.get("status").asText());
    assertEquals(2, second.get("funnel").get("events_read").asInt());
    assertEquals(reads + 1, f.eventReads);
    assertEquals(1, second.get("baskets").size() > 0 ? 1 : 0);
  }

  @Test void argumentsAreValidated() {
    MarketBasketScan s = scan(venues(polymarketAbove(0.60)), 60_000L);

    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> s.scan(args("{'min_edge':0.1}"))).getMessage().contains("unknown key 'min_edge'"));
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> s.scan(args("{'driver':'nonsense'}"))).getMessage().contains("is not a driver"));
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> s.scan(args("{'search':7}"))).getMessage().contains("search must be 2 to 4"));
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> s.scan(args("{'min_floor':-0.1}"))).getMessage().contains("min_floor"));
  }

  @Test void aLabelIsReadOnlyInTheFormsItStatesANumber() {
    MarketPricing.Condition above = MarketPricing.Condition.ofLabel("Above 4.5%");
    assertEquals("above", above.kind);
    assertEquals(4.5, above.low, 1e-12);
    assertEquals("below", MarketPricing.Condition.ofLabel("<0.5%").kind);
    assertEquals("at_least", MarketPricing.Condition.ofLabel("≥3.0%").kind);
    MarketPricing.Condition atMost = MarketPricing.Condition.ofLabel("≤0.0%");
    assertEquals("at_most", atMost.kind);
    assertEquals(0.0, atMost.low, 1e-12);
    MarketPricing.Condition range = MarketPricing.Condition.ofLabel("0.5–1.0%");
    assertEquals("between", range.kind);
    assertEquals(0.5, range.low, 1e-12);
    assertEquals(1.0, range.high, 1e-12);
    MarketPricing.Condition exact = MarketPricing.Condition.ofLabel("0.3%");
    assertEquals("between", exact.kind);
    assertEquals(0.3, exact.low, 1e-12);
    assertEquals(0.3, exact.high, 1e-12);
    assertEquals(-0.2, MarketPricing.Condition.ofLabel("Below −0.2%").low, 1e-12);
    assertEquals(250000, MarketPricing.Condition.ofLabel("Over 250k").low, 1e-6);
    assertEquals(1500, MarketPricing.Condition.ofLabel("$1,000 to $1,500").high, 1e-6);

    assertNull(MarketPricing.Condition.ofLabel("25 bps decrease"));
    assertNull(MarketPricing.Condition.ofLabel("No change"));
    assertNull(MarketPricing.Condition.ofLabel("250k+"));
    assertNull(MarketPricing.Condition.ofLabel("100k–200m"));
    assertNull(MarketPricing.Condition.ofLabel("3.0–2.0%"));
    assertNull(MarketPricing.Condition.ofLabel(null));
  }

  @Test void aValueTwoLabelledRangesBothClaimWinsForNeitherSide() throws Exception {
    ObjectNode ev = polymarketEvent();
    ArrayNode markets = (ArrayNode) ev.get("markets");
    polymarketMarket(markets, "m1", "2.5–3.0%", 0.30, 0.32);
    polymarketMarket(markets, "m2", "3.0–3.5%", 0.30, 0.32);
    polymarketMarket(markets, "m3", "Above 3.5%", 0.30, 0.32);
    PredictionMarkets.Event event =
        PredictionMarkets.fetchEvent(venues(ev), "polymarket", POLY_ID).event;

    Map<String, MarketPricing.Condition> read = MarketPricing.labelConditions(event);
    assertEquals(3, read.size());
    MarketPricing.Condition lower = read.get("m1");
    MarketPricing.Condition upper = read.get("m2");
    assertTrue(lower.holds(3.0) && upper.holds(3.0));
    assertFalse(lower.wins("yes", 3.0));
    assertFalse(lower.wins("no", 3.0));
    assertFalse(upper.wins("yes", 3.0));
    assertFalse(upper.wins("no", 3.0));
    // Away from the shared end the range is an ordinary condition.
    assertTrue(lower.wins("yes", 2.7));
    assertTrue(lower.wins("no", 3.2));
    assertTrue(lower.wins("yes", 2.5), "an end no other range claims is not in doubt");
    assertTrue(upper.wins("yes", 3.5), "Above 3.5% is not a range and shares no end");
    assertTrue(read.get("m3").wins("no", 3.5));
  }

  @Test void aFinishedScanCarriesItsBoardAndForecastsTheTopBasketFirst() throws Exception {
    String raw = scan(venues(polymarketAbove(0.60)), 60_000L).scan(args("{}"));
    JsonNode out = MAPPER.readTree(new MarketPresentation().basketScan(raw));

    assertTrue(out.get("dashboard_layout").asText().startsWith("basketscan:"));
    assertEquals(6, out.get("dashboard_panels").size());
    JsonNode followUps = out.get("follow_ups");
    assertEquals("price_market_event", followUps.get(0).get("tool").asText());
    assertTrue(followUps.get(0).get("arguments").get("build_forecast").asBoolean());
    assertEquals("price_market_event", followUps.get(1).get("tool").asText());
    boolean depth = false;
    for (JsonNode f : followUps) {
      depth |= "market_price_history".equals(f.get("tool").asText());
    }
    assertTrue(depth);
  }

  @Test void anUnfinishedScanCarriesNoBoard() throws Exception {
    String raw = scan(venues(polymarketAbove(0.60)), -1L).scan(args("{}"));

    assertEquals(raw, new MarketPresentation().basketScan(raw));
  }
}
