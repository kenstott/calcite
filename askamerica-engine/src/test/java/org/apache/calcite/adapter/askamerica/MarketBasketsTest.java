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

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Structural locks, basket recipes and the payoff curve. */
@Tag("unit")
class MarketBasketsTest {

  private static final double RATE = 0.07;

  private static PredictionMarkets.Market market(String id, String type, Double floor,
      Double cap, double bid, double ask) {
    PredictionMarkets.Market m = new PredictionMarkets.Market();
    m.source = "kalshi";
    m.eventId = "EV1";
    m.marketId = id;
    m.title = id;
    m.strikeType = type;
    m.floorStrike = floor;
    m.capStrike = cap;
    m.yesBid = bid;
    m.yesAsk = ask;
    m.yesPrice = (bid + ask) / 2;
    return m;
  }

  private static PredictionMarkets.Event event(String id, String driver, String close,
      PredictionMarkets.Market... legs) {
    PredictionMarkets.Event e = new PredictionMarkets.Event();
    e.source = "kalshi";
    e.eventId = id;
    e.eventTitle = id;
    e.driver = PredictionMarkets.driverNamed(driver);
    e.closeTime = close;
    e.legs.addAll(Arrays.asList(legs));
    return e;
  }

  private static Map<String, Double> rates(PredictionMarkets.Event e, double rate) {
    Map<String, Double> r = new HashMap<>();
    for (PredictionMarkets.Market m : e.legs) {
      r.put(m.marketId, rate);
    }
    return r;
  }

  /** Four buckets that tile the line on a 0.1 grid: below 3, 3.0-3.4, 3.5-3.9, above 3.9. */
  private static PredictionMarkets.Event buckets(double ask) {
    return event("EV1", "inflation", "2026-11-01T00:00:00Z",
        market("LOW", "less", null, 3.0, ask - 0.02, ask),
        market("B1", "between", 3.0, 3.4, ask - 0.02, ask),
        market("B2", "between", 3.5, 3.9, ask - 0.02, ask),
        market("HIGH", "greater", 3.9, null, ask - 0.02, ask));
  }

  // ─── Bucket partitions ─────────────────────────────────────────────────────

  @Test void aPartitionUnderOneBeforeFeesLocksWithoutFees() {
    PredictionMarkets.Event e = buckets(0.22);
    ObjectNode n = MarketBaskets.partitionLocks(e, rates(e, 0), 0.1).get(0);
    assertEquals("established", n.get("exhaustive").asText());
    assertTrue(n.get("exclusive").asBoolean());
    assertTrue(n.get("lock").asBoolean());
    ObjectNode yes = (ObjectNode) n.get("buy_all_yes");
    assertEquals(0.88, yes.get("cost").asDouble(), 1e-9);
    assertEquals(1, yes.get("guaranteed_payout").asInt());
    assertEquals(0.12, yes.get("floor_profit").asDouble(), 1e-9);
    assertEquals(4, yes.get("legs").size());
  }

  @Test void feesCanEatAPartitionLock() {
    PredictionMarkets.Event e = buckets(0.24);
    assertTrue(MarketBaskets.partitionLocks(e, rates(e, 0), 0.1).get(0)
        .get("lock").asBoolean());
    ObjectNode n = MarketBaskets.partitionLocks(e, rates(e, RATE), 0.1).get(0);
    assertFalse(n.get("lock").asBoolean());
    ObjectNode yes = (ObjectNode) n.get("buy_all_yes");
    // 4 * (0.24 + 0.07 * 0.24 * 0.76) = 1.0111 > 1.
    assertEquals(1.0111, yes.get("cost").asDouble(), 1e-4);
    assertTrue(yes.get("floor_profit").asDouble() < 0);
  }

  @Test void aPartitionWithoutTailsOrWithGapsIsNotALock() {
    PredictionMarkets.Event e = event("EV1", "inflation", "2026-11-01T00:00:00Z",
        market("B1", "between", 3.0, 3.4, 0.1, 0.2),
        market("B2", "between", 3.5, 3.9, 0.1, 0.2),
        market("B3", "between", 4.0, 4.4, 0.1, 0.2));
    ObjectNode n = MarketBaskets.partitionLocks(e, rates(e, 0), 0.1).get(0);
    assertEquals("not_established", n.get("exhaustive").asText());
    assertFalse(n.get("lock").asBoolean());
    assertEquals(2, n.get("exhaustive_reasons").size());
    // The buckets sum to 0.6, yet nothing is claimed.
    assertEquals(0.6, n.get("buy_all_yes").get("cost").asDouble(), 1e-9);
  }

  @Test void anUnknownSettlementGridLeavesGapsOpen() {
    PredictionMarkets.Event e = buckets(0.22);
    ObjectNode n = MarketBaskets.partitionLocks(e, rates(e, 0), null).get(0);
    assertEquals("not_established", n.get("exhaustive").asText());
    assertFalse(n.get("lock").asBoolean());
  }

  @Test void overlappingBucketsAreNotExclusive() {
    PredictionMarkets.Event e = event("EV1", "inflation", "2026-11-01T00:00:00Z",
        market("LOW", "less", null, 3.5, 0.1, 0.2),
        market("B1", "between", 3.0, 3.6, 0.1, 0.2),
        market("B2", "between", 3.6, 3.9, 0.1, 0.2),
        market("HIGH", "greater", 3.9, null, 0.1, 0.2));
    ObjectNode n = MarketBaskets.partitionLocks(e, rates(e, 0), 0.1).get(0);
    assertFalse(n.get("exclusive").asBoolean());
    assertFalse(n.get("lock").asBoolean());
  }

  @Test void theNoBasketLocksWhenNoAsksSumUnderNMinusOne() {
    PredictionMarkets.Event e = event("EV1", "inflation", "2026-11-01T00:00:00Z",
        market("LOW", "less", null, 3.0, 0.30, 0.50),
        market("B1", "between", 3.0, 3.4, 0.30, 0.50),
        market("B2", "between", 3.5, 3.9, 0.30, 0.50),
        market("HIGH", "greater", 3.9, null, 0.30, 0.50));
    ObjectNode n = MarketBaskets.partitionLocks(e, rates(e, 0), 0.1).get(0);
    // YES asks sum to 2.0 (no lock); NO costs 4 * 0.70 = 2.8 against a payout of 3.
    assertFalse(n.get("buy_all_yes").get("lock").asBoolean());
    ObjectNode no = (ObjectNode) n.get("buy_all_no");
    assertEquals(3, no.get("guaranteed_payout").asInt());
    assertEquals(0.2, no.get("floor_profit").asDouble(), 1e-9);
    assertTrue(n.get("lock").asBoolean());
  }

  @Test void statedConditionsStandInForStrikesTheVenueDoesNotGive() {
    // A Polymarket-style event: no strike metadata, one market per published value.
    PredictionMarkets.Event e = event("EV1", "inflation", "2026-11-01T00:00:00Z",
        market("LOW", null, null, null, 0.20, 0.22),
        market("V1", null, null, null, 0.20, 0.22),
        market("V2", null, null, null, 0.20, 0.22),
        market("HIGH", null, null, null, 0.20, 0.22));
    assertTrue(MarketBaskets.partitionLocks(e, rates(e, 0), 0.1).isEmpty());
    Map<String, MarketPricing.Condition> given = new HashMap<>();
    given.put("LOW", new MarketPricing.Condition("at_most", 2.9, 2.9));
    given.put("V1", new MarketPricing.Condition("between", 3.0, 3.0));
    given.put("V2", new MarketPricing.Condition("between", 3.1, 3.1));
    given.put("HIGH", new MarketPricing.Condition("at_least", 3.2, 3.2));
    ObjectNode n = MarketBaskets.partitionLocks(e, rates(e, 0), 0.1, given).get(0);
    assertEquals("established", n.get("exhaustive").asText(), n.toString());
    assertTrue(n.get("lock").asBoolean());
    assertEquals(0.88, n.get("buy_all_yes").get("cost").asDouble(), 1e-9);
    // With no grid, the 0.1 between the values is an open gap.
    assertEquals("not_established", MarketBaskets.partitionLocks(e, rates(e, 0), null, given)
        .get(0).get("exhaustive").asText());

    // The same statement makes a ladder: "at least 3.2" bid over "at least 3.0" asked.
    PredictionMarkets.Event ladder = event("EV2", "inflation", "2026-11-01T00:00:00Z",
        market("A", null, null, null, 0.30, 0.34),
        market("B", null, null, null, 0.40, 0.44));
    Map<String, MarketPricing.Condition> rungs = new HashMap<>();
    rungs.put("A", new MarketPricing.Condition("at_least", 3.0, 3.0));
    rungs.put("B", new MarketPricing.Condition("at_least", 3.2, 3.2));
    assertTrue(MarketBaskets.ladderLocks(ladder, rates(ladder, 0)).isEmpty());
    List<ObjectNode> locks = MarketBaskets.ladderLocks(ladder, rates(ladder, 0), rungs);
    assertEquals(1, locks.size());
    assertEquals(0.06, locks.get(0).get("gross_gap").asDouble(), 1e-9);
    assertTrue(locks.get(0).get("lock_after_fees").asBoolean());
  }

  @Test void aMissingFeeRateIsAnError() {
    PredictionMarkets.Event e = buckets(0.22);
    assertThrows(IllegalArgumentException.class,
        () -> MarketBaskets.partitionLocks(e, new HashMap<>(), 0.1));
  }

  // ─── Non-monotone ladders ──────────────────────────────────────────────────

  @Test void aLadderGapLocksBeforeAndAfterFees() {
    PredictionMarkets.Event e = event("EV1", "inflation", "2026-11-01T00:00:00Z",
        market("K3", "greater", 3.0, null, 0.28, 0.30),
        market("K4", "greater", 4.0, null, 0.50, 0.52));
    List<ObjectNode> l = MarketBaskets.ladderLocks(e, rates(e, RATE));
    assertEquals(1, l.size());
    ObjectNode n = l.get(0);
    assertEquals("K3", n.get("legs").get(0).get("market_id").asText());
    assertEquals("yes", n.get("legs").get(0).get("side").asText());
    assertEquals("K4", n.get("legs").get(1).get("market_id").asText());
    assertEquals("no", n.get("legs").get(1).get("side").asText());
    assertEquals(0.20, n.get("gross_gap").asDouble(), 1e-9);
    assertTrue(n.get("lock_after_fees").asBoolean());
    // 0.30 + 0.0147 + 0.50 + 0.07 * 0.5 * 0.5 = 0.8322.
    assertEquals(0.8322, n.get("cost").asDouble(), 1e-4);
    assertEquals(0.1678, n.get("floor_profit").asDouble(), 1e-4);
  }

  @Test void feesCanEatALadderLock() {
    PredictionMarkets.Event e = event("EV1", "inflation", "2026-11-01T00:00:00Z",
        market("K3", "greater", 3.0, null, 0.38, 0.40),
        market("K4", "greater", 4.0, null, 0.43, 0.45));
    ObjectNode n = MarketBaskets.ladderLocks(e, rates(e, RATE)).get(0);
    assertTrue(n.get("lock_before_fees").asBoolean());
    assertFalse(n.get("lock_after_fees").asBoolean());
    assertTrue(n.get("floor_profit").asDouble() < 0);
    assertTrue(MarketBaskets.ladderLocks(e, rates(e, 0)).get(0)
        .get("lock_after_fees").asBoolean());
  }

  @Test void aMonotoneLadderHasNoLockAndADownLadderIsChecked() {
    PredictionMarkets.Event ok = event("EV1", "inflation", "2026-11-01T00:00:00Z",
        market("K3", "greater", 3.0, null, 0.58, 0.60),
        market("K4", "greater", 4.0, null, 0.38, 0.40));
    assertTrue(MarketBaskets.ladderLocks(ok, rates(ok, 0)).isEmpty());
    // "below 3" priced above "below 4" is the same break, on the other ladder.
    PredictionMarkets.Event down = event("EV1", "inflation", "2026-11-01T00:00:00Z",
        market("C3", "less", null, 3.0, 0.50, 0.52),
        market("C4", "less", null, 4.0, 0.28, 0.30));
    ObjectNode n = MarketBaskets.ladderLocks(down, rates(down, 0)).get(0);
    assertEquals("C4", n.get("legs").get(0).get("market_id").asText());
    assertEquals("C3", n.get("legs").get(1).get("market_id").asText());
  }

  // ─── Recipes ───────────────────────────────────────────────────────────────

  @Test void theRangeRecipeNamesTheTwoOuterStrikes() {
    PredictionMarkets.Event e = event("EV1", "inflation", "2026-11-01T00:00:00Z",
        market("K4", "greater", 4.0, null, 0.1, 0.2),
        market("K2", "greater", 2.0, null, 0.6, 0.7),
        market("K3", "greater", 3.0, null, 0.3, 0.4));
    List<MarketBaskets.Basket> b = MarketBaskets.propose(Arrays.asList(e), "range", null);
    assertEquals(1, b.size());
    assertEquals("range:kalshi:EV1", b.get(0).name);
    assertTrue(b.get(0).why.contains("YES on K2"), b.get(0).why);
    assertTrue(b.get(0).why.contains("NO on K4"), b.get(0).why);
    PredictionMarkets.Event one = event("EV2", "inflation", "2026-11-01T00:00:00Z",
        market("K3", "greater", 3.0, null, 0.3, 0.4));
    assertTrue(MarketBaskets.propose(Arrays.asList(one), "range", null).isEmpty());
  }

  @Test void theCalendarRecipePairsAdjacentPeriodsOfOneDriver() {
    List<PredictionMarkets.Event> events = new ArrayList<>();
    events.add(event("M1", "inflation", "2026-11-12T00:00:00Z"));
    events.add(event("M2", "inflation", "2026-12-10T00:00:00Z"));
    events.add(event("M3", "inflation", "2027-01-13T00:00:00Z"));
    events.add(event("J1", "payrolls", "2026-11-06T00:00:00Z"));
    List<MarketBaskets.Basket> b = MarketBaskets.propose(events, "calendar", null);
    assertEquals(2, b.size());
    assertTrue(b.get(0).name.startsWith("calendar:kalshi:inflation:2026-11-12/2026-12-10")
        || b.get(1).name.startsWith("calendar:kalshi:inflation:2026-11-12/2026-12-10"));
    for (MarketBaskets.Basket x : b) {
      assertEquals(2, x.events.size());
      assertTrue(x.why.contains("series_run"), x.why);
    }
  }

  @Test void linkedDriversStatesTheDirectionOfTheLink() {
    List<PredictionMarkets.Event> events = Arrays.asList(
        event("P1", "policy_rate", "2026-11-12T00:00:00Z"),
        event("I1", "inflation", "2026-11-12T00:00:00Z"),
        event("M1", "mortgage_rate", "2026-11-12T00:00:00Z"));
    List<MarketBaskets.Basket> b = MarketBaskets.propose(events, "linked_drivers",
        "prices_and_policy");
    assertEquals(1, b.size());
    assertEquals(Arrays.asList("inflation", "policy_rate", "mortgage_rate"),
        b.get(0).direction);
    assertTrue(b.get(0).why.contains("inflation -> policy_rate -> mortgage_rate"), b.get(0).why);
    assertEquals(3, b.get(0).toJson(0).get("link_direction").size());
  }

  @Test void theNewRecipesComeAfterTheExistingOnes() {
    assertEquals(Arrays.asList("cross_venue", "same_place", "linked_drivers", "series_run",
        "range", "calendar"), MarketBaskets.RECIPES);
  }

  // ─── Payoff curve ──────────────────────────────────────────────────────────

  private static MarketPricing.Leg leg(String id, String side, String kind, double strike,
      double price, double fee, String column) {
    MarketPricing.Leg l = new MarketPricing.Leg();
    l.id = id;
    l.side = side;
    l.price = price;
    l.fee = fee;
    l.column = column;
    l.condition = new MarketPricing.Condition(kind, strike, strike);
    return l;
  }

  private static double profitAt(ObjectNode curve, double value) {
    for (com.fasterxml.jackson.databind.JsonNode p : curve.get("curve")) {
      if (p.get("value").asDouble() == value) {
        return p.get("profit_per_cost").asDouble();
      }
    }
    throw new AssertionError("no point at " + value);
  }

  @Test void theCurveIsCostedAtPriceAndFeeAndPaysTwoBetweenStrikes() {
    List<MarketPricing.Leg> legs = Arrays.asList(
        leg("K3", "yes", "above", 3.0, 0.40, 0.01, "cpi"),
        leg("K4", "no", "above", 4.0, 0.50, 0.01, "cpi"));
    ObjectNode c = MarketBaskets.payoffCurve(legs, new double[] {2, 3, 3.5, 4, 5});
    assertEquals(0.92, c.get("cost").asDouble(), 1e-9);
    assertEquals(0.087, profitAt(c, 2), 1e-3);
    // Strictly above 3: at 3 the YES leg loses, so the pair pays 1.
    assertEquals(0.087, profitAt(c, 3), 1e-3);
    assertEquals(1.174, profitAt(c, 3.5), 1e-3);
    // At 4 the NO leg on "above 4" wins (4 is not above 4) and the YES leg wins: 2.
    assertEquals(1.174, profitAt(c, 4), 1e-3);
    assertEquals(0.087, profitAt(c, 5), 1e-3);
    assertEquals(0.087, c.get("floor_profit_per_cost").asDouble(), 1e-3);
    assertEquals(1.174, c.get("max_profit_per_cost").asDouble(), 1e-3);
    assertEquals(0, c.get("break_even_values").size());
    assertTrue(c.get("floor_is_fee_inclusive").asBoolean());
  }

  @Test void theTieConventionSeparatesAboveFromAtLeast() {
    double[] grid = {2.9, 3.0, 3.1};
    ObjectNode strict = MarketBaskets.payoffCurve(Arrays.asList(
        leg("K3", "yes", "above", 3.0, 0.50, 0, "cpi")), grid);
    ObjectNode inclusive = MarketBaskets.payoffCurve(Arrays.asList(
        leg("K3", "yes", "at_least", 3.0, 0.50, 0, "cpi")), grid);
    assertEquals(-1.0, profitAt(strict, 3.0), 1e-9);
    assertEquals(1.0, profitAt(inclusive, 3.0), 1e-9);
    assertEquals(-1.0, profitAt(inclusive, 2.9), 1e-9);
    assertEquals(1.0, profitAt(inclusive, 3.1), 1e-9);
    // The break-even is the strike, with the profit either side of it.
    ObjectNode be = (ObjectNode) inclusive.get("break_even_values").get(0);
    assertEquals(3.0, be.get("value").asDouble(), 1e-9);
    assertEquals(-1.0, be.get("profit_below").asDouble(), 1e-9);
    assertEquals(1.0, be.get("profit_above").asDouble(), 1e-9);
    assertEquals(-1.0, inclusive.get("floor_profit_per_cost").asDouble(), 1e-9);
    assertEquals(1.0, inclusive.get("max_profit_per_cost").asDouble(), 1e-9);
  }

  @Test void legsOnDifferentQuantitiesShareNoAxis() {
    List<MarketPricing.Leg> legs = Arrays.asList(
        leg("CPI3", "yes", "above", 3.0, 0.40, 0, "cpi"),
        leg("JOBS", "yes", "above", 150, 0.40, 0, "payrolls"));
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> MarketBaskets.payoffCurve(legs, new double[] {3}));
    assertTrue(ex.getMessage().contains("CPI3:yes"), ex.getMessage());
    assertTrue(ex.getMessage().contains("JOBS:yes"), ex.getMessage());
  }
}
