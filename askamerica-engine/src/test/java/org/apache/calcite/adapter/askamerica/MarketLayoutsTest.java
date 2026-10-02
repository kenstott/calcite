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

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The three market dashboards, fed outputs shaped as the tools emit them. Each layout is pushed
 * through the report tool's own panel reader and the dashboard renderer, so a layout the
 * renderer rejects fails here.
 */
@Tag("unit")
class MarketLayoutsTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** JSON written with single quotes, for fixtures. */
  private static JsonNode json(String text) {
    try {
      return MAPPER.readTree(text.replace('\'', '"'));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /** Reads every panel as the report tool does and renders the dashboard; returns the SVG. */
  private static String render(ObjectNode dashboard) {
    List<DashboardLayout.Panel> panels = new ArrayList<>();
    for (JsonNode pn : dashboard.get("panels")) {
      panels.add(McpServer.readPanel(pn));
    }
    int columns = dashboard.get("columns").asInt();
    int[] size = DashboardLayout.defaultSize(panels, columns);
    return DashboardLayout.compose(dashboard.get("title").asText(),
        dashboard.get("subtitle").asText(),
        dashboard.has("footnote") ? dashboard.get("footnote").asText() : null, panels,
        columns, size[0], size[1]).toSvg();
  }

  private static List<String> titles(ObjectNode dashboard) {
    List<String> out = new ArrayList<>();
    for (JsonNode p : dashboard.get("panels")) {
      out.add(p.has("title") ? p.get("title").asText() : p.get("label").asText());
    }
    return out;
  }

  private static JsonNode panel(ObjectNode dashboard, String chartType) {
    for (JsonNode p : dashboard.get("panels")) {
      if (chartType.equals(p.path("chart_type").asText())) {
        return p;
      }
    }
    throw new AssertionError("no " + chartType + " panel in " + titles(dashboard));
  }

  // ─── Opportunity card ──────────────────────────────────────────────────────

  private static ObjectNode market(String id, String condition, double fair, String bid,
      String ask, String edgeFields) {
    return (ObjectNode) json("{'market_id':'" + id + "','title':'CPI " + id + "','condition':"
        + condition + ",'fee_rate':0.07,'fair':" + fair + ",'se':0.0111,'yes_bid':" + bid
        + ",'yes_ask':" + ask + "," + edgeFields + "}");
  }

  private static ObjectNode priced(boolean fan, int strikes) {
    ObjectNode p = (ObjectNode) json("{'source':'kalshi','event_id':'KXCPI-26SEP',"
        + "'event_title':'CPI in September','close_time':'2026-10-14T12:30:00Z',"
        + "'forecast':{'form':'samples','n':4000,'median':3.2,'p05':2.9,'p95':3.5},"
        + "'quotes_read_at':'2026-10-02T14:00:00Z','venue_series':'KXCPI',"
        + "'fee_basis':'read from kalshi; fee per contract = rate * price * (1 - price)',"
        + "'confidence':{'tier':'weak','reasons':['no backtest of series KXCPI has run'],"
        + "'backtest':null}}");
    ArrayNode rows = p.putArray("priced_markets");
    rows.add(market("B", "{'above':3.4}", 0.12, "0.05", "0.08",
        "'side':null,'verdict':'no_quote','days_to_settlement':11.9"));
    if (strikes > 1) {
      rows.add(market("A", "{'above':3.0}", 0.81, "0.58", "0.60",
          "'side':'yes','price':0.6,'fee':0.0168,'edge':0.1932,'verdict':'mispriced',"
          + "'days_to_settlement':11.9"));
      rows.add(market("C", "{'between':[3.2,3.4]}", 0.30, "0.31", "null",
          "'side':'no','price':0.69,'fee':0.015,'edge':0.001,'verdict':'within_min_edge',"
          + "'days_to_settlement':11.9"));
    } else {
      ObjectNode only = (ObjectNode) rows.get(0);
      only.put("side", "yes");
      only.put("price", 0.08);
      only.put("fee", 0.0052);
      only.put("edge", 0.0348);
      only.put("verdict", "mispriced");
    }
    if (fan) {
      p.set("forecast_built", json("{'series':'CPI YoY','units':'percent',"
          + "'fan':{'categories':['Apr','May','Jun','Jul','Aug','Sep'],"
          + "'series':[{'name':'history','values':[3.0,3.1,3.1,null,null,null]},"
          + "{'name':'median forecast','values':[null,null,3.1,3.15,3.2,3.2]}],"
          + "'bands':[{'name':'50% interval','low':[null,null,3.1,3.1,3.1,3.1],"
          + "'high':[null,null,3.1,3.2,3.3,3.3]},{'name':'90% interval',"
          + "'low':[null,null,3.1,3.0,2.9,2.9],'high':[null,null,3.1,3.3,3.5,3.5]}]}}"));
    }
    return p;
  }

  @Test void aCardOfThreeStrikesCarriesEveryPanelAndRenders() {
    ObjectNode card = MarketLayouts.opportunityCard(priced(true, 3));
    assertEquals(7, card.get("panels").size(), titles(card).toString());
    JsonNode edge = card.get("panels").get(0);
    assertEquals("+19.3 pts", edge.get("value").asText());
    assertTrue(edge.get("delta").asText().contains("buy YES"), edge.toString());
    assertEquals("weak", card.get("panels").get(2).get("value").asText());
    assertEquals("11.9", card.get("panels").get(3).get("value").asText());
    // Strikes are in numeric order: 3, 3.2 to 3.4, > 3.4.
    JsonNode ladder = panel(card, "bar");
    assertEquals("> 3", ladder.get("categories").get(0).asText());
    assertEquals("3.2 to 3.4", ladder.get("categories").get(1).asText());
    assertEquals("> 3.4", ladder.get("categories").get(2).asText());
    // The market quoted on neither side has no edge to draw.
    assertEquals(0.1932, ladder.get("series").get(0).get("values").get(0).asDouble(), 1e-9);
    assertTrue(ladder.get("series").get(0).get("values").get(2).isNull());
    JsonNode quotes = panel(card, "line");
    assertTrue(quotes.get("series").get(1).get("values").get(1).isNull());
    // The fan marks the best market's strike, not the whole ladder.
    JsonNode fan = panel(card, "fan");
    assertEquals(1, fan.get("reference_lines").size());
    assertEquals(3.0, fan.get("reference_lines").get(0).get("value").asDouble(), 1e-9);
    assertEquals("strike 3", fan.get("reference_lines").get(0).get("label").asText());
    assertEquals(2, fan.get("bands").size());
    String svg = render(card);
    assertTrue(svg.contains("band-90"), svg);
  }

  @Test void aCardPricedFromACallersForecastHasNoFanPanel() {
    ObjectNode card = MarketLayouts.opportunityCard(priced(false, 3));
    assertEquals(6, card.get("panels").size());
    for (JsonNode p : card.get("panels")) {
      assertFalse("fan".equals(p.path("chart_type").asText()), p.toString());
    }
    assertFalse(render(card).isEmpty());
  }

  @Test void aCardOfOneStrikeRenders() {
    ObjectNode card = MarketLayouts.opportunityCard(priced(true, 1));
    assertEquals(1, panel(card, "bar").get("categories").size());
    assertEquals(1, panel(card, "fan").get("reference_lines").size());
    assertFalse(render(card).isEmpty());
  }

  @Test void aCardWithNoCloseTimeLeavesOutTheDaysTile() {
    ObjectNode p = priced(false, 3);
    for (JsonNode m : p.get("priced_markets")) {
      ((ObjectNode) m).putNull("days_to_settlement");
    }
    ObjectNode card = MarketLayouts.opportunityCard(p);
    assertEquals(5, card.get("panels").size());
    assertFalse(render(card).isEmpty());
  }

  @Test void aCardRefusesWhatIsMissingInsteadOfFillingIt() {
    ObjectNode noConfidence = priced(true, 3);
    noConfidence.remove("confidence");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> MarketLayouts.opportunityCard(noConfidence));
    assertTrue(e.getMessage().contains("confidence"), e.getMessage());

    ObjectNode noForecast = priced(true, 3);
    noForecast.putNull("forecast");
    assertThrows(IllegalArgumentException.class,
        () -> MarketLayouts.opportunityCard(noForecast));

    ObjectNode noFair = priced(true, 3);
    ((ObjectNode) noFair.get("priced_markets").get(0)).remove("fair");
    e = assertThrows(IllegalArgumentException.class,
        () -> MarketLayouts.opportunityCard(noFair));
    assertTrue(e.getMessage().contains("fair"), e.getMessage());

    ObjectNode noFan = priced(true, 3);
    ((ObjectNode) noFan.get("forecast_built")).remove("fan");
    e = assertThrows(IllegalArgumentException.class,
        () -> MarketLayouts.opportunityCard(noFan));
    assertTrue(e.getMessage().contains("fan"), e.getMessage());
  }

  // ─── Scan board ────────────────────────────────────────────────────────────

  private static ObjectNode scan(int passed) {
    ObjectNode s = (ObjectNode) json("{'status':'complete',"
        + "'listing_read_at':'2026-10-02T13:00:00Z','min_edge':0.1,"
        + "'funnel':{'events_matched':40,'events_to_evaluate':30,'events_evaluated':30,"
        + "'events_forecast':12,'events_passed':" + passed + "},"
        + "'forecast_is':'past changes of the settlement series applied to its latest value',"
        + "'not_forecast':[]}");
    ArrayNode opps = s.putArray("opportunities");
    if (passed >= 1) {
      opps.add(json("{'source':'kalshi','event_id':'E1','event_title':'CPI in September',"
          + "'markets':[{'market_id':'a','title':'Above 3.0','side':'yes','price':0.6,"
          + "'fair':0.81,'se':0.01,'fee':0.017,'edge':0.19},{'market_id':'b','title':"
          + "'Above 3.2','side':'no','price':0.4,'fair':0.5,'se':0.02,'fee':0.017,"
          + "'edge':0.12}]}"));
    }
    if (passed >= 2) {
      opps.add(json("{'source':'polymarket','event_id':'E2','event_title':'Unemployment rate',"
          + "'markets':[{'market_id':'c','title':'Above 4.5','side':'yes','price':0.3,"
          + "'fair':0.5,'se':0.015,'fee':0.01,'edge':0.15}]}"));
    }
    return s;
  }

  @Test void aScanBoardHasStatsFunnelRankingAndScatter() {
    ObjectNode board = MarketLayouts.scanBoard(scan(2));
    assertEquals(7, board.get("panels").size(), titles(board).toString());
    assertEquals("+19.0 pts", board.get("panels").get(3).get("value").asText());
    JsonNode funnel = board.get("panels").get(4);
    assertEquals(4, funnel.get("categories").size());
    assertEquals(12, funnel.get("series").get(0).get("values").get(2).asInt());
    JsonNode ranked = board.get("panels").get(5);
    assertEquals("desc", ranked.get("sort").asText());
    assertEquals(2, ranked.get("categories").size());
    JsonNode scatter = panel(board, "scatter");
    assertEquals(3, scatter.get("points").get(0).get("x").size());
    String svg = render(board);
    assertTrue(svg.contains("Edge against standard error"), svg);
  }

  @Test void aScanWithNothingPassedKeepsStatsAndFunnelOnly() {
    ObjectNode board = MarketLayouts.scanBoard(scan(0));
    assertEquals(5, board.get("panels").size(), titles(board).toString());
    assertEquals("Edge threshold", board.get("panels").get(3).get("label").asText());
    assertEquals(4, board.get("panels").get(4).get("span").asInt());
    assertFalse(render(board).isEmpty());
  }

  @Test void aScanWhoseMarketsCarryNoStandardErrorHasNoScatter() {
    ObjectNode s = scan(1);
    for (JsonNode m : s.get("opportunities").get(0).get("markets")) {
      ((ObjectNode) m).putNull("se");
    }
    ObjectNode board = MarketLayouts.scanBoard(s);
    assertEquals(6, board.get("panels").size());
    assertFalse(render(board).isEmpty());
  }

  @Test void anUnfinishedScanSaysSoAndMissingFieldsAreRefused() {
    ObjectNode s = scan(1);
    s.put("status", "scanning");
    ((ObjectNode) s.get("funnel")).put("events_evaluated", 10);
    ObjectNode board = MarketLayouts.scanBoard(s);
    assertTrue(board.get("subtitle").asText().contains("10 of 30"), board.toString());
    ObjectNode broken = scan(1);
    ((ObjectNode) broken.get("funnel")).remove("events_forecast");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> MarketLayouts.scanBoard(broken));
    assertTrue(e.getMessage().contains("events_forecast"), e.getMessage());
    ObjectNode noEdge = scan(1);
    ((ObjectNode) noEdge.get("opportunities").get(0).get("markets").get(0)).remove("edge");
    assertThrows(IllegalArgumentException.class, () -> MarketLayouts.scanBoard(noEdge));
  }

  // ─── Basket sheet ──────────────────────────────────────────────────────────

  private static ObjectNode basket(String rulesMatch, boolean curve) {
    ObjectNode b = (ObjectNode) json("{'events':[{'source':'kalshi','event_title':'CPI Sep'},"
        + "{'source':'polymarket','event_title':'CPI above 3%'}],"
        + "'venues':['kalshi','polymarket'],"
        + "'venue_note':'Every event here is on two venues.',"
        + "'scenarios':9,'scenario_basis':'outcome grid at the strikes',"
        + "'payout':'one contract per leg','legs_alone':["
        + "{'title':'Above 3.0','condition':{'above':3.0},'cost':0.5},"
        + "{'title':'Below 3.0','condition':{'at_most':3.0},'cost':0.45}],"
        + "'all_legs':{'legs':[{'id':'K1','title':'Above 3.0','source':'kalshi',"
        + "'side':'yes','price':0.5,"
        + "'fee':0.0175},{'id':'P1','title':'Below 3.0','source':'polymarket','side':'yes','price':0.45,"
        + "'fee':0.0}],'events':2,'venues':['kalshi','polymarket'],'cost':0.9675,"
        + "'floor':0.0336,'worst':0.0325,'best':0.0325},"
        + "'limits':'Fees are a taker order at the quote.'}");
    if (curve) {
      b.set("payoff_curve", json("{'column':'cpi_yoy','cost':0.9675,'curve':["
          + "{'value':2.5,'payout':1,'profit_per_cost':0.0336},"
          + "{'value':3.0,'payout':1,'profit_per_cost':0.0336},"
          + "{'value':3.5,'payout':1,'profit_per_cost':0.0336}],"
          + "'break_even_values':[],'floor_profit_per_cost':0.0336,"
          + "'floor_where':'below 3.0','max_profit_per_cost':0.0336,"
          + "'max_where':'at 3.0','floor_is_fee_inclusive':true,'of':'all_legs'}"));
    }
    if (rulesMatch != null) {
      b.put("rules_match", rulesMatch);
      b.set("rules", json("[{'a':'kalshi:E1','b':'polymarket:E2','rules_match':'"
          + rulesMatch + "','differing':" + ("differ".equals(rulesMatch)
              ? "['rounding']" : "[]") + ",'unknown':" + ("unverified".equals(rulesMatch)
              ? "['tie_handling']" : "[]") + "}]"));
    }
    return b;
  }

  @Test void aBasketSheetOfAMatchingLockHasEveryPanel() {
    ObjectNode sheet = MarketLayouts.basketSheet(basket("match", true));
    assertEquals(10, sheet.get("panels").size(), titles(sheet).toString());
    assertEquals("$0.9675", sheet.get("panels").get(0).get("value").asText());
    assertEquals("+3.4%", sheet.get("panels").get(1).get("value").asText());
    assertEquals("$0.0175", sheet.get("panels").get(3).get("value").asText());
    JsonNode payoff = panel(sheet, "line");
    assertEquals(3, payoff.get("categories").size());
    assertEquals(0.0336, payoff.get("reference_lines").get(0).get("value").asDouble(), 1e-9);
    JsonNode rules = null;
    for (JsonNode p : sheet.get("panels")) {
      if ("Rules verdict".equals(p.path("label").asText())) {
        rules = p;
      }
    }
    assertEquals("match", rules.get("value").asText());
    assertEquals("up", rules.get("delta_direction").asText());
    assertTrue(render(sheet).contains("Profit by settlement value"));
  }

  @Test void aBasketWhoseRulesDifferShowsWhatDiffers() {
    ObjectNode sheet = MarketLayouts.basketSheet(basket("differ", true));
    JsonNode rules = null;
    for (JsonNode p : sheet.get("panels")) {
      if ("Rules verdict".equals(p.path("label").asText())) {
        rules = p;
      }
    }
    assertEquals("differ", rules.get("value").asText());
    assertEquals("down", rules.get("delta_direction").asText());
    assertTrue(rules.get("caption").asText().contains("differing [rounding]"),
        rules.toString());
    assertFalse(render(sheet).isEmpty());
    ObjectNode unverified = MarketLayouts.basketSheet(basket("unverified", true));
    assertTrue(titles(unverified).contains("Rules verdict"));
    assertFalse(render(unverified).isEmpty());
  }

  @Test void aBasketOfOneVenueAndNoCurveStillRenders() {
    ObjectNode b = basket(null, false);
    ObjectNode sheet = MarketLayouts.basketSheet(b);
    assertEquals(8, sheet.get("panels").size(), titles(sheet).toString());
    for (JsonNode p : sheet.get("panels")) {
      assertFalse("line".equals(p.path("chart_type").asText()));
      if ("Rules verdict".equals(p.path("label").asText())) {
        assertEquals("one venue", p.get("value").asText());
      }
    }
    assertFalse(render(sheet).isEmpty());
  }

  @Test void aSearchedBasketIsLaidOutFromItsBestSubset() {
    ObjectNode b = basket("match", true);
    b.set("search", json("{'best':[{'legs':[{'id':'P1','title':'Below 3.0',"
        + "'source':'polymarket','side':'yes',"
        + "'price':0.45,'fee':0.0}],'cost':0.45,'floor':0.5,'worst':0.2,'best':0.3,"
        + "'yield':0.12}]}"));
    ObjectNode sheet = MarketLayouts.basketSheet(b);
    assertEquals("$0.4500", sheet.get("panels").get(0).get("value").asText());
    assertEquals("Yield", sheet.get("panels").get(2).get("label").asText());
    assertEquals(1, panel(sheet, "bar").get("categories").size());
    assertFalse(render(sheet).isEmpty());
    b.set("search", json("{'best':[]}"));
    assertThrows(IllegalArgumentException.class, () -> MarketLayouts.basketSheet(b));
  }

  @Test void aBasketMissingAFieldIsRefusedByName() {
    ObjectNode b = basket("match", true);
    b.remove("scenarios");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> MarketLayouts.basketSheet(b));
    assertTrue(e.getMessage().contains("scenarios"), e.getMessage());
  }
}
