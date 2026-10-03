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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Follow-ups carry complete, schema-valid calls built from the result they follow. */
@Tag("unit")
class MarketFollowUpsTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static JsonNode json(String s) {
    try {
      return MAPPER.readTree(s.replace('\'', '"'));
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static final String TICKET = "{'source':'kalshi','event_id':'KXCPI-26OCT',"
      + "'market_id':'KXCPI-26OCT-T0.3','side':'yes','limit_price':0.41,'tick':0.01,"
      + "'fair':0.52,'fee_rate':0.07,'min_edge':0.03,'edge_at_limit':0.0312,"
      + "'quote_time':'2026-10-02T12:00:00Z','void_conditions':["
      + "{'type':'settlement_close','time':'2026-10-14T12:30:00Z'}]}";

  private static String priced(String forecast, String built, String ticket, String confidence,
      String counterparts) {
    return "{'source':'kalshi','event_id':'KXCPI-26OCT','event_title':'CPI Oct',"
        + "'series':'KXCPI','venue_series':'KXCPI','driver':'inflation','basis':'release',"
        + "'close_time':'2026-10-14T12:30:00Z','forecast':" + forecast + built
        + ",'priced_markets':[{'market_id':'KXCPI-26OCT-T0.2','verdict':'within_min_edge'},"
        + "{'market_id':'KXCPI-26OCT-T0.3','verdict':'mispriced','side':'yes','price':0.41,"
        + "'edge':0.08,'ticket':" + ticket + "}],"
        + "'other_venue':{'venue':'polymarket','status':'found','counterparts':" + counterparts
        + "}" + confidence + "}";
  }

  private static final String CP = "[{'source':'polymarket','event_id':'88123',"
      + "'event_title':'CPI Oct','driver':'inflation'}]";
  private static final String BUILT = ",'forecast_built':{'flags':[]}";
  private static final String FORECAST = "{'median':0.3}";
  private static final String NO_BACKTEST = ",'confidence':{'tier':'weak','backtest':null}";

  private static JsonNode fullPriced() {
    return json(priced(FORECAST, BUILT, TICKET, NO_BACKTEST, CP));
  }

  private static void assertCall(JsonNode f, String tool, String arguments) {
    assertEquals(tool, f.get("tool").asText());
    assertEquals(json(arguments), f.get("arguments"), f.get("question").asText());
    assertTrue(f.get("question").asText().endsWith("?"));
  }

  @Test void aPricedEventOffersTheTicketTheBookTheCounterpartTheRulesAndTheBacktest() {
    ArrayNode f = MarketFollowUps.forPricedEvent(fullPriced());
    assertEquals(5, f.size());
    assertCall(f.get(0), "requote_market_opportunity", "{'ticket':" + TICKET + "}");
    assertCall(f.get(1), "market_price_history",
        "{'source':'kalshi','market_id':'KXCPI-26OCT-T0.3'}");
    assertCall(f.get(2), "price_market_event",
        "{'source':'polymarket','event_id':'88123','build_forecast':true}");
    assertCall(f.get(3), "compare_settlement_rules",
        "{'a':{'source':'kalshi','event_id':'KXCPI-26OCT'},"
            + "'b':{'source':'polymarket','event_id':'88123'}}");
    assertCall(f.get(4), "backtest_market_forecast", "{'source':'kalshi','series':'KXCPI'}");
  }

  @Test void aCallerGivenForecastIsNotRebuiltOnTheOtherVenueAndHasNoBacktest() {
    JsonNode p = json(priced(FORECAST, "", TICKET,
        ",'confidence':{'tier':'weak','backtest':null}", CP));
    ArrayNode f = MarketFollowUps.forPricedEvent(p);
    List<String> tools = tools(f);
    assertEquals(java.util.Arrays.asList("requote_market_opportunity", "market_price_history",
        "compare_settlement_rules"), tools);
  }

  @Test void aTicketlessMispricedMarketOmitsTheRequoteButKeepsTheBook() {
    ArrayNode f = MarketFollowUps.forPricedEvent(
        json(priced(FORECAST, BUILT, "null", NO_BACKTEST, CP)));
    assertEquals(java.util.Arrays.asList("market_price_history", "price_market_event",
        "compare_settlement_rules", "backtest_market_forecast"), tools(f));
  }

  @Test void aBacktestedSeriesAndNoCounterpartLeaveOnlyTheTicketAndTheBook() {
    ArrayNode f = MarketFollowUps.forPricedEvent(json(priced(FORECAST, BUILT, TICKET,
        ",'confidence':{'tier':'backtested','backtest':{'verdict':'inconclusive'}}", "[]")));
    assertEquals(java.util.Arrays.asList("requote_market_opportunity", "market_price_history"),
        tools(f));
  }

  @Test void aPolymarketEventIsNeverBacktested() {
    String s = priced(FORECAST, BUILT, TICKET, NO_BACKTEST, CP)
        .replace("'source':'kalshi','event_id':'KXCPI-26OCT','event_title'",
            "'source':'polymarket','event_id':'88123','event_title'");
    assertTrue(!tools(MarketFollowUps.forPricedEvent(json(s)))
        .contains("backtest_market_forecast"));
  }

  @Test void quotesOnlyOfAnEventWithADriverOffersTheBuiltForecast() {
    JsonNode p = json("{'source':'kalshi','event_id':'KXCPI-26OCT','driver':'inflation',"
        + "'forecast':null,'priced_markets':[{'market_id':'KXCPI-26OCT-T0.2',"
        + "'verdict':'not_forecast'}],'other_venue':{'status':'none','counterparts':[]}}");
    ArrayNode f = MarketFollowUps.forPricedEvent(p);
    assertEquals(1, f.size());
    assertCall(f.get(0), "price_market_event",
        "{'source':'kalshi','event_id':'KXCPI-26OCT','build_forecast':true}");
  }

  @Test void quotesOnlyOfAnEventWithNoDriverOffersNothing() {
    JsonNode p = json("{'source':'kalshi','event_id':'KXOTHER','driver':null,"
        + "'forecast':null,'priced_markets':[],'other_venue':{'status':'not_screened'}}");
    assertEquals(0, MarketFollowUps.forPricedEvent(p).size());
  }

  @Test void aPricedEventMissingAnIdentifierThrowsNamingIt() {
    JsonNode noEvent = fullPriced();
    ((com.fasterxml.jackson.databind.node.ObjectNode) noEvent).remove("event_id");
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> MarketFollowUps.forPricedEvent(noEvent)).getMessage().contains("event_id"));
    JsonNode noSeries = fullPriced();
    ((com.fasterxml.jackson.databind.node.ObjectNode) noSeries).remove("venue_series");
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> MarketFollowUps.forPricedEvent(noSeries)).getMessage().contains("venue_series"));
    JsonNode badCounterpart = json(priced(FORECAST, BUILT, TICKET, NO_BACKTEST,
        "[{'source':'polymarket'}]"));
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> MarketFollowUps.forPricedEvent(badCounterpart)).getMessage()
        .contains("event_id"));
  }

  private static final String SCAN_ROW = "{'source':'kalshi','event_id':'%s',"
      + "'venue_series':'KXPAYROLLS','driver':'payrolls','forecast':{'median':150000},"
      + "'confidence':{'tier':'weak','backtest':null},'markets':[{'market_id':'m1',"
      + "'edge':0.09}]}";

  private static JsonNode scan(String rows, String structural) {
    return json("{'funnel':{},'opportunities':[" + rows + "],'not_forecast':[],'structural':["
        + structural + "]}");
  }

  @Test void aScanOffersTheTopTicketTheBacktestTheOtherVenueTheLockAndTheSecondEvent() {
    String structural = "{'source':'polymarket','event_id':'7001','locks':[]}";
    ArrayNode f = MarketFollowUps.forScan(scan(
        String.format(SCAN_ROW, "KXPAYROLLS-26OCT") + "," + String.format(SCAN_ROW, "KXU3-26OCT"),
        structural));
    assertEquals(5, f.size());
    assertCall(f.get(0), "price_market_event",
        "{'source':'kalshi','event_id':'KXPAYROLLS-26OCT','build_forecast':true}");
    assertCall(f.get(1), "backtest_market_forecast",
        "{'source':'kalshi','series':'KXPAYROLLS'}");
    assertCall(f.get(2), "find_market_baskets", "{'recipe':'cross_venue','match':'payrolls'}");
    assertCall(f.get(3), "price_market_event", "{'source':'polymarket','event_id':'7001'}");
    assertCall(f.get(4), "price_market_event",
        "{'source':'kalshi','event_id':'KXU3-26OCT','build_forecast':true}");
  }

  @Test void aBacktestedTopOpportunityOmitsTheBacktest() {
    String row = String.format(SCAN_ROW, "KXPAYROLLS-26OCT").replace("'backtest':null",
        "'backtest':{'verdict':'inconclusive'}");
    assertEquals(java.util.Arrays.asList("price_market_event", "find_market_baskets"),
        tools(MarketFollowUps.forScan(scan(row, ""))));
  }

  @Test void anEmptyScanOffersNothingAndAScanMissingAListThrows() {
    assertEquals(0, MarketFollowUps.forScan(scan("", "")).size());
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> MarketFollowUps.forScan(json("{'opportunities':[]}"))).getMessage()
        .contains("structural"));
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> MarketFollowUps.forScan(scan(
            String.format(SCAN_ROW, "X").replace("'driver':'payrolls',", ""), "")))
        .getMessage().contains("driver"));
  }

  private static final String LEGS = "[{'id':'KXCPI-26OCT-T0.3','source':'kalshi',"
      + "'side':'yes','price':0.41,'fee':0.017},{'id':'88123-a','source':'polymarket',"
      + "'side':'no','price':0.52,'fee':0.0}]";

  private static String basket(String events, String rulesMatch, String rest) {
    return "{'events':" + events + ",'venues':['kalshi','polymarket']" + rulesMatch
        + ",'all_legs':{'legs':" + LEGS + ",'cost':0.95,'floor':0.02}" + rest + "}";
  }

  private static final String TWO_VENUES = "[{'source':'kalshi','event_id':'KXCPI-26OCT',"
      + "'driver':'inflation','forecast':null},{'source':'polymarket','event_id':'88123',"
      + "'driver':'inflation','forecast':null}]";

  @Test void aCrossVenueBasketWhoseRulesDifferOffersTheRulesDiffAndTheBooks() {
    ArrayNode f = MarketFollowUps.forBasket(json(basket(TWO_VENUES, ",'rules_match':'differ'",
        "")));
    assertEquals(4, f.size());
    assertCall(f.get(0), "compare_settlement_rules",
        "{'a':{'source':'kalshi','event_id':'KXCPI-26OCT'},"
            + "'b':{'source':'polymarket','event_id':'88123'}}");
    assertCall(f.get(1), "market_price_history",
        "{'source':'kalshi','market_id':'KXCPI-26OCT-T0.3'}");
    assertCall(f.get(2), "market_price_history",
        "{'source':'polymarket','market_id':'88123-a'}");
    assertCall(f.get(3), "price_market_event",
        "{'source':'kalshi','event_id':'KXCPI-26OCT','build_forecast':true}");
  }

  @Test void aMatchingBasketOmitsTheRulesDiffAndASearchUsesItsBestSubsetLegs() {
    String search = ",'search':{'best':[{'legs':[{'id':'88123-a','source':'polymarket',"
        + "'side':'no','price':0.52,'fee':0.0}]}]}";
    ArrayNode f = MarketFollowUps.forBasket(json(basket(TWO_VENUES, ",'rules_match':'match'",
        search)));
    assertEquals(java.util.Arrays.asList("market_price_history", "price_market_event"),
        tools(f));
    assertEquals("88123-a", f.get(0).get("arguments").get("market_id").asText());
  }

  @Test void aSearchThatKeptNothingOffersNoBookAndASingleVenueBasketOffersTheOtherVenue() {
    String one = "[{'source':'kalshi','event_id':'KXCPI-26OCT','driver':'inflation',"
        + "'forecast':{'median':0.3}}]";
    ArrayNode f = MarketFollowUps.forBasket(json(basket(one, "", ",'search':{'best':[]}")));
    assertEquals(1, f.size());
    assertCall(f.get(0), "find_market_baskets", "{'recipe':'cross_venue','match':'inflation'}");
  }

  @Test void aBasketMissingAnIdentifierThrowsNamingIt() {
    String noId = "[{'source':'kalshi','driver':'inflation','forecast':null}]";
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> MarketFollowUps.forBasket(json(basket(noId, "", "")))).getMessage()
        .contains("event_id"));
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> MarketFollowUps.forBasket(json("{'events':[]}"))).getMessage()
        .contains("events"));
    String noLegId = basket(TWO_VENUES, ",'rules_match':'differ'", "")
        .replace("{'id':'KXCPI-26OCT-T0.3',", "{");
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> MarketFollowUps.forBasket(json(noLegId))).getMessage().contains("id"));
  }

  @Test void everyFollowUpNamesARegisteredToolAndOnlyArgumentsItsSchemaAccepts() {
    List<ArrayNode> all = new ArrayList<>();
    all.add(MarketFollowUps.forPricedEvent(fullPriced()));
    all.add(MarketFollowUps.forScan(scan(String.format(SCAN_ROW, "KXPAYROLLS-26OCT"),
        "{'source':'polymarket','event_id':'7001','locks':[]}")));
    all.add(MarketFollowUps.forBasket(json(basket(TWO_VENUES, ",'rules_match':'differ'", ""))));
    int checked = 0;
    for (ArrayNode follow : all) {
      assertTrue(follow.size() <= MarketFollowUps.MAX);
      for (JsonNode f : follow) {
        JsonNode def = null;
        for (JsonNode t : McpServer.toolDefs()) {
          if (t.get("name").asText().equals(f.get("tool").asText())) {
            def = t;
          }
        }
        assertNotNull(def, f.get("tool").asText() + " is not registered");
        JsonNode props = def.get("inputSchema").get("properties");
        Iterator<String> names = f.get("arguments").fieldNames();
        while (names.hasNext()) {
          String n = names.next();
          assertTrue(props.has(n), f.get("tool").asText() + " does not accept " + n);
        }
        for (JsonNode r : def.get("inputSchema").get("required")) {
          assertTrue(f.get("arguments").has(r.asText()),
              f.get("tool").asText() + " requires " + r.asText());
        }
        checked++;
      }
    }
    assertTrue(checked >= 12, "checked " + checked);
  }

  private static List<String> tools(ArrayNode f) {
    List<String> out = new ArrayList<>();
    for (JsonNode n : f) {
      out.add(n.get("tool").asText());
    }
    return out;
  }
}
