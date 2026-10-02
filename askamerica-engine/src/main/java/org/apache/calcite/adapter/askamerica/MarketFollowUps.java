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

import java.util.ArrayList;
import java.util.List;

/**
 * The next questions a prediction-market result can answer, each with the complete call that
 * answers it: {@code {question, tool, arguments}}. Every argument is read from the result
 * itself, so a follow-up is only emitted when its call can be fully formed from it. A
 * condition that makes a call meaningless omits the follow-up; an identifier the result
 * should carry and does not is a defect in the result and throws, naming the field.
 *
 * <p>At most {@link #MAX} follow-ups are returned, most useful first.
 */
final class MarketFollowUps {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Most follow-ups returned for one result. */
    static final int MAX = 5;

    private static final String KALSHI = "kalshi";
    private static final String MATCH = "match";

    private MarketFollowUps() {
    }

    /**
     * Follow-ups for a {@code price_market_event} result, in order: the engine's own forecast
     * when none was built or given; the re-quote of the first mispriced market's ticket; that
     * market's price history and book; the other venue's counterpart priced with its own
     * built forecast; the rules diff against that counterpart; the series backtest when a
     * built forecast on a Kalshi series has none.
     */
    static ArrayNode forPricedEvent(JsonNode priced) {
        String source = text(priced, "source", "priced event");
        String eventId = text(priced, "event_id", "priced event");
        if (!priced.has("forecast")) {
            throw new IllegalArgumentException("priced event: missing field forecast");
        }
        JsonNode markets = array(priced, "priced_markets", "priced event");
        boolean forecastGiven = !priced.get("forecast").isNull();
        boolean built = priced.hasNonNull("forecast_built");
        ArrayNode out = MAPPER.createArrayNode();

        if (!forecastGiven && !built && priced.hasNonNull("driver")) {
            ObjectNode a = event(source, eventId);
            a.put("build_forecast", true);
            add(out, "What does the engine's own forecast say this event is worth?",
                "price_market_event", a);
        }

        JsonNode mispriced = null;
        for (JsonNode m : markets) {
            if ("mispriced".equals(text(m, "verdict", "priced_markets entry"))) {
                mispriced = m;
                break;
            }
        }
        if (mispriced != null) {
            String marketId = text(mispriced, "market_id", "priced_markets entry");
            if (mispriced.hasNonNull("ticket")) {
                ObjectNode a = MAPPER.createObjectNode();
                a.set("ticket", mispriced.get("ticket"));
                add(out, "Is the " + marketId + " opportunity still there at its limit price?",
                    "requote_market_opportunity", a);
            }
            add(out, "How long has " + marketId + " been at this price, and how deep is its book?",
                "market_price_history", market(source, marketId));
        }

        JsonNode other = priced.path("other_venue");
        JsonNode counterpart = other.path("counterparts").path(0);
        if (!counterpart.isMissingNode()) {
            String otherSource = text(counterpart, "source", "other_venue.counterparts entry");
            String otherEvent = text(counterpart, "event_id", "other_venue.counterparts entry");
            if (!forecastGiven || built) {
                ObjectNode a = event(otherSource, otherEvent);
                a.put("build_forecast", true);
                add(out, "What does " + otherSource + " price the same quantity at?",
                    "price_market_event", a);
            }
            ObjectNode pair = MAPPER.createObjectNode();
            pair.set("a", event(source, eventId));
            pair.set("b", event(otherSource, otherEvent));
            add(out, "Do " + source + " and " + otherSource + " settle on the same number?",
                "compare_settlement_rules", pair);
        }

        if (built && KALSHI.equals(source)) {
            JsonNode confidence = object(priced, "confidence", "priced event");
            if (confidence.path("backtest").isNull()) {
                String series = text(priced, "venue_series", "priced event");
                add(out, "Has the engine's forecast beaten the " + series + " price in the past?",
                    "backtest_market_forecast", backtest(series));
            }
        }
        return capped(out);
    }

    /**
     * Follow-ups for a {@code scan_market_opportunities} result, in order: the top
     * opportunity priced for its ticket and depth; the backtest of its Kalshi series when it
     * has none; its driver on the other venue; the first structural lock priced net of fees;
     * the second opportunity priced.
     */
    static ArrayNode forScan(JsonNode scan) {
        JsonNode opportunities = array(scan, "opportunities", "scan");
        JsonNode structural = array(scan, "structural", "scan");
        ArrayNode out = MAPPER.createArrayNode();

        if (opportunities.size() > 0) {
            JsonNode top = opportunities.get(0);
            String source = text(top, "source", "opportunities entry");
            String eventId = text(top, "event_id", "opportunities entry");
            String driver = text(top, "driver", "opportunities entry");
            ObjectNode a = event(source, eventId);
            a.put("build_forecast", true);
            add(out, "What is the order ticket, limit price and depth for " + eventId + "?",
                "price_market_event", a);
            if (KALSHI.equals(source)) {
                JsonNode confidence = object(top, "confidence", "opportunities entry");
                if (confidence.path("backtest").isNull()) {
                    String series = text(top, "venue_series", "opportunities entry");
                    add(out, "Has the engine's forecast beaten the " + series
                        + " price in the past?", "backtest_market_forecast", backtest(series));
                }
            }
            ObjectNode b = MAPPER.createObjectNode();
            b.put("recipe", "cross_venue");
            b.put("match", driver);
            add(out, "Is the same " + driver + " quantity quoted on both venues for a lock?",
                "find_market_baskets", b);
        }
        if (structural.size() > 0) {
            JsonNode s = structural.get(0);
            String eventId = text(s, "event_id", "structural entry");
            add(out, "Does the structural lock in " + eventId + " survive fees?",
                "price_market_event", event(text(s, "source", "structural entry"), eventId));
        }
        if (opportunities.size() > 1) {
            JsonNode second = opportunities.get(1);
            String eventId = text(second, "event_id", "opportunities entry");
            ObjectNode a = event(text(second, "source", "opportunities entry"), eventId);
            a.put("build_forecast", true);
            add(out, "What is the order ticket, limit price and depth for " + eventId + "?",
                "price_market_event", a);
        }
        return capped(out);
    }

    /**
     * Follow-ups for a {@code price_market_basket} result, in order: the rules diff of a
     * pair spanning two venues whose verdict is not {@code match}; the cross-venue baskets of
     * the driver when every event is on one venue; the book behind each of the first two legs
     * of the best subset (all legs when no search ran); the engine's forecast for the first
     * event when it was priced with none.
     */
    static ArrayNode forBasket(JsonNode basket) {
        JsonNode events = array(basket, "events", "basket");
        if (events.size() == 0) {
            throw new IllegalArgumentException("basket: field events is empty");
        }
        ArrayNode out = MAPPER.createArrayNode();

        JsonNode first = null;
        JsonNode second = null;
        for (JsonNode e : events) {
            String s = text(e, "source", "basket events entry");
            text(e, "event_id", "basket events entry");
            if (first == null) {
                first = e;
            } else if (second == null && !s.equals(first.get("source").asText())) {
                second = e;
            }
        }
        if (second != null) {
            if (!MATCH.equals(basket.path("rules_match").asText(null))) {
                ObjectNode pair = MAPPER.createObjectNode();
                pair.set("a", event(first.get("source").asText(), first.get("event_id").asText()));
                pair.set("b", event(second.get("source").asText(),
                    second.get("event_id").asText()));
                add(out, "Do the two venues settle on the same number?",
                    "compare_settlement_rules", pair);
            }
        } else {
            String driver = text(first, "driver", "basket events entry");
            ObjectNode a = MAPPER.createObjectNode();
            a.put("recipe", "cross_venue");
            a.put("match", driver);
            add(out, "Is the same " + driver + " quantity quoted on the other venue?",
                "find_market_baskets", a);
        }

        JsonNode legs = legsOf(basket);
        for (int i = 0; i < legs.size() && i < 2; i++) {
            JsonNode leg = legs.get(i);
            String marketId = text(leg, "id", "basket leg");
            add(out, "How deep is the book behind leg " + marketId + "?", "market_price_history",
                market(text(leg, "source", "basket leg"), marketId));
        }

        if (first.path("forecast").isNull() && first.hasNonNull("driver")) {
            String eventId = first.get("event_id").asText();
            ObjectNode a = event(first.get("source").asText(), eventId);
            a.put("build_forecast", true);
            add(out, "Does the engine's forecast agree with the quotes of " + eventId + "?",
                "price_market_event", a);
        }
        return capped(out);
    }

    /** Legs of the best subset a search kept; of all legs when no search ran. */
    private static JsonNode legsOf(JsonNode basket) {
        JsonNode search = basket.path("search");
        if (!search.isMissingNode()) {
            JsonNode best = array(search, "best", "basket search");
            return best.size() == 0 ? best : array(best.get(0), "legs", "basket search.best entry");
        }
        return array(object(basket, "all_legs", "basket"), "legs", "basket all_legs");
    }

    private static ObjectNode event(String source, String eventId) {
        ObjectNode o = MAPPER.createObjectNode();
        o.put("source", source);
        o.put("event_id", eventId);
        return o;
    }

    private static ObjectNode market(String source, String marketId) {
        ObjectNode o = MAPPER.createObjectNode();
        o.put("source", source);
        o.put("market_id", marketId);
        return o;
    }

    private static ObjectNode backtest(String series) {
        ObjectNode o = MAPPER.createObjectNode();
        o.put("source", KALSHI);
        o.put("series", series);
        return o;
    }

    private static void add(ArrayNode out, String question, String tool, ObjectNode arguments) {
        ObjectNode o = out.addObject();
        o.put("question", question);
        o.put("tool", tool);
        o.set("arguments", arguments);
    }

    private static ArrayNode capped(ArrayNode all) {
        ArrayNode out = MAPPER.createArrayNode();
        List<JsonNode> kept = new ArrayList<>();
        for (JsonNode n : all) {
            if (kept.size() < MAX) {
                kept.add(n);
            }
        }
        out.addAll(kept);
        return out;
    }

    private static String text(JsonNode node, String field, String where) {
        JsonNode v = node.get(field);
        if (v == null || !v.isTextual() || v.asText().isEmpty()) {
            throw new IllegalArgumentException(where + ": missing field " + field);
        }
        return v.asText();
    }

    private static JsonNode array(JsonNode node, String field, String where) {
        JsonNode v = node.get(field);
        if (v == null || !v.isArray()) {
            throw new IllegalArgumentException(where + ": missing array field " + field);
        }
        return v;
    }

    private static JsonNode object(JsonNode node, String field, String where) {
        JsonNode v = node.get(field);
        if (v == null || !v.isObject()) {
            throw new IllegalArgumentException(where + ": missing object field " + field);
        }
        return v;
    }
}
