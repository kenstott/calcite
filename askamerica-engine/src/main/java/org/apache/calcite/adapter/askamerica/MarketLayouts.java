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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;

/**
 * Ready-made dashboards for the prediction-market tools: each method turns one tool's output
 * into the argument of {@code compose_dashboard}, so the model passes it on instead of
 * drawing the charts by hand.
 *
 * <p>The functions are pure and show only numbers the tool returned, apart from a leg total
 * (fees) and a difference already implied by two fields. A field a layout needs that is
 * missing is a bug in the caller or the tool and throws {@link IllegalArgumentException}
 * naming it; a panel is left out only in the cases each method documents.
 */
final class MarketLayouts {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Settlement conditions of a priced market, as {@code Condition.writeTo} writes them. */
    private static final List<String> CONDITION_KINDS =
        Arrays.asList("above", "at_least", "below", "at_most", "between");

    private static final int LABEL_MAX = 28;

    private MarketLayouts() {
    }

    // ─── Opportunity card ──────────────────────────────────────────────────────

    /**
     * The card for one event priced against a forecast. Panels, in order: best edge, fair value
     * against price, confidence, days to settlement (stats; the last is left out when the
     * venue gave the best market no close time); the fair-versus-quote ladder; forecast
     * probability against the market's bid and ask, strike by strike; and the series history
     * with its forecast fan, each strike a reference line.
     *
     * <p>The fan panel is left out when {@code forecast_built} is absent: the event was
     * priced from a forecast the caller supplied, so there is no history to draw.
     *
     * @param priced the output of {@code price_market_event} with a forecast
     */
    static ObjectNode opportunityCard(JsonNode priced) {
        String where = "price_market_event output";
        JsonNode forecast = required(priced, "forecast", where);
        if (forecast.isNull()) {
            throw new IllegalArgumentException(where + ": forecast is null; the card needs an "
                + "event priced against a forecast");
        }
        JsonNode markets = requiredArray(priced, "priced_markets", where);
        List<JsonNode> rows = new ArrayList<>();
        for (int i = 0; i < markets.size(); i++) {
            JsonNode m = markets.get(i);
            String at = where + ": priced_markets[" + i + "]";
            required(m, "condition", at);
            // A market the forecast does not price (verdict not_forecast) has no fair value.
            if (required(m, "fair", at).isNull()) {
                continue;
            }
            required(m, "yes_bid", at);
            required(m, "yes_ask", at);
            rows.add(m);
        }
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(where + ": no priced market has a fair value");
        }
        rows.sort(Comparator.comparingDouble((JsonNode m) -> strike(m.get("condition"))[0])
            .thenComparingDouble(m -> strike(m.get("condition"))[1]));
        JsonNode best = null;
        for (JsonNode m : rows) {
            if (m.path("edge").isNumber()
                    && (best == null || m.get("edge").asDouble() > best.get("edge").asDouble())) {
                best = m;
            }
        }
        if (best == null) {
            throw new IllegalArgumentException(where + ": no priced market has an edge (none is "
                + "quoted on a side that can be bought)");
        }
        String bestAt = where + ": the best priced market";
        String side = requiredText(best, "side", bestAt);
        requiredNumber(best, "price", bestAt);
        requiredNumber(best, "fee", bestAt);
        required(best, "days_to_settlement", bestAt);
        JsonNode confidence = required(priced, "confidence", where);
        String eventTitle = requiredText(priced, "event_title", where);
        String source = requiredText(priced, "source", where);

        ObjectNode out = MAPPER.createObjectNode();
        out.put("title", eventTitle + " (" + source + ")");
        out.put("subtitle", "Forecast median " + number(requiredNumber(forecast, "median",
            where + ": forecast")) + ", 90% range " + number(requiredNumber(forecast, "p05",
            where + ": forecast")) + " to " + number(requiredNumber(forecast, "p95",
            where + ": forecast")) + "; quotes read " + requiredText(priced,
            "quotes_read_at", where));
        out.put("footnote", requiredText(priced, "fee_basis", where));
        out.put("columns", 4);
        ArrayNode panels = out.putArray("panels");

        String title = best.path("title").asText(best.path("market_id").asText());
        panels.add(stat("Best edge after fees", signedPoints(best.get("edge").asDouble()),
            "buy " + side.toUpperCase(Locale.ROOT) + ": " + shorten(title),
            best.get("edge").asDouble() > 0 ? "up" : "down", null));
        JsonNode se = best.path("se");
        panels.add(stat("Fair value of YES vs price", percent(best.get("fair").asDouble()),
            "buy " + side.toUpperCase(Locale.ROOT) + " at "
                + percent(best.get("price").asDouble()) + " + fee "
                + percent(best.get("fee").asDouble()), "flat",
            se.isNumber() ? "Sampling error of the fair value: " + percent(se.asDouble())
                : null));
        String tier = requiredText(confidence, "tier", where + ": confidence");
        JsonNode reasons = required(confidence, "reasons", where + ": confidence");
        List<String> why = new ArrayList<>();
        for (JsonNode r : reasons) {
            why.add(r.asText());
        }
        panels.add(stat("Confidence", tier, null, "weak".equals(tier) ? "down" : "up",
            why.isEmpty() ? null : String.join("; ", why)));
        JsonNode days = best.get("days_to_settlement");
        if (days.isNumber()) {
            panels.add(stat("Days to settlement", String.format(Locale.ROOT, "%.1f",
                days.asDouble()), null, "flat",
                "Closes " + requiredText(priced, "close_time", where)));
        }

        List<String> labels = new ArrayList<>();
        ArrayNode fair = MAPPER.createArrayNode();
        ArrayNode ask = MAPPER.createArrayNode();
        ArrayNode bid = MAPPER.createArrayNode();
        ArrayNode edge = MAPPER.createArrayNode();
        for (JsonNode m : rows) {
            // A market quoted on neither side has no edge field.
            if (m.path("edge").isNumber()) {
                edge.add(m.get("edge").asDouble());
            } else {
                edge.addNull();
            }
            labels.add(strikeLabel(m.get("condition")));
            fair.add(m.get("fair").asDouble());
            addNullable(ask, m.get("yes_ask"));
            addNullable(bid, m.get("yes_bid"));
        }
        ObjectNode ladder = chart("bar", "Edge after fees on the better side, by strike",
            labels, 2);
        ladder.put("y_label", "edge after fees");
        ladder.put("value_format", ".1%");
        addSeries(ladder, "Edge after fees", edge);
        panels.add(ladder);

        ObjectNode implied = chart("line",
            "Forecast probability against the market's quote, by strike", labels, 2);
        implied.put("x_label", "strike");
        implied.put("y_label", "probability of YES");
        implied.put("value_format", ".0%");
        addSeries(implied, "Forecast probability", fair);
        addSeries(implied, "Market YES ask", ask);
        addSeries(implied, "Market YES bid", bid);
        panels.add(implied);

        JsonNode built = priced.get("forecast_built");
        if (built != null) {
            String at = where + ": forecast_built";
            JsonNode fan = required(built, "fan", at);
            ObjectNode panel = MAPPER.createObjectNode();
            panel.put("type", "chart");
            panel.put("chart_type", "fan");
            panel.put("span", 4);
            panel.put("title", requiredText(built, "series", at)
                + ": history and forecast to settlement");
            panel.put("x_label", "period");
            JsonNode units = built.get("units");
            if (units != null && !units.isNull()) {
                panel.put("y_label", units.asText());
            }
            panel.set("categories", required(fan, "categories", at + ".fan").deepCopy());
            panel.set("series", required(fan, "series", at + ".fan").deepCopy());
            panel.set("bands", required(fan, "bands", at + ".fan").deepCopy());
            // The best market's strikes only: a line for every strike of a ladder hides
            // the fan behind them.
            TreeSet<Double> strikes = new TreeSet<>();
            for (double k : strike(best.get("condition"))) {
                strikes.add(k);
            }
            ArrayNode refs = panel.putArray("reference_lines");
            for (double k : strikes) {
                ObjectNode r = refs.addObject();
                r.put("value", k);
                r.put("label", "strike " + number(k));
            }
            panels.add(panel);
        }
        return out;
    }

    // ─── Scan board ────────────────────────────────────────────────────────────

    /**
     * The board for one scan. Panels, in order: matched, forecast built, passed and best edge
     * (stats; the last is the edge threshold when nothing passed); the funnel from matched to
     * passed; the opportunities ranked by edge; edge against standard error.
     *
     * <p>The ranking and the scatter are left out when no event passed. The scatter is also
     * left out when no passed market carries a standard error, as when the forecasts are not
     * sampled.
     *
     * @param scan the output of {@code scan_market_opportunities}
     */
    static ObjectNode scanBoard(JsonNode scan) {
        String where = "scan_market_opportunities output";
        String status = requiredText(scan, "status", where);
        double minEdge = requiredNumber(scan, "min_edge", where);
        JsonNode funnel = required(scan, "funnel", where);
        String at = where + ": funnel";
        int matched = (int) requiredNumber(funnel, "events_matched", at);
        int toEvaluate = (int) requiredNumber(funnel, "events_to_evaluate", at);
        int evaluated = (int) requiredNumber(funnel, "events_evaluated", at);
        int forecast = (int) requiredNumber(funnel, "events_forecast", at);
        int passed = (int) requiredNumber(funnel, "events_passed", at);
        JsonNode opps = requiredArray(scan, "opportunities", where);

        List<String> names = new ArrayList<>();
        List<Double> edges = new ArrayList<>();
        List<Double> xs = new ArrayList<>();
        List<Double> ys = new ArrayList<>();
        List<String> tips = new ArrayList<>();
        double bestEdge = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < opps.size(); i++) {
            JsonNode o = opps.get(i);
            String oat = where + ": opportunities[" + i + "]";
            JsonNode markets = requiredArray(o, "markets", oat);
            if (markets.size() == 0) {
                throw new IllegalArgumentException(oat + ".markets is empty");
            }
            String name = requiredText(o, "event_title", oat) + " ("
                + requiredText(o, "source", oat) + ")";
            double top = requiredNumber(markets.get(0), "edge", oat + ".markets[0]");
            names.add(shorten(name));
            edges.add(top);
            bestEdge = Math.max(bestEdge, top);
            for (int j = 0; j < markets.size(); j++) {
                JsonNode m = markets.get(j);
                if (m.path("se").isNumber()) {
                    double edge = requiredNumber(m, "edge", oat + ".markets[" + j + "]");
                    xs.add(m.get("se").asDouble());
                    ys.add(edge);
                    tips.add(name + ": " + m.path("title").asText() + ", edge "
                        + signedPoints(edge) + ", se " + percent(m.get("se").asDouble()));
                }
            }
        }

        ObjectNode out = MAPPER.createObjectNode();
        out.put("title", "Prediction-market scan: events priced against a forecast");
        out.put("subtitle", "complete".equals(status)
            ? "Scan complete; listing read " + requiredText(scan, "listing_read_at", where)
            : "Scan " + status + ": " + evaluated + " of " + toEvaluate + " events evaluated");
        out.put("footnote", requiredText(scan, "forecast_is", where));
        out.put("columns", 4);
        ArrayNode panels = out.putArray("panels");
        panels.add(stat("Events matched", String.valueOf(matched), null, "flat", null));
        panels.add(stat("Forecast built", String.valueOf(forecast), evaluated + " evaluated",
            "flat", null));
        panels.add(stat("Passed", String.valueOf(passed), null, passed > 0 ? "up" : "flat",
            "Edge of at least " + percent(minEdge) + " after fees, outside sampling error"));
        if (opps.size() > 0) {
            panels.add(stat("Best edge", signedPoints(bestEdge), null, "up", null));
        } else {
            panels.add(stat("Edge threshold", percent(minEdge), null, "flat",
                "No event passed it"));
        }

        List<String> stages = Arrays.asList("Matched", "Evaluated", "Forecast built", "Passed");
        ObjectNode funnelPanel = chart("bar", "From matched events to opportunities", stages,
            opps.size() > 0 ? 2 : 4);
        funnelPanel.put("orientation", "horizontal");
        funnelPanel.put("value_labels", true);
        funnelPanel.put("value_format", ",.0f");
        ArrayNode counts = MAPPER.createArrayNode();
        counts.add(matched).add(evaluated).add(forecast).add(passed);
        addSeries(funnelPanel, "Events", counts);
        panels.add(funnelPanel);

        if (opps.size() > 0) {
            ObjectNode ranked = chart("bar", "Opportunities by edge after fees", names, 2);
            ranked.put("orientation", "horizontal");
            ranked.put("sort", "desc");
            ranked.put("value_labels", true);
            ranked.put("value_format", ".1%");
            ArrayNode values = MAPPER.createArrayNode();
            for (double e : edges) {
                values.add(e);
            }
            addSeries(ranked, "Best edge", values);
            panels.add(ranked);
        }
        if (!xs.isEmpty()) {
            ObjectNode scatter = MAPPER.createObjectNode();
            scatter.put("type", "chart");
            scatter.put("chart_type", "scatter");
            scatter.put("span", 4);
            scatter.put("title", "Edge against standard error");
            scatter.put("x_label", "standard error of the fair value");
            scatter.put("y_label", "edge after fees");
            ObjectNode s = scatter.putArray("points").addObject();
            s.put("name", "Markets past the threshold");
            ArrayNode x = s.putArray("x");
            ArrayNode y = s.putArray("y");
            ArrayNode t = s.putArray("tooltips");
            for (int i = 0; i < xs.size(); i++) {
                x.add(xs.get(i));
                y.add(ys.get(i));
                t.add(tips.get(i));
            }
            panels.add(scatter);
        }
        return out;
    }

    // ─── Basket sheet ──────────────────────────────────────────────────────────

    /**
     * The sheet for one priced basket, of the best subset the search kept, or of every leg
     * when no search ran. Panels, in order: cost, floor net of fees, yield (best case when the
     * scenarios carry no probabilities) and fees (stats); the payoff diagram; the fee of each
     * leg; then what breaks the lock as stats: the rules verdict, the scenario count, where the
     * floor falls with its break-evens, and the venues.
     *
     * <p>The payoff diagram and the floor tile are left out when the tool returned no
     * {@code payoff_curve}: the legs settle on several quantities. The rules tile reads
     * "one venue" when the tool wrote no verdict because no pair spans two venues.
     *
     * @param basket the output of {@code price_market_basket}
     */
    static ObjectNode basketSheet(JsonNode basket) {
        String where = "price_market_basket output";
        JsonNode score;
        JsonNode search = basket.get("search");
        if (search != null) {
            JsonNode kept = requiredArray(search, "best", where + ": search");
            if (kept.size() == 0) {
                throw new IllegalArgumentException(where + ": search.best is empty; no basket "
                    + "was kept to lay out");
            }
            score = kept.get(0);
        } else {
            score = required(basket, "all_legs", where);
        }
        String at = where + ": the scored basket";
        double cost = requiredNumber(score, "cost", at);
        double floor = requiredNumber(score, "floor", at);
        JsonNode legs = requiredArray(score, "legs", at);
        List<String> names = new ArrayList<>();
        ArrayNode fees = MAPPER.createArrayNode();
        double feeTotal = 0;
        for (JsonNode l : legs) {
            String id = requiredText(l, "id", at + ".legs");
            String leg = requiredText(l, "side", at + ".legs");
            String src = requiredText(l, "source", at + ".legs");
            String title = requiredText(l, "title", at + ".legs[" + id + "]");
            double fee = requiredNumber(l, "fee", at + ".legs");
            feeTotal += fee;
            fees.add(fee);
            names.add(shorten(leg.toUpperCase(Locale.ROOT) + " " + title + " (" + src + ")"));
        }

        JsonNode curve = basket.get("payoff_curve");
        JsonNode events = requiredArray(basket, "events", where);
        List<String> eventTitles = new ArrayList<>();
        for (JsonNode e : events) {
            eventTitles.add(requiredText(e, "event_title", where + ": events"));
        }
        ObjectNode out = MAPPER.createObjectNode();
        out.put("title", "Basket: " + shorten(String.join(" + ", eventTitles)));
        out.put("subtitle", legs.size() + " legs, one contract each; "
            + requiredText(basket, "scenario_basis", where));
        out.put("footnote", requiredText(basket, "limits", where));
        out.put("columns", 4);
        ArrayNode panels = out.putArray("panels");
        panels.add(stat("Cost", dollars(cost), null, "flat",
            "Prices plus taker fees, one contract per leg"));
        panels.add(stat("Floor net of fees", signedPercent(floor), null,
            floor > 0 ? "up" : "down", "Worst scenario, as a share of cost"));
        if (score.has("yield")) {
            double y = requiredNumber(score, "yield", at);
            panels.add(stat("Yield", signedPercent(y), null, y > 0 ? "up" : "down",
                "Expected profit as a share of cost"));
        } else {
            panels.add(stat("Best case", dollars(requiredNumber(score, "best", at)), null,
                "flat", "The scenarios carry no probabilities, so no yield"));
        }
        panels.add(stat("Fees", dollars(feeTotal), legs.size() + " legs", "flat",
            "Taker fee at the quote, included in the cost"));

        if (curve != null) {
            String cat = where + ": payoff_curve";
            JsonNode points = requiredArray(curve, "curve", cat);
            List<String> values = new ArrayList<>();
            ArrayNode profit = MAPPER.createArrayNode();
            for (JsonNode p : points) {
                values.add(number(requiredNumber(p, "value", cat + ".curve")));
                profit.add(requiredNumber(p, "profit_per_cost", cat + ".curve"));
            }
            ObjectNode payoff = chart("line", "Profit by settlement value", values, 2);
            payoff.put("x_label", requiredText(curve, "column", cat));
            payoff.put("y_label", "profit per unit of cost");
            payoff.put("value_format", "+.0%");
            addSeries(payoff, "Profit", profit);
            ObjectNode ref = payoff.putArray("reference_lines").addObject();
            double floorLine = requiredNumber(curve, "floor_profit_per_cost", cat);
            ref.put("value", floorLine);
            ref.put("label", "Floor net of fees " + signedPercent(floorLine));
            panels.add(payoff);
        }
        ObjectNode feePanel = chart("bar", "Taker fee by leg, " + dollars(feeTotal) + " in all",
            names, curve != null ? 2 : 4);
        feePanel.put("orientation", "horizontal");
        feePanel.put("value_labels", true);
        feePanel.put("value_format", "$.3f");
        addSeries(feePanel, "Fee", fees);
        panels.add(feePanel);

        String rulesValue;
        String rulesDirection;
        String rulesCaption;
        if (basket.has("rules_match")) {
            rulesValue = requiredText(basket, "rules_match", where);
            rulesDirection = MarketRules.MATCH.equals(rulesValue) ? "up"
                : MarketRules.DIFFER.equals(rulesValue) ? "down" : "flat";
            List<String> pairs = new ArrayList<>();
            for (JsonNode p : requiredArray(basket, "rules", where)) {
                pairs.add(requiredText(p, "a", where + ": rules") + " vs "
                    + requiredText(p, "b", where + ": rules") + " "
                    + requiredText(p, "rules_match", where + ": rules")
                    + listed(" differing ", p.get("differing"))
                    + listed(" unknown ", p.get("unknown")));
            }
            if (basket.has("rule_pairs_not_compared")) {
                pairs.add(basket.get("rule_pairs_not_compared").asInt()
                    + " pairs not compared");
            }
            rulesCaption = String.join("; ", pairs);
        } else {
            rulesValue = "one venue";
            rulesDirection = "flat";
            rulesCaption = "No pair of events spans two venues, so no rule texts were compared";
        }
        panels.add(stat("Rules verdict", rulesValue, null, rulesDirection, rulesCaption));
        panels.add(stat("Scenarios", String.valueOf((int) requiredNumber(basket, "scenarios",
            where)), null, "flat", "A lock is only as complete as its scenarios"));
        if (curve != null) {
            List<String> breaks = new ArrayList<>();
            for (JsonNode b : requiredArray(curve, "break_even_values", where
                    + ": payoff_curve")) {
                breaks.add(number(requiredNumber(b, "value", where + ": break_even_values")));
            }
            panels.add(stat("Floor falls", requiredText(curve, "floor_where",
                where + ": payoff_curve"), null, "flat", breaks.isEmpty()
                    ? "Profit crosses zero at no strike"
                    : "Profit crosses zero at " + String.join(", ", breaks)));
        }
        List<String> venues = new ArrayList<>();
        for (JsonNode v : requiredArray(basket, "venues", where)) {
            venues.add(v.asText());
        }
        panels.add(stat("Venues", String.join(", ", venues), null, "flat",
            basket.has("venue_note") ? requiredText(basket, "venue_note", where) : null));
        return out;
    }

    // ─── Shared helpers ────────────────────────────────────────────────────────

    private static ObjectNode stat(String label, String value, String delta, String direction,
            String caption) {
        ObjectNode p = MAPPER.createObjectNode();
        p.put("type", "stat");
        p.put("label", label);
        p.put("value", value);
        if (delta != null) {
            p.put("delta", delta);
        }
        p.put("delta_direction", direction);
        if (caption != null) {
            p.put("caption", caption);
        }
        return p;
    }

    private static ObjectNode chart(String type, String title, List<String> categories,
            int span) {
        ObjectNode p = MAPPER.createObjectNode();
        p.put("type", "chart");
        p.put("chart_type", type);
        p.put("span", span);
        p.put("title", title);
        ArrayNode cats = p.putArray("categories");
        for (String c : categories) {
            cats.add(c);
        }
        p.putArray("series");
        return p;
    }

    private static void addSeries(ObjectNode chart, String name, ArrayNode values) {
        ObjectNode s = ((ArrayNode) chart.get("series")).addObject();
        s.put("name", name);
        s.set("values", values);
    }

    private static void addNullable(ArrayNode to, JsonNode v) {
        if (v.isNull()) {
            to.addNull();
        } else {
            to.add(v.asDouble());
        }
    }

    /** The low and high of a condition: both the strike for a one-sided kind. */
    private static double[] strike(JsonNode condition) {
        for (String kind : CONDITION_KINDS) {
            JsonNode v = condition.get(kind);
            if (v == null) {
                continue;
            }
            if ("between".equals(kind)) {
                return new double[]{v.get(0).asDouble(), v.get(1).asDouble()};
            }
            return new double[]{v.asDouble(), v.asDouble()};
        }
        throw new IllegalArgumentException("condition " + condition + " names none of "
            + CONDITION_KINDS);
    }

    private static String strikeLabel(JsonNode condition) {
        double[] k = strike(condition);
        if (condition.has("between")) {
            return number(k[0]) + " to " + number(k[1]);
        }
        String sign = condition.has("above") ? "> " : condition.has("at_least") ? ">= "
            : condition.has("below") ? "< " : "<= ";
        return sign + number(k[0]);
    }

    private static String listed(String prefix, JsonNode items) {
        if (items == null || items.size() == 0) {
            return "";
        }
        List<String> names = new ArrayList<>();
        for (JsonNode i : items) {
            names.add(i.asText());
        }
        return prefix + names;
    }

    private static String shorten(String text) {
        return text.length() > LABEL_MAX ? text.substring(0, LABEL_MAX - 1) + "…" : text;
    }

    private static String number(double v) {
        return new BigDecimal(v).setScale(4, java.math.RoundingMode.HALF_EVEN)
            .stripTrailingZeros().toPlainString();
    }

    private static String percent(double v) {
        return String.format(Locale.ROOT, "%.1f%%", v * 100);
    }

    private static String signedPercent(double v) {
        return String.format(Locale.ROOT, "%+.1f%%", v * 100);
    }

    private static String signedPoints(double v) {
        return String.format(Locale.ROOT, "%+.1f pts", v * 100);
    }

    private static String dollars(double v) {
        return String.format(Locale.ROOT, "$%.4f", v);
    }

    private static JsonNode required(JsonNode owner, String field, String where) {
        JsonNode v = owner.get(field);
        if (v == null) {
            throw new IllegalArgumentException(where + ": field '" + field + "' is missing");
        }
        return v;
    }

    private static JsonNode requiredArray(JsonNode owner, String field, String where) {
        JsonNode v = required(owner, field, where);
        if (!v.isArray()) {
            throw new IllegalArgumentException(where + ": field '" + field + "' is not an array");
        }
        return v;
    }

    private static double requiredNumber(JsonNode owner, String field, String where) {
        JsonNode v = required(owner, field, where);
        if (!v.isNumber()) {
            throw new IllegalArgumentException(where + ": field '" + field + "' is " + v
                + ", not a number");
        }
        return v.asDouble();
    }

    private static String requiredText(JsonNode owner, String field, String where) {
        JsonNode v = required(owner, field, where);
        if (!v.isTextual()) {
            throw new IllegalArgumentException(where + ": field '" + field + "' is " + v
                + ", not text");
        }
        return v.asText();
    }
}
