/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What a market tool's result is shown with: its ready-made dashboard and its follow-ups.
 *
 * <p>A dashboard is kept here and the result carries only its id, {@code dashboard_layout}.
 * The caller passes {@code {"layout": id}} where a dashboard is taken and {@link #resolve}
 * puts the panels in. Returning the panels themselves would have the caller copy several
 * kilobytes of chart data back, token by token, to publish what the engine already holds.
 */
final class MarketPresentation {
    static final String LAYOUT = "layout";
    static final String LAYOUT_FIELD = "dashboard_layout";
    static final String REPORT_TOOL = "create_report_artifact";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int KEPT = 40;
    private static final String[] HEADINGS = {"title", "subtitle", "footnote", "columns"};

    /** Layouts by id, oldest first; the oldest is dropped past {@link #KEPT}. */
    private final Map<String, ObjectNode> layouts =
        new LinkedHashMap<String, ObjectNode>() {
            @Override protected boolean removeEldestEntry(Map.Entry<String, ObjectNode> e) {
                return size() > KEPT;
            }
        };
    /** Ids returned since the last report: a report now owes one of them. */
    private final Set<String> owed = new LinkedHashSet<>();
    private int sequence;

    /** Adds the opportunity card and the follow-ups to a {@code price_market_event} result. */
    synchronized String event(String result) throws Exception {
        ObjectNode json = (ObjectNode) MAPPER.readTree(result);
        if (carded(json)) {
            String id = "event:" + json.get("source").asText() + ":"
                + json.get("event_id").asText();
            keep(json, id, MarketLayouts.opportunityCard(json), "You MUST pass dashboard: "
                + "{\"layout\": \"" + id + "\"} to " + REPORT_TOOL + " for a report on this "
                + "event. A report on several priced events passes their ids as a list.");
        }
        followUps(json, MarketFollowUps.forPricedEvent(json));
        return MAPPER.writeValueAsString(json);
    }

    /** Adds the scan board and the follow-ups to a finished scan's result. */
    synchronized String scan(String result) throws Exception {
        ObjectNode json = (ObjectNode) MAPPER.readTree(result);
        if (!MarketScan.COMPLETE.equals(json.path("status").asText())) {
            return result;
        }
        String id = "scan:" + ++sequence;
        keep(json, id, MarketLayouts.scanBoard(json), "You MUST pass dashboard: {\"layout\": \""
            + id + "\"} to " + REPORT_TOOL + " for a report on several of these events. A "
            + "report on one of them passes the layout price_market_event returns for it.");
        followUps(json, MarketFollowUps.forScan(json));
        return MAPPER.writeValueAsString(json);
    }

    /** Adds the basket sheet and the follow-ups to a {@code price_market_basket} result. */
    synchronized String basket(String result) throws Exception {
        ObjectNode json = (ObjectNode) MAPPER.readTree(result);
        JsonNode search = json.get("search");
        if (search == null || search.path("best").size() > 0) {
            String id = "basket:" + ++sequence;
            keep(json, id, MarketLayouts.basketSheet(json), "You MUST pass dashboard: "
                + "{\"layout\": \"" + id + "\"} to " + REPORT_TOOL + " for a report on this "
                + "basket.");
        }
        followUps(json, MarketFollowUps.forBasket(json));
        return MAPPER.writeValueAsString(json);
    }

    /** A card needs a forecast and a market that can be bought on one side. */
    private static boolean carded(JsonNode json) {
        if (!json.hasNonNull("forecast")) {
            return false;
        }
        for (JsonNode m : json.path("priced_markets")) {
            if (m.path("edge").isNumber()) {
                return true;
            }
        }
        return false;
    }

    private void keep(ObjectNode json, String id, ObjectNode layout, String use) {
        layouts.remove(id);
        layouts.put(id, layout);
        owed.add(id);
        json.put(LAYOUT_FIELD, id);
        ArrayNode titles = json.putArray("dashboard_panels");
        for (JsonNode p : layout.get("panels")) {
            titles.add(p.hasNonNull("title") ? p.get("title").asText()
                : p.path("label").asText());
        }
        json.put("dashboard_use", use + " compose_dashboard takes the same layout. Panels "
            + "given beside layout are placed after it.");
    }

    private static void followUps(ObjectNode json, ArrayNode followUps) {
        if (followUps.size() == 0) {
            return;
        }
        json.set("follow_ups", followUps);
        json.put("follow_ups_use", "You MUST end the answer with the follow_ups questions. "
            + "When one is taken up, call its tool with its arguments as they are.");
    }

    /**
     * Replaces {@code layout} in a dashboard argument with the panels it names, followed by
     * the panels the caller gave. Title, subtitle, footnote and columns come from the layout
     * where the caller gave none; several layouts each open with a heading tile, and the
     * board takes a title of its own and their distinct footnotes.
     *
     * @param dashboard the arguments of {@code compose_dashboard}, or a report's dashboard
     * @return whether the argument named a layout
     */
    synchronized boolean resolve(ObjectNode dashboard) {
        JsonNode ref = dashboard.get(LAYOUT);
        if (ref == null || ref.isNull()) {
            return false;
        }
        List<String> ids = new ArrayList<>();
        if (ref.isTextual()) {
            ids.add(ref.asText());
        } else if (ref.isArray() && ref.size() > 0) {
            for (JsonNode r : ref) {
                if (!r.isTextual()) {
                    throw new IllegalArgumentException("layout must hold layout ids, got " + r);
                }
                ids.add(r.asText());
            }
        } else {
            throw new IllegalArgumentException("layout must be a layout id or a list of them, "
                + "got " + ref);
        }
        ArrayNode panels = MAPPER.createArrayNode();
        ObjectNode first = null;
        Set<String> footnotes = new LinkedHashSet<>();
        int placed = 0;
        for (String id : ids) {
            ObjectNode layout = layouts.get(id);
            if (layout == null) {
                throw new IllegalArgumentException("layout '" + id + "' is not one a market "
                    + "tool returned in this session; " + (layouts.isEmpty() ? "none has been "
                    + "returned" : "returned: " + layouts.keySet()));
            }
            if (first == null) {
                first = layout;
            }
            footnotes.add(layout.path("footnote").asText());
            if (ids.size() > 1) {
                // Several layouts on one board: each opens with a tile naming what it shows.
                ObjectNode head = panels.addObject();
                head.put("type", "stat");
                head.put("span", layout.get("columns").asInt());
                head.put("label", ++placed + " of " + ids.size());
                head.put("value", layout.get("title").asText());
                head.put("caption", layout.path("subtitle").asText());
            }
            for (JsonNode p : layout.get("panels")) {
                panels.add(p.deepCopy());
            }
        }
        JsonNode own = dashboard.get("panels");
        if (own != null && !own.isNull()) {
            if (!own.isArray()) {
                throw new IllegalArgumentException("panels must be an array, got " + own);
            }
            panels.addAll((ArrayNode) own);
        }
        if (ids.size() > 1) {
            if (!dashboard.hasNonNull("title")) {
                dashboard.put("title", "Prediction-market opportunities");
            }
            if (!dashboard.hasNonNull("subtitle")) {
                dashboard.put("subtitle", ids.size() + " priced views; each opens with its "
                    + "own heading");
            }
            if (!dashboard.hasNonNull("footnote")) {
                footnotes.remove("");
                dashboard.put("footnote", String.join(" ", footnotes));
            }
        }
        for (String h : HEADINGS) {
            if (!dashboard.hasNonNull(h) && first.has(h)) {
                dashboard.set(h, first.get(h));
            }
        }
        dashboard.remove(LAYOUT);
        dashboard.set("panels", panels);
        return true;
    }

    /**
     * The report gate: once a market tool has returned a layout, the report is drawn from one.
     *
     * @param resolved whether the report's dashboard named a layout
     * @return the problem, or null when there is none
     */
    synchronized String gate(boolean resolved) {
        if (resolved || owed.isEmpty()) {
            return null;
        }
        return "the report has no market dashboard. You MUST pass dashboard: {\"layout\": id} "
            + "with the " + LAYOUT_FIELD + " a market tool returned for what the report "
            + "covers: one event's, a list of several events', a scan's or a basket's. "
            + "Returned since the last report: " + owed + ".";
    }

    /** Called once a report has cleared its gates: the next report owes nothing yet. */
    synchronized void reset() {
        owed.clear();
    }
}
