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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Worked analysis patterns, loaded once from the bundled {@code /recipes.json}.
 *
 * <p>Backs the {@code find_recipe} MCP tool. The top-level server instructions carry
 * only a one-line trigger index naming when a recipe exists — full worked content
 * (the formula, the common wrong-but-plausible shortcut, why it's wrong) is fetched
 * only by a caller who calls this tool, so a growing catalog costs nothing on
 * connections that never need one. Add an entry when a real run is traced to a
 * caller doing something plausible the instructions never actually taught, not for
 * generically "common" questions.
 */
final class RecipeCatalog {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String RESOURCE = "/recipes.json";

    private static volatile JsonNode root;

    /** Same threshold rationale as ExternalSources: one weak prose hit should not
     *  surface a recipe: a real trigger-word match is required. */
    private static final int MIN_RELEVANT_SCORE = 4;

    private RecipeCatalog() {
    }

    private static JsonNode root() {
        JsonNode r = root;
        if (r == null) {
            synchronized (RecipeCatalog.class) {
                r = root;
                if (r == null) {
                    r = load();
                    root = r;
                }
            }
        }
        return r;
    }

    private static JsonNode load() {
        try (InputStream is = RecipeCatalog.class.getResourceAsStream(RESOURCE)) {
            if (is == null) {
                // Bundled in the jar — absence is a packaging failure, not a runtime
                // state to paper over with an empty catalog.
                throw new IllegalStateException("Missing bundled resource " + RESOURCE);
            }
            return MAPPER.readTree(is).path("recipes");
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Unreadable resource " + RESOURCE, e);
        }
    }

    static String find(String topic, int limit) {
        ArrayNode matches = MAPPER.createArrayNode();
        if (topic == null || topic.trim().isEmpty()) {
            int n = 0;
            for (JsonNode r : root()) {
                if (n++ >= limit) {
                    break;
                }
                matches.add(entry(r, 0));
            }
        } else {
            String[] rawToks = topic.toLowerCase(Locale.ROOT).split("\\s+");
            List<String> toks = new ArrayList<>();
            for (String tk : rawToks) {
                if (!tk.isEmpty() && !Catalog.STOPWORDS.contains(tk)) {
                    toks.add(tk);
                }
            }
            List<ObjectNode> hits = new ArrayList<>();
            for (JsonNode r : root()) {
                int score = score(toks, r);
                if (score >= MIN_RELEVANT_SCORE) {
                    hits.add(entry(r, score));
                }
            }
            hits.sort((a, b) -> Integer.compare(b.path("score").asInt(), a.path("score").asInt()));
            int n = 0;
            for (ObjectNode h : hits) {
                if (n++ >= limit) {
                    break;
                }
                matches.add(h);
            }
        }
        ObjectNode out = MAPPER.createObjectNode();
        out.set("recipes", matches);
        if (matches.size() == 0) {
            out.put("note",
                "No recipe catalogued for that topic. This is not evidence the analysis is "
                + "fine as planned — it means this catalog has not yet covered it. Proceed "
                + "carefully and use report_issue if you find a real gap worth adding.");
        }
        return out.toString();
    }

    /** Trigger phrases carry the most weight; title/body prose breaks ties. */
    private static int score(List<String> toks, JsonNode r) {
        int score = 0;
        for (String tk : toks) {
            if (tk.isEmpty()) {
                continue;
            }
            for (JsonNode t : r.path("triggers")) {
                String trigger = t.asText("").toLowerCase(Locale.ROOT);
                if (trigger.equals(tk)) {
                    score += 10;
                } else if (trigger.contains(tk)) {
                    score += 4;
                }
            }
            if (text(r, "title").contains(tk)) {
                score += 3;
            }
            if (text(r, "body").contains(tk)) {
                score += 1;
            }
        }
        return score;
    }

    private static String text(JsonNode r, String field) {
        return r.path(field).asText("").toLowerCase(Locale.ROOT);
    }

    private static ObjectNode entry(JsonNode r, int score) {
        ObjectNode o = MAPPER.createObjectNode();
        o.put("id", r.path("id").asText());
        o.put("title", r.path("title").asText());
        o.put("body", r.path("body").asText());
        o.put("score", score);
        return o;
    }
}
