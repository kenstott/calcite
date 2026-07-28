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
 * Keyless public API endpoints that sit next to the askamerica corpus, loaded once
 * from the bundled {@code /external-sources.json}.
 *
 * <p>Backs the {@code suggest_external_sources} MCP tool. These are pointers, not
 * data: nothing here is fetched by this server, and none of it carries the ETL's
 * versioning or DQ guarantees. The caveat returned alongside every result exists to
 * keep that distinction from collapsing once the caller has both an askamerica row
 * and a live API response in hand.
 */
final class ExternalSources {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String RESOURCE = "/external-sources.json";

    /**
     * Returned with every result. The failure this guards against is not the caller
     * being unable to find data — it is the caller silently blending a live figure
     * with an ingested one whose vintage differs, and reporting a single number.
     */
    private static final String CAVEAT =
        "These are external endpoints, NOT askamerica data. They carry none of this "
        + "server's versioning, DQ checks, or reproducibility. Rules for using them: "
        + "(1) askamerica remains the primary source — reach for these only for a "
        + "genuine coverage gap or for periods past a table's declared coverage window "
        + "(see describe_table). "
        + "(2) Never merge a value fetched here into the same table or total as an "
        + "askamerica value without labelling which is which — revised series make the "
        + "two legitimately disagree. "
        + "(3) Cite the endpoint and the date you fetched it. "
        + "(4) You need a fetch capability to call these; if you have none, report the "
        + "gap and the endpoint rather than guessing at the numbers. "
        + "(5) Endpoint shapes and rate limits drift — verify the response before "
        + "relying on it, and report a persistent gap with report_issue.";

    private static volatile JsonNode root;

    private ExternalSources() {
    }

    private static JsonNode root() {
        JsonNode r = root;
        if (r == null) {
            synchronized (ExternalSources.class) {
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
        try (InputStream is = ExternalSources.class.getResourceAsStream(RESOURCE)) {
            if (is == null) {
                // Bundled in the jar — absence is a packaging failure, not a runtime state
                // to paper over with an empty list.
                throw new IllegalStateException(
                    "Missing bundled resource " + RESOURCE);
            }
            return MAPPER.readTree(is).path("sources");
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Unreadable resource " + RESOURCE, e);
        }
    }

    /**
     * Sources ranked against a free-text topic, capped at {@code limit}. An empty
     * topic returns the full list, which is small enough to read whole.
     */
    static String suggest(String topic, int limit) {
        ArrayNode matches = MAPPER.createArrayNode();
        if (topic == null || topic.trim().isEmpty()) {
            int n = 0;
            for (JsonNode s : root()) {
                if (n++ >= limit) {
                    break;
                }
                matches.add(entry(s, 0));
            }
        } else {
            String[] toks = topic.toLowerCase(Locale.ROOT).split("\\s+");
            List<ObjectNode> hits = new ArrayList<>();
            for (JsonNode s : root()) {
                int score = score(toks, s);
                if (score > 0) {
                    hits.add(entry(s, score));
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
        out.put("caveat", CAVEAT);
        out.set("sources", matches);
        if (matches.size() == 0) {
            out.put("note",
                "No keyless public endpoint is catalogued for that topic. Say the data is "
                + "unavailable rather than substituting an unsourced figure, and consider "
                + "report_issue so the gap is recorded.");
        }
        return out.toString();
    }

    /** Topic keywords carry the most weight; prose fields break ties. */
    private static int score(String[] toks, JsonNode s) {
        int score = 0;
        for (String tk : toks) {
            if (tk.isEmpty()) {
                continue;
            }
            for (JsonNode t : s.path("topics")) {
                String topic = t.asText("").toLowerCase(Locale.ROOT);
                if (topic.equals(tk)) {
                    score += 10;
                } else if (topic.contains(tk)) {
                    score += 4;
                }
            }
            if (text(s, "complements").equals(tk)) {
                score += 6;
            }
            if (text(s, "name").contains(tk)) {
                score += 3;
            }
            if (text(s, "covers").contains(tk) || text(s, "gap").contains(tk)) {
                score += 1;
            }
        }
        return score;
    }

    private static String text(JsonNode s, String field) {
        return s.path(field).asText("").toLowerCase(Locale.ROOT);
    }

    private static ObjectNode entry(JsonNode s, int score) {
        ObjectNode o = MAPPER.createObjectNode();
        o.put("id", s.path("id").asText());
        o.put("name", s.path("name").asText());
        o.put("base_url", s.path("base_url").asText());
        o.put("auth", s.path("auth").asText());
        o.put("docs", s.path("docs").asText());
        if (s.hasNonNull("complements")) {
            o.put("complements_schema", s.path("complements").asText());
        }
        o.put("covers", s.path("covers").asText());
        o.put("gap", s.path("gap").asText());
        o.put("notes", s.path("notes").asText());
        o.put("score", score);
        return o;
    }
}
