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

    /** Trigger phrases carry the most weight; title/body prose breaks ties.
     *
     * <p>Exact/substring matching alone misses ordinary morphological variation (a caller's
     * topic says "explains", a trigger says "explain"; "dominates" vs. "dominate") and small
     * typos, so a topic phrased in perfectly reasonable but different-enough words scores zero
     * against a genuinely relevant recipe. The two fuzzy checks below close that gap without a
     * real semantic/embedding search (not available in this environment - see the "embedder:
     * none found" startup notice): a shared-prefix stem check catches common suffix variation
     * (-s, -es, -ed, -ing, -ion), and a bounded edit-distance check catches near-miss
     * typos/spelling variants. Both are intentionally conservative (short minimum word length,
     * small edit-distance bound) to avoid false hits between short, unrelated words.
     */
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
                } else {
                    score += fuzzyWordScore(trigger, tk);
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

    /** Checks {@code tk} against each individual word of a (possibly multi-word) trigger
     *  phrase for a stem or near-miss-typo match. Returns the best single match found, 0 if
     *  none - never stacks multiple word-level hits for one token against one trigger, so a
     *  long trigger phrase can't accumulate score just by having more words. */
    private static int fuzzyWordScore(String trigger, String tk) {
        int best = 0;
        for (String tw : trigger.split("\\s+")) {
            if (tw.isEmpty() || tw.equals(tk)) {
                continue; // exact whole-trigger-phrase equality already scored above
            }
            if (sharesStem(tw, tk)) {
                best = Math.max(best, 6);
            } else if (isNearMissTypo(tw, tk)) {
                best = Math.max(best, 5);
            }
        }
        return best;
    }

    /** True when two words share enough of a common prefix to plausibly be the same stem
     *  under ordinary English suffixing (concentration/concentrate/concentrated;
     *  explain/explains/explaining; dominate/dominates). Requires both words to be at least 5
     *  characters and share their first 5 - short words are excluded because a 5-character
     *  shared prefix is far more likely to be coincidental between unrelated short words. */
    private static boolean sharesStem(String a, String b) {
        if (a.length() < 5 || b.length() < 5) {
            return false;
        }
        return a.regionMatches(0, b, 0, 5);
    }

    /** True when two words are close enough (bounded Levenshtein distance) to plausibly be a
     *  typo or minor spelling variant of each other, not two different words. The bound scales
     *  with word length so short words need a near-exact match (distance 1 only, and only at
     *  length >= 4) while longer words tolerate a slightly larger edit distance - both to avoid
     *  two genuinely different short words (e.g. "tax" / "tab") scoring as a match. */
    private static boolean isNearMissTypo(String a, String b) {
        int shorter = Math.min(a.length(), b.length());
        if (shorter < 4) {
            return false;
        }
        int bound = shorter >= 7 ? 2 : 1;
        if (Math.abs(a.length() - b.length()) > bound) {
            return false;
        }
        return levenshtein(a, b, bound) <= bound;
    }

    /** Classic bounded Levenshtein distance - returns a value > maxBound the moment every cell
     *  in the current row exceeds it, so a wildly different pair of words (the common case,
     *  most tokens don't match most triggers) exits in O(maxBound * min(len)) rather than
     *  computing the full O(len*len) table for every single comparison this scorer makes. */
    private static int levenshtein(String a, String b, int maxBound) {
        int la = a.length();
        int lb = b.length();
        int[] prev = new int[lb + 1];
        int[] curr = new int[lb + 1];
        for (int j = 0; j <= lb; j++) {
            prev[j] = j;
        }
        for (int i = 1; i <= la; i++) {
            curr[0] = i;
            int rowMin = curr[0];
            char ca = a.charAt(i - 1);
            for (int j = 1; j <= lb; j++) {
                int cost = ca == b.charAt(j - 1) ? 0 : 1;
                curr[j] = Math.min(Math.min(curr[j - 1] + 1, prev[j] + 1), prev[j - 1] + cost);
                rowMin = Math.min(rowMin, curr[j]);
            }
            if (rowMin > maxBound) {
                return rowMin; // definitively exceeds the bound; caller only checks <= maxBound
            }
            int[] tmp = prev;
            prev = curr;
            curr = tmp;
        }
        return prev[lb];
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
