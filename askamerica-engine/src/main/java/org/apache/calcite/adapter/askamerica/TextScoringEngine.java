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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Deterministic, dependency-free sentiment and relevance scoring of arbitrary text.
 *
 * <p>Sentiment is a lexicon method: finance/news terms from {@code sentiment-lexicon.json},
 * negation flipping polarity within a few tokens in the same sentence, intensifiers and
 * dampeners scaling the next term. It is a transparent heuristic, not a trained model — it has
 * no notion of sarcasm, of who the sentiment is about, or of a term's sense beyond the
 * lexicon's. Every result therefore carries the evidence behind it (term counts, matched
 * terms) so a caller can judge how much weight the number deserves, and text with no lexicon
 * terms at all is reported as {@code no_signal} with a null score rather than a neutral zero.
 *
 * <p>Relevance is query-term coverage plus an exact-phrase/alias bonus plus a lead-position
 * bonus, in [0,1]. It measures how much of the query the text mentions and where — not
 * whether the text is factually about the query's subject.
 */
final class TextScoringEngine {

    static final int MAX_TEXTS = 200;
    static final int MAX_TEXT_CHARS = 200_000;
    static final double POSITIVE_THRESHOLD = 0.15;
    /** Evidence (weighted term mass) at which confidence reaches one half. */
    static final double CONFIDENCE_HALF_EVIDENCE = 3.0;
    private static final int NEGATION_WINDOW = 3;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern SENTENCE_SPLIT = Pattern.compile("(?<=[.!?;:])\\s+|\\n+");
    private static final Pattern TOKEN = Pattern.compile("[\\p{L}\\p{N}][\\p{L}\\p{N}'\\-]*");

    static final String DEFAULT_DOMAIN = "general";
    /** Domains with a lexicon; "general" is the base file itself, every other domain is an
     *  overlay file on top of it. */
    static final List<String> DOMAINS = java.util.Arrays.asList("general", "finance",
        "health", "politics", "manufacturing", "energy", "agriculture", "environment",
        "legal", "housing", "labor", "public_safety", "technology");
    private static final Map<String, Lexicon> LEXICONS =
        new java.util.concurrent.ConcurrentHashMap<>();

    private TextScoringEngine() {}

    // ─── Lexicon ───────────────────────────────────────────────────────────────

    static final class Lexicon {
        final Map<String, Double> positive = new HashMap<>();
        final Map<String, Double> negative = new HashMap<>();
        final Set<String> uncertainty = new HashSet<>();
        final Set<String> negators = new HashSet<>();
        final Map<String, Double> intensifiers = new HashMap<>();
        final Set<String> stopwords = new HashSet<>();
        int maxPhraseWords = 1;
    }

    /** The lexicon for {@code domain}: the base file with the domain's overlay applied. */
    static Lexicon lexicon(String domain) {
        if (!DOMAINS.contains(domain)) {
            throw new IllegalArgumentException("unknown domain '" + domain + "'; available: "
                + DOMAINS);
        }
        return LEXICONS.computeIfAbsent(domain, TextScoringEngine::loadLexicon);
    }

    private static JsonNode readResource(String name) {
        try (InputStream in = TextScoringEngine.class.getResourceAsStream(name)) {
            if (in == null) {
                throw new IllegalStateException(name + " is missing from the engine resources");
            }
            return MAPPER.readTree(in);
        } catch (IOException e) {
            throw new IllegalStateException("could not read " + name, e);
        }
    }

    private static Lexicon loadLexicon(String domain) {
        JsonNode root = readResource("/sentiment-lexicon.json");
        Lexicon lx = new Lexicon();
        addTerms(lx, lx.positive, root.path("positive"));
        addTerms(lx, lx.negative, root.path("negative"));
        for (JsonNode n : root.path("uncertainty")) {
            lx.uncertainty.add(n.asText());
            lx.uncertainty.add(stem(n.asText()));
        }
        for (JsonNode n : root.path("negators")) {
            lx.negators.add(n.asText());
        }
        Iterator<Map.Entry<String, JsonNode>> it = root.path("intensifiers").fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            lx.intensifiers.put(e.getKey(), e.getValue().asDouble());
        }
        for (JsonNode n : root.path("stopwords")) {
            lx.stopwords.add(n.asText());
        }
        if (!"general".equals(domain)) {
            applyOverlay(lx, readResource("/sentiment-lexicon-" + domain + ".json"));
        }
        if (lx.positive.isEmpty() || lx.negative.isEmpty()) {
            throw new IllegalStateException("lexicon for '" + domain + "' has an empty "
                + "positive or negative list");
        }
        return lx;
    }

    /** An overlay re-files terms: a term listed positive/negative leaves the opposite list
     *  and joins this one; a term listed under "neutral" is dropped from both. */
    private static void applyOverlay(Lexicon lx, JsonNode overlay) {
        for (JsonNode n : overlay.path("neutral")) {
            removeTerm(lx.positive, n.asText());
            removeTerm(lx.negative, n.asText());
        }
        for (JsonNode n : overlay.path("positive")) {
            removeTerm(lx.negative, termOf(n.asText()));
        }
        for (JsonNode n : overlay.path("negative")) {
            removeTerm(lx.positive, termOf(n.asText()));
        }
        addTerms(lx, lx.positive, overlay.path("positive"));
        addTerms(lx, lx.negative, overlay.path("negative"));
        for (JsonNode n : overlay.path("uncertainty")) {
            lx.uncertainty.add(n.asText());
            lx.uncertainty.add(stem(n.asText()));
        }
    }

    private static String termOf(String raw) {
        int colon = raw.lastIndexOf(':');
        return (colon > 0 ? raw.substring(0, colon) : raw).toLowerCase();
    }

    private static void removeTerm(Map<String, Double> m, String term) {
        m.remove(term);
        m.remove(stem(term));
    }

    private static void addTerms(Lexicon lx, Map<String, Double> into, JsonNode list) {
        for (JsonNode n : list) {
            String raw = n.asText();
            double weight = 1.0;
            int colon = raw.lastIndexOf(':');
            if (colon > 0) {
                weight = Double.parseDouble(raw.substring(colon + 1));
                raw = raw.substring(0, colon);
            }
            String term = raw.toLowerCase();
            into.put(term, weight);
            int words = term.split("\\s+").length;
            if (words == 1) {
                into.put(stem(term), weight);
            }
            lx.maxPhraseWords = Math.max(lx.maxPhraseWords, words);
        }
    }

    /** Light suffix stripping, applied to both lexicon entries and text tokens so they meet
     *  in the middle ("surged" and "surging" both become "surg"). */
    static String stem(String w) {
        String s = w;
        if (s.length() > 5 && s.endsWith("ing")) {
            s = s.substring(0, s.length() - 3);
        } else if (s.length() > 4 && s.endsWith("ed")) {
            s = s.substring(0, s.length() - 2);
        } else if (s.length() > 4 && s.endsWith("es")) {
            s = s.substring(0, s.length() - 2);
        } else if (s.length() > 3 && s.endsWith("s") && !s.endsWith("ss")) {
            s = s.substring(0, s.length() - 1);
        } else if (s.length() > 5 && s.endsWith("ly")) {
            s = s.substring(0, s.length() - 2);
        }
        if (s.length() > 3 && s.endsWith("e")) {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }

    private static Double lookup(Map<String, Double> m, String token) {
        Double w = m.get(token);
        if (w != null) {
            return w;
        }
        return m.get(stem(token));
    }

    private static List<String> tokenize(String sentence) {
        List<String> out = new ArrayList<>();
        Matcher m = TOKEN.matcher(sentence.toLowerCase().replace('’', '\''));
        while (m.find()) {
            out.add(m.group());
        }
        return out;
    }

    static void checkText(String text, int index) {
        if (text == null || text.trim().isEmpty()) {
            throw new IllegalArgumentException("text #" + index + " is empty");
        }
        if (text.length() > MAX_TEXT_CHARS) {
            throw new IllegalArgumentException("text #" + index + " is " + text.length()
                + " characters; the limit is " + MAX_TEXT_CHARS + " — split it and score the "
                + "parts");
        }
    }

    // ─── Sentiment ─────────────────────────────────────────────────────────────

    static final class Sentiment {
        Double score;          // null when no lexicon term matched
        String label;
        double positiveMass;
        double negativeMass;
        int positiveTerms;
        int negativeTerms;
        int negatedTerms;
        int uncertaintyTerms;
        int tokens;
        double confidence;
        final Map<String, Integer> matched = new LinkedHashMap<>();

        ObjectNode toJson() {
            ObjectNode o = MAPPER.createObjectNode();
            if (score == null) {
                o.putNull("score");
            } else {
                o.put("score", score);
            }
            o.put("label", label);
            o.put("confidence", confidence);
            o.put("positive_mass", positiveMass);
            o.put("negative_mass", negativeMass);
            o.put("positive_terms", positiveTerms);
            o.put("negative_terms", negativeTerms);
            o.put("negated_terms", negatedTerms);
            o.put("uncertainty_terms", uncertaintyTerms);
            o.put("tokens", tokens);
            ObjectNode m = o.putObject("matched_terms");
            for (Map.Entry<String, Integer> e : matched.entrySet()) {
                m.put(e.getKey(), e.getValue());
            }
            return o;
        }
    }

    /** Sentiment of one text: (P - N) / (P + N) over weighted term mass, in [-1, 1]. */
    static Sentiment sentiment(String text) {
        return sentiment(text, DEFAULT_DOMAIN);
    }

    static Sentiment sentiment(String text, String domain) {
        Lexicon lex = lexicon(domain);
        Sentiment s = new Sentiment();
        for (String sentence : SENTENCE_SPLIT.split(text)) {
            List<String> tok = tokenize(sentence);
            s.tokens += tok.size();
            int i = 0;
            while (i < tok.size()) {
                int consumed = 1;
                String term = tok.get(i);
                Double pos = null;
                Double neg = null;
                // Longest phrase first, so "beat expectations" is one strong term rather
                // than a weak "beat" followed by nothing.
                for (int len = Math.min(lex.maxPhraseWords, tok.size() - i); len >= 2;
                        len--) {
                    String phrase = String.join(" ", tok.subList(i, i + len));
                    Double wp = lex.positive.get(phrase);
                    Double wn = lex.negative.get(phrase);
                    if (wp != null || wn != null) {
                        pos = wp;
                        neg = wn;
                        term = phrase;
                        consumed = len;
                        break;
                    }
                }
                if (consumed == 1) {
                    pos = lookup(lex.positive, tok.get(i));
                    neg = lookup(lex.negative, tok.get(i));
                    if (lex.uncertainty.contains(tok.get(i))
                        || lex.uncertainty.contains(stem(tok.get(i)))) {
                        s.uncertaintyTerms++;
                    }
                }
                if (pos == null && neg == null) {
                    i++;
                    continue;
                }
                double weight = pos != null ? pos : neg;
                boolean positive = pos != null;
                // An intensifier or dampener directly before ("sharply lower") or after
                // ("fell sharply") the term scales it; before wins if both are present.
                if (i > 0 && lex.intensifiers.containsKey(tok.get(i - 1))) {
                    weight *= lex.intensifiers.get(tok.get(i - 1));
                } else if (i + consumed < tok.size()
                    && lex.intensifiers.containsKey(tok.get(i + consumed))) {
                    weight *= lex.intensifiers.get(tok.get(i + consumed));
                }
                boolean negated = false;
                for (int b = Math.max(0, i - NEGATION_WINDOW); b < i; b++) {
                    if (lex.negators.contains(tok.get(b))) {
                        negated = true;
                        break;
                    }
                }
                if (negated) {
                    positive = !positive;
                    s.negatedTerms++;
                    // "not good" is weaker than "bad": a flipped term carries half weight.
                    weight *= 0.5;
                }
                if (positive) {
                    s.positiveMass += weight;
                    s.positiveTerms++;
                } else {
                    s.negativeMass += weight;
                    s.negativeTerms++;
                }
                s.matched.merge((negated ? "not " : "") + term, 1, Integer::sum);
                i += consumed;
            }
        }
        double evidence = s.positiveMass + s.negativeMass;
        if (evidence == 0) {
            s.score = null;
            s.label = "no_signal";
            s.confidence = 0;
        } else {
            s.score = (s.positiveMass - s.negativeMass) / evidence;
            s.label = s.score > POSITIVE_THRESHOLD ? "positive"
                : s.score < -POSITIVE_THRESHOLD ? "negative" : "neutral";
            s.confidence = evidence / (evidence + CONFIDENCE_HALF_EVIDENCE);
        }
        return s;
    }

    // ─── Relevance ─────────────────────────────────────────────────────────────

    static final class Relevance {
        double score;
        double coverage;
        boolean exactPhrase;
        boolean aliasHit;
        boolean leadHit;
        double density;
        List<String> matchedTerms = new ArrayList<>();
        List<String> missingTerms = new ArrayList<>();

        ObjectNode toJson() {
            ObjectNode o = MAPPER.createObjectNode();
            o.put("score", score);
            o.put("term_coverage", coverage);
            o.put("exact_phrase_match", exactPhrase);
            o.put("alias_match", aliasHit);
            o.put("mentioned_in_lead", leadHit);
            o.put("mentions_per_100_tokens", density);
            ArrayNode m = o.putArray("matched_terms");
            for (String t : matchedTerms) {
                m.add(t);
            }
            ArrayNode x = o.putArray("missing_terms");
            for (String t : missingTerms) {
                x.add(t);
            }
            return o;
        }
    }

    static List<String> queryTerms(String query) {
        Set<String> out = new LinkedHashSet<>();
        for (String t : tokenize(query)) {
            if (!lexicon(DEFAULT_DOMAIN).stopwords.contains(t)) {
                out.add(t);
            }
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("query has no content words after removing "
                + "stopwords: '" + query + "'");
        }
        return new ArrayList<>(out);
    }

    /**
     * Relevance of {@code text} to {@code query}: 0.5 * coverage of the query's content words
     * + 0.3 if the whole query or any alias appears verbatim + 0.2 if a query word appears in
     * the first quarter of the text. {@code aliases} are extra surface forms of the subject
     * (a ticker, a company's former name) any one of which counts as a match.
     */
    static Relevance relevance(String text, String query, List<String> aliases) {
        List<String> terms = queryTerms(query);
        String lower = text.toLowerCase().replace('’', '\'');
        List<String> tok = tokenize(text);
        Set<String> stems = new HashSet<>();
        for (String t : tok) {
            stems.add(t);
            stems.add(stem(t));
        }
        Relevance r = new Relevance();
        int hits = 0;
        for (String t : terms) {
            if (stems.contains(t) || stems.contains(stem(t))) {
                hits++;
                r.matchedTerms.add(t);
            } else {
                r.missingTerms.add(t);
            }
        }
        r.coverage = (double) hits / terms.size();
        String q = String.join(" ", tokenize(query));
        r.exactPhrase = !q.isEmpty() && String.join(" ", tok).contains(q);
        for (String a : aliases) {
            String norm = String.join(" ", tokenize(a));
            if (!norm.isEmpty() && containsWord(String.join(" ", tok), norm)) {
                r.aliasHit = true;
                r.matchedTerms.add(a);
            }
        }
        int leadEnd = Math.max(1, tok.size() / 4);
        Set<String> leadStems = new HashSet<>();
        for (int i = 0; i < Math.min(leadEnd, tok.size()); i++) {
            leadStems.add(tok.get(i));
            leadStems.add(stem(tok.get(i)));
        }
        for (String t : terms) {
            if (leadStems.contains(t) || leadStems.contains(stem(t))) {
                r.leadHit = true;
                break;
            }
        }
        for (String a : aliases) {
            for (String at : tokenize(a)) {
                if (leadStems.contains(at)) {
                    r.leadHit = true;
                }
            }
        }
        int mentions = 0;
        for (String t : tok) {
            if (terms.contains(t) || terms.contains(stem(t))) {
                mentions++;
            }
        }
        r.density = tok.isEmpty() ? 0 : 100.0 * mentions / tok.size();
        double coverage = r.aliasHit ? Math.max(r.coverage, 1.0) : r.coverage;
        r.score = 0.5 * coverage + 0.3 * (r.exactPhrase || r.aliasHit ? 1 : 0)
            + 0.2 * (r.leadHit ? 1 : 0);
        if (lower.isEmpty()) {
            r.score = 0;
        }
        return r;
    }

    private static boolean containsWord(String haystack, String needle) {
        return (" " + haystack + " ").contains(" " + needle + " ");
    }

    // ─── Batch tools ───────────────────────────────────────────────────────────

    /** One input item: text, plus an optional id and title (the title is prepended so lead
     *  weighting sees it first). */
    static final class Item {
        final String id;
        final String text;

        Item(String id, String text) {
            this.id = id;
            this.text = text;
        }
    }

    static List<Item> parseItems(JsonNode texts) {
        if (texts == null || !texts.isArray() || texts.size() == 0) {
            throw new IllegalArgumentException("texts must be a non-empty array of strings or "
                + "{id, title, text} objects");
        }
        if (texts.size() > MAX_TEXTS) {
            throw new IllegalArgumentException("at most " + MAX_TEXTS + " texts per call, got "
                + texts.size());
        }
        List<Item> items = new ArrayList<>();
        int idx = 0;
        for (JsonNode n : texts) {
            idx++;
            if (n.isTextual()) {
                checkText(n.asText(), idx);
                items.add(new Item(String.valueOf(idx), n.asText()));
            } else if (n.isObject() && n.hasNonNull("text")) {
                String body = n.get("text").asText();
                String title = n.hasNonNull("title") ? n.get("title").asText() : "";
                String full = title.isEmpty() ? body : title + ". " + body;
                checkText(full, idx);
                items.add(new Item(n.hasNonNull("id") ? n.get("id").asText()
                    : String.valueOf(idx), full));
            } else {
                throw new IllegalArgumentException("text #" + idx + " must be a string or an "
                    + "object with a 'text' field");
            }
        }
        return items;
    }

    static ObjectNode scoreSentiment(List<Item> items) {
        return scoreSentiment(items, DEFAULT_DOMAIN);
    }

    static ObjectNode scoreSentiment(List<Item> items, String domain) {
        ObjectNode out = MAPPER.createObjectNode();
        ArrayNode results = out.putArray("results");
        double sum = 0;
        int scored = 0;
        for (Item it : items) {
            Sentiment s = sentiment(it.text, domain);
            ObjectNode r = s.toJson();
            r.put("id", it.id);
            results.add(r);
            if (s.score != null) {
                sum += s.score;
                scored++;
            }
        }
        ObjectNode agg = out.putObject("aggregate");
        agg.put("n_texts", items.size());
        agg.put("n_scored", scored);
        agg.put("n_no_signal", items.size() - scored);
        if (scored == 0) {
            agg.putNull("mean_score");
        } else {
            agg.put("mean_score", sum / scored);
        }
        out.put("domain", domain);
        out.put("method", "in-house lexicon with negation and intensifier handling; "
            + "a heuristic, not a trained model");
        return out;
    }

    static ObjectNode scoreRelevance(List<Item> items, String query, List<String> aliases) {
        ObjectNode out = MAPPER.createObjectNode();
        out.put("query", query);
        ArrayNode results = out.putArray("results");
        double sum = 0;
        for (Item it : items) {
            ObjectNode r = relevance(it.text, query, aliases).toJson();
            r.put("id", it.id);
            results.add(r);
            sum += r.get("score").asDouble();
        }
        ObjectNode agg = out.putObject("aggregate");
        agg.put("n_texts", items.size());
        agg.put("mean_score", sum / items.size());
        out.put("method", "query-term coverage + exact-phrase/alias match + lead-position "
            + "bonus; measures textual overlap with the query, not topical truth");
        return out;
    }

    /** Both scores per text, plus a mean sentiment weighted by relevance so an off-topic
     *  article cannot swing the aggregate. */
    static ObjectNode scoreText(List<Item> items, String query, List<String> aliases) {
        return scoreText(items, query, aliases, DEFAULT_DOMAIN);
    }

    static ObjectNode scoreText(List<Item> items, String query, List<String> aliases,
            String domain) {
        ObjectNode out = MAPPER.createObjectNode();
        out.put("query", query);
        out.put("domain", domain);
        ArrayNode results = out.putArray("results");
        double wSum = 0;
        double wTot = 0;
        double relSum = 0;
        int scored = 0;
        for (Item it : items) {
            Sentiment s = sentiment(it.text, domain);
            Relevance rel = relevance(it.text, query, aliases);
            ObjectNode r = MAPPER.createObjectNode();
            r.put("id", it.id);
            r.set("sentiment", s.toJson());
            r.set("relevance", rel.toJson());
            results.add(r);
            relSum += rel.score;
            if (s.score != null) {
                scored++;
                wSum += s.score * rel.score;
                wTot += rel.score;
            }
        }
        ObjectNode agg = out.putObject("aggregate");
        agg.put("n_texts", items.size());
        agg.put("n_sentiment_scored", scored);
        agg.put("mean_relevance", relSum / items.size());
        if (wTot == 0) {
            agg.putNull("relevance_weighted_sentiment");
            agg.put("note", "no text was both relevant and carried a sentiment signal");
        } else {
            agg.put("relevance_weighted_sentiment", wSum / wTot);
        }
        return out;
    }
}
