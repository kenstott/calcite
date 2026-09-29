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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Finds candidate organization-name mentions in free text and, once the caller has learned
 * which normalized names exist in the entity registry, picks the mentions that stand.
 *
 * <p>Deliberately split in two so the registry lookup can be one exact-equality probe: this
 * class only generates candidates and resolves overlaps; the SQL lives in {@link McpServer}.
 * The registry has millions of names and no index on the name column, so prefix or fuzzy
 * matching per candidate is a full scan each — candidates are therefore matched by equality
 * on the normalized name only.
 *
 * <p>Candidates are runs of capitalized tokens (with connectors such as "of", "&amp;" allowed
 * between capitalized tokens) and every 1-to-{@value #MAX_NGRAM}-token window inside each run,
 * because "Shares of Bank of America rose" makes the run "Shares" and "Bank of America" one
 * capitalized stretch only when a headline capitalizes everything, and a window search finds
 * the real name in both cases.
 */
final class EntityMentionExtractor {

    static final int MAX_NGRAM = 6;
    static final int MAX_CANDIDATES = 2000;
    static final int MAX_TICKERS = 50;

    private static final Pattern TOKEN =
        Pattern.compile("[\\p{L}][\\p{L}\\p{N}'’&.\\-]*|\\d+|&");
    private static final Set<String> CONNECTORS = new HashSet<>(Arrays.asList(
        "of", "and", "the", "for", "de", "du", "la", "le", "van", "von", "&", "y", "et"));
    private static final Set<String> COMMON_CAPITALIZED = new HashSet<>(Arrays.asList(
        "january", "february", "march", "april", "may", "june", "july", "august",
        "september", "october", "november", "december", "monday", "tuesday", "wednesday",
        "thursday", "friday", "saturday", "sunday", "the", "a", "an", "this", "that", "these",
        "those", "in", "on", "at", "for", "of", "and", "but", "or", "as", "by", "with", "from",
        "he", "she", "it", "they", "we", "i", "you", "his", "her", "its", "their", "our",
        "after", "before", "while", "when", "if", "shares", "stock", "stocks", "market",
        "markets", "company", "companies", "inc", "corp", "co", "ltd", "llc", "new", "us",
        "u.s.", "usa", "mr", "mrs", "ms", "dr", "said", "says", "according", "however"));
    /** "(NASDAQ: AAPL)", "NYSE:IBM", "$TSLA", "ticker MSFT". */
    private static final Pattern TICKER = Pattern.compile(
        "(?:\\((?:NYSE|NASDAQ|NYSEARCA|AMEX|OTC|TSX|LSE)\\s*[:\\-]\\s*([A-Z]{1,5}(?:[.\\-][A-Z])?)\\))"
        + "|(?:\\b(?:NYSE|NASDAQ|AMEX)\\s*:\\s*([A-Z]{1,5}(?:[.\\-][A-Z])?)\\b)"
        + "|(?:\\$([A-Z]{1,5})\\b)");

    private EntityMentionExtractor() {}

    /** One candidate span in the source text. */
    static final class Candidate {
        final int start;
        final int end;
        final String surface;
        final String norm;
        final int tokens;
        /** "first last" (lowercase, middle names/initials and suffixes dropped) when the span
         *  reads as a personal name, else null. */
        final String personKey;

        Candidate(int start, int end, String surface, String norm, int tokens,
                String personKey) {
            this.start = start;
            this.end = end;
            this.surface = surface;
            this.norm = norm;
            this.tokens = tokens;
            this.personKey = personKey;
        }
    }

    private static final class Tok {
        final int start;
        final int end;
        final String text;

        Tok(int start, int end, String text) {
            this.start = start;
            this.end = end;
            this.text = text;
        }

        boolean capitalized() {
            return Character.isUpperCase(text.charAt(0));
        }

        boolean connector() {
            return CONNECTORS.contains(text.toLowerCase());
        }
    }

    private static String stripTrailingPunct(String s) {
        int e = s.length();
        while (e > 1 && (s.charAt(e - 1) == '.' || s.charAt(e - 1) == '-'
            || s.charAt(e - 1) == '\'')) {
            e--;
        }
        return s.substring(0, e);
    }

    private static List<Tok> tokenize(String text) {
        List<Tok> toks = new ArrayList<>();
        Matcher m = TOKEN.matcher(text);
        while (m.find()) {
            String raw = m.group();
            String clean = stripTrailingPunct(raw);
            // A trailing period on "Inc." belongs to the abbreviation; on "Corp" it is the
            // sentence end. Either way the span excludes it, and normalization drops the suffix.
            toks.add(new Tok(m.start(), m.start() + clean.length(), clean));
        }
        return toks;
    }

    /** Distinct candidate spans, one per window position; the same surface at two positions
     *  yields two spans. Throws when the text would need more than {@value #MAX_CANDIDATES}
     *  distinct normalized names probed. */
    static List<Candidate> candidates(String text) {
        List<Tok> toks = tokenize(text);
        List<Candidate> out = new ArrayList<>();
        Set<String> distinct = new LinkedHashSet<>();
        int i = 0;
        while (i < toks.size()) {
            if (!toks.get(i).capitalized() || toks.get(i).connector()) {
                i++;
                continue;
            }
            // Extend the run over capitalized tokens and connectors that sit between them.
            int j = i;
            while (j + 1 < toks.size()) {
                Tok next = toks.get(j + 1);
                if (!joins(text, toks.get(j), next)) {
                    break;
                }
                if (next.capitalized()) {
                    j++;
                } else if (next.connector() && j + 2 < toks.size()
                    && toks.get(j + 2).capitalized() && joins(text, next, toks.get(j + 2))) {
                    j += 2;
                } else {
                    break;
                }
            }
            for (int a = i; a <= j; a++) {
                if (toks.get(a).connector()) {
                    continue;
                }
                for (int b = a; b <= j && b - a < MAX_NGRAM; b++) {
                    if (toks.get(b).connector()) {
                        continue;
                    }
                    String surface = text.substring(toks.get(a).start, toks.get(b).end);
                    String norm = McpServer.normalizeOrgName(surface);
                    if (norm.length() < 2 || allCommon(toks, a, b)) {
                        continue;
                    }
                    distinct.add(norm);
                    if (distinct.size() > MAX_CANDIDATES) {
                        throw new IllegalArgumentException("the text yields more than "
                            + MAX_CANDIDATES + " distinct candidate names; split it into "
                            + "smaller parts and call again");
                    }
                    out.add(new Candidate(toks.get(a).start, toks.get(b).end, surface, norm,
                        b - a + 1, personKey(toks, a, b)));
                }
            }
            i = j + 1;
        }
        return out;
    }

    private static final Set<String> NAME_SUFFIXES = new HashSet<>(Arrays.asList(
        "jr", "sr", "ii", "iii", "iv", "md", "phd", "esq"));
    private static final Pattern NAME_WORD = Pattern.compile("[\\p{L}][\\p{L}'\u2019\\-]+");

    /** "first last" for a 2-4 token window of plain alphabetic words, or null. Middle names
     *  and initials are dropped because the person registry keeps only a parsed first and last
     *  name; a trailing generational suffix is skipped to reach the real surname. */
    private static String personKey(List<Tok> toks, int a, int b) {
        int n = b - a + 1;
        if (n < 2 || n > 4) {
            return null;
        }
        int lastIdx = b;
        while (lastIdx > a + 1 && NAME_SUFFIXES.contains(
            toks.get(lastIdx).text.toLowerCase().replace(".", ""))) {
            lastIdx--;
        }
        String first = toks.get(a).text;
        String last = toks.get(lastIdx).text;
        if (!NAME_WORD.matcher(first).matches() || !NAME_WORD.matcher(last).matches()
            || COMMON_CAPITALIZED.contains(first.toLowerCase())
            || COMMON_CAPITALIZED.contains(last.toLowerCase())) {
            return null;
        }
        return first.toLowerCase() + " " + last.toLowerCase();
    }

    /** Abbreviations whose trailing period does not end a sentence. */
    private static final Set<String> ABBREVIATIONS = new HashSet<>(Arrays.asList(
        "jr", "sr", "inc", "corp", "co", "ltd", "st", "mr", "mrs", "ms", "dr", "rep", "sen",
        "gov", "gen", "col", "lt", "sgt", "hon", "prof", "u.s", "no"));

    /** Whether {@code b} continues the same name as {@code a}: separated by whitespace only,
     *  or by a period that belongs to an initial or a known abbreviation ("Nancy P. Pelosi",
     *  "Sen. Warren") and so does not end a sentence. */
    private static boolean joins(String text, Tok a, Tok b) {
        String gap = text.substring(a.end, b.start);
        if (gap.trim().isEmpty()) {
            return true;
        }
        String g = gap.trim();
        return ".".equals(g) && (a.text.length() == 1
            || ABBREVIATIONS.contains(a.text.toLowerCase()));
    }

    private static boolean allCommon(List<Tok> toks, int a, int b) {
        for (int k = a; k <= b; k++) {
            if (!COMMON_CAPITALIZED.contains(toks.get(k).text.toLowerCase())
                && !toks.get(k).connector()) {
                return false;
            }
        }
        return true;
    }

    static Set<String> distinctNorms(List<Candidate> cs) {
        Set<String> s = new LinkedHashSet<>();
        for (Candidate c : cs) {
            s.add(c.norm);
        }
        return s;
    }

    /** Ticker symbols written the way news copy writes them: "(NASDAQ: AAPL)", "NYSE: IBM",
     *  "$TSLA". A bare capitalized word is never treated as a ticker. */
    static Set<String> tickers(String text) {
        Set<String> out = new LinkedHashSet<>();
        Matcher m = TICKER.matcher(text);
        while (m.find()) {
            for (int g = 1; g <= 3; g++) {
                if (m.group(g) != null) {
                    out.add(m.group(g));
                }
            }
        }
        if (out.size() > MAX_TICKERS) {
            throw new IllegalArgumentException("more than " + MAX_TICKERS + " distinct "
                + "tickers in the text; split it and call again");
        }
        return out;
    }

    /**
     * The mentions that stand: longest match first, no two overlapping. "Bank of America
     * Merrill Lynch" beats the "Bank of America" and "Merrill Lynch" inside it when all three
     * are in the registry, and a name inside an already-accepted longer one is not counted
     * separately.
     */
    static List<Candidate> resolve(List<Candidate> all, Set<String> matchedNorms) {
        return resolve(all, c -> matchedNorms.contains(c.norm));
    }

    static List<Candidate> resolve(List<Candidate> all,
            java.util.function.Predicate<Candidate> matched) {
        List<Candidate> hits = new ArrayList<>();
        for (Candidate c : all) {
            if (matched.test(c)) {
                hits.add(c);
            }
        }
        hits.sort(Comparator.<Candidate>comparingInt(c -> -c.tokens)
            .thenComparingInt(c -> c.start));
        List<Candidate> accepted = new ArrayList<>();
        for (Candidate c : hits) {
            boolean overlaps = false;
            for (Candidate a : accepted) {
                if (c.start < a.end && a.start < c.end) {
                    overlaps = true;
                    break;
                }
            }
            if (!overlaps) {
                accepted.add(c);
            }
        }
        accepted.sort(Comparator.comparingInt(c -> c.start));
        return accepted;
    }

    /** Groups accepted mentions by normalized name, preserving first-appearance order. */
    static Map<String, List<Candidate>> groupByNorm(List<Candidate> accepted) {
        Map<String, List<Candidate>> m = new LinkedHashMap<>();
        for (Candidate c : accepted) {
            m.computeIfAbsent(c.norm, k -> new ArrayList<>()).add(c);
        }
        return m;
    }

    /** Sentence boundary offsets: [start, end) pairs covering the text. */
    static List<int[]> sentenceSpans(String text) {
        List<int[]> spans = new ArrayList<>();
        Matcher m = Pattern.compile("(?<=[.!?])\\s+|\\n+").matcher(text);
        int start = 0;
        while (m.find()) {
            if (m.start() > start) {
                spans.add(new int[]{start, m.start()});
            }
            start = m.end();
        }
        if (start < text.length()) {
            spans.add(new int[]{start, text.length()});
        }
        return spans;
    }

    /**
     * Later bare-surname mentions of people already identified by full name ("Pelosi said"
     * after "Nancy Pelosi"). {@code lastNameToGroup} maps a lowercase surname to the group key
     * of the ONE accepted person who has it; a surname shared by two accepted people is left
     * out by the caller, since guessing which one is exactly the error this must not make.
     * Only single-token spans not overlapping an accepted mention qualify.
     */
    static Map<String, List<Candidate>> surnameMentions(List<Candidate> all,
            List<Candidate> accepted, Map<String, String> lastNameToGroup) {
        Map<String, List<Candidate>> out = new LinkedHashMap<>();
        for (Candidate c : all) {
            if (c.tokens != 1) {
                continue;
            }
            String group = lastNameToGroup.get(c.surface.toLowerCase());
            if (group == null) {
                continue;
            }
            boolean overlaps = false;
            for (Candidate a : accepted) {
                if (c.start < a.end && a.start < c.end) {
                    overlaps = true;
                    break;
                }
            }
            if (!overlaps) {
                out.computeIfAbsent(group, k -> new ArrayList<>()).add(c);
            }
        }
        return out;
    }
}
