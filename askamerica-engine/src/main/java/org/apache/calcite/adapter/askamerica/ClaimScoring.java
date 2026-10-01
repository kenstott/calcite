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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Who a validation's claims belong to, and what they say about that party's honesty and bias.
 *
 * <p>Every claim is sorted into a block: the piece's author, one block per speaker the piece
 * covers, or the source audit ({@code fidelity}: a source the piece cites, relayed). Only the
 * author and the speakers are scored. Relaying a source accurately is the floor and earns
 * nothing, so a piece cannot look better by citing sources its own assertions contradict or
 * stretch ("citejacking"): each scored claim names the claims it rests on and how well that
 * evidence carries it, and evidence that does not carry it costs the claim its credit.
 *
 * <p>Both scores are computed here from the graded claims, never chosen by the caller, so a
 * score cannot disagree with the verdicts under it. Honesty is how much of what a party
 * asserted held up; bias is which way that party's errors lean.
 */
final class ClaimScoring {
    static final String GROUP_FIDELITY = "fidelity";
    static final String GROUP_AUTHOR = "author_claims";
    static final String GROUP_SUBJECT = "subject_claims";

    static final String SUPPORT_OVERREACH = "overreach";
    static final String SUPPORT_CONTRADICTED = "contradicted";
    static final List<String> SUPPORTS = Arrays.asList("supported", SUPPORT_OVERREACH,
        SUPPORT_CONTRADICTED, "decorative");

    static final String ERRS_TOWARD = "toward_thesis";
    static final String ERRS_AGAINST = "against_thesis";
    static final List<String> ERRS = Arrays.asList(ERRS_TOWARD, ERRS_AGAINST, "neutral");

    /** Fewer graded claims, or fewer errors, than this is a number without a characterization:
     *  one or two claims are not grounds for calling anyone dishonest or biased. */
    static final int MIN_FOR_LABEL = 3;

    static final String KIND_CAUSAL = "causal";
    static final String KIND_ATTACK = "attack";
    static final List<String> KINDS = Arrays.asList("fact", KIND_CAUSAL, KIND_ATTACK);

    /** Asserted as fact with no evidence offered and none found: an insult, an appeal to what
     *  "lots of people think" or "everyone knows". Unlike a claim that cannot be checked here,
     *  it is graded: a piece does not look honest by asserting what nobody can show. */
    static final String VERDICT_UNSUPPORTED = "unsupported";

    /** A party's honesty cannot exceed their central claim's own credit by more than this many
     *  points: accurate detail does not redeem a case whose main assertion failed. */
    static final int CENTRAL_CAP_MARGIN = 25;

    /** Wording that joins a fact to a cause. A sentence carrying it asserts the cause, and is
     *  graded on the cause: "snow falls because planes drop it" is not half right. */
    static final Pattern CAUSAL_WORDING = Pattern.compile(
        "\\b(because|due to|caused by|as a result of|thanks to|owing to|driven by|led to|"
        + "leads to|resulted in|results in|is why|responsible for|blamed? (?:on|for))\\b",
        Pattern.CASE_INSENSITIVE);

    private static final String SUBJECT_PREFIX = GROUP_SUBJECT + ":";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ClaimScoring() {
    }

    /** The claim's {@code group}, lower-cased; empty when the claim carries none. */
    static String group(JsonNode c) {
        return c.path("group").asText("").trim().toLowerCase(Locale.ROOT);
    }

    static String speaker(JsonNode c) {
        return c.path("speaker").asText("").trim();
    }

    /** The block a claim renders and scores in: {@link #GROUP_AUTHOR}, {@code
     *  subject_claims:<speaker>}, {@link #GROUP_FIDELITY}, or empty for a claim with no
     *  recognized group (a single-claim validation). */
    static String block(JsonNode c) {
        String g = group(c);
        if (GROUP_SUBJECT.equals(g)) {
            return SUBJECT_PREFIX + speaker(c);
        }
        return GROUP_AUTHOR.equals(g) || GROUP_FIDELITY.equals(g) ? g : "";
    }

    /** The blocks holding at least one claim, in reading order: the author, each speaker in
     *  order of first appearance, the source audit, then ungrouped claims. */
    static List<String> blocks(JsonNode claims) {
        List<String> speakers = new ArrayList<>();
        boolean hasAuthor = false;
        boolean hasFidelity = false;
        boolean hasUngrouped = false;
        for (JsonNode c : claims) {
            String b = block(c);
            if (GROUP_AUTHOR.equals(b)) {
                hasAuthor = true;
            } else if (GROUP_FIDELITY.equals(b)) {
                hasFidelity = true;
            } else if (b.isEmpty()) {
                hasUngrouped = true;
            } else if (!speakers.contains(b)) {
                speakers.add(b);
            }
        }
        List<String> out = new ArrayList<>();
        if (hasAuthor) {
            out.add(GROUP_AUTHOR);
        }
        out.addAll(speakers);
        if (hasFidelity) {
            out.add(GROUP_FIDELITY);
        }
        if (hasUngrouped) {
            out.add("");
        }
        return out;
    }

    static boolean isScored(String block) {
        return GROUP_AUTHOR.equals(block) || block.startsWith(SUBJECT_PREFIX);
    }

    /** The speaker a subject block belongs to; null for any other block. */
    static String blockSpeaker(String block) {
        return block.startsWith(SUBJECT_PREFIX) ? block.substring(SUBJECT_PREFIX.length()) : null;
    }

    /** Heading for one block: whose claims they are and what a verdict in it grades. */
    static String label(String block) {
        if (GROUP_AUTHOR.equals(block)) {
            return "Author's Claims — what the author asserts in their own voice";
        }
        if (GROUP_FIDELITY.equals(block)) {
            return "Source Audit — did the piece represent the sources it cites accurately "
                + "(not scored)";
        }
        String speaker = blockSpeaker(block);
        if (speaker == null) {
            return "";
        }
        return "Claims by " + (speaker.isEmpty() ? "an unnamed speaker" : speaker)
            + " — what they assert, as the piece reports it";
    }

    private static String verdict(JsonNode c) {
        return c.path("verdict").asText("").trim().toLowerCase(Locale.ROOT);
    }

    static boolean isCausal(JsonNode c) {
        return KIND_CAUSAL.equals(c.path("kind").asText("").trim().toLowerCase(Locale.ROOT));
    }

    /** A personal attack: an unsupported claim like any other, and counted on its own line. */
    static boolean isAttack(JsonNode c) {
        return KIND_ATTACK.equals(c.path("kind").asText("").trim().toLowerCase(Locale.ROOT));
    }

    /** True for the one claim a party's case depends on. */
    static boolean isCentral(JsonNode c) {
        return c.path("central").asBoolean(false);
    }

    /**
     * The 1-based numbers of a block's claims in the order they are shown. A scored block
     * leads with the party's central claim, then the claims that held up least, so the
     * assertion that matters is not buried under the accurate ones around it; claims that
     * could not be graded come last. The source audit keeps the order given.
     */
    static List<Integer> order(JsonNode claims, String block) {
        List<Integer> out = new ArrayList<>();
        int n = 0;
        for (JsonNode c : claims) {
            n++;
            if (block.equals(block(c))) {
                out.add(Integer.valueOf(n));
            }
        }
        if (isScored(block)) {
            out.sort((a, b) -> Double.compare(rank(claims.get(a.intValue() - 1)),
                rank(claims.get(b.intValue() - 1))));
        }
        return out;
    }

    private static double rank(JsonNode c) {
        if (isCentral(c)) {
            return -1;
        }
        double credit = credit(c);
        return credit < 0 ? 2 : credit;
    }

    static String support(JsonNode c) {
        return c.path("support").asText("").trim().toLowerCase(Locale.ROOT);
    }

    private static boolean restsOnSomething(JsonNode c) {
        return c.path("rests_on").isArray() && c.path("rests_on").size() > 0;
    }

    /** True when the claim leans on cited evidence that does not carry it. */
    static boolean isCitejacked(JsonNode c) {
        String s = support(c);
        return restsOnSomething(c)
            && (SUPPORT_OVERREACH.equals(s) || SUPPORT_CONTRADICTED.equals(s));
    }

    /**
     * How much of the claim held up, 0 to 1, or -1 when it could not be graded (not checkable
     * here, stale vintage). Evidence that is narrower than the claim caps it at half credit;
     * evidence that contradicts the claim leaves it none, whatever its verdict.
     */
    static double credit(JsonNode c) {
        double base;
        switch (verdict(c)) {
        case "true":
            base = 1.0;
            break;
        case "mostly true":
            base = 0.75;
            break;
        case "partially false":
            base = 0.5;
            break;
        case "mostly false":
        case VERDICT_UNSUPPORTED:
            base = 0.25;
            break;
        case "false":
            base = 0.0;
            break;
        default:
            return -1;
        }
        if (isCitejacked(c)) {
            return SUPPORT_CONTRADICTED.equals(support(c)) ? 0.0 : Math.min(base, 0.5);
        }
        return base;
    }

    /**
     * One scored block's result: how many claims were graded and how many could not be, the
     * honesty score (0-100, the mean credit of the graded claims, capped by the central
     * claim) with its characterization, the bias score (-100 to 100, the net share of the
     * error that favours the party's own case) with its characterization, how many of the
     * claims are personal attacks, and the numbers of the citejacked claims.
     */
    static ObjectNode score(JsonNode claims, String block) {
        int graded = 0;
        int excluded = 0;
        int errors = 0;
        double creditSum = 0;
        double errorWeight = 0;
        double lean = 0;
        int attacks = 0;
        int central = 0;
        double centralCredit = -1;
        ArrayNode citejacked = MAPPER.createArrayNode();
        int n = 0;
        for (JsonNode c : claims) {
            n++;
            if (!block.equals(block(c))) {
                continue;
            }
            if (isAttack(c)) {
                attacks++;
            }
            if (isCitejacked(c)) {
                citejacked.add(n);
            }
            double credit = credit(c);
            if (isCentral(c) && central == 0) {
                central = n;
                centralCredit = credit;
            }
            if (credit < 0) {
                excluded++;
                continue;
            }
            graded++;
            creditSum += credit;
            if (credit < 1) {
                errors++;
                double w = 1 - credit;
                errorWeight += w;
                String errs = c.path("errs").asText("").trim().toLowerCase(Locale.ROOT);
                lean += ERRS_TOWARD.equals(errs) ? w : ERRS_AGAINST.equals(errs) ? -w : 0;
            }
        }
        ObjectNode out = MAPPER.createObjectNode();
        out.put("graded", graded);
        out.put("excluded", excluded);
        out.put("attacks", attacks);
        if (central > 0) {
            out.put("central", central);
        }
        if (graded > 0) {
            int honesty = (int) Math.round(100 * creditSum / graded);
            if (centralCredit >= 0) {
                int cap = (int) Math.round(100 * centralCredit) + CENTRAL_CAP_MARGIN;
                if (honesty > cap) {
                    out.put("honesty_before_cap", honesty);
                    honesty = cap;
                }
            }
            out.put("honesty_score", honesty);
            out.put("honesty", graded < MIN_FOR_LABEL
                ? "too few checkable claims to characterize" : honestyLabel(honesty));
        } else {
            out.put("honesty", "no checkable claims");
        }
        out.put("errors", errors);
        if (errors > 0) {
            int bias = (int) Math.round(100 * lean / errorWeight);
            out.put("bias_score", bias);
            out.put("bias", errors < MIN_FOR_LABEL
                ? "too few errors to characterize" : biasLabel(bias));
        } else {
            out.put("bias", "no errors to lean either way");
        }
        out.set("citejacked", citejacked);
        return out;
    }

    static String honestyLabel(int score) {
        return score >= 90 ? "honest"
            : score >= 75 ? "mostly honest"
            : score >= 50 ? "mixed"
            : score >= 25 ? "dishonest" : "very dishonest";
    }

    static String biasLabel(int score) {
        return score >= 75 ? "very biased"
            : score >= 50 ? "biased"
            : score >= 25 ? "somewhat biased"
            : score > -25 ? "no consistent lean" : "errs against their own case";
    }

    /**
     * Refuses a validation of two or more claims whose claims are not sorted and linked well
     * enough to score: every claim in a group, every subject claim with its speaker, every
     * scored claim naming the claims it rests on, how well that evidence carries it and what
     * the evidence itself found, every claim that fell short saying which way it errs, and
     * the author and each speaker with exactly one central claim. Returns null when the claims
     * can be scored.
     */
    static String enforce(JsonNode claims) {
        if (claims.size() < 2) {
            return null;
        }
        List<Integer> ungrouped = new ArrayList<>();
        List<String> problems = new ArrayList<>();
        java.util.Map<String, Integer> centrals = new java.util.LinkedHashMap<>();
        int n = 0;
        for (JsonNode c : claims) {
            n++;
            String g = group(c);
            if (GROUP_FIDELITY.equals(g)) {
                continue;
            }
            if (!GROUP_AUTHOR.equals(g) && !GROUP_SUBJECT.equals(g)) {
                ungrouped.add(Integer.valueOf(n));
                continue;
            }
            if (GROUP_SUBJECT.equals(g) && speaker(c).isEmpty()) {
                problems.add("claim " + n + " is grouped `" + GROUP_SUBJECT + "` with no "
                    + "`speaker`: name the person or organization who made the assertion");
            }
            String kind = c.path("kind").asText("").trim().toLowerCase(Locale.ROOT);
            if (!kind.isEmpty() && !KINDS.contains(kind)) {
                problems.add("claim " + n + " has `kind` '" + kind + "': it is "
                    + String.join(" | ", KINDS));
            }
            String b = block(c);
            centrals.put(b, Integer.valueOf((centrals.containsKey(b)
                ? centrals.get(b).intValue() : 0) + (isCentral(c) ? 1 : 0)));
            if (isAttack(c)) {
                if (!VERDICT_UNSUPPORTED.equals(verdict(c))) {
                    problems.add("claim " + n + " is `kind`: \"" + KIND_ATTACK + "\" with "
                        + "verdict '" + verdict(c) + "': a personal attack offers no evidence, "
                        + "so its verdict is '" + VERDICT_UNSUPPORTED + "'. A factual assertion "
                        + "inside it is a separate claim with its own verdict");
                }
                if (isCentral(c)) {
                    problems.add("claim " + n + " is a personal attack marked `central`: the "
                        + "central claim is the factual or causal assertion the case depends "
                        + "on");
                }
            }
            Matcher causal = CAUSAL_WORDING.matcher(c.path("assertion").asText(""));
            if (causal.find() && !isCausal(c)) {
                problems.add("claim " + n + " asserts a cause ('" + causal.group()
                    + "') but is not `kind`: \"" + KIND_CAUSAL + "\". Split the sentence: the "
                    + "fact is one claim, the cause is a second claim with `kind`: \""
                    + KIND_CAUSAL + "\" that rests on it. The causal claim's verdict grades the "
                    + "evidence for the cause alone; the fact being true does not raise it");
            }
            JsonNode restsOn = c.path("rests_on");
            if (!restsOn.isArray()) {
                problems.add("claim " + n + " carries no `rests_on`: list the numbers of the "
                    + "claims it offers as its evidence, or [] when it offers none");
            } else {
                for (JsonNode r : restsOn) {
                    int ref = r.asInt(0);
                    if (!r.isInt() || ref < 1 || ref > claims.size() || ref == n) {
                        problems.add("claim " + n + " has `rests_on` entry " + r + ": each "
                            + "entry is the number (1-" + claims.size() + ") of another claim");
                    }
                }
                if (restsOn.size() > 0) {
                    if (!SUPPORTS.contains(support(c))) {
                        problems.add("claim " + n + " rests on cited evidence but carries no "
                            + "valid `support` (" + String.join(" | ", SUPPORTS) + ")");
                    }
                    if (c.path("source_finding").asText("").trim().isEmpty()) {
                        problems.add("claim " + n + " rests on cited evidence but carries no "
                            + "`source_finding`: state what that evidence itself found, from "
                            + "the source, not from the piece's description of it");
                    }
                }
            }
            double credit = credit(c);
            if (credit >= 0 && credit < 1
                    && !ERRS.contains(c.path("errs").asText("").trim().toLowerCase(Locale.ROOT))) {
                problems.add("claim " + n + " fell short of true and supported but carries no "
                    + "valid `errs` (" + String.join(" | ", ERRS) + ")");
            }
        }
        for (java.util.Map.Entry<String, Integer> e : centrals.entrySet()) {
            if (e.getValue().intValue() != 1) {
                String speaker = blockSpeaker(e.getKey());
                problems.add((speaker == null ? "the author's claims" : "the claims by " + speaker)
                    + " have " + e.getValue() + " claims marked `central`: mark exactly one "
                    + "with `central`: true — the assertion that party's case depends on, the "
                    + "one a reader would repeat");
            }
        }
        if (!ungrouped.isEmpty()) {
            problems.add(0, "claim(s) " + ungrouped + " carry no valid `group`: `"
                + GROUP_FIDELITY + "` when the assertion relays a study, release, report, "
                + "official figure or other source cited as evidence, `" + GROUP_AUTHOR
                + "` when the piece's author asserts it in their own voice, `" + GROUP_SUBJECT
                + "` (with `speaker`) when a person or organization the piece covers asserts it");
        }
        if (problems.isEmpty()) {
            return null;
        }
        return "validation refused: " + String.join(" ALSO: ", problems) + ".";
    }
}
