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
        case "partially true":
            base = 0.5;
            break;
        case "mostly false":
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
     * honesty score (0-100, the mean credit of the graded claims) with its characterization,
     * the bias score (-100 to 100, the net share of the error that favours the party's own
     * case) with its characterization, and the numbers of the citejacked claims.
     */
    static ObjectNode score(JsonNode claims, String block) {
        int graded = 0;
        int excluded = 0;
        int errors = 0;
        double creditSum = 0;
        double errorWeight = 0;
        double lean = 0;
        ArrayNode citejacked = MAPPER.createArrayNode();
        int n = 0;
        for (JsonNode c : claims) {
            n++;
            if (!block.equals(block(c))) {
                continue;
            }
            if (isCitejacked(c)) {
                citejacked.add(n);
            }
            double credit = credit(c);
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
        if (graded > 0) {
            int honesty = (int) Math.round(100 * creditSum / graded);
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
     * the evidence itself found, and every claim that fell short saying which way it errs.
     * Returns null when the claims can be scored.
     */
    static String enforce(JsonNode claims) {
        if (claims.size() < 2) {
            return null;
        }
        List<Integer> ungrouped = new ArrayList<>();
        List<String> problems = new ArrayList<>();
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
