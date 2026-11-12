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
    /** The headline's block when the author's own claims are too few to rate alone: the
     *  author's claims together with the fidelity claims. Speakers' claims never join it. */
    static final String BLOCK_AUTHOR_WITH_FIDELITY = "author_claims+fidelity";
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
    static final String KIND_OPINION = "opinion";
    static final List<String> KINDS = Arrays.asList("fact", KIND_CAUSAL, KIND_ATTACK, KIND_OPINION);

    /** Asserted as fact with no evidence offered and none found: an insult, an appeal to what
     *  "lots of people think" or "everyone knows". Unlike a claim that cannot be checked here,
     *  it is graded: a piece does not look honest by asserting what nobody can show. */
    static final String VERDICT_UNSUPPORTED = "unsupported";
    static final String VERDICT_NOT_CHECKABLE = "not checkable here";

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

    /** A personal attack: not checkable, so excluded from the score, and counted on its own line. */
    static boolean isAttack(JsonNode c) {
        return KIND_ATTACK.equals(c.path("kind").asText("").trim().toLowerCase(Locale.ROOT));
    }

    /** True for an opinion or belief on a personal basis that no evidence can test. */
    static boolean isOpinion(JsonNode c) {
        return KIND_OPINION.equals(c.path("kind").asText("").trim().toLowerCase(Locale.ROOT));
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
     * honesty score (0-100, the mean credit of the claims checked against evidence, capped by
     * the central claim; an 'unsupported' claim was never shown false, so it is left out of
     * honesty, accuracy and bias and counted beside them) with its characterization, the bias score (-100 to 100, the net share of the
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
        int checked = 0;
        int unsupported = 0;
        int inaccurate = 0;
        double checkedSum = 0;
        ArrayNode citejacked = MAPPER.createArrayNode();
        int n = 0;
        for (JsonNode c : claims) {
            n++;
            if (!inBlock(c, block)) {
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
            boolean isUnsupported = VERDICT_UNSUPPORTED.equals(verdict(c));
            if (isUnsupported) {
                unsupported++;
            } else {
                checked++;
                if (credit <= 0.5) {
                    inaccurate++;
                }
                checkedSum += credit;
                creditSum += credit;
            }
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
        boolean centralUnsupported = central > 0
            && VERDICT_UNSUPPORTED.equals(verdict(claims.get(central - 1)));
        if (checked > 0) {
            int honesty = (int) Math.round(100 * creditSum / checked);
            if (centralCredit >= 0 && !centralUnsupported) {
                int cap = (int) Math.round(100 * centralCredit) + CENTRAL_CAP_MARGIN;
                if (honesty > cap) {
                    out.put("honesty_before_cap", honesty);
                    honesty = cap;
                }
            }
            out.put("honesty_score", honesty);
            out.put("honesty", checked < MIN_FOR_LABEL
                ? "too few checkable claims to characterize" : honestyLabel(honesty));
        } else {
            out.put("honesty", "no checkable claims");
        }
        out.put("checked", checked);
        out.put("unsupported", unsupported);
        out.put("inaccurate", inaccurate);
        if (checked > 0) {
            int accuracy = (int) Math.round(100 * checkedSum / checked);
            if (centralCredit >= 0 && !centralUnsupported) {
                // Accuracy, and so the Pinocchio rating, is never better than the central
                // claim's own grade: true secondary detail does not dilute a failed headline.
                accuracy = Math.min(accuracy, (int) Math.round(100 * centralCredit));
            }
            out.put("accuracy_score", accuracy);
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

    /**
     * The headline's verdict on a block's claims — "are very biased and mostly false", "are
     * balanced and accurate" — from that block's scores. Bias is left out when the errors are
     * too few to show a lean. Accuracy is judged on the claims checked against evidence; an
     * 'unsupported' claim was never shown false, so it is counted beside the verdict instead.
     */
    static String verdictClause(JsonNode score) {
        if (score.path("checked").asInt() < MIN_FOR_LABEL) {
            return "are too few to rate" + unsupportedNote(score);
        }
        int checked = score.path("accuracy_score").asInt();
        boolean inaccurate = score.path("inaccurate").asInt() > 0;
        String accuracy = checked >= 90 && !inaccurate ? "accurate"
            : checked >= 75 ? "mostly accurate"
            : checked >= 50 ? "partly false"
            : checked >= 25 ? "mostly false" : "false";
        int errors = score.path("errors").asInt();
        String clause;
        if (errors == 0) {
            clause = "are balanced and " + accuracy;
        } else if (errors < MIN_FOR_LABEL) {
            clause = "are " + accuracy;
        } else {
            int bias = score.path("bias_score").asInt();
            clause = bias <= -25 ? "are " + accuracy + " and cut against its own case"
                : "are " + (bias >= 75 ? "very biased" : bias >= 50 ? "biased"
                    : bias >= 25 ? "somewhat biased" : "balanced") + " and " + accuracy;
        }
        return clause + unsupportedNote(score);
    }

    /**
     * The Washington Post Fact Checker rating the headline's block earns, 0-4, from the same
     * accuracy score as the headline's verdict: accurate 0, mostly accurate 1, partly false 2,
     * mostly false 3, false 4. A block with any checked claim graded partially false or worse
     * is never rated 0: 0 means no inaccuracy at all. -1 when no single party speaks for the piece or its checked
     * claims are too few to rate.
     */
    static int pinocchios(JsonNode claims) {
        String block = headlineBlock(claims);
        if (block == null) {
            return -1;
        }
        JsonNode score = score(claims, block);
        if (score.path("checked").asInt() < MIN_FOR_LABEL) {
            return -1;
        }
        int a = score.path("accuracy_score").asInt();
        boolean inaccurate = score.path("inaccurate").asInt() > 0;
        return a >= 90 && !inaccurate ? 0 : a >= 75 ? 1 : a >= 50 ? 2 : a >= 25 ? 3 : 4;
    }

    private static final java.util.regex.Pattern PINOCCHIO_RATING =
        java.util.regex.Pattern.compile(
            "(?i)(?<![-\u2013\\d])\\b([0-4]|zero|one|two|three|four)\\s+pinocchios?\\b");

    /**
     * A delivered verdict MUST state the engine's rating and no other; null when it does or
     * the engine gave none.
     */
    static String pinocchioProblem(String markdown, int engine) {
        if (engine < 0) {
            return null;
        }
        String want = engine + (engine == 1 ? " Pinocchio" : " Pinocchios");
        java.util.regex.Matcher m = PINOCCHIO_RATING.matcher(markdown == null ? "" : markdown);
        boolean stated = false;
        java.util.List<String> words = Arrays.asList("zero", "one", "two", "three", "four");
        while (m.find()) {
            String g = m.group(1).toLowerCase(Locale.ROOT);
            int n = words.contains(g) ? words.indexOf(g) : Integer.parseInt(g);
            if (n != engine) {
                return "deliver_report refused: the verdict states \"" + m.group()
                    + "\", and the engine's rating from the claim scores is " + want
                    + ". The rating MUST be " + want + ".";
            }
            stated = true;
        }
        return stated ? null
            : "deliver_report refused: the verdict MUST state the engine's rating, " + want + ".";
    }

    private static String unsupportedNote(JsonNode score) {
        int unsupported = score.path("unsupported").asInt();
        return unsupported == 0 ? ""
            : "; " + unsupported + (unsupported == 1 ? " claim" : " claims")
                + " could not be confirmed";
    }

    /** Whether claim {@code c} is scored in {@code block}. */
    static boolean inBlock(JsonNode c, String block) {
        String b = block(c);
        if (BLOCK_AUTHOR_WITH_FIDELITY.equals(block)) {
            return GROUP_AUTHOR.equals(b) || GROUP_FIDELITY.equals(b);
        }
        return block.equals(b);
    }

    /** The block a validation's headline speaks for: the author's claims (joined by the
     *  fidelity claims when the author's alone are too few to rate), or the only speaker's
     *  when the piece has no author's voice; null when several speakers share it. */
    static String headlineBlock(JsonNode claims) {
        List<String> all = blocks(claims);
        List<String> scored = new ArrayList<>();
        for (String b : all) {
            if (isScored(b)) {
                scored.add(b);
            }
        }
        if (!scored.contains(GROUP_AUTHOR)) {
            return scored.size() == 1 ? scored.get(0) : null;
        }
        boolean tooFew = score(claims, GROUP_AUTHOR).path("checked").asInt() < MIN_FOR_LABEL;
        return tooFew && all.contains(GROUP_FIDELITY) ? BLOCK_AUTHOR_WITH_FIDELITY : GROUP_AUTHOR;
    }

    /**
     * A validation's headline: the subject the caller named, completed with the verdict its
     * scores give — "The Ledger article's claims about rents and wages are very biased and
     * mostly false." The subject is returned as given when no single party speaks for the
     * piece.
     */
    static String headline(String subject, JsonNode claims) {
        String block = headlineBlock(claims);
        String s = subject.trim().replaceAll("[.!?\\s]+$", "");
        if (block == null) {
            return s;
        }
        String clause = verdictClause(score(claims, block));
        return s.endsWith(clause) ? s + "." : s + " " + clause + ".";
    }

    /** A validation's {@code title} must be the headline's subject; null when it is. */
    static String headlineProblem(String title) {
        if (title != null && HEADLINE_SUBJECT.matcher(title).find()) {
            return null;
        }
        return "validation refused: for a validation `title` is the subject of the headline "
            + "only — \"The <publication> article's claims about <a>, <b> and <c>\" — naming the "
            + "publication and the two or three topics its claims are about, with no verdict. "
            + "The engine completes the sentence from the scores. Rewrite `title` in that form.";
    }

    private static final java.util.regex.Pattern HEADLINE_SUBJECT =
        java.util.regex.Pattern.compile("(?i)\\bclaims\\b");

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
    /** Words that describe how firm a figure is or how far it was checked, not how it differs
     *  from the evidence. */
    private static final java.util.regex.Pattern HEDGE = java.util.regex.Pattern.compile(
        "estimate|projection|projected|preliminary|forward[- ]looking|on track|on pace"
        + "|primary|secondary|confirm|verif|could not|cannot|unable|not (in|found in) the "
        + "warehouse|not in warehouse|not tested|untested|plausib|consistent with|relayed"
        + "|via |attributed|unnamed|characteri[sz]ation|wording|not a record|not official"
        + "|not (directly )?measured|expert view|opinion"
        + "|not yet final|revis|subject to change",
        java.util.regex.Pattern.CASE_INSENSITIVE);

    /** Words that say the claim and the evidence differ. */
    private static final java.util.regex.Pattern DIFFERENCE = java.util.regex.Pattern.compile(
        "differ|rather than|instead of|overstat|understat|exaggerat|inflat|higher than"
        + "|lower than|more than|less than|incorrect|wrong|mislabel|omits?|, not [-$0-9'\"]|\\bvs\\.? "
        + "|\\bbut (the evidence|it|the data|nhtsa|the source)",
        java.util.regex.Pattern.CASE_INSENSITIVE);

    /** An outright statement that nothing in the claim is wrong. */
    private static final java.util.regex.Pattern NO_ERROR = java.util.regex.Pattern.compile(
        "not (an? )?(error|mistake)|no (error|discrepancy|difference)|not wrong|matches",
        java.util.regex.Pattern.CASE_INSENSITIVE);

    /**
     * A graded verdict below 'true' MUST name what in the claim differs from the evidence, in
     * `inaccuracy`. A hedge about the figure's firmness or how far it was checked is not an
     * inaccuracy: such a claim is 'true' when the evidence matches it and 'unsupported' when
     * no evidence was found.
     */
    static String inaccuracyProblem(JsonNode c, int n) {
        double credit = credit(c);
        if (credit < 0 || credit >= 1 || VERDICT_UNSUPPORTED.equals(verdict(c))) {
            return null;
        }
        String inaccuracy = c.path("inaccuracy").asText("").trim();
        String fix = "If nothing in the claim differs from the evidence, the verdict MUST be "
            + "'true'; if no evidence was found, it MUST be 'unsupported'";
        if (inaccuracy.isEmpty()) {
            return "claim " + n + " is graded '" + verdict(c) + "' with no `inaccuracy`: it "
                + "MUST state what in the claim differs from the evidence. " + fix;
        }
        boolean noError = NO_ERROR.matcher(inaccuracy).find();
        // A cause no evidence establishes is graded false or mostly false, and saying so is its
        // inaccuracy; a cause graded mostly right on that ground is a hedge like any other.
        boolean causalFalse = isCausal(c) && credit < 0.5;
        boolean hedgeOnly = !causalFalse && HEDGE.matcher(inaccuracy).find()
            && !DIFFERENCE.matcher(inaccuracy).find();
        if (noError || hedgeOnly) {
            return "claim " + n + " is graded '" + verdict(c) + "' and its `inaccuracy` (\""
                + inaccuracy + "\") names no difference between the claim and the evidence. "
                + "`inaccuracy` MUST say what the claim gets wrong (\"the claim says X; the "
                + "evidence says Y\"). An estimate, projection, preliminary, revised or "
                + "secondary-source figure, or one the warehouse does not hold, MUST NOT be "
                + "graded below 'true' on that ground. " + fix;
        }
        return null;
    }

    private static void addIfPresent(List<String> problems, String p) {
        if (p != null) {
            problems.add(p);
        }
    }

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
                if (isCentral(c)) {
                    problems.add("claim " + n + " is grouped `" + GROUP_FIDELITY + "` and marked "
                        + "`central`: the source audit is not scored, so a central claim there "
                        + "counts for nothing. Remove `central` from it. When the piece repeats "
                        + "the figure as its own assertion, that is a separate `" + GROUP_AUTHOR
                        + "` claim, and it carries `central`");
                }
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
            if (isOpinion(c) && !VERDICT_NOT_CHECKABLE.equals(verdict(c))) {
                problems.add("claim " + n + " is `kind`: \"" + KIND_OPINION + "\" with verdict '"
                    + verdict(c) + "': an opinion or belief no evidence can test is graded '"
                    + VERDICT_NOT_CHECKABLE + "'. A factual assertion inside it is a separate "
                    + "claim with its own verdict");
            }
            if (isAttack(c)) {
                if (!VERDICT_NOT_CHECKABLE.equals(verdict(c))) {
                    problems.add("claim " + n + " is `kind`: \"" + KIND_ATTACK + "\" with "
                        + "verdict '" + verdict(c) + "': no evidence can test a personal attack, "
                        + "so its verdict is '" + VERDICT_NOT_CHECKABLE + "'. A factual assertion "
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
            addIfPresent(problems, inaccuracyProblem(c, n));
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
        if (centrals.isEmpty() && ungrouped.isEmpty()) {
            problems.add("every claim is grouped `" + GROUP_FIDELITY + "`, so nothing is scored "
                + "and no claim is central. The piece's own assertions, including a figure it "
                + "repeats from a source as its own, are `" + GROUP_AUTHOR + "` claims, and a "
                + "person or organization it covers asserting something is a `" + GROUP_SUBJECT
                + "` claim; mark exactly one of each party's claims `central`");
        }
        int fidelity = 0;
        for (JsonNode c : claims) {
            if (GROUP_FIDELITY.equals(group(c))) {
                fidelity++;
            }
        }
        int scored = claims.size() - fidelity - ungrouped.size();
        if (scored > 0 && fidelity > scored) {
            problems.add(fidelity + " claims are grouped `" + GROUP_FIDELITY + "` and only "
                + scored + " are scored. A figure or event the piece reports as fact is the "
                + "piece's own `" + GROUP_AUTHOR + "` claim, graded on the evidence, whether or "
                + "not the piece attributes it (\"fell 2.5% to 16,750, NHTSA said\" is the "
                + "author's claim that deaths fell 2.5% to 16,750). `" + GROUP_FIDELITY + "` "
                + "audits only whether the piece relayed a source it cites faithfully; it is "
                + "never the only place a reported figure is graded");
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
