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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The ambient teaching surfaces: the good-question rubric, the contrastive exemplars
 * carried in tool descriptions, and the parameterized question templates exposed as MCP
 * prompts.
 *
 * <p>This server does not own the conversation — a host model decides what to ask, and
 * the answer's quality is capped by the question's. The only channels the server controls
 * are the ones assembled here (tool descriptions, the {@code initialize} instructions, and
 * the prompts list), so the teaching lives in them rather than in a client-side hint the
 * server cannot deliver.
 *
 * <p>Exemplars are <em>contrastive</em> — a vague question paired with its sharpened
 * rewrite — because the rewrite is where the teaching is. An answer-only sample shows what
 * a good result looks like without showing the move that produced it. At least one exemplar
 * per set is an honest refusal: a question the corpus cannot answer as asked, rewritten into
 * the narrower thing it can answer. Without that, a set of exemplars teaches only that every
 * question has an answer here, which is the failure mode a grounding tool exists to prevent.
 */
final class QuestionGuidance {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private QuestionGuidance() {
    }

    /** A vague question, its sharpened rewrite, and the question shape it illustrates. */
    static final class Exemplar {
        final String shape;
        final String vague;
        final String sharpened;
        final String why;

        Exemplar(String shape, String vague, String sharpened, String why) {
            this.shape = shape;
            this.vague = vague;
            this.sharpened = sharpened;
            this.why = why;
        }
    }

    /**
     * The rubric a well-formed question satisfies. Stated in the server's {@code initialize}
     * instructions as the global backstop, and referenced from the tool descriptions.
     */
    static final String RUBRIC =
        "QUESTION QUALITY — the answer's quality is capped by the question's. A question is "
        + "answerable here when it is: (1) BOUNDED — a specific comparison or quantity, not a "
        + "sweeping verdict; (2) MARGINAL — framed at the margin ('one more dollar: A or B'), "
        + "not the absolute total; (3) OPERATIONALIZED — names a measurable outcome and admits "
        + "it is a proxy (NAEP grade-8 reading, not 'student success'); (4) MINIMALLY AND "
        + "ORTHOGONALLY CONTROLLED — names the one or two structural confounds that matter and "
        + "are not near-duplicates of each other (poverty AND median income against 51 "
        + "observations buys nothing); (5) GRAIN-MATCHED — the unit of analysis fits the data, "
        + "and the question knows when state grain (n=51) is too coarse to answer causally; "
        + "(6) TIME/VINTAGE-ANCHORED — names the window and respects revisions; (7) EFFECT-SIZE "
        + "SEEKING — asks 'how strong, and how sure?' with the confound it is NOT holding fixed "
        + "stated out loud, rather than 'which side is right?'. Meta-test: if you cannot state "
        + "the SHAPE of the answer in advance (a coefficient, a ranked list, a number with an "
        + "interval), it is a topic, not a question — narrow it before querying. Each analytical "
        + "tool's description carries worked vague-to-sharpened rewrites; the prompts list "
        + "carries the same rewrites as fill-in templates.";

    /**
     * Contrastive exemplars spanning the question shapes this corpus supports, including two
     * honest refusals. Order is stable so the rendered description text is deterministic.
     */
    static final List<Exemplar> EXEMPLARS = Collections.unmodifiableList(Arrays.asList(
        new Exemplar(
            "comparison",
            "Does education spending work?",
            "At state grain over 2015-2022, in a two-way fixed-effects panel of NAEP grade-8 "
            + "reading on per-pupil instructional spending and pupil-teacher ratio, what is the "
            + "coefficient on spending and its cluster-robust standard error?",
            "'Work' names no outcome and no comparison. The rewrite picks a measured proxy, a "
            + "grain, a window, and one orthogonal control, and asks for an effect size with its "
            + "uncertainty — so the shape of the answer is statable before it runs."),
        new Exemplar(
            "comparison",
            "Which is better for jobs, solar or gas?",
            "For 2018-2023, regress county employment in NAICS 2211 on state generation capacity "
            + "added, separately for solar and for natural gas — report each slope per MW with a "
            + "95% interval and n counties.",
            "The vague form asks for a winner. The rewrite asks at the margin (per MW), names the "
            + "employment series, and returns two comparable slopes rather than a verdict."),
        new Exemplar(
            "trend",
            "Is crime getting worse?",
            "For FBI UCR agencies reporting in every year 2015-2023 — a balanced panel, so the "
            + "trend is not a reporting-coverage artifact — what is the annual change in violent "
            + "crime per 100k, and over how many agencies?",
            "UCR participation changes year to year, so an unbalanced panel can manufacture a "
            + "trend out of who reported. The rewrite fixes the panel and names n."),
        new Exemplar(
            "ranking",
            "Which states are doing best economically?",
            "Rank the 50 states by real GDP-per-capita growth from 2019 to 2023 (BEA, chained "
            + "2017 dollars), returning the growth value alongside the rank so ties and near-ties "
            + "are visible.",
            "'Best' is a verdict; growth in a stated series is a quantity. Returning the value "
            + "next to the rank keeps a 0.1-point gap from reading as a meaningful ordering."),
        new Exemplar(
            "premise-check",
            "Why did housing permits collapse in 2023?",
            "First, did they? Compare Census new-residential permits nationally for 2022 and 2023 "
            + "and report both counts and the percent change — then ask why, only if the premise "
            + "holds.",
            "A 'why' question smuggles in its premise and gets it confirmed. Checking the premise "
            + "as its own query is the cheapest guard against explaining something that did not "
            + "happen."),
        new Exemplar(
            "effect-size",
            "Are higher vaccination rates linked to lower mortality?",
            "At county grain, regress age-adjusted all-cause mortality on vaccination rate "
            + "controlling for median age and poverty rate; report the coefficient, its 95% "
            + "interval, and n — and state that unmeasured confounding is not addressed by this "
            + "design.",
            "'Linked to' invites a yes/no. The rewrite asks how strong and how sure, names the "
            + "two structural controls, and says out loud what it is not holding fixed."),
        new Exemplar(
            "honest-refusal",
            "Did the 2021 minimum-wage increases cause the rise in unemployment?",
            "This cannot be answered causally at state grain: with about 51 units, a single "
            + "treatment year, and a pandemic recovery running underneath it, no coefficient here "
            + "separates the policy from the recovery. What IS answerable: the unemployment path "
            + "for raising versus non-raising states, 2019-2023, reported as a difference in "
            + "trends and labelled descriptive, not causal.",
            "The honest move is to name the mechanical reason the causal question fails and hand "
            + "back the narrower question the data does support — not to run the regression "
            + "anyway and caveat it afterward."),
        new Exemplar(
            "honest-refusal",
            "What was the poverty rate last year?",
            "Call describe_table on the ACS table first and read its coverage window. If the year "
            + "you want is past the last published year, say the source has not published it yet "
            + "— an empty result outside the window is not a zero — and offer the most recent "
            + "published year instead, or suggest_external_sources for a nowcast.",
            "This snapshot is versioned, not live. Substituting a remembered figure for a year "
            + "the corpus does not carry is the single most damaging failure available here, "
            + "because the answer still looks grounded.")));

    /** Shapes present in {@link #EXEMPLARS}, in first-seen order. */
    static Set<String> exemplarShapes() {
        Set<String> shapes = new LinkedHashSet<>();
        for (Exemplar e : EXEMPLARS) {
            shapes.add(e.shape);
        }
        return shapes;
    }

    /**
     * The cross-reference every analytical tool carries instead of a second copy of the
     * worked exemplars in {@link #USAGE_GUIDE}. A host reads every tool description in the
     * same {@code tools/list} response, so repeating the exemplars in every tool would spend
     * the host's context to say nothing new — and would risk the same truncation this whole
     * design works around (see {@link #USAGE_GUIDE}'s class doc).
     */
    static final String EXEMPLAR_POINTER =
        " ASKING WELL — before choosing a specification, apply the vague-to-sharpened rewrites "
        + "in get_usage_guide_section_6 and _7: name a measured outcome, a grain, a window, and "
        + "the one or two orthogonal confounds that matter; ask for an effect size with its "
        + "uncertainty; and say out loud which confound you are not holding fixed. When the "
        + "data cannot support the question as asked, say so and offer the narrower question "
        + "it can support, rather than estimating anyway and caveating afterward.";

    /**
     * An 8-part usage guide, exposed as its own zero-argument tools ({@code get_usage_guide_
     * section_1} through {@code _8}) rather than folded into any single tool's description or
     * the server's {@code initialize.instructions} banner.
     *
     * <p>Two real, independently confirmed client bugs motivate this design, not a stylistic
     * preference:
     *
     * <ul>
     * <li>{@code initialize.instructions} — though populated correctly on the wire (verified
     * directly against this server's raw JSON-RPC response) — never reaches the model in any
     * Claude client tested (Claude Code subagent, and per public reports, Claude Desktop):
     * see github.com/anthropics/claude-code/issues/23808 ("MCP server instructions from
     * initialize response are not passed to the model"). Content that lives only in
     * {@code instructions} is silently unreachable, not merely under-prioritized.
     * <li>Every tool's {@code description} field IS the one channel that reliably reaches the
     * model (resent on every request), but Claude Code silently truncates it at 2048
     * characters with no error and no visible marker: see
     * github.com/anthropics/claude-code/issues/81268. A single large description (this
     * project's own {@code query}/{@code critique_query} tools once carried the exemplars
     * inline, at ~4900-5000 chars) is mangled mid-sentence past that point, not merely
     * shortened at a clean boundary.
     * </ul>
     *
     * <p>The fix threads both needles: content is split across several small, genuinely
     * distinct tools, each kept safely under 2048 characters, so the whole guide reaches the
     * model intact via the one channel proven to work. Content already covered by another
     * tool's own description (e.g. {@code data_coverage}'s own explanation of declared-vs-
     * actual coverage windows, or each statistics tool's own explanation of itself) is
     * deliberately NOT repeated here — seven sections cover only what no other tool
     * description states.
     */
    static final List<String> USAGE_GUIDE = Collections.unmodifiableList(Arrays.asList(
        "AskAmerica usage guide 1/8 — research-first workflow, part 1. This is guidance no "
        + "individual tool's own description carries.\n\n"
        + "Measured, most recently in a 25-run reaudit: the average answer cites only ~3.5 "
        + "distinct external sources against a ~10 target, and connector callers as a group "
        + "cite fewer sources than callers without this corpus and lose questions to them as "
        + "a result — a warehouse in front of you displaces the reading you'd otherwise do. "
        + "Treat any answer citing fewer than 5 distinct sources on a question with a "
        + "real-world antecedent as a signal you stopped searching too early, not as "
        + "adequate.\n\n"
        + "Read the literature first, and keep reading — not a citation hunt but learning "
        + "the subject well enough to recognize a good answer. Run at least 2-3 searches "
        + "from genuinely different angles before querying: the question directly, the "
        + "methodology/metric it needs, and a search for disagreement. Count DISTINCT "
        + "organizations/authors, not distinct pages. Extract three things, kept separate: "
        + "FACTS (figures + vintages), METHODOLOGY (what design identifies the effect), "
        + "METRICS (how the field operationalises the quantity — the commonest way a clean "
        + "answer answers the wrong question is the wrong metric). Be adversarial toward "
        + "what you read. A broken or blocked table is a reason to lean HARDER on this "
        + "pass, not lighter — a schema defect should never be the reason a delivered "
        + "report ends up thin on sources. (Continued in get_usage_guide_section_2.)",

        "AskAmerica usage guide 2/8 — research-first workflow, part 2 (steps after the "
        + "literature pass in section 1).\n\n"
        + "Bring in the warehouse ADDITIVELY — never just relay what you read. ARBITRATE: "
        + "where studies disagree, let the data settle it. PROVE: recompute a published "
        + "figure — matching validates, missing is a finding. EXTEND: run the design on "
        + "data the authors never saw, or at finer grain. IMPROVE: a better metric/"
        + "denominator/control the original lacked. RESEARCH FURTHER: answer what the "
        + "literature left open. Replace a cited figure with a computed one wherever you "
        + "can.\n\n"
        + "Missing variable? Go back to the literature FOR IT — policy indices, hand-coded "
        + "classifications, crosswalks are routinely published. The nearest column is not "
        + "a substitute; using it quietly answers a different question.\n\n"
        + "Verify before you believe it. Reproduce a known estimate where the window "
        + "allows, confirm with a second independent measure, and ask what would have to "
        + "be true for the result to be an artefact, then test that. Where this corpus "
        + "reaches back far enough, replicate a paper's ORIGINAL window first, then report "
        + "both windows/estimates. ALWAYS check a genuinely new computed finding against "
        + "your reading before presenting it — a result that quietly contradicts "
        + "established research is not itself a finding until you know whether it "
        + "should.\n\n"
        + "Say what you added: which numbers you computed vs. passed through. Say plainly "
        + "when the literature already answers it and this corpus adds nothing — that is "
        + "a finding, not a failure.",

        "AskAmerica usage guide 3/8 — query mechanics this warehouse needs that no single "
        + "table's own description covers.\n\n"
        + "VIEW JOIN PUSHDOWN. A normalized VIEW over a much larger base table (e.g. "
        + "sec.financial_facts = financial_line_items LEFT JOIN filing_contexts) may not "
        + "push your filter through the join before it scans — the symptom is a query that "
        + "never returns or is far slower than its filter implies, not an error. Fix: "
        + "reissue the filter directly against the view's base table (describe_table on "
        + "it), add a further scoping key (filing/accession id, period, document id) to "
        + "narrow to one record, then bring in joined columns via your own JOIN. DISTINCT "
        + "or an unfiltered aggregate over such a view is the riskiest shape.\n\n"
        + "DIMENSIONAL SUB-CATEGORY BREAKDOWNS (loan class, segment, geography, product "
        + "line) are usually a JOIN, not a column. Filter the dimension table's descriptive "
        + "column with ILIKE for the category name, read off the matching id(s), then join "
        + "back. A missing 'concept' name does not mean the data is absent.\n\n"
        + "THE ref SCHEMA IS THE JOIN LAYER — where 'these firms, across those two sources' "
        + "gets answered exactly instead of fuzzily. ref.canonical_org_entity: one row per "
        + "real-world organisation, nullable FK per source (sec_cik, patents_assignee_id, "
        + "fec_committee_id, eia_utility_id, fmcsa_dot_number, exempt_org_ein, "
        + "ipeds_unitid, clinical_trials_nct_id, nsf_herd_inst_id, +more, each with a "
        + "_confidence sibling). ref.current_gleif_parents walks ownership; "
        + "ref.gleif_cik_mapping bridges LEI↔CIK. Check whether ref already carries the "
        + "key BEFORE matching by name.",

        "AskAmerica usage guide 4/8 — question quality rubric (not carried by any single "
        + "tool). A question is answerable here when it is: (1) BOUNDED — a specific "
        + "comparison or quantity, not a sweeping verdict; (2) MARGINAL — framed at the "
        + "margin, not the absolute total; (3) OPERATIONALIZED — names a measurable outcome "
        + "and admits it is a proxy; (4) MINIMALLY AND ORTHOGONALLY CONTROLLED — names the "
        + "one or two structural confounds that matter and are not near-duplicates of each "
        + "other; (5) GRAIN-MATCHED — the unit of analysis fits the data, and knows when "
        + "state grain (n=51) is too coarse to answer causally; (6) TIME/VINTAGE-ANCHORED "
        + "— names the window and respects revisions; (7) EFFECT-SIZE SEEKING — asks 'how "
        + "strong, and how sure?' with the confound it is NOT holding fixed stated out "
        + "loud, rather than 'which side is right?'. Meta-test: if you cannot state the "
        + "SHAPE of the answer in advance (a coefficient, a ranked list, a number with an "
        + "interval), it is a topic, not a question — narrow it before querying. "
        + "get_usage_guide_section_6 and _7 carry worked vague-to-sharpened rewrites "
        + "illustrating this rubric.\n\n"
        + "Cross-tool routing this warehouse's tool descriptions don't state relative to "
        + "each other: diff_in_diff only handles two periods — use panel_fixed_effects for "
        + "more or for staggered treatment timing. Run correlation_matrix/VIF BEFORE "
        + "treating several candidate predictors as independent evidence. adjust_inflation "
        + "corrects for TIME only, not PLACE — comparing states, not just years, needs "
        + "per_capita too.",

        "AskAmerica usage guide 5/8 — rules no single tool's description states.\n\n"
        + "A 'premium'/'gap' claim (group X gets N-times group Y) computed as a raw ratio "
        + "of two group means is confounded by everything else that differs between the "
        + "groups — run ols_regression with the grouping as an indicator, or diff_in_diff "
        + "if there's a before/after, before reporting a bare multiplier.\n\n"
        + "A coefficient that has not been leave-one-out tested is not a finding. Before "
        + "reporting ANY regression result, run sensitivity_analysis with the same SQL and "
        + "a jurisdiction group_col — it leaves out one jurisdiction at a time and reports "
        + "whether a single unit carries the effect, flips its sign, or crosses p=0.05.\n\n"
        + "officials.state_political_index's composite score is SPI (State Political "
        + "Index) — NEVER call it CPI, which means Consumer Price Index everywhere else in "
        + "this corpus and would silently conflate a political-lean score with an "
        + "inflation deflator. get_usage_guide_section_8 carries the methodology checklist "
        + "to run BEFORE CALLING publish_report — a different check than question_coverage, "
        + "and read it even if question_coverage looks complete.\n\n"
        + "Sanity-check any headline aggregate against a published figure before building "
        + "on it. If your number is half or double what a widely-cited outside source "
        + "reports for the same quantity, stop and find out why before publishing — this "
        + "is the cheapest check available and it catches the largest errors, including "
        + "ones in this connector's own data you would otherwise have no way to detect.\n\n"
        + "Any URL you would otherwise fetch with WebFetch MUST instead be fetched with "
        + "web_fetch. Never call WebFetch or Fetch.",

        "AskAmerica usage guide 6/8 — ASKING WELL, worked rewrites (vague question -> "
        + "sharpened question -> why), part 1/2. Apply the same move to the user's "
        + "question before querying; when a rewrite is not possible, say so as in the "
        + "honest-refusal examples (section 7) rather than answering the vague form.\n\n"
        + "(1) [comparison] VAGUE: \"Does education spending work?\" -> SHARPENED: \"At "
        + "state grain over 2015-2022, in a two-way fixed-effects panel of NAEP grade-8 "
        + "reading on per-pupil instructional spending and pupil-teacher ratio, what is "
        + "the coefficient on spending and its cluster-robust standard error?\" -> WHY: "
        + "'Work' names no outcome and no comparison. The rewrite picks a measured proxy, "
        + "a grain, a window, and one orthogonal control, and asks for an effect size "
        + "with its uncertainty.\n\n"
        + "(2) [comparison] VAGUE: \"Which is better for jobs, solar or gas?\" -> "
        + "SHARPENED: \"For 2018-2023, regress county employment in NAICS 2211 on state "
        + "generation capacity added, separately for solar and for natural gas — report "
        + "each slope per MW with a 95% interval and n counties.\" -> WHY: The vague form "
        + "asks for a winner. The rewrite asks at the margin (per MW) and returns two "
        + "comparable slopes rather than a verdict.\n\n"
        + "(3) [trend] VAGUE: \"Is crime getting worse?\" -> SHARPENED: \"For FBI UCR "
        + "agencies reporting in every year 2015-2023 — a balanced panel, so the trend is "
        + "not a reporting-coverage artifact — what is the annual change in violent crime "
        + "per 100k, and over how many agencies?\" -> WHY: UCR participation changes year "
        + "to year, so an unbalanced panel can manufacture a trend out of who reported.\n\n"
        + "(4) [ranking] VAGUE: \"Which states are doing best economically?\" -> "
        + "SHARPENED: \"Rank the 50 states by real GDP-per-capita growth from 2019 to "
        + "2023 (BEA, chained 2017 dollars), returning the growth value alongside the "
        + "rank.\" -> WHY: 'Best' is a verdict; growth in a stated series is a quantity.",

        "AskAmerica usage guide 7/8 — ASKING WELL, worked rewrites, part 2/2 (continued "
        + "from section 6).\n\n"
        + "(5) [premise-check] VAGUE: \"Why did housing permits collapse in 2023?\" -> "
        + "SHARPENED: \"First, did they? Compare Census new-residential permits nationally "
        + "for 2022 and 2023 and report both counts and the percent change — then ask "
        + "why, only if the premise holds.\" -> WHY: A 'why' question smuggles in its "
        + "premise and gets it confirmed.\n\n"
        + "(6) [effect-size] VAGUE: \"Are higher vaccination rates linked to lower "
        + "mortality?\" -> SHARPENED: \"At county grain, regress age-adjusted all-cause "
        + "mortality on vaccination rate controlling for median age and poverty rate; "
        + "report the coefficient, its 95% interval, and n.\" -> WHY: 'Linked to' invites "
        + "a yes/no. The rewrite asks how strong and how sure, and says out loud what it "
        + "is not holding fixed.\n\n"
        + "(7) [honest-refusal] VAGUE: \"Did the 2021 minimum-wage increases cause the "
        + "rise in unemployment?\" -> SHARPENED: \"This cannot be answered causally at "
        + "state grain: with about 51 units, a single treatment year, and a pandemic "
        + "recovery running underneath it, no coefficient here separates the policy from "
        + "the recovery. What IS answerable: the unemployment path for raising versus "
        + "non-raising states, 2019-2023, labelled descriptive, not causal.\" -> WHY: Name "
        + "the mechanical reason the causal question fails and hand back the narrower "
        + "question the data does support.\n\n"
        + "(8) [honest-refusal] VAGUE: \"What was the poverty rate last year?\" -> "
        + "SHARPENED: \"Call describe_table on the ACS table first. If the year you want "
        + "is past the last published year, say the source has not published it yet, and "
        + "offer the most recent published year instead.\" -> WHY: This snapshot is "
        + "versioned, not live.",

        "AskAmerica usage guide 8/8 — the methodology checklist: verifying the SPECIFIC "
        + "method a question names, not just that every clause was touched.\n\n"
        + "BEFORE CALLING publish_report, beyond question_coverage (which checks every "
        + "CLAUSE was addressed) and beyond what publish_report's own description says: "
        + "re-read the question a SECOND time for any SPECIFIC method it names — a "
        + "denominator, a pairing or design, a required per-source attribution — and check "
        + "what you actually did against it word for word, not against something merely "
        + "similar.\n\n"
        + "This is a DIFFERENT failure than an uncovered clause. Every clause can be "
        + "technically addressed while still using the wrong instrument: a self-reported "
        + "survey rate where the question calls for an eligible-population denominator "
        + "(e.g. CPS self-report turnout instead of VEP); a state-level panel where the "
        + "question specifies paired adjacent counties; a headline figure spliced from two "
        + "studies and cited under one source instead of attributing each number to its "
        + "own. question_coverage's checklist structure cannot catch this on its own — a "
        + "clause you never recognized as distinct from a broader one you did address will "
        + "never appear as its own false entry, so the omission stays invisible to the "
        + "reader unless you catch it here, by name, before publishing.\n\n"
        + "If the answer reports that a ranking or comparison holds, this check also "
        + "includes testing whether the GAP driving it — not just each side's own point "
        + "estimate — survives its own uncertainty.\n\n"
        + "MANDATORY provenance check: for every number attributed to a named primary source "
        + "(an agency report, an official dataset), confirm you actually parsed that source's "
        + "own data — not a secondary site's claim to republish it, and not the primary "
        + "source's narrative/preface text when the fetch never returned its table. If a "
        + "primary-source fetch failed or returned no usable table and the number came from "
        + "elsewhere, say exactly that in the answer — never describe it as read from the "
        + "primary document."
    ));

    // ── MCP prompts (opt-in question templates) ───────────────────────────────
    //
    // Tool descriptions reach every capable client; prompts support is uneven, so these
    // are the bonus surface, not the primary one. They carry the same rubric as fill-in
    // templates for a user who wants to start from a well-formed question rather than
    // rely on the host having read the exemplars.

    /** One parameterized template: a name, a description, and its argument names. */
    static final class Template {
        final String name;
        final String description;
        final String[] args;
        final String[] argDescriptions;
        final String body;

        Template(String name, String description, String[] args, String[] argDescriptions,
                String body) {
            this.name = name;
            this.description = description;
            this.args = args;
            this.argDescriptions = argDescriptions;
            this.body = body;
        }
    }

    static final List<Template> TEMPLATES = Collections.unmodifiableList(Arrays.asList(
        new Template(
            "marginal_comparison",
            "Compare two options at the margin on one measured outcome, with an effect size "
            + "and its uncertainty.",
            new String[]{"outcome", "option_a", "option_b", "grain", "window"},
            new String[]{
                "The measured outcome, named as a specific series (e.g. 'NAEP grade-8 reading').",
                "First option being compared (e.g. 'per-pupil instructional spending').",
                "Second option being compared (e.g. 'pupil-teacher ratio').",
                "Unit of analysis: state, county, agency, firm, ...",
                "Year range, e.g. '2015-2022'."},
            "At {grain} grain over {window}, on the margin, does {option_a} or {option_b} do more "
            + "for {outcome}? Estimate both in one model so the coefficients are comparable, "
            + "report each effect size with its standard error and n, and state explicitly which "
            + "confounds the model does NOT hold fixed. If {grain} is too coarse to separate "
            + "these from each other or from a common trend, say so and give the descriptive "
            + "comparison instead of a causal one."),
        new Template(
            "trend_check",
            "Measure the direction and size of a trend on a balanced panel, so composition "
            + "change cannot manufacture it.",
            new String[]{"measure", "grain", "window"},
            new String[]{
                "The series to trend (e.g. 'violent crime per 100k').",
                "Unit of analysis: state, county, agency, ...",
                "Year range, e.g. '2015-2023'."},
            "Over {window}, what is the annual change in {measure} at {grain} grain? Restrict to "
            + "units present in every year so the trend is not a reporting-coverage artifact, "
            + "report the slope with its uncertainty and the number of units, and note how many "
            + "units the balancing dropped."),
        new Template(
            "ranking",
            "Rank a universe on one stated quantity, keeping the values visible so near-ties "
            + "do not read as an ordering.",
            new String[]{"measure", "universe", "window"},
            new String[]{
                "The quantity to rank on, named as a specific series.",
                "What is being ranked (e.g. '50 states', 'counties in Texas').",
                "Year or year range the measure covers."},
            "Rank {universe} by {measure} for {window}. Return the measured value next to each "
            + "rank so ties and near-ties are visible, state the source table and its coverage "
            + "window, and flag any unit missing from the source rather than silently omitting "
            + "it."),
        new Template(
            "premise_check",
            "Test whether a claim's premise holds before explaining it.",
            new String[]{"claim"},
            new String[]{"The claim to check, as stated (e.g. 'housing permits collapsed in "
                + "2023')."},
            "Before explaining it: is \"{claim}\" true in this data? Identify the series that "
            + "would show it, report the actual values for the relevant periods with the percent "
            + "change, and say plainly whether the premise holds. Only if it holds, then ask "
            + "why."),
        new Template(
            "policy_effect",
            "Estimate a policy's effect where the design supports it, and refuse where it "
            + "does not.",
            new String[]{"policy", "outcome", "treated_units", "window"},
            new String[]{
                "The policy or event, with its date.",
                "The measured outcome series.",
                "Which units were treated (e.g. 'states that raised the minimum wage in 2021').",
                "Year range covering enough pre-periods to check trends."},
            "For {window}, estimate the effect of {policy} on {outcome}, treating {treated_units} "
            + "as treated. First check that pre-period trends are parallel and report that check. "
            + "If they are not, or if the number of treated units is too small to separate the "
            + "policy from concurrent shocks, say the causal estimate is not supported and give "
            + "the descriptive pre/post comparison instead — labelled descriptive."),
        new Template(
            "coverage_check",
            "Establish what the corpus actually carries for a topic and window before asking "
            + "anything of it.",
            new String[]{"topic", "window"},
            new String[]{
                "What you want data about.",
                "Year or year range you need."},
            "Which tables here carry {topic}, and what is each one's declared coverage window? "
            + "Use search_catalog then describe_table. If {window} falls outside a table's "
            + "window, say the source has not published it rather than returning or inferring a "
            + "figure — an empty result outside coverage is not a zero.")));

    /** {@code prompts/list} payload. */
    static ObjectNode promptsList() {
        ArrayNode prompts = MAPPER.createArrayNode();
        for (Template t : TEMPLATES) {
            ObjectNode p = MAPPER.createObjectNode();
            p.put("name", t.name);
            p.put("description", t.description);
            ArrayNode args = MAPPER.createArrayNode();
            for (int i = 0; i < t.args.length; i++) {
                ObjectNode a = MAPPER.createObjectNode();
                a.put("name", t.args[i]);
                a.put("description", t.argDescriptions[i]);
                a.put("required", true);
                args.add(a);
            }
            p.set("arguments", args);
            prompts.add(p);
        }
        ObjectNode out = MAPPER.createObjectNode();
        out.set("prompts", prompts);
        return out;
    }

    /** Template by name, or null. */
    static Template template(String name) {
        if (name == null) {
            return null;
        }
        String n = name.toLowerCase(Locale.ROOT);
        for (Template t : TEMPLATES) {
            if (t.name.equals(n)) {
                return t;
            }
        }
        return null;
    }

    /**
     * {@code prompts/get} payload for a template. An argument the caller did not supply is
     * left as its own {@code {placeholder}} rather than dropped — a template that silently
     * loses its grain or window renders as a vague question, which is the thing these exist
     * to prevent.
     */
    static ObjectNode promptGet(Template t, com.fasterxml.jackson.databind.JsonNode arguments) {
        String text = t.body;
        List<String> missing = new ArrayList<>();
        for (String arg : t.args) {
            String value = arguments == null ? null
                : (arguments.hasNonNull(arg) ? arguments.get(arg).asText("") : null);
            if (value == null || value.trim().isEmpty()) {
                missing.add(arg);
                continue;
            }
            text = text.replace("{" + arg + "}", value.trim());
        }

        StringBuilder body = new StringBuilder(text);
        if (!missing.isEmpty()) {
            body.append(" (Unfilled placeholders: ");
            for (int i = 0; i < missing.size(); i++) {
                if (i > 0) {
                    body.append(", ");
                }
                body.append('{').append(missing.get(i)).append('}');
            }
            body.append(" — fill these in before querying; a template rendered with a missing "
                + "grain or window is exactly the vague question it was meant to replace.)");
        }

        ObjectNode message = MAPPER.createObjectNode();
        message.put("role", "user");
        ObjectNode content = MAPPER.createObjectNode();
        content.put("type", "text");
        content.put("text", body.toString());
        message.set("content", content);

        ArrayNode messages = MAPPER.createArrayNode();
        messages.add(message);

        ObjectNode out = MAPPER.createObjectNode();
        out.put("description", t.description);
        out.set("messages", messages);
        return out;
    }
}
