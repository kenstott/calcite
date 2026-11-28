# Judge — news q1, Breitbart "States with Highest Unemployment Rates Kept Democrats in Power" (2026-11-26), 2026-11-28

**Question/claim:** Fact-check the article (https://www.breitbart.com/politics/2026/11/26/states-with-highest-unemployment-rates-kept-democrats-in-power/): the five highest-unemployment states (AZ, OR, CA, MI, CT) re-elected or kept Democratic governors, and the five lowest are Republican-led. Central claim: those five states and their rates.

Rank: [askamerica, everyman, expert]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files open with 123456789 and close with 987654321 (composer-generated); askamerica ran the production news-check prompt verbatim. No unaided arm's agent.md mentions askamerica. Grep of everyman and expert agent.md for "askamerica" returned nothing, so no unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Article text | WebFetch gave only a summary; claims from the summary | Fetched in full | Not stated |
| Data used | BLS LAUS rank page, news | Warehouse econ.regional_employment (2026 M10), BLS page, ALEC page, news | BLS LAUS page and release, news |
| Vintage | Oct 2026 prelim | Oct 2026 prelim | Oct 2026 prelim |
| Central claim verdict | True (caveat DC) | True (DC 5.4 not a state) | True (DC not a state) |
| Overall grade | 2 Pinocchios | 1 Pinocchio | 2 Pinocchios |
| Claims graded | 12 | 13 rows in agent.md, 12 in claims.json | 10 |
| Becerra "former Senator" error | Caught | Caught | Missed (graded "True; typo") |
| Hawaii omission | Caught | Caught | Caught |
| ALEC Williams quote | Not checkable | True, verbatim on alec.org | Not checkable |
| Causal headline | Misleading | Mostly false | Misleading, with extra counterexamples (GA, IA, OH) |
| Visual | None | Dashboard and report.html | Dashboard and report |

## Verification

Central claim checked against the BLS LAUS state ranking page (fetched today, October 2026, seasonally adjusted, preliminary). Highest: DC 5.4, CT 5.1, CA 5.0, MI 5.0, AZ 4.9, OR 4.9, NM 4.8. Lowest: SD 2.0, ND 2.2, NH 2.5, VT 2.6, HI 2.8 tied with NE 2.8. All three arms match this exactly. The central claim is true. The lowest-five claim is misleading because Hawaii ties Nebraska and is omitted. All three arms caught that.

Becerra: the article calls him a "former Senator". All sources the arms cite and askamerica's claims.json say he served in the U.S. House, as California AG and as HHS Secretary. Everyman and askamerica flagged it. Expert did not and graded the claim True.

Kotek margin: 53.4 to 43.7 (9.7) cited by expert and askamerica via OPB. Everyman reported an early 6-point count and called it plausible, unconfirmed. Expert and askamerica are better here.

ALEC quote: askamerica fetched alec.org and found the quote verbatim. Everyman and expert said not checkable. Their "not checkable" is honest but it was a checkable claim. I did not re-fetch alec.org.

Askamerica engine rating: report.html states "Rating: 1 Pinocchio". agent.md states "Rating: 1 Pinocchio". No mismatch. claims.json marks the headline claim "mostly false" with support "overreach", so a 1 Pinocchio overall is lenient given one clear factual error, a dropped Democratic state and a mostly false causal headline. Two Pinocchios looks better calibrated. This is a grading calibration point, not a verification error.

claims.json omits the Benson claim and the Newsom clemency and "far-left" lines that appear in agent.md. The first preview_report call was refused for a graded Hobbs claim with no evidence; it was fixed before publishing.

Open item for the orchestrator: the warehouse query (econ.regional_employment, year 2026, period M10) cannot be rerun by me. Its values match BLS, so I do not expect a problem.

## Ranking rationale

1. askamerica. Read the full article, so claim extraction is faithful. Caught Becerra and Hawaii. Verified the ALEC quote verbatim. Used a warehouse figure plus BLS for the central claim. Used "not found" and "opinion" sparingly and correctly. Weak point: 1 Pinocchio is lenient.
2. everyman. Caught both errors and graded 2 Pinocchios. Worked from a summary only and said so. Kotek margin left unconfirmed and quote not checked.
3. expert. Good counterexamples (GA 3.3, IA 3.3, OH 3.4 flipped to Democrats) and the best sourcing on Kotek. But it missed the one clear factual error (Becerra never a Senator), which an arm must catch in a fact-check. It also did not check the quote.

What would change the ranking: if the orchestrator finds the ALEC quote on alec.org does not match the article's wording, askamerica's quote verdict falls and everyman moves ahead. If the grade is weighted heavily, 2 Pinocchios (everyman, expert) is more defensible than 1, which would narrow askamerica's lead but not change its place on coverage.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

Askamerica used the existing recipe `fact-check-a-news-article` (served on its first call) and followed it: warehouse first for the rates, web for election results, claims.json with verdicts. Expert's extra pattern (counterexample states that flipped despite low unemployment) is a framing check that fits under existing `test-the-competing-explanation-not-just-the-implied-one`. Everyman's early-versus-later vote count comparison for Kotek is a minor habit, not worth a recipe. Askamerica should add the counterexample check to its causal-headline grading, but that is a small improvement to existing recipes.

Proposed resolution: d — askamerica ranked first; no actionable cause beyond optionally leaning on the existing competing-explanation recipe for causal headlines.

## Corpus gaps stated

- "No warehouse table covers elections, so the unemployment figures are the only warehouse-checked claims." Election results came from news reports.
- Newsom clemency for 548 (Trump caption): "Not found verbatim"; the 548 commutations were confirmed.
- Whitmer and Newsom as "far-left": "Opinion. Not testable."
- Arizona "statewide offices" flip: "not verified".
