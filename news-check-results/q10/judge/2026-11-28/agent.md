# Judge — news q10, Medical Daily "Black-White death rate gap shrank by over half in 30 years", 2026-11-28

**Question/claim:** Fact-check the Medical Daily article (ACS/Annals study, 2026-11-25): https://www.medicaldaily.com/black-white-mortality-gap-shrank-us-study-1993-2023-481710

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files open with 123456789 and close with 987654321 (composer-generated); askamerica ran the production news-check prompt verbatim. No unaided arm's agent.md mentions askamerica (grep returned no match for either), so none cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web: ACS release, Annals abstract via search, Yale/JAMA | Web: ACS release, Yale, JAMA Onc, MHE, FDA; warehouse health.chr_premature_death | Web: ACS release, US News, JAMA Onc, Yale |
| Vintage/window | 1993-2023 study; JAMA 1999-2020 | Same; CHR 2025 release for the "gap remains" check | Same |
| Headline figures | 56% men, 68% women; 62.3 vs 72.6 and 69.7 vs 78.0 age at death | 56%/68%; lung 31.1% + prostate 25.8% = 56.9%; CHR YPLL Black 13,698 vs white 8,038 (1.70) | 56%/68%; 31.1 + 25.8 = 56.9%; 10.3 and 8.4 years younger |
| Pinocchio grade | 0 | 0 (engine report also 0; honesty 97/100) | 1 |
| Claims graded | 13 | 9 (8 true, 1 mostly true, 0 not gradable) | 13 (some marked not checked) |
| Central verdict | Accurate | True, fidelity to ACS release only | Accurate, but "deaths" vs "rate" wording |
| Visual | none | report.html and dashboard.png | agent-report.html and dashboard.png |

## Verification

Central claim checked at the ACS release (WebFetch, pressroom.cancer.org): "All-cause excess mortality rates for Black compared with White individuals fell by 56% for males and 68% for females compared with 1993." The release says rates, not deaths. The article's lede says "excess deaths." All three arms confirm the 56%/68% figures. Expert grades the rate/deaths wording as a 1-Pinocchio issue. Everyman and askamerica note it as a framing risk and keep 0. The wording is a minor imprecision; the headline "death rate gap" is correct. Both 0 and 1 are defensible. A 0 with a flagged framing note is slightly better calibrated to the Fact Checker scale than a 1 for a wording slip the article itself explains later.

Lacasse: the release quote covers "prevention, early detection and treatment services." Expert flagged that the article's paraphrase (tobacco control, screening, personalized medicine) is wider than the quote. Everyman and askamerica did not mention it. This is a catch only expert made. I did not verify the article's exact wording (not fetched by me).

Askamerica's 56.9% (31.1 + 25.8) matches expert's figure. Everyman's mean-age figures (62.3/72.6, 69.7/78.0) are consistent with expert's gaps (10.3, 8.4 years). Not quoted by askamerica.

Everyman says WebFetch gave only a summary of the article, so its verdicts rest on summarized text. That caveat is honest. Askamerica's logs show it fetched the article and the sibling links.

Askamerica claims.json vs report: engine overall rating 0 Pinocchios, 9 claims graded, 0 not gradable. agent.md states 0 Pinocchios, 9 claims (8 true, 1 mostly true), honesty 97. No mismatch.

Open item for the orchestrator: askamerica's CHR numbers (13,698 vs 8,038) can only be checked by a live query of health.chr_premature_death (geo_level='nation'). Note it is a different measure (premature death/YPLL) and vintage from the study.

## Ranking rationale

1. askamerica: correct central verdict, every graded claim sourced to a primary release, and the one engine-computed rating matches the stated grade. It states plainly that the central figure rests on the sponsor's release only. It used the warehouse for one real check and was honest that the warehouse has no race-specific series. Its framing flags (peak baseline, overdose headline, sponsor-only sourcing) are the strongest set. It marked two items not independently checked rather than guessing. Weakness: 9 claims, fewer than the others, and it missed the Lacasse paraphrase.
2. expert: caught the Lacasse overreach and the rate/deaths issue most sharply, and marked unverifiable items as "not checked" correctly. Lost points for rating 1 Pinocchio on a wording point the article clarifies, a terse format, and no stated claim for the Heckler "planned to mark" nuance.
3. everyman: correct verdict and good claim coverage, but it worked from a summary of the article and not its text, and did not catch the Lacasse paraphrase or the Heckler intent nuance. Its caveat was honest.

What would change the ranking: if the Lacasse paraphrase is confirmed as a real misquote, expert moves up to first. If the orchestrator finds the CHR figures wrong, askamerica drops to second.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

Recipe `fact-check-a-news-article` exists and askamerica served it (calls.jsonl: procedure_served). It covers the pattern. Expert's unique catch (attributed-quote versus paraphrase width) is a method inside that recipe. A line in that recipe telling the arm to compare each attributed paraphrase against the exact quoted words in the primary release would cover it. Askamerica ran two search_catalog calls for race-specific mortality and then correctly stopped; the CDC mortality tables lack race and sex breakdown (filed govdata-ops #1639).

Proposed resolution: b — add a line to the fact-check-a-news-article recipe to check each paraphrase of a named person's statement against the quoted words in the primary release.

## Corpus gaps stated

- "the warehouse holds no race-specific 1993-2023 all-cause series" (agent.md and report.html; reason: Annals paper 403, no race-specific series in the warehouse).
- claims.json claim 1: "warehouse lacks it."
- "Not independently checked: 'often free through local health departments' and the age-45 colorectal screening guideline."
- Orchestrator note: the warehouse's CDC mortality tables have no race or sex breakdown (checked the schema); filed govdata-ops #1639.
