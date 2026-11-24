# Judge — News q3 (the74-alabama-naep-4th-grade-reading-rank), 2026-11-24

**Question/claim:** The 74 (2026-11-18, Liana Loewus interview): Alabama ranked 45th on 4th-grade NAEP reading in 2015, 18th in 2019, 13th in 2024; 2024 score two points below national; ARI funding $58M (2013), $40.7M (2018), $152M now.

Rank: [askamerica, everyman, expert]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md carry both markers; askamerica received the product's verbatim news prompt, which carries no markers by design. A grep of the everyman and expert answers (agent.md, agent-report.html) found no mention of askamerica or askamerica.ai. No discount applies.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web only (state, Fordham via summary, PARCA, press). No NAEP Data Explorer. | Warehouse edu.naep_scores (rank query, national average) plus the Fordham report read directly | NCES state snapshot, governor release, AL Reporter. Fordham report not read. |
| Window | 2015, 2019, 2024 | 2015, 2019, 2024 | 2019, 2022, 2024 |
| Headline | Rank claim FALSE/unsupported. State: 49th (2019), 34th (2024); PARCA 33rd. Sequence "attached to Mississippi" in secondary write-ups, "or an unlabelled adjusted ranking. I could not tell which." | Rank claim FALSE. Warehouse AL 40th/48th/33rd of 51. Fordham gives 45th/18th (+5) for Mississippi. | Rank claim "not confirmed". ADE: 49th to 34th unadjusted; "one poverty-adjusted analysis 33rd->19th. 13th likely other metric." |
| Overall grade | 3 Pinocchios | 3 Pinocchios | 2 Pinocchios |
| Claims graded | 8 | 4 graded plus 3 listed as not verified | 5 (several bundled) |
| Central verdict | False, cause left open (Mississippi or adjusted) | False, cause identified (Mississippi passage in Fordham) | Unconfirmed, leans to an adjusted metric |
| Visual | none | dashboard.png, report.html | agent-dashboard.png, agent-report.html |

## Verification

Central claim, checked against primary sources.

- Fordham report (fetched today): "rising from 45th among states on the fourth-grade National Assessment of Educational Progress (NAEP) in 2015 ... to 18th in 2019. In 2024, it leapt another five spots." The subject is Mississippi. Five spots from 18th is 13th. The article's 45th/18th/13th sequence is therefore Mississippi's sequence, copied onto Alabama. The Fordham report says nothing about demographic adjustment for these ranks.
- Fordham on Alabama: "By 2019, the state ranked last in the country in fourth-grade reading." It gives no 2024 reading rank. Alabama math was 51st in 2019 and 31st in 2024. Fordham says Alabama's 2024 score "was 213, just two points below the national average."
- Adjusted-ranking hypothesis: a web search found a poverty-based NAEP analysis (the Institute) that moves Alabama grade 4 reading from 33rd to 19th. That is not 45th/18th/13th. I could not tie any adjusted series to 45th/18th/13th. The exact match with Fordham's Mississippi text makes the mix-up the supported explanation. The adjusted-ranking idea is not.
- Unadjusted ranks: governor's office and ADE give 49th (2019) and 34th (2024). PARCA gives 33rd for 2024. Warehouse (per askamerica's SQL) gives 48th in 2019 and 33rd in 2024. Fordham says last in 2019. The 48th versus 49th/last gap is a small ranking-basis difference (51 jurisdictions, ties). The warehouse 2015 rank of 40th is not corroborated by another source here.
- Gap to national: askamerica warehouse 213.2 vs 214.3 (about 1 point); Fordham and the article say 2 points. askamerica flagged both. Expert cited NCES 213 vs 214 (1 point) and called the article's 2-point figure a mismatch; the article repeats Fordham, so that is a rounding difference.
- ARI funding: Fordham gives $58M, $40.7M, $152M (2026-27), $85,000 per school. askamerica graded this true and said it checks fidelity to Fordham only. Everyman graded it supported. Expert verified only $58M/$40.7M and left $152M and $85,000 unverified because it never read Fordham.

By arm.

- everyman: correct verdict on the central claim and the right two candidate causes. It named the Mississippi mix-up but did not confirm it, and it said "or an unlabelled adjusted ranking". It said it could not tell. Its rank numbers (49th/34th, 33rd) are right. It caught the Kay Ivey/Kate and Terri/Terry name errors and graded the quotes as "not checkable here" with a stated reason, which is calibrated. It did not read the Fordham passage directly (summaries only).
- askamerica: correct verdict and the confirmed cause, from the primary Fordham text. It used the warehouse for the rank and for the national gap. It graded the interviewee's "nearly 1,000" claim as unsupported under Loewus (subject_claims). Its reason is stated: the only "1,000" in Fordham is a $1,000 stipend. That is a fair "unsupported", not "false". A live check of the 48th (2019) warehouse rank is an open item for the orchestrator.
- expert: weakest on the central question. It did not read Fordham, so it missed the Mississippi source. It said "13th likely other metric" and cited a poverty-adjusted 33rd to 19th, which does not produce 13th. It graded 2 Pinocchios on a false headline rank, which is too lenient. It left $152M, $85,000, 45th and 18th as not verified, although all are in the primary report. Penalise: several claims left unchecked that were checkable.

askamerica rating check. The engine report.html has no Pinocchio rating. It shows per-group Honesty/Bias lines ("too few checkable claims to characterize"), verdicts (false, mostly true, true, unsupported) and a subtitle "headline NAEP rank claims are mostly false". The agent.md states 3 Pinocchios. I found no contradiction, but the engine produced no numeric grade to compare; the 3 is the agent's own. Flag as a minor mismatch of form: the report shows "1 claims graded" for the author group and gives no overall grade.

## Ranking rationale

1. askamerica. Correct central verdict, confirmed cause (Mississippi text in Fordham), warehouse ranks and gap, primary-source reading, and the largest set of checked claims. It stated "not verified" for the Massachusetts, "42-page" and "nine years" items. 3 Pinocchios is right for a false headline claim with mostly faithful body.
2. everyman. Same verdict and grade, solid ranks, wide claim coverage (8 claims), and honest limits. It did not confirm the cause and did not read Fordham.
3. expert. Soft verdict (2 Pinocchios, "not confirmed"), wrong lean toward an adjusted metric, and primary report unread.

What would change the ranking: if the warehouse 2015 (40th) or 2019 (48th) rank proves wrong on a live query, askamerica and everyman would be close, since everyman's ranks come from state sources.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

- Pattern worth keeping: askamerica searched the primary report for the claimed numbers and found they belong to another state. Recipe `fact-check-a-news-article` (recipes.json line 1443) covers the news path. A grep of recipes.json for adjusted/poverty/demograph found no recipe on state rankings that differ by adjustment. A short addition to the NAEP ranking guidance is warranted: when a stated rank does not match the unadjusted NAEP rank, test whether the same sequence appears for a neighbouring state in the cited source, then check for an adjusted series. Warehouse-native method: edu.naep_scores, rank over year for the named state and the candidate state.
- askamerica is #1, so no cause classification is needed for a loss.

Proposed resolution: d — askamerica ranked first; no fix needed, optionally add the "same sequence belongs to a neighbouring state" check to the fact-check recipe.

## Corpus gaps stated

- "the warehouse has no state literacy funding table" (ARI funding graded true to Fordham only; claim 3 warehouse_value "not in warehouse").
- "Nearly 1,000 people are employed by the state to just focus on literacy": graded unsupported; search_catalog for "Alabama literacy coaches state employees count" found nothing; reason stated: Fordham's only "1,000" is a $1,000 teacher stipend.
- "Not verified: Massachusetts 'almost 400 districts,' the '42-page' length, and 'Dr. Mackey nearly nine years'".
- "Her explanations for why Alabama gained ... are judgment, not testable here."
