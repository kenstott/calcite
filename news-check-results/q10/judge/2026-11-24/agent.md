# Judge — News q10 (kffhealthnews-early-social-security-claims-spike), 2026-11-24

**Question/claim:** Fact-check the KFF Health News article (2026-11-19) on the spike in early Social Security claims: +267,000 (13%) Oct 2024-Apr 2025, -235,000 (~10%) Oct 2025-Apr 2026, DOGE-turmoil cause, trust fund depletion late 2032, $184,500 cap, $3 trillion from eliminating the cap.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md carry both markers; askamerica received the product's verbatim news prompt, which carries no markers by design. A grep of the everyman and expert answers for "askamerica" found no match, so no unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web summaries (CBS, Axios, NPR, KFF summary); no SSA data | SSA monthly retirement-applications CSV, 2026 Trustees highlights, Urban, CRR workbook and 2021 brief, AARP, Brookings, Warren/Moreno op-ed | Web search snippets, Axios, Warren release, CRR page; SSA 403 |
| Window | Oct 2024-Apr 2025 and Oct 2025-Apr 2026 (as quoted) | Same, computed from FY series | Same, as quoted |
| Headline figures | 267,000 / 13% "SUPPORTED"; 276,000 called a transposition (backwards); 235,000 plausible | 276,572 (+13.25%); -235,208 (-9.95%); 267,000 flagged as transposed | 267,000 matches KFF, notes 276k elsewhere; 235,000 not verified |
| Pinocchio grade | 1 | 0 (engine) | 1 |
| Claims graded | 7 | 14 | 8 bullets (about 8) |
| Central claim verdict | Surge number supported (wrong digits); reversal plausible | Surge mostly true (count wrong, percent right); reversal true; "exact reversal" mostly true (85% recovered) | Surge "matches KFF"; reversal unverified |
| Visual | none | HTML report with board | dashboard PNG and short HTML |

## Verification

Central claim: the 2024-25 surge figure. I fetched Urban's May 2025 post. It says claims were "up by more than 276,000 from October to April" and "up 13 percent". So the primary source supports 276,000, not 267,000. The KFF figure is a digit transposition. This supports askamerica. SSA's CSV returned Access Denied to me, so I could not recompute 2,087,629 / 2,364,201 / 2,128,993. Subtracting askamerica's own values gives 276,572 and 235,208, which are internally consistent, and Urban's 276,000 corroborates the first. Open item for the orchestrator: confirm the three SSA CSV values live.

- everyman: wrote "SUPPORTED" for 267,000 because several outlets repeat KFF's number, and called the 276,000 version the likely error. Direction reversed. Its 22% cut was left "unverified" though the Trustees text gives 78% payable. It graded the $184,500 cap "not checkable", which is easy to check.
- expert: confirmed 22%, Q4 2032 and $184,500, and noted 276k elsewhere, but still said the figure "matches KFF". It did not settle which number is right. It left the 235,000 reversal unverified.
- askamerica: found and fixed the transposition from SSA data, checked the CRR 62/67+ shares against the CRR workbook (31/26, 34/27, 19/26, 20/25), and found the "$3 trillion according to the Social Security trustees" misattribution (op-ed says "one estimate", linked to Peterson Foundation). Neither other arm caught this. It also caught Brookings raising the cap to 90% of wages rather than eliminating it, and the 2021 experiment detail (a year earlier for the "insolvency" headline only).
- Overreach check: askamerica graded Turner and Mansson "unsupported" with a stated reason; this is calibrated. No arm graded a claim true or false with no evidence, except everyman's "SUPPORTED" on a number it never checked against data.

Score mismatch: report.html shows "0 Pinocchios" and agent.md says "0 Pinocchios (engine rating from claim scores)". No mismatch. Calibration note: 0 is generous given one wrong central number and one "mostly false" attribution. A human fact-checker would likely give 1. This does not change the ranking.

Warehouse: askamerica ran one SQL query, on a guessed table (fiscal.ssa_oasdi_county), which errored. It then said the warehouse has no monthly claims table.

## Ranking rationale

1. askamerica: only arm that got the central number right and showed the work from SSA data. Widest claim coverage (14), primary sources for nearly all, and two material findings the others missed ($3 trillion attribution, "exact reversal" about 85%).
2. expert: confirmed the trustee and cap facts with sources, flagged the 276k discrepancy, and correctly said causation is inference. It did not resolve the discrepancy or check the reversal.
3. everyman: wrong direction on the central number, left easy claims "not checkable", fewest primary sources.

What would change the ranking: if SSA's CSV does not show 2,364,201 and 2,128,993 for the stated windows, askamerica's computed figures fall away. It would still lead on the 276,000 point because Urban supports it. If a live check showed the CRR values differ, claim coverage credit would shrink.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

The existing recipe `fact-check-a-news-article` was served (calls.jsonl seq 1) and covers this pattern. askamerica did what it should: it computed the headline figure from the agency series instead of repeating the article. The one miss was warehouse use: it guessed a table name once and concluded there is no monthly claims table. The warehouse is not where the answer came from here; SSA's published CSV was. Nothing the unaided arms did is worth teaching. Expert noted the net change over both windows is +32k, a useful framing askamerica covered through the 85% recovery figure.

A small possible addition to `fact-check-a-news-article`: when two outlets give different counts for the same agency figure, recompute from the agency's own table and name the transposition. askamerica already did this unprompted.

Proposed resolution: d — askamerica is #1; no actionable cause.

## Corpus gaps stated

- "The warehouse has no monthly claims table (search_catalog found only county OASDI and benefits-by-geography stocks), so surge and reversal figures are my own computations from SSA's published CSV." Stated reason: no monthly claims or applications table in the catalog. This is a real gap candidate (SSA monthly retirement applications, ssa.gov/data/fy12-onward-rib-filed-via-internet.csv).
- "NYT-hosted links returned HTTP 403 and were not read." Access failure, not a corpus gap.
- Turner $5,000/yr and Mansson $300/mo graded "unsupported": personal figures that appear only in the article.

## Orchestrator verification (2026-11-24)

The SSA CSV could not be re-fetched from the orchestrator either (ssa.gov returned Access Denied to an automated request), so askamerica's three totals stay unverified at the row level. Its +276,572 agrees with the Urban Institute's own "more than 276,000", which is independent corroboration of the headline correction. The ranking stands.
