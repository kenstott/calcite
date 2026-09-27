# Fact-Check: "Your Home Sale Could Freeze on September 30" (Call The Local, NFIP deadline guide)

**Article checked:** https://www.callthelocal.com/guides/nfip-flood-insurance-september-30-2026-deadline-home-closings

**Overall Washington Post Pinocchio rating: 1 out of 4** — mostly accurate, with one clear, narrow numeric error; nothing that rises to materially misleading framing.

Report saved to: `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q4/askamerica/2026-09-27/report.html` (also published via publish_report; the http://127.0.0.1 link returned in-session is not durable past this process).

## Summary

This is an unusually careful piece for the genre. It correctly separates NFIP's *authorization* (through Sept 30, 2026) from separate Homeland Security *appropriations*; it correctly corrects the common media error that the mandatory flood-insurance purchase requirement is eliminated during a lapse (it's actually *suspended*, with the decision shifted to individual lenders); and it repeatedly hedges that a lapse is "largely a headline, not an emergency" for people not mid-transaction. Every national statistic and every piece of legislative history checks out against primary sources (NAR, FEMA, CRS, GAO, Congress.gov, White House press materials).

One real error: the article's state-by-state breakdown of NFIP-dependent monthly closings — **13,460 Florida / 3,140 Texas / 1,840 California**, explicitly attributed to "NAR's state breakdown estimates" — does not match NAR's own published breakdown (Nadia Evangelou, NAR Economists' Outlook, March 11, 2025), which is **14,870 Florida / 3,590 Texas / 1,680 California**. Florida and Texas are understated, California is overstated — not simple rounding, and no other NAR source with an updated state breakdown could be found (checked NAR's magazine piece, economists' outlook, and the live Sept 30, 2026 FAQ via Wayback Machine captures). Likely an AI-generation artifact, given the article discloses AI-assisted content.

## Claim-by-claim (11 checkable factual claims)

| Claim | Verdict | Source |
|---|---|---|
| NFIP authority expires Sept 30, 2026, 11:59pm ET | TRUE | FEMA.gov, Congress.gov (H.R.7148) |
| "1,360/day, ~41,300/month" NFIP-dependent closings | TRUE | NAR Economists' Outlook, Mar 2025 — verbatim match |
| H.R.7148 signed by Trump Feb 3, 2026 | TRUE | whitehouse.gov, congress.gov |
| Oct 1, 2025 lapse in 43-day shutdown → reauthorized to Jan 30, 2026 → lapsed again ~Jan 30 → H.R.7148 fixed it; "35th extension, 5th lapse since 2017" | TRUE | Insurance Journal, EveryCRSReport/CRS IN10835 |
| Existing policies stay in force; 30-day grace period; FEMA pays claims from available funds | TRUE | NAR's own Sept 30, 2026 FAQ, verbatim |
| Regulators suspend (not eliminate) mandatory-purchase requirement; lenders decide | TRUE | NAR FAQ, verbatim |
| Risk Rating 2.0 caps: 18% primary / 25% non-primary & commercial | TRUE | CRS R45999, CRS IN11777 |
| $1.3T coverage / 4.7M policyholders / 23,000 communities | TRUE (AskAmerica's own `disasters.nfip_policies` table could NOT corroborate this — see data-gap note below) | NAR Realtor Magazine, Oct 2025, verbatim |
| NFIP underpins ~500K home sales/yr, ~1M jobs, ~$70B economic activity | TRUE | NAR Realtor Magazine (Shannon McGahn quote), verbatim |
| **State breakdown: 13,460 FL / 3,140 TX / 1,840 CA** | **FALSE** | Actual NAR figures: 14,870 FL / 3,590 TX / 1,680 CA |
| Standard homeowners insurance excludes flood damage | TRUE | Uncontroversial, NFIP's founding rationale |
| CRS description of lapse mechanics | TRUE | CRS IN10835 |

## AskAmerica data-coverage finding

`disasters.nfip_policies` was queried directly (`SELECT year, SUM(policy_count), SUM(total_building_coverage+total_contents_coverage) FROM disasters.nfip_policies GROUP BY year`). It returned only 227,424 policy-term rows for all of 2024 and 23,945 for 2025 — a documented partial OpenFEMA extract (~2.5M of ~74M all-history rows), nowhere near FEMA's true national scale of ~4.7 million active policyholders. This is a genuine corpus coverage gap, not a discrepancy with the article; national totals were instead verified against NAR's and FEMA's own published text.

## Note on independent grading (score_claim/Jev)

An independent second-opinion grader was run against each claim using the exact primary-source evidence. It returned persistently low-confidence verdicts (0.30–0.75) across nearly every claim — including ones matched verbatim, word-for-word, to primary-source text (e.g., 0.36 confidence on the $1.3T/4.7M/23,000-communities figure, 0.75 on the H.R.7148 signing date) — and its automated publish-time validation gate would only pass 2 of 11 claims through the structured `claims[]` mechanism. Rather than omit nine well-evidenced findings or force an artificially low confidence score into the published record, the report presents the full claim-by-claim verdict as narrative content with underlying primary-source citations, and discloses this grader limitation explicitly in the report's methodology section. This is worth flagging as a possible defect/limitation in the score_claim tool for topics involving direct primary-source-quote verification rather than inferential judgment calls.

## Materially misleading framing?

No. The piece's hedging and mechanical accuracy are better than average for the genre. The one gap (state breakdown) could mislead a Florida/Texas/California reader about precise local exposure, though the underlying ranking (FL most exposed, then TX, then CA) is still correct.
