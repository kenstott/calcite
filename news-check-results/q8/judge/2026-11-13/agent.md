# q8 judge: FingerLakes1 "College graduate unemployment eases, but many still work in jobs that don't require degrees"

Rank: [askamerica, everyman, expert]

The story ran Nov 10, 2026 and reports the New York Fed's recent-graduate labor market update. Judged 2026-11-13. I verified against the NY Fed College Labor Market page (via WebFetch, a summarizing tool) and the NY Fed media advisory of 2026-05-28. The article itself came back only as a summary, so exact wording could not be confirmed for any arm or for me.

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| askamerica | 0 | NY Fed college-labor-market page; NY Fed 2026-05-28 media advisory; BLS context only (warehouse has no NY Fed series) |
| everyman | 0 (tentative) | NY Fed college-labor-market page; 2026-05-28 advisory via search summary |
| expert | 1 | NY Fed page; advisory; CNBC cross-check |

## Assessment
- **Correct verdict: 0 Pinocchios.** Every number traces to the NY Fed. The only blemishes are trivial and not shading.
  - **Q3 2026 unemployment 5.2%, "lowest level in more than a year": true.** The page states the phrase itself.
  - **Underemployment 42.5%, "ticked up": true.**
  - **Ages 22-27, bachelor's or higher, national: true.**
  - **Underemployed definition (job that typically does not require a degree, O*NET based): true.** The page also says many such jobs are fairly skilled and well paid and graduates later move to better roles.
  - **3.6% (March 2019) to 5.6% (March 2026): true.** It matches the advisory text exactly. The advisory is a media advisory announcing a Liberty Street Economics post, so the article's "May research advisory" label is loose, but the numbers and date are right.
  - **"Low-paid or unskilled" wording:** the fetched summary of the article presents it as a paraphrase of the Fed's caveat, not a verbatim quotation. The Fed page does not use that phrase, but the substance is on the page. This is not a misquote.
  - **Monthly vs quarterly mixing:** the article attributes each figure to its own period (Q3 2026 vs March 2026 vs March 2019). It does not claim a like-for-like series, so it does not mislead.
- **askamerica (1st):** correct rating, best handling of limits.
  - It checked all six claims against the primary page and was honest that the warehouse has no NY Fed series.
  - It caught that the 2019 to 2026 source is a media advisory, not a research advisory, and rated that MOSTLY TRUE without penalizing it.
  - It noted the grain difference (quarterly 5.2% vs monthly 5.6%) as non-material and that the Fed says its data are not official estimates.
  - Weakness: it did not cite the page's O*NET statement or the stepping-stone text in detail.
- **everyman (2nd):** correct rating, a bit looser.
  - It verified the core claims and the 22-27 definition.
  - It did not flag the "research advisory" mislabel. It asserted Q1 about 5.7% and Q2 about 5.6% from "earlier versions of the page"; the current page gives no prior-quarter values, so those are unsupported. It was honest about its tentative rating and the summary-only article.
- **expert (3rd):** wrong rating.
  - Its 1 Pinocchio rests on a quote it could not find verbatim on the Fed page. The article does not present it as a quotation, so the charge is not supported. It also called the quarter-over-quarter comparison unverifiable while the page states the "lowest in more than a year" claim.
  - Positives: it read the primary page, found the "fairly skilled and well paid" text, and noted that mixing the monthly March figure with quarterly averages is a grain difference.

## Recipe check
Partly. A recipe for "news quotes a statistical agency" checks would help:
- When an article attributes a phrase to a source, decide whether it is quoted or paraphrased before penalizing a missing verbatim match. If only a summary of the article is available, a verbatim finding cannot be a Pinocchio.
- Classify the type of document behind each figure (media advisory vs research post) and treat a loose label as a note, not a rating change.
- Do not cite earlier page versions as evidence unless they are retrievable.
- No warehouse table carries the NY Fed recent-graduate series, so the web was the correct route.

## Severity invariant
**Not triggered:** askamerica ranks above everyman. askamerica's rating (0) matches the correct verdict.
