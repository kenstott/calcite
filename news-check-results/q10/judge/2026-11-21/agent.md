# Judge: Fierce Healthcare, Lown Institute nonprofit hospital tax breaks vs community spending (news q10, 2026-11-21)

Rank: [expert, askamerica, everyman]

Story: https://www.fiercehealthcare.com/providers/most-nonprofit-hospitals-tax-breaks-outpaced-their-community-spending-2023-lown-institue (2026-11-17). Claimant: Lown Institute (Hospital Fair Share, IRS Form 990 and CMS data).

Verdicts: expert 1 Pinocchio, askamerica no engine rating (2 graded claims; its own provisional view 1), everyman provisional 1. The article returned HTTP 403 to all three arms; every arm checked the claims as carried by same-day coverage against Lown's own pages.

## Ranking

**1. expert.** Confirmed the most Lown figures against primary pages: 52% of 3,037 hospitals; $15.5B deficit ($27.5B exemptions vs $12.0B returned among deficit hospitals); $38.8B total exemptions, about $13M per hospital; the top three deficits (Mayo Rochester -$366M, Tisch -$294M, Stanford -$255M); the Saini quote in Lown's release. Its arithmetic that surplus hospitals' $13.8B nets the national gap to about -$1.7B is the single most useful context point, and it names the excluded categories and that exemption values are Lown estimates.

**2. askamerica.** Graded only two claims formally (52% of 3,037; Mass General fifth with a $219M deficit) because it could not reconstruct the article's sentences, so the engine declined to rate. It matched the $15.5B, $13.8B surplus and Mayo's $366M without grading them, and gave the strongest rebuttal context (Mass General Brigham's $507M IRS-defined community benefit; expensive real estate structurally inflates estimated exemptions; data three years old). Its warehouse search found no Schedule H data; the CMS cost-report query it ran does not bear on the claim. Close to everyman on content; ahead on context.

**3. everyman.** Confirmed 52%, 3,037, the $15.5B split, Cleveland Clinic's $238M vs $98M and Ohio's $1.3B, and noted the excluded categories, from search snippets with Becker's also blocked. Did not note the surplus offset.

What would change the order: if graded-claim count is weighted over context, everyman and askamerica are effectively tied; both trail expert.

## Severity invariant

askamerica ranks above everyman, narrowly. Not flagged as a defect, but close enough to call out: on a story where the article itself is unreachable, the connector added only a negative warehouse search.

## Claim-shape check

Two `author_claims`, no quoted person graded; compliant. The engine refused to publish a stated rating with two scoreable claims; agent.md states a provisional 1. Recorded as `pinocchios: null`.

## Resolution (one)

Govdata-ops **#1415**, `fiscal: add Form 990 Schedule H (hospital community benefit line items) to exempt_org_990` (type:sourcing, kind:gap, schema:fiscal, status:open). With Schedule H line items, askamerica could have recomputed hospitals' community benefit both with and without the categories Lown excludes, the core of the dispute, from government filings.

## Recipe check

expert's netting of surplus against deficit hospitals is simple arithmetic askamerica also gestured at; no recipe.
