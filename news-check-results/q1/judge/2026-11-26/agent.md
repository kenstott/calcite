# Judge: Honolulu Star-Advertiser, "State jobless rate holds at 2-year high of 2.8%", 2026-11-26

**Claim/piece:** https://www.staradvertiser.com/2026/11/23/hawaii-news/state-jobless-rate-holds-at-2-year-high-of-2-8/. Fact-check it, extract the claims, and grade it on the 0-4 Pinocchio scale.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint: the everyman and expert prompt.md files each open with 123456789 and close with 987654321. Askamerica ran the product's verbatim news-check prompt on askamerica-news-desktop, which has no composer prompt by design. The everyman and expert outputs contain no "askamerica" string.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Article access | HTTP 403. Relied on Maui Now's relay of the DBEDT release | Fetched the article through the connector's fetch, so it had the full text | HTTP 403, archive blocked |
| Data used | Secondary news only | BLS LAUS state series from the warehouse (`econ.regional_employment`), BLS ranking page, DBEDT release, Fed/PIMCO | BLS state table and the DBEDT release |
| Claims graded | None from the article itself | 15 (12 author, 1 fidelity, 2 subject with speaker Seth Colby) | None from the article itself |
| Overall grade | Not graded | 1 Pinocchio (engine-computed) | Provisional 0 on the headline only; overall not assigned |
| Visual | none | dashboard.png, report.html | agent-dashboard.png, agent-report.html |

## Verification

I spot-checked the BLS API myself (series LASST150000000000006 and ...03):
- Hawaii's labor force in Sep 2026 was **694,853** and in Oct 2026 was 697,472. Askamerica's finding that the article's "694,350" is wrong is correct. The article's figure is 503 too low, so the implied monthly rise is about 2,600, not 3,100.
- Hawaii's unemployment rate for Sep 2025 is **2.1%**. The article's table shows 2.4%. Askamerica caught this and also traced the 2.4% to DBEDT's own July 2026 chart, which shows a pre-revision vintage. Neither unaided arm could see the table, so neither could catch it.
- The Oct 2026 rate is 2.8%, unchanged from Sep, so the headline holds. All three arms agree.

Askamerica graded the "squeezed out others looking for work" lead mostly false, because the unemployed count also rose (19,400 to 19,650). That grade follows from the article's own figures, which BLS confirms.

The county rates for September (Hawaii County 3.5%, Kauai 5.6%) are graded "unsupported". The warehouse holds LAUS at state level only, so this is a genuine corpus gap, filed as govdata-ops #1539.

## Ranking rationale

1. **askamerica.** It was the only arm that read the article. It graded every claim, found two real numeric errors (both confirmed above), and identified the misleading causal framing in the lead. The engine produced its rating. It took a correction pass on claim grouping, because its first publish put every claim under `author_claims`. After a resume, it published with the split shape.
2. **expert.** It checked the headline correctly against primary BLS and DBEDT data, and it correctly declined to grade claims it could not read. It found nothing beyond the headline.
3. **everyman.** It reached the same headline conclusion from secondary reporting only and did not grade the piece. Of the three, it is the least sourced.

The ranking would change only if the article text the connector fetched were not the published text. Nothing suggests that.

## Recipe check

Nothing to teach. Askamerica already did the warehouse-native check (`econ.regional_employment`). The one shortfall is data, county LAUS, and it is filed as #1539. No recipe or tool-description change is warranted.
