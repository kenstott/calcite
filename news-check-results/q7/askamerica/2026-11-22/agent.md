# The Fox News article's claims about tariff lobbying spending, AAF's report and tariff costs are somewhat biased and mostly accurate; 2 claims could not be confirmed.

Article: https://www.foxnews.com/politics/industry-drove-dc-lobbying-337-percent-trump-whats-driving-push (fetched directly; loaded fine)

**Rating: 1 Pinocchio.**

## Summary
The article accurately relays Advancing American Freedom's (AAF) figures and discloses that its spending total may include issues beyond tariffs. My independent check of Senate LDA filings (law.lobbying_filings and lobbying_activities, Q3 2024 vs Q3 2026) confirms a real surge but a smaller one in dollars. Filings mentioning tariffs rose 251% (374 to 1,312) and their reported dollars rose 197% ($108.1M to $321.6M). The piece reports 239% and 337%.

## Claim-by-claim
| # | Claim | Verdict | Evidence |
|---|---|---|---|
| 1 (central) | Spending on those filings jumped 337% | Mostly true | Warehouse +197%. AAF did publish 337% (Political.org copy), so the article is faithful to AAF, but the number depends on AAF's own undisclosed definition. |
| 2 | Disclosures mentioning tariffs up 239% | Mostly true | Warehouse +251%. |
| 3 | 14 organizations over $1M vs 2 in Q3 2024 | Partially false | Broad tariff-mention definition gives 73 vs 21. |
| 4 | 105 over $100K vs 26 | Partially false | Broad definition gives 436 vs 130. |
| 5 | NY Fed: tariffs added 2.9 percentage points to goods prices | True | NY Fed Staff Report 1201 as covered by multiple outlets, and Fox's linked article. |
| 6 | AAF estimate: 75,000 fewer manufacturing jobs, up to 1M fewer jobs nationwide | True | Matches AAF's estimate in Fox's linked article. This is a modelled estimate. |
| 7 | Tariffs caused those job losses (causal) | Unsupported | Counterfactual model from an advocacy group; no independent test found. |
| 8 | US-Canada trade fell $50B (6.5%) before the Section 338 tariffs | Unsupported | No page found carrying the figure; warehouse trade tables end 2025. |
| 9 | Greszler: firms pay lobbyists for exemptions instead of investing, hiring, or raising wages | Unsupported | Filings show more tariff mentions, not purpose or foregone investment. |

Section 338 tariffs on Canada are real (proclaimed July 20, effective Aug 22, 2026; White House fact sheet).

## Exclusion disclosure
The filter t.filing_uuid IS NOT NULL excluded 20,897 filings in 2024 and 23,062 in 2026 whose activity text never mentions tariff. Those filings total $1,161M and $1,328M. With them included, all-filing spending rose only 30%, against +197% for tariff-mentioning filings. Any mention of tariff includes multi-issue filings, which inflates dollars relative to a tariff-only measure.

## Materially misleading context
- The surge is framed as "driven by Trump" and companies buying exemptions. Filings show only more tariff mentions, not why.
- The source is a Pence-founded group opposed to tariffs.
- The article omits that AAF's Q1 (+655%) and Q2 (+690%) readings were larger.
- The 2024 base predates the tariff regime.
- The article gives the 14-vs-2 and 105-vs-26 counts without a definition. They are not reproducible on public filings under a straightforward definition.

## Limits
- I did not locate AAF's original report. AAF figures were graded from a Political.org copy and Fox's own linked AAF-jobs article.
- The Canada trade figure is unverified.
- Report: http://127.0.0.1:60514/a/bad24b08a51935309d27d1a19a7d36a0.html (report_id 20261122-072618-the-fox-news-article-s-claims-about-tari)
