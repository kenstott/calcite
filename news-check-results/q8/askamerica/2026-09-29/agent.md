# Fact-check: Independent Institute / Craig Eyermann, "The U.S. Government's Worsening Deficit" (Sept 23, 2026)

**Report saved to:** `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q8/askamerica/2026-09-29/report.html`
**Source article checked:** https://www.independent.org/article/2026/09/23/governments-worsening-deficit/ (fetched via the syndicated Eurasia Review copy, https://www.eurasiareview.com/24092026-the-u-s-governments-worsening-deficit-oped/, since independent.org returned HTTP 403)

## Verdict

**Every individual dollar figure checks out. The article's central inference does not.**

- **~$2.1 trillion FY2026 deficit** — mostly true. AskAmerica's `econ.mts_outlays_by_function` (Treasury MTS Table 9) shows a YTD deficit of $1.966T through Aug 31, 2026 (receipts $4.8455T, outlays $6.8110T), matching Reuters' Sept 11, 2026 report of "$1.97 trillion" flat YTD. CBO's own Aug 2026 Monthly Budget Review projects a full-year deficit of $2.1T. The ">$5.2T" revenue figure is a forward projection (FY not yet closed), hence "mostly true" not "true."
- **$125.2B tariff refund following the Feb 2026 SCOTUS IEEPA ruling** — true. Matches Reuters/Treasury (Sept 11, 2026): $292.5B gross customs duties collected FYTD, $125.2B refunded, $167.3B net — confirmed against AskAmerica's Customs Duties receipts line.
- **$117B / 14% rise in interest costs, first 10 months of FY2026** — true, as an accurate quote of CBO's own Monthly Budget Review (via Fox Business). Note: AskAmerica's own Treasury MTS "Net Interest" line shows a smaller ~$90.6B/10.8% FYTD rise through July — a different, narrower Treasury accounting concept than CBO's net-interest calculation, not a contradiction of CBO's figure.
- **Debt >$40T, roughly doubled in 10 years** — true. AskAmerica `econ.federal_debt`: $40.07T (Sept 24, 2026) vs $19.57T (Sept 2016), ~2.05x.

**The article's core inferential claim — that the $125.2B tariff refund plus the $117B interest increase ($242B combined) exceeded the $147B year-over-year rise in cumulative spending through August, implying underlying spending growth is slowing and "might even have shrunk" — is FALSE**, for three compounding reasons:

1. **Mismatched time windows.** The $117B interest figure and $125.2B tariff-refund figure are both drawn from the first-10-months (through July) comparison per CBO's own Monthly Budget Review. But the $147B spending-increase figure is the through-*August* (11-month) comparison — a different, later window. In the correctly time-matched 10-month window, AskAmerica's own MTS data shows total outlays rose **$309.1B** YoY (FYTD $6.2842T vs prior-year $5.9752T) — this matches Fox Business/CBO's own reported "$308 billion" figure for that identical window almost exactly. $309B is more than double the $147B figure used, and is *not* exceeded by the $242B combined factor at all — directly contradicting the article's inference.

2. **Category error.** The $125.2B tariff refund is a revenue-side item — Treasury nets it against customs *receipts* (confirmed: Customs Duties sits in the receipts line-code range 10-120 in `econ.mts_outlays_by_function`, distinct from the outlays range 130-340 where Net Interest and Total Outlays live). It is not government spending. Adding it to a spending item (interest) and calling the sum "increased spending" misclassifies a revenue reduction as an expenditure.

3. **Undisclosed calendar confound.** Reuters/Treasury (Sept 11, 2026) reported that Aug 1, 2026 fell on a Saturday, shifting Social Security/Medicare payments normally paid in August into July — Treasury said that adjusting for this, the August deficit was actually *up* $7B YoY, not down. This one-time timing artifact likely explains much of the apparent deceleration from $308B (July) to $147B (August) — not a genuine slowdown in underlying spending growth. The article never mentions it.

## Pinocchio rating (SPLIT — claims attributed to Fox Business/CBO/Reuters)

- **Fidelity: 1/4.** Sources are quoted and linked accurately (the CBO interest figure, the Reuters tariff-refund figure, the deficit figure). The failing is an omission: nothing discloses that the $117B figure and the $147B figure come from different-length windows, even though both are identifiable from the very sources cited.
- **Claims accuracy: 3/4.** The headline inference — that interest and tariff refunds explain (and exceed) the entire YoY spending increase, implying spending "might even have shrunk" otherwise — runs opposite to what a correctly time-matched reading of the article's own cited sources shows (spending up $308B, not $147B, in the matched window). This is a significant logical/factual error built from otherwise-accurate individual numbers, not invented figures, hence 3 rather than 4.

## Methodology note

Two `score_claim` independent-verification calls came back at low confidence (0.38–0.54) for the deficit, tariff-refund, and interest figures — plausibly because these are 2026 fiscal-year-in-progress figures beyond the scoring model's likely training window. I overrode with `score_claim_override_reason` in each case, citing the primary Reuters (Sept 11, 2026) and CBO Monthly Budget Review (Aug 2026) sources fetched directly this session as independent corroboration. The central "$242B vs $147B" claim was scored independently at high confidence (0.84, verdict "false"), consistent with the warehouse-based analysis above.

## Sources consulted
- Independent Institute / Eurasia Review (Craig Eyermann, Sept 23–24, 2026)
- Fox Business, "Federal budget deficit projected to reach $2.1 trillion in FY2026" (citing CBO Monthly Budget Review, Aug 2026)
- Reuters, "US budget deficit shrinks in August, year-to-date flat at $1.97 trillion" (Sept 11, 2026)
- CBO, Monthly Budget Review: August 2026
- AskAmerica `econ.mts_outlays_by_function` (Treasury MTS Table 9) and `econ.federal_debt`

## Deliverables
- Full report with dashboard (2 charts + 2 stat tiles) and claim-by-claim table: `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q8/askamerica/2026-09-29/report.html`
