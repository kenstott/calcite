# Judge — news q7 aaf-customs-duties-down-39-percent-oct-2026

**Story:** "Debt and Deficit Lack of Progress Report: October 2026" (American Action Forum). Claim of interest: customs duties fell $12 billion (39 percent) because of tariff changes made by executive action. The October deficit was $404B, up $120B.

Rank: [expert, askamerica, everyman]

Judged 2026-11-17 on three answers produced the same day (this resume run).

## Why
- **expert (1st):** 1 Pinocchio, 19 checkable claims, 17 matched. All headline figures match MTS and Debt to the Penny. It also found three things that change how the year-over-year changes read. October 2025 fell inside the Oct 1-Nov 12, 2025 shutdown, so defense growth against a normal base (October 2024, $103.5B) is about 25%, not 29%. "Lower short-term rates" holds only for the average rate on outstanding bills; the 3-month yield rose from 3.92% to 4.16%. SNAP explains only $1.3B of the $3.7B income-security decline the piece attributes to the One Big Beautiful Bill. Customs refunds rising from $1.7B to $6.7B supports the refund explanation.
- **askamerica (2nd):** engine rating 0 Pinocchios, 14 claims. Read the piece in full and matched every figure against `econ.mts_outlays_by_function` and the Treasury fiscaldata tables. It added the monthly customs path from March to October (net customs negative in June and July as refunds exceeded collections), which shows "declined since May" is loose and the October drop is against a strong October 2025. It confirmed the Feb 20, 2026 IEEPA ruling and the May start of refunds, and noted CBO's $409B estimate. It missed the shutdown base effect and the payment-timing shift. It did not test "lower short-term rates", so 0 Pinocchios is lenient against expert's findings.
- **everyman (3rd):** 1 Pinocchio. It worked from a WebFetch summary and AAF's press release, not the piece's text. It matched the Treasury figures it pulled and raised the payment-timing and shutdown context. It graded the tariff-ruling cause unsupported ("found no source for the ruling link"), which is wrong: both other personas confirmed the ruling and the refunds. Its timing point is muddled: the calendar-adjusted increase it cites ($94B, 49%) is a larger percentage than the unadjusted 42%.

What would change the ranking: askamerica vs everyman is close. everyman has the base-effect context that askamerica lacks, but it misgraded the piece's central causal claim and never read the full text. askamerica got every figure and the causal claim right. Had everyman's timing point been clean and its causal grade right, it would rank second, and the severity invariant would fire.

## Recipe check
Yes. Both unaided personas tested whether the base month was abnormal (shutdown, payment shifts), and askamerica did not. That is a general method for any one-month year-over-year claim, and it is not in `recipes.json`. The evidence comes from the unaided personas, which is the stronger kind. The warehouse-native form: compute the change against two years earlier from the same table (`econ.mts_outlays_by_function` holds 2015 onward).

## Resolution
Recipe: broadened `fact-check-a-news-article` with a step 9 (test one-period year-over-year changes against two years earlier, and name a shutdown, payment shift, deadline delay or one-time payment when the base was abnormal). Rebuilt, smoke-tested and installed to engine-dev.

Rerun (Step 6.3, `news-check-results/q7/rerun/2026-11-17/`, askamerica only, same prompt except run_subpath): **confirmed.** The rerun queried `econ.mts_outlays_by_function` for 2024-10-31 alongside 2025 and 2026. It reported defense at $103.5B / $99.9B / $128.9B (about 25% against a normal base, not 29%) and the deficit up $147B (57%) on October 2024. It named the Oct 1-Nov 12, 2025 shutdown, saying the dates came from general knowledge, not a fetched source. It reasoned correctly that the weekend payment shift hit both Octobers, so it cancels in the one-year comparison. Engine rating still 0 Pinocchios. The engine labelled the piece "very biased", which the rerun itself says does not match its reading. Not a rejudge: the frozen rank above stands.

## Shape
Reviewed. 14 `author_claims`, no speakers. The piece has no quotes, and the figures are AAF's own reading of Treasury data. Compliant.

## Severity invariant
Not triggered, narrowly. askamerica ranks above everyman.
