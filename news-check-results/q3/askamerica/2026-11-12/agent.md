**The Center Square (via yournews) article's claims about the GAO debt audit, federal interest costs and the debt limit are accurate; 1 claim could not be confirmed.**

Rating: **0 Pinocchios.**

Article: https://yournews.com/2026/11/11/7262862/interest-on-the-debt-cost-1-4-trillion-last-year-audit/ (The Center Square, Nov 11, 2026). The article loaded normally. Report page: http://127.0.0.1:51760/a/1b367cade07a4149fcfa4353bcaed97e.html (report_id 20261112-064846-the-center-square-via-yournews-article-s).

## Central claim: true
The GAO audit of the Schedules of Federal Debt (GAO-27-108795, Nov 9, 2026) reports $1.4 trillion total FY2026 interest. I read it through the client WebFetch, because the engine's own fetch got 403. Its figures:
- About $1.1 trillion of the interest is on debt held by the public.
- Interest on debt held by the public was about $392 billion in FY2021.
- Total federal debt was $40.2 trillion, up about $2.5 trillion in the year.
- Schedules fairly presented, internal controls effective, no reportable noncompliance.
- GAO attributes the interest rise to growing debt and higher rates, and the growth in public debt to spending outpacing revenue.

## Warehouse check (econ.federal_debt, Treasury Fiscal Data)
| Date | Total public debt |
|---|---|
| 9/30/2026 | $40.17T |
| 9/30/2025 | $37.64T (so +$2.53T) |
| 9/30/2016 | $19.57T (so +105%) |

These match "$40.2 trillion", "up about $2.5 trillion" and "about double the $19.6 trillion a decade earlier".

## Other claims
- **True:**
  - CBO FY2026 deficit of about $2.0T, up from $1.8T. Net interest up $115B (11%). The $1.14T level is only implied by that growth, not read from CBO. Sources: AAF's summary of CBO, and GAO's citation of CBO's $2.4T annual deficit average for FY2027-36.
  - CRFB: debt limit suspended eight times since 2013, and the OBBBA raised the ceiling by $5T to $41.1T in July 2025.
  - P.L. 119-103 funds the government through Dec 11, 2026.
  - Veterans Day, Wednesday Nov 11, 2026 (OPM holiday list).
  - Dickerson's February 2027 debt-limit contact date and end-of-November 2027 resource exhaustion. These are projections. NTU's published range includes February as the early bound (its central case is May 2027). Bank of America independently projects February contact and a November 2027 X-date.
- **Unsupported (the "1 claim"):** Net interest "trailed only Social Security... more than Medicare or Medicaid... about $225 billion more than the War Department." CBO's pages returned 403 to both fetch tools. AAF confirms growth rates that fit (interest +11%, defense +5%, Medicare +8%, Medicaid +8%) but not levels. The warehouse's OMB budget-function table (econ.federal_outlays_by_function) shows 2026 net interest $1.017T below Medicare $1.070T, Social Security $1.676T and defense $966B. That is on a different basis from CBO's $1.14T net interest on the public debt, so it neither confirms nor refutes the claim. I did not find evidence that it is false.
- **Unsupported:** Dickerson's "just under $1.5 trillion in cash on hand" at the debt limit. No source carried a projected cash balance.
- **Not checkable:** David Ditch's remark that debt-limit votes are the only time Congress will vote for meaningful deficit reduction. It is an opinion about congressional behavior.

## Misleading-framing check
- The $1.4 trillion headline includes about $0.3 trillion of interest credited to federal trust funds, which is not a cost to outside creditors. The article states this in its second sentence and separates the $1.1 trillion and CBO's net figure, so I do not rate it misleading.
- GAO's interest figures are accrual expense, not cash. "Paid out to bondholders" is therefore slightly loose.
- The headline "cost $1.4 trillion" could mislead a reader who stops at the title. The body corrects this.

## Method notes
- The report tool produced the rating (0 Pinocchios) and the headline above. Author-claims honesty is scored 100.
- I did not run score_claim, so there is no second-opinion cross-check on the grades.
- I never reached a primary CBO page (cbo.gov returned 403), so every CBO figure rests on AAF or GAO copies.
- I computed the three warehouse debt values and the OMB outlay comparison. The GAO, CBO, CRFB, NTU, CRS and OPM figures are passed through from those sources.
