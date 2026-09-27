# Fact check: "U.S. unemployment remained at a one-year low of 4.1% through August 2026, with nonfarm payrolls up 162,000 in August 2026"

Checked: 2026-09-26

## Step 1 — Confirm the headline unemployment rate for August 2026

**Answer:** Confirmed. The seasonally adjusted unemployment rate was 4.1% in August 2026, unchanged from July 2026.

**Key values:** LNS14000000 (BLS series, unemployment rate, seasonally adjusted) = 4.1 for period 2026-M08.

**Sources:**
- BLS Public Data API, series LNS14000000, https://api.bls.gov/publicAPI/v2/timeseries/data/LNS14000000 (primary)
- BLS Employment Situation Summary — August 2026, https://www.bls.gov/news.release/empsit.nr0.htm (primary, published 2026-09-04)

**Confidence:** High — retrieved directly from the BLS public timeseries API and cross-checked against the narrative release text ("unchanged" from prior month).

## Step 2 — Confirm the nonfarm payroll change for August 2026

**Answer:** Confirmed. Total nonfarm payroll employment rose by 162,000 in August 2026.

**Key values:** CES0000000001 (BLS series, total nonfarm employment, thousands, seasonally adjusted): Aug 2026 = 159,075; Jul 2026 = 158,913. Difference = +162 (thousand) = +162,000. Value carries a "P" (preliminary) footnote.

**Sources:**
- BLS Public Data API, series CES0000000001, https://api.bls.gov/publicAPI/v2/timeseries/data/CES0000000001 (primary)
- CNBC, "U.S. payrolls rose 162,000 in August, much more than expected; unemployment rate at 4.1%", https://www.cnbc.com/2026/09/04/jobs-report-august-2026.html (secondary cross-check)
- Reuters via Investing.com, "US nonfarm payrolls surge in August; unemployment rate steady at 4.1%", https://www.investing.com/news/economy-news/us-nonfarm-payrolls-surge-in-august-unemployment-rate-steady-at-41-4889606 (secondary cross-check)

**Confidence:** High, with the caveat that the figure is preliminary and subject to the standard two-round BLS revision (confirmed active in this same release: June 2026 revised +11,000 and July 2026 revised +44,000).

## Step 3 — Verify the "one-year low" claim against the trailing 12-month record

**Answer:** Confirmed. Pulling the full trailing-12-month unemployment-rate series (Sep 2025–Aug 2026) shows 4.1% is the lowest monthly reading in that window, first hit in July 2026 and held in August 2026 — consistent with "remained at a one-year low."

**Key values (LNS14000000, seasonally adjusted, percent):**
| Month | Rate |
|---|---|
| Sep 2025 | 4.4 |
| Oct 2025 | n/a (lapse in appropriations / shutdown — not published) |
| Nov 2025 | 4.5 |
| Dec 2025 | 4.4 |
| Jan 2026 | 4.3 (revised for updated population controls) |
| Feb 2026 | 4.4 |
| Mar 2026 | 4.3 |
| Apr 2026 | 4.3 |
| May 2026 | 4.3 |
| Jun 2026 | 4.2 |
| Jul 2026 | 4.1 |
| Aug 2026 | 4.1 |

n = 12 target months, 11 with published values (Oct 2025 missing).

For additional context: the rate was also 4.1% in June 2025, 14 months before August 2026 — so 4.1% is a trailing-12-month low, not an all-time or multi-year low. The claim under review only asserts the former.

**Sources:**
- BLS Public Data API, series LNS14000000, https://api.bls.gov/publicAPI/v2/timeseries/data/LNS14000000 (primary)

**Confidence:** High for the trailing-12-month conclusion. Medium-high confidence that including October 2025 (unpublished due to the government shutdown) would not change the result — the adjacent September (4.4%) and November (4.5%) readings make an unpublished October value below 4.1% implausible, but this is inference, not a directly observed data point.

## Step 4 — Check for a methodology break or unflagged revision spanning the window

**Answer:** No evidence of a mid-window definitional break was found. BLS's routine annual benchmark revision for the establishment survey (normally published each February) predates the window's start point for the values used here, and the release itself documents ordinary two-month revisions to June and July 2026 payrolls rather than any redefinition of the series. The August 2026 figures used in this check are the originally-published (preliminary) values as of 2026-09-26 and have not yet gone through a full revision cycle.

**Sources:**
- BLS Employment Situation Summary — August 2026, https://www.bls.gov/news.release/empsit.nr0.htm (primary)

**Confidence:** Medium — based on the absence of a footnote/discussion of a methodology change in the primary release; did not exhaustively audit BLS's technical notes archive for the full window.

## Overall verdict

**TRUE.** Both headline figures (4.1% unemployment, +162,000 nonfarm payrolls, August 2026) match the primary BLS release, and the "one-year low" characterization is accurate against the trailing-12-month record (tied with July 2026, the only other 4.1% reading in the past year). Caveat: both figures are preliminary and subject to revision in subsequent BLS releases; one month (October 2025) in the comparison window has no published data due to the 2025 government shutdown.

## Deliverables
- HTML report: news-check-results/q5/expert/2026-09-26/agent-report.html
- Chart: news-check-results/q5/expert/2026-09-26/agent-dashboard.png
