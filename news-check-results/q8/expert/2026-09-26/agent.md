# Q8 — August 2026 new-home sales claim check

Claim: "August 2026 new-home sales rose more than 6% from July levels, with the seasonally
adjusted annual rate (SAAR) climbing to 684,000 units."

## Step 1 — Pull the primary Census/HUD release for August 2026

**Answer:** Confirmed. Census/HUD's current New Residential Sales release (CB26-155, released
September 24, 2026, covering August 2026) states: "Sales of new single-family houses in August
2026 were at a seasonally-adjusted annual rate of 684,000 ... This is 6.4 percent (±19.5 percent)
above the July 2026 rate of 643,000."

**Key values:** Aug 2026 SAAR = 684,000 units; stated July 2026 comparison base = 643,000 units;
% change = +6.4%; 90% CI = ±19.5 pp (interval includes zero — not statistically significant at
90% confidence, per Census's own footnote).

**Sources:** https://www.census.gov/construction/nrs/pdf/newressales.pdf (fetched and parsed
directly with pdfplumber, n=1 primary document).

**Confidence:** High. Text extracted verbatim from the primary PDF.

## Step 2 — Identify whether "July levels" means originally-published or revised July

**Answer:** The 643,000 July figure used in the August release's own 6.4% comparison is the
**revised** July estimate, not the figure Census originally published for July. The original
(preliminary) July 2026 release (CB26-128, released August 25, 2026) reported July SAAR at
607,000 — 5.9% lower than the revised 643,000.

**Key values:** Originally-published July 2026 SAAR (preliminary, as of Aug 25, 2026 release) =
607,000; revised July 2026 SAAR (as of Sep 24, 2026 release) = 643,000; upward revision = +5.9%.
Using the originally-published July baseline, Aug-over-July change = (684-607)/607 = +12.7%,
roughly double the headline 6.4%.

**Sources:** https://www.census.gov/construction/nrs/pdf/newressales_202607.pdf (primary PDF,
fetched and parsed directly, n=1).

**Confidence:** High. Both PDFs fetched and parsed directly; figures read verbatim from Census's
own tables/text, not from a secondary summary.

## Step 3 — Cross-check headline figures against independent secondary sources

**Answer:** Independent coverage (Calculated Risk, CryptoBriefing, WRE News, CTASC) all report
the same 684,000 SAAR and +6.4% vs. 643,000 July figure, and the same median price ($393,700).
No discrepancy found among sources for the headline release-day numbers.

**Key values:** 684,000 SAAR; +6.4% MoM; $393,700 median price — consistent across n=4
independent secondary sources plus the 1 primary source.

**Sources:**
- https://calculatedrisk.substack.com/p/new-home-sales-increase-to-684000
- https://cryptobriefing.com/us-new-home-sales-august-2026/
- https://wrenews.com/new-home-sales-august-2026-684000-median-price/
- https://ctasc.com/new-single-family-home-sales-up-6-4-in-august/

**Confidence:** High. Multiple independent secondary sources agree with the primary document.

## Step 4 — Build the 12-month SAAR trend

**Answer:** Extracted the Table 1a (seasonally adjusted) monthly series for Sep 2025–Aug 2026
directly from the August 2026 Census PDF (n=12 monthly observations): 714, 652, 757, 723, 576,
630, 659, 641, 636(r), 672(r), 643(r), 684(p) thousand units SAAR. Chart built and saved as
agent-dashboard.png; embedded in agent-report.html.

**Key values:** n=12 months (Sep-25 through Aug-26); units = thousands of houses, SAAR; range
576,000 (Jan-26) to 757,000 (Nov-25).

**Sources:** https://www.census.gov/construction/nrs/pdf/newressales.pdf, Table 1a.

**Confidence:** High.

## Overall verdict

TRUE as literally stated, and it exactly reproduces Census's own published headline comparison
(684,000 SAAR, +6.4% vs. the revised July figure of 643,000). The caveat: the "July levels"
being compared to is the revised July number, not the 607,000 originally reported for July a
month earlier. Against the originally-published July figure, the implied increase is closer to
+12.7%, roughly double the "more than 6%" headline — a real instance of the revision effect the
task warned about, though it does not make the literal claim false since it matches Census's own
official comparison basis.
