# Q7 — 2026 U.S. wildfire statistics claim check
Run date: 2026-09-26

## Claim under test
"Through August 2026, the U.S. has recorded 52,253 total wildfires burning 8,238,284
acres, with 9 deaths reported. Earlier in the year (as of April 10, 2026), year-to-date
acreage burned stood at 1,707,778 acres, representing 231 percent of the ten-year
average."

---

## Step 1 — Identify the right NIFC/NICC series
**Answer:** The figures belong to NICC's Incident Management Situation Report (IMSR)
"Fires and Acres Year-to-Date (by Protection)" table — the all-jurisdiction (BIA, BLM,
FWS, NPS, ST/OT, USFS), all-fire (not large-fire-only) national total. This is distinct
from NIFC's "large fires" statistics and from federal-only counts.
**Key values:** n/a (identification step)
**Sources:** NICC IMSR PDFs, e.g. https://www.nifc.gov/sites/default/files/NICC/1-Incident%20Information/IMSR/2026/August/IMSR_CY26_08312026_0.pdf
**Confidence:** High

## Step 2 — Verify the August 31, 2026 cumulative totals (primary source)
**Answer:** Confirmed exactly. The NICC IMSR dated Monday, August 31, 2026 – 0730 MDT
reports, in its "Fires and Acres Year-to-Date" table: TOTAL FIRES: 52,253; TOTAL ACRES:
8,238,284. Both numbers match the claim digit-for-digit.
**Key values:** 52,253 fires (n, YTD through Aug 31, 2026); 8,238,284 acres (YTD through
Aug 31, 2026). Ten-year average (2016–2025) for the same date: 41,362 fires (126% of
avg) and 4,959,302 acres (166% of avg) — both percentages are printed directly on this
report.
**Sources:** NICC IMSR, Aug 31, 2026: https://www.nifc.gov/sites/default/files/NICC/1-Incident%20Information/IMSR/2026/August/IMSR_CY26_08312026_0.pdf
Cross-check: NIFC "Statistics" live page (fetched 2026-09-26, showing later Sept data of
57,087 fires / 8,559,888 acres, consistent with continued fire growth since Aug 31):
https://www.nifc.gov/fire-information/statistics
**Confidence:** High

## Step 3 — Verify the April 10, 2026 YTD acreage figure (primary source)
**Answer:** Confirmed exactly. The NICC IMSR dated Friday, April 10, 2026 – 0730 MDT
reports TOTAL ACRES (YTD): 1,707,778 (and TOTAL FIRES YTD: 19,102, not quoted in the
claim). This matches the claim's acreage figure precisely.
**Key values:** 1,707,778 acres (n, YTD through Apr 10, 2026); ten-year average (2016–
2025) for the same date: 788,185 acres.
**Sources:** NICC IMSR, Apr 10, 2026: https://www.nifc.gov/sites/default/files/NICC/1-Incident%20Information/IMSR/2026/April/IMSR_CY26_04102026.pdf
**Confidence:** High

## Step 4 — Check the "231 percent of ten-year average" figure for April 10
**Answer:** NOT SUPPORTED — and traceable to a specific, identifiable error. Two
independent problems:
1. The April 10, 2026 IMSR does not publish a "% of Ten Year Average" column at all —
   that column was not yet part of the report format in January–May 2026 (confirmed
   absent in the Jan 30, Feb 27, Mar 31, Apr 10, Apr 30, and May 31 reports; it first
   appears starting with the June 30, 2026 report). So no NICC report ever asserted
   "231%" for April 10.
2. Computing the ratio directly from the report's own published numbers
   (1,707,778 ÷ 788,185) gives 216.7%, not 231%.
3. However, 230.9% (rounds to 231%) IS the correct ten-year-average ratio for a
   *different* date: March 31, 2026 (1,615,683 acres ÷ 699,816-acre average = 230.9%).
   This strongly suggests the claim spliced the March 31 percentage onto the April 10
   acreage total — a mismatched-date error, not a genuine April 10 statistic.
**Key values:** Apr 10, 2026 actual ratio = 216.7% (n: 1,707,778 acres / 788,185-acre
avg). Mar 31, 2026 actual ratio = 230.9% (n: 1,615,683 acres / 699,816-acre avg, source
NICC IMSR Mar 31, 2026).
**Sources:** NICC IMSR Mar 31, 2026: https://www.nifc.gov/sites/default/files/NICC/1-Incident%20Information/IMSR/2026/March/IMSR_CY26_03312026.pdf ;
NICC IMSR Apr 10, 2026 (as above); NICC IMSR Jun 30 and Jul 31, 2026 (used to confirm
when the % column first appears and that computed ratios there match NICC's own
printed percentages: Jun 30 acres 3,138,394/1,977,688=158.7% vs NICC's printed 159%;
Jul 31 acres 4,818,250/3,634,306=132.6% vs NICC's printed 133%) — this cross-check
validates that "acres ÷ ten-year-average acres" is the correct, NICC-consistent
computation, which is why the 216.7% figure for April 10 (versus the claimed 231%) can
be trusted as the real value.
**Confidence:** High (that 231% is wrong for Apr 10); Medium-high (that the specific
mechanism is a March 31/April 10 splice — this is the best-fitting explanation found,
not an admission from the claim's source).

## Step 5 — Check the "9 deaths" figure
**Answer:** NOT VERIFIABLE against NIFC/NICC primary sources — and separately dated.
NICC's IMSR reports (checked across Jan–Aug 2026) do not carry a running wildland-fire
fatality count field; NIFC's fatality/safety data is tracked elsewhere (e.g., NWCG
Safety Awareness for Emergency Responders reports) and was not part of the material
reachable in this session. A general web search surfaced "9 deaths" attributed to
Wikipedia's "2026 United States wildfires" page — but as of that page's July 2026
vintage, not "through August" as the claim implies. Bundling a July-dated fatality count
with August-dated fire/acreage totals, presented as one as-of-August snapshot, is a
second internal inconsistency in the claim, independent of the 231% problem.
**Key values:** 9 deaths (n, secondary source, dated ~July 2026, not corroborated against
a NIFC/NICC primary source in this session).
**Sources:** Secondary/unconfirmed: Wikipedia "2026 United States wildfires" (via web
search summary, not independently opened in full); no NIFC/NICC primary source located.
**Confidence:** Low (on both the number itself and its currency as of "through August")

## Step 6 — Ten-year-average baseline definition
**Answer:** The baseline is a fixed calendar window: "2016 – 2025 as of today" (i.e.,
the trailing ten full calendar years before 2026), printed verbatim on every 2026 IMSR
report checked (Jan 30 through Aug 31). It does not roll within 2026 — the same 2016–
2025 window is used all year — but the *cumulative point* compared against does shift
by report date (e.g., "Acres … as of today" is a different cumulative figure on Apr 10
than on Aug 31). No evidence of a mid-year redefinition of the ten-year window itself;
the only mid-year change found was the *addition* of the "% of Ten Year Average" display
column (see Step 4).
**Key values:** n/a
**Sources:** NICC IMSR reports, Jan 30 – Aug 31, 2026 (as cited above)
**Confidence:** High

## Overall verdict
**Partly false / internally inconsistent.** The two headline totals through August 2026
(52,253 fires; 8,238,284 acres) and the April 10, 2026 acreage figure (1,707,778 acres)
are all confirmed exactly against NICC's own Incident Management Situation Reports. The
"231 percent of the ten-year average" figure attached to April 10 is wrong — NICC's own
numbers for that date compute to 216.7%, and no NICC report published a percentage for
that date at all (the metric wasn't added to the report format until ~June 2026); 231%
instead matches March 31, 2026's ratio, suggesting a date-mismatched splice. The "9
deaths" figure could not be confirmed against any NIFC/NICC primary source and, per the
one secondary source found, appears to be several weeks stale relative to "through
August."
