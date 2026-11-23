# Fact-check: Minnesota median household income growth 49th of 50 (American Experiment)

**Rating: 1 Pinocchio.** Every number reproduces; the headline rank is fragile and presented without that caveat.

## Step 1 - Verify claims
**Answer:** All checkable claims verified. Not checkable: dates/content of the two earlier posts the piece links; the piece has no quotes.
**Key Values:**
- MN GDP per capita minus US (chained 2017$): +$4,658 (2014), +$656 (2021), +$742 (2022), -$312 (2023), -$2,767 (2025). Family-of-four: $18,632 and $11,068 correct. The premium fell every year except 2022.
- GDP per capita growth 1997-2014: +30.7%, rank 17 of 50. 2014-2025: +8.8% (US +22.5%), rank 46.
- Real median household income growth (2025$, CPS H-8): 1997-2014 +11.1%, rank 16; 2014-2025 +7.3% (US +23.6%), rank 49. ND +6.2% is 50th; Iowa +7.4% is 48th.
- Correlation of GDP per capita growth with median income growth 2014-25: r=0.52, p=0.0001, n=50; r=0.48 without North Dakota.
**Sources:** Census CPS ASEC Table H-8 (h08.xlsx); BEA SAGDP9 and SAINC1 (apps.bea.gov/regional/zip).
**Confidence:** High for GDP claims; medium for income ranks (CPS sampling error).

## Step 2 - Misleading context
**Answer:** The 49th rank is not statistically firm. MN's SE is 5.5% of median in 2025 (4.8% in 2014). A Monte Carlo using published SEs gives rank >=49 in about 25% of draws (median 45, 5th-95th percentile 28-50). 3-year averages (2013-15 vs 2023-25) give 45th (+7.1%). MN's level rank was 6th in 2014 and 17th in 2025 ($95,160 vs US $87,460), so part of this is convergence from a high base. The 1997-2014 window crosses the 2013 CPS redesign. The correlation is ecological, not causal.
**Sources:** same as above. The Census ACS API requires a key and was not used, so no ACS cross-check.
**Confidence:** Medium.

Outputs: agent-dashboard.png and agent-report.html in the same directory.
