# q9 judge — wtkr-hampton-roads-state-of-transportation-bridges-air-travel (2026-11-19)

Rank: [askamerica, expert, everyman]

Story: WTKR, about 2026-11-13, "Hampton Roads bridges show improvement, air travel surges in 2026 transportation report". It relays the HRTPO report: 2.7% of bridges in poor condition (from over 5% in 2017), air travel up 46% from 2015 to 2024, 17 hours lost to congestion, and HRTPO engineer Keith Nichols saying traffic is about 4% below pre-pandemic.

All three rate it 1 Pinocchio. The difference is whether the figures were checked against data independent of the report.

**askamerica (#1).** It is the only persona that went past the HRTPO report to the underlying federal data in the warehouse. From the FHWA National Bridge Inventory: 34 of 1,271 bridges in the 16 localities were poor in 2025 (2.68%), against 62 of 1,240 in 2017 (5.0%). From BTS T-100: departing domestic passengers at ORF+PHF rose 1,733,373 → 2,541,047 (+46.6%). It found what the regional figure hides: Newport News *fell* from 202,509 to 69,342 while Norfolk rose about 61%. It graded Nichols's 4% quote partially false (`subject_claims`, speaker set), because the report says VMT is back to pre-pandemic and count stations fell 0.6% in 2023-24. It also catches the congestion year (2024, not 2023), the Amtrak bus substitution, the 2024-data "2026 surge" headline, and the omitted fatality-rate rise and 32nd-of-41 bridge ranking.

**expert (#2).** Checked every claim against the full HRTPO report: the bridge share (34 of 1,279, 5.5% in 2017), 46% boardings against 23% nationally, fatalities, the TomTom year error, the Nichols figure not in the report, the Amtrak bus and the Cancun timing. A thorough read of the report, but it relies on it alone ("I did not fetch FAA or BTS") and misses the Norfolk/Newport News split.

**everyman (#3).** Could not fetch the report PDF and worked from HRTPO web pages. It confirmed the bridge share (with a 2016/2017 baseline wrinkle) and flagged the "surge" framing, but left the air-travel number, congestion hours, fatalities and quotes unverified.

**What would change it.** askamerica's lead is clear. Independently reproducing the two headline figures from NBI and T-100, and finding the airport divergence, is exactly what the connector is for. expert over everyman is also clear.

Severity check: askamerica above everyman. No defect.

Claim shape: Nichols's quote is `subject_claims` with speaker; the rest are author claims. Correct at first delivery. Report and agent.md agree at 1 Pinocchio.

Gap audit: "the warehouse has no table for regional vehicle-miles traveled or the TomTom congestion measure". TomTom is private (not filed). Regional VMT: filed govdata-ops#1362 (FHWA TVT and Highway Statistics VM-2/HM-71; type:sourcing, kind:gap, schema:transport, status:open).

## Recipe check

askamerica already did the warehouse-native check (NBI, T-100) on its own, and split the region by airport. No recipe. askamerica ranked #1.
