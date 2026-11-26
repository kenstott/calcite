# Judge: The Boston Foundation, "2026 Housing Report Card highlights housing market uncertainty…", 2026-11-26

**Piece:** https://www.tbf.org/news-and-insights/press-releases/2026/november/greater-boston-housing-report-card-release-20261110 (released 2026-11-10). Each persona was asked to fact-check the piece, extract its claims, and grade it on the 0-4 Pinocchio scale.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. The everyman and expert prompt.md files carry both composer markers. Askamerica ran the verbatim news-check prompt on askamerica-news-desktop. No unaided arm mentions askamerica.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Sources read | News coverage only (Boston Indicators, WGBH) | Release, report landing page, 69-page Core Metrics PDF, WBUR, Globe | Release, WBUR, Governing; Census API refused ("Missing Key"); Axios 403 |
| Independent primary data | None | Warehouse Census building permits for 5 MA counties: 7,746 units in 2025, lowest of 2015-2025. Warehouse ACS 2020-24 renter cost burden: 51.2% (50.6% of those severe) | None |
| "Fell by more than half" | Coverage says "nearly 50%" | 15,241 vs 29,024 = -47.5%; graded mostly true (fidelity, overstated); spike-base caveat from the report | 15,241 vs 29,024 = -47.5%; overstated; pandemic-delay inflated base |
| Six communities under $500K | Unresolved (WGBH names four) | True, report p.32, single-family medians, inflation-adjusted | True |
| $181K / one in six | Supported by coverage | True, report p.37, model outputs | Not checkable |
| 9th of 11 metros | Not checkable | Unsupported | Not checkable |
| Grade | 1 | 0 (engine-computed) | 1 |

## Verification

- Askamerica's permit and rent-burden figures come from the warehouse, not from the sponsor's report. They are the only figures in any arm that are independent of the Boston Foundation. The 7,746 units for 2025 matches expert's press-sourced figure, so the warehouse permits table is current through 2025.
- The rent-burden check is a real finding: 51.2% (ACS 5-year, five counties) against the release's "over 48%". The release understates burden by about 3 points, probably because it uses a different ACS product or geography. Askamerica graded it mostly true. No other arm could test it.
- The "more than half" overstatement was caught by all three. Askamerica and expert quantified it identically.
- Claim shape: the first publish put everything in `author_claims`. A resume moved the Census Address Count relay to `fidelity`, and I verified that in claims.json (author 8, fidelity 1).

## Ranking rationale

1. **askamerica** read the primary report and verified two headline figures against federal data from its own warehouse. It surfaced the report's own caveats that the release omits: the spike base, and that Massachusetts still exceeded its production goal. It also flagged the circularity of a sponsor's release matching its own report. Its 0 rating, against the others' 1, rests on grading the "more than half" relay as mostly true rather than as an inaccuracy. That is a calibration difference, not a missed finding.
2. **expert** quantified the main overstatement and caught a quote mismatch (Schuster). It had no primary data, because the Census API needed a key.
3. **everyman** relied only on coverage and left the six-communities claim unresolved.

What would change the ranking: if the overstatement should cost a Pinocchio, expert's grade is better calibrated, but askamerica still has the stronger evidence base.

## Recipe check

Nothing to teach. Askamerica used the warehouse-native tables (building permits, ACS cost burden) without prompting. No corpus gap worth filing. The municipal median-price data in the report is proprietary (Warren Group), not a public federal dataset.
