# Fact-check: Climate Central, "Billion-Dollar U.S. Wildfires" (Nov 11, 2026)

Method: fetched the article (WebFetch returned a summary, not verbatim text, so quote-level checking was not possible) and checked against Climate Central's own database state page (California) and news/AP/Cal Fire reporting. No Python or charts used.

## Claims and verdicts

| Claim | Verdict |
|---|---|
| Palisades 2025 $36.3B; Eaton 2025 $26.8B; Camp 2018 $20.9B; Tubbs 2017 $17.0B; Oakland Hills 1991 $8.0B; all five costliest are in California | Supported. Matches the Climate Central California database page exactly. |
| Palisades 12 deaths / Eaton 19 deaths (database) | Supported. Matches AP one-year review (31 total) and database page. |
| California: 23 of 31 events (74%), about $167B | Supported. Database page lists 23 CA wildfire events summing to $167.1B. 23/31 = 74.2%. |
| Hawaii $6B for 2023 Lahaina fire is next | Plausible. Consistent with the roughly $5.5-6B widely cited, but not independently re-verified here. |
| "At least 28 times more than any other state" | Roughly right. 167.1/6 = 27.9, so "28x" is a marginal round-up. Minor. |
| 31 events, about $186B, 373 deaths, 79,500+ structures since 1980 (through Sept 2026) | Partly checked. CA portion consistent (CA deaths sum to 245, structures about 69k). National totals not independently reproducible because the full non-CA event list was not accessed. Not checkable here in full. |
| 2007-2026 vs 1987-2006: 5 to 26 events, $15.6B to $170B; "more than five-fold" frequency, "more than 10-fold" cost | Arithmetic holds (5.2x and 10.9x). Underlying event list only partly verified. |
| Third most frequent and third most costly disaster type in 2016-2025 | Not checkable here (needs the full database ranking). |
| Fires mostly Aug-Nov, peak October | Consistent with the CA event dates (many Oct events). Supported in pattern. |
| Billion-dollar disasters since 1980: 453 events, $3.2T, about 17,200 deaths; 2020 third-highest count with a record six wildfires | Not checkable here. Consistent in magnitude with the long-running NOAA series. |
| Old calendar-year method would give 25 events, $221B | Not checkable here (method comparison internal to the database). |
| Costs CPI-adjusted to 2026 dollars; 2026 preliminary; Adam Smith led the analysis, formerly led NOAA's project | Methodology statement confirmed on the database page. Smith's role matches public record. |

No attributed quotes were identified in the summary retrieved; I could not confirm the piece contains none.

## Pinocchio rating

**Overall: 0 Pinocchios (Geppetto checkmark), with caveats.** Every figure I could check matches the publisher's own database and independent death and structure counts. The article is a data release by the dataset owner, so its numbers are self-consistent but are not independent confirmation.

## Materially misleading or contextual flags

1. **Single-source, modeled costs.** Figures are Climate Central estimates, not audited losses. The same database page notes state costs are shown as binned ranges and full event costs are attributed to a state, which can overstate the California total.
2. **CPI adjustment only.** Rising costs reflect more homes and higher values in wildfire-prone areas (exposure), not only fire behavior or climate. Headline "frequency grew five-fold, costs ten-fold" does not separate climate, development, and valuation effects. A few huge events (Palisades, Eaton, Camp, Tubbs) dominate the 10x.
3. **Methodology change.** Moving from calendar-year to per-fire events changed the count (25 vs 31) and totals ($221B vs $186B). Comparisons with older NOAA-style tallies are not like for like.
4. **Preliminary 2026 data and 20-year windows** make the comparison sensitive to endpoint choice.
5. **Not checkable here:** the disaster-type rankings, 453-event/$3.2T/17,200 deaths totals, and the 2020 ranking.
