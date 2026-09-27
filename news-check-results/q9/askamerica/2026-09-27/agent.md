# Fact-check: "Storm damage to Hawaii infrastructure and schools costing hundreds of millions" (The Garden Island, Sept 24, 2026)

**Durable report:** https://feature-test.askamerica.ai/a1bb6f5c-3041-4f40-8f44-6a75fcea9655

**Verdict: 1 Pinocchio (minor shading, not a significant distortion).**

## Summary

This is a Honolulu Star-Advertiser wire story (republished by The Garden Island and several other Hawaii papers) reporting preliminary HIEMA (Hawaii Emergency Management Agency) and DOE damage estimates from four 2026 Hawaii disasters: back-to-back March "Kona-low" storms, a May 22 earthquake, Hurricane Lala (August), and Hurricane Lowell (September).

Every fact checkable against a government primary source or an independent hurricane record held up exactly:

- **Kona-low storm window (March 10-24, 2026):** FEMA's `disasters.disaster_declarations` table (queried live) shows DR-4909 ("SEVERE STORMS, FLOODING, LANDSLIDES, AND MUDSLIDES") for Hawaii/Honolulu/Maui counties with `incident_begin_date` 2026-03-10 and `incident_end_date` 2026-03-24 — an exact match. NOAA's `disasters.storm_events` table independently shows a dense cluster of flash-flood/high-wind events across those same counties and dates, including a $1 billion property-damage flash flood on Oahu, March 20-21.
- **May 22 magnitude-6.0 earthquake:** FEMA's DR-4936 ("EARTHQUAKE") declaration for Hawaii County carries incident date 2026-05-22, matching the article. The USGS Earthquake Catalog, fetched live, returns event hv74966427: "M 6.0 - 13 km S of Honaunau-Napoopoo, Hawaii," magnitude 5.96, dated 2026-05-22 — an exact independent confirmation.
- **Hurricane Lala's Category 1 status** near Hawaii Island (Aug 15-16) and its Ka'u District impacts, and **Hurricane Lowell's Category 2 status** near Niihau/Kauai (Sept 7-8) with impacts reaching Lanai, are both confirmed by independent hurricane-tracking sources (Wikipedia's Hurricane Lala and Hurricane Lowell (2026) articles).

## The one clear discrepancy: the death toll

The article states "five fatalities reported from Lala and Lowell." Independent tallies do not support this. Wikipedia's storm infoboxes list Hurricane Lala at 3 direct + 1 indirect fatalities (4 total) and Hurricane Lowell at 3 direct fatalities (naming three specific victims: a 74-year-old man killed by storm surge on Kauai, a 59-year-old man found dead in Wainiha, and a homeless man on Oahu killed by a falling tree) — combining to **7**. Even the most conservative count available around the article's Sept 24 publish date (Lala's 4 plus Lowell's earlier-reported "at least 2") totals **6**, still above the article's "five." This reads as a stale, uncorrected running total rather than a fabrication, and it doesn't affect the article's core financial-damage narrative — but it is the one number in the piece that fails independent verification.

## Materially misleading even though individual facts check out

The Lala/Lowell storm-category framing, taken as a whole, understates how powerful these storms actually were. Both category statements are accurate for the moment each storm affected Hawaii (Lala Category 1 near the Big Island; Lowell Category 2 near Kauai). But the article never mentions that Lala later intensified to a **Category 4** major hurricane (130 mph) over open ocean, or that Lowell twice reached **Category 5** — the first Central Pacific Cat 5 since Hurricane Walaka in 2018, and the strongest hurricane to threaten the main Hawaiian Islands since Iniki (1992). A reader relying solely on this article would come away thinking Hawaii was hit by a comparatively modest Cat 1 and Cat 2 storm, understating the severity of the parent systems.

## What could not be independently verified (a data gap, not a finding of inaccuracy)

The article's core dollar figures — the $212 million statewide public-infrastructure total, the $174.5 million DOE school-repair estimate, the county-by-county breakdowns (Kauai $93.7M from Lowell, etc.), work-order counts, and the Lihue Civic Building's $24.75 million fire-damage estimate — are unpublished internal HIEMA/DOE preliminary assessments. No public dataset carrying these exact figures exists yet (FEMA's own `public_assistance_projects`/`hazard_mitigation_projects` tables won't reflect these events until federal obligations are processed, which hasn't happened for this still-open 2026 disaster season). These figures are internally consistent and appear identically across every independent wire republication checked (Star-Advertiser original, West Hawaii Today, Hawaii Tribune-Herald, Yahoo News), but that is corroboration of consistent relay, not independent confirmation against a primary published dataset. No FEMA disaster declaration for "Lowell" exists yet either (declarations can lag events by weeks), so the $100.5M Lowell figure rests entirely on HIEMA's internal estimate for now.

The article's quoted line from unnamed "department officials" about federal reimbursement uncertainty could not be checked against a primary DOE statement within this session, though it is consistent with DOE's other on-record caveats reported elsewhere in the piece.

## Sources used
- AskAmerica `disasters.disaster_declarations` and `disasters.storm_events` (FEMA/NOAA, live query)
- USGS Earthquake Catalog (fetched live, https://earthquake.usgs.gov/fdsnws/event/1/)
- USGS HVO "Volcano Minute" and Hawaii Public Radio (earthquake corroboration)
- Wikipedia: Hurricane Lala, Hurricane Lowell (2026)
- Big Island Now, Kauai County press releases, Hawaii News Now (Lihue Civic Building fire)
- Multiple wire republications of the original Star-Advertiser story (for internal-consistency check only)

## Note on process
Several `score_claim` (Jev) independent-scorer calls on the Lala/Lowell category and Kona-low storm claims returned low-confidence results, most likely because these are 2026 events outside that scoring model's own knowledge and it was assessing purely from the evidence text provided rather than independent knowledge. Those claims were verified directly against USGS/FEMA/Wikipedia primary and near-primary sources in the report body instead of being force-fit into the formal graded-claims table; only the two claims with high-confidence, unambiguous scorer agreement (earthquake magnitude; death toll) are included in the report's formal claims table.
