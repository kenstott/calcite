# Fact-check: "The Fastest Rent Cut in America Is Named ICE" (Twitchy, 2026-09-25)

**URL checked:** https://twitchy.com/justmindy/2026/09/25/rents-drop-when-illegals-leave-n2432727

**Overall grade: 1 Pinocchio (fidelity) / 3 Pinocchios (claims accuracy)** — split rating, since the piece faithfully relays a set of tweets whose central causal claim does not survive scrutiny.

## What the article actually is
Twitchy's post is not original reporting — it's a compilation of tweets (DHS, "Insider Wire," Sen. Eric Schmitt, MP Robert Jenrick, Rep. Tom Tiffany, and several pseudonymous accounts) asserting that ICE deportations are causing rents to fall, especially in states that cooperate with enforcement, plus Twitchy's own brief editorializing.

## Claims checked

| Claim | Verdict | Basis |
|---|---|---|
| Texas accounted for ~25% of July 2026 ICE arrests | **True** | Confirmed independently by San Antonio Current and Hoodline reporting on the same DHS data. AskAmerica carries no ICE arrest/deportation table (`search_catalog` returned no match for "deportations"), so this was checked against independent reporting, not recomputed here. |
| San Antonio rent -4.8%, Austin -4.3%, Dallas/Houston ~-3% (DHS tweet) | **Mostly true** | In the right range of independent rent trackers (Apartment List puts San Antonio at -4.8% to -5.1% YoY, the steepest large-metro decline nationally). Exact figures vary slightly by tracker/month; no government monthly metro-rent index exists to reproduce the figure exactly. |
| "DHS is reducing your rent" / ICE cooperation and deportations are what's driving these rent declines | **False** — and the central, most-repeated claim in the piece | See below. |
| Tom Tiffany's tweet: research estimates illegal immigration drove ~30% of home-price growth and ~20% of rent growth in U.S. metros, 2021-24 | **True as a citation**, but topically mismatched | Accurately reflects a real Dallas Fed working paper. But it describes price *growth* during an immigration *inflow* period (2021-24) — a different question from whether 2026 *deportations* are now causing price *declines*. Placing it beside the DHS claim implies a symmetry the study doesn't establish. |

## Why the central claim is false, not just unproven
1. **Timing rules it out.** AskAmerica's own BEA data (`econ.regional_price_parities`, Services: Rents, MSA level) shows Austin's rent-price index already falling from a 2023 peak (126.36) to 2024 (120.36), and Houston falling from 2022 (107.8) to 2024 (104.5) — a year or more *before* the 2025-26 ICE enforcement surge the article credits. Independently, Realtor.com counted 37 consecutive months of rent declines in the 50 largest U.S. markets through August 2026, a streak that began in 2023.
2. **A well-documented alternative cause exists.** Pew and the Dallas Fed's own regional research attribute the Texas/Sun Belt rent slide to a multifamily construction oversupply boom (Austin alone added ~120,000 units, a 30% stock increase, 2015-2024) now forcing landlord concessions.
3. **A natural out-of-sample test fails.** New York City and San Francisco are large, high-immigration metros that do *not* cooperate with ICE. Under the "fewer immigrant renters" mechanism, their rents should be flat or falling — instead both *rose* in 2026 (~+1.5% and +11.9%).
4. **Geographic confounding.** The states DHS cites as enforcement successes are the same Sun Belt states that built the most new apartments — cooperation with ICE and construction oversupply move together, so the correlation DHS draws proves nothing about causation on its own.
5. **A bait-and-switch statistic.** The Tiffany citation is a real study but answers a different question (past price growth from immigration inflows, not present-day declines from deportation).

This is scored via an independent evidence-based check (score_claim) as **false** with 0.94 confidence — the causal mechanism is not merely uncertain, it's contradicted by the timing and by the non-cooperating-metro comparison.

## Published report
Full dashboard, sourcing, and claim-by-claim table: `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q1/askamerica/2026-09-27/report.html` (local link died with the session; durable publish was not requested).

## Data-coverage note
AskAmerica has no ICE arrest/deportation table and no monthly metro-level rent index — the closest table (`econ.regional_price_parities`, annual, through 2024) was used to independently corroborate the *timing* problem in the causal claim, but the exact cited percentages had to be checked against independent secondary reporting (Apartment List, Realtor.com, San Antonio Current, Hoodline).
