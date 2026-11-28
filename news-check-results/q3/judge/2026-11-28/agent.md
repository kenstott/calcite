# Judge — news q3, Fox Business "Amtrak hits record 37.1M riders amid high flight, gas prices" (2026-11-26), 2026-11-28

**Question/claim:** Fact-check the article: Amtrak's record 37.1M riders (+7.4%), revenue and loss figures, the claim that high airfares and gas prices pushed people to rail, quotes from Byl Herrmann and Hayley Berg, and fleet and funding statements. Grade on the 0-4 Pinocchio scale.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files open with 123456789 and close with 987654321 (composer-generated); askamerica ran the production news-check prompt verbatim. No unaided arm's agent.md mentions askamerica. I grepped both agent.md files for "askamerica": no hits, so no unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Trade press and aggregators (Railway Age, American Rails); Hopper analysis. No Amtrak release read. | Amtrak Nov 24 release (fetched), Railway Supply (Reuters relay), five linked Fox pages; warehouse: EIA retail gasoline, BLS airline-fare CPI | Amtrak media page, CNBC, american-rails, Fox Nov 20 story, AAA gas prices; trains.com and railwayage 403 |
| Vintage/window | FY26 (to Sept 30, 2026) | FY26; gas by fiscal-year average and by month; CPI 2015-2026 | FY26; Iran-war timing; gas through Nov 28 |
| Headline figures | 37,067,685 vs 34,528,695; $4.1B +4.9%; $492.7M | 37.1M +7.4%; $4.1B +4.9%; ($492.7M) +18.9%; gross ticket revenue +1%; FY26 gas $3.61 vs $3.12; airfare CPI 2026 YTD 306.9 vs prior max 292.2 | Same ridership, revenue and loss; state-supported +9.5%; gas $4.10 to $3.53; $22B IIJA confirmed |
| Overall grade | 1 Pinocchio | 0 Pinocchios (engine-computed) | 1 Pinocchio |
| Claims graded | 9 (4 supported, 1 mostly, 1 partly, 3 not checked) | 11 in claims.json (6 true, 1 mostly true, 1 mostly true central causal, 2 unsupported, 1 not checkable) | about 8 grouped items; no per-claim count |
| Central claim verdict | Figures true; causal link a plausible hypothesis, Amtrak's own assertion | Figures true; "fueled by" is overreach, rests on one executive's belief; plausible, not shown | Figures true; causation rests on Herrmann's quote; no evidence beyond it |
| Visual | none | dashboard.png and report.html | agent-dashboard.png and agent-report.html |

## Verification

Central claim, checked against the primary source. I fetched Amtrak's release (media.amtrak.com, Nov 2026). It states 37.1M trips, +7.4%; operating revenue $4.1B, +4.9%; gross ticket revenue $3B, +1%; adjusted operating earnings ($492.7M), 18.9% better, unaudited. It credits better customer experience, NextGen Acela, Airo trains, infrastructure, the World Cup and America's 250th. It mentions neither fuel nor airfares. This supports all three arms on the figures. On causation it supports all three, and it backs askamerica and expert most directly, since both say the release never ties growth to fuel or fares. Everyman could not read the release and said so.

Number cross-checks (commas and currency stripped):
- 37,067,685 and 34,528,695 appear in everyman and expert. Ratio 37,067,685/34,528,695 = 1.0735, which rounds to +7.4%. Consistent with askamerica's "37.1M, +7.4%".
- $492.7M appears in all three. 18.9% (askamerica, expert) vs "19%" (article, everyman): consistent.
- Everyman cites FY25 adjusted operating loss $598.4M. 492.7/598.4 = 0.823, so about 17.7% better, not 19%. Everyman called 19% "plausible" using a number that does not fit. Minor; the release says 18.9%, so the base figure it cites is likely off.
- Expert's "$494.7M operating loss" from Trains.com is single-source and expert flagged it as such.
- Askamerica warehouse figures (gas FY26 $3.61, FY25 $3.12, FY24 $3.38; airfare CPI 306.9 vs 292.2) cannot be checked by me. Open item: the orchestrator should rerun the two logged queries (calls.jsonl seq 13 and 24). Note the CPI series sits in a table named econ.new_vehicles_cpi; the series id CUUR0000SETG01 is the airline-fare series, so the pick is right even though the table name is odd.
- Expert's "gas now falling, AAA $3.53 on Nov 28" is a web figure; I did not recheck it.

Askamerica claims.json vs agent.md vs report.html: engine-computed rating in report.html is "0 Pinocchios (engine-computed rating)"; agent.md states "Rating: 0 Pinocchios". No mismatch. One tension: the central causal claim is graded "mostly true" with support "overreach" and "errs toward_thesis", yet the overall rating is 0. The prose caveats are fair, but a zero sits oddly beside an overreach flag. Expert's 1 is the better calibrated overall grade on that point.

Penalties and misses:
- Everyman graded three claims "not checked" (fleet, Acela timeline, funding) with no source found, which is calibrated. It did not open the primary release. It missed the +1% gross ticket revenue and the label "adjusted net loss" vs "adjusted operating earnings".
- Askamerica graded the funding claim ($2.4B plus $22B) and the Northeast Regional 2028 claim "unsupported", not false; that is calibrated. Expert did better here: it confirmed $22B IIJA over FY22-26 and $2.4B for FY24/FY25. Askamerica missed that.
- Askamerica caught the +1% gross ticket revenue and fuel-price timing by month (spike from March 2026), which neither other arm reported with data. Expert caught state-supported growth (+9.5%) and the Iran-war timing; askamerica and everyman did not give the +9.5% figure (everyman said "more than half of the increase").
- Askamerica graded Berg's quote "true" using CPI data rather than locating the Hopper statement itself; the claim concerned a quote about airfare levels, and the CPI check tests the substance. Acceptable.
- Everyman gave a loose "partly supported" on Berg and could not find "jet fuel about double".

## Ranking rationale

1. askamerica. Read the primary release, graded 11 claims with sources, and tested the central causal claim with warehouse data: gas by fiscal-year average and by month, plus airfare CPI. It surfaced facts the release omits (gross ticket revenue +1%, release silent on fuel). It used "unsupported" and "not checkable here" in calibrated ways. Weak points: overall 0 Pinocchios sits against its own overreach flag, and it missed the confirmable funding figures.
2. expert. Strong on content: correct figures, label correction, state-supported growth, Iran-war timing, current gas trend, and confirmed funding items. Grade of 1 is well calibrated. It lacks warehouse-grade data on the gas and airfare mechanism and had two blocked sources. Close to askamerica.
3. everyman. Right figures, but only from secondary sources, three claims left unchecked, and a shaky base figure. Grade of 1 is reasonable.

What would change the ranking: if the orchestrator's live rerun shows the gas or CPI numbers are wrong, askamerica drops below expert. If the 0 rating is judged a miscalibration against the overreach flag, expert moves to first.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

Existing recipe `fact-check-a-news-article` was served (calls.jsonl seq 1 and 2) and covers the method. Askamerica followed it well. Worth teaching: when the article's central claim is causal and the primary release is silent on the cause, test the proposed driver against a warehouse series by fiscal window and by month (done here, good). Expert's extras (funding figures verified from Amtrak filings, subgroup growth) are web patterns, not warehouse ones. A small addition to the recipe is reasonable: when a causal claim is graded "overreach", the overall rating should not be 0.
Askamerica is #1, so no loss classification applies.

Proposed resolution: d — askamerica ranks first; no loss to classify, only a minor note that the recipe could tie the overall rating to an overreach flag on the central claim.

## Corpus gaps stated

- "No Amtrak table exists in the warehouse (catalog search: "amtrak" unmatched), so Amtrak figures were checked against the primary release." Orchestrator note: the warehouse has no Amtrak or intercity rail ridership table (orchestrator checked the transport schema table list); filed as govdata-ops #1635.
- "$2.4 billion annual funding plus $22 billion approved in 2021": "not in the warehouse or any page read. Graded unsupported."
- Replace Northeast Regional trainsets by 2028: "release says only the first Airo Northeast Regional trainset is in testing." Graded unsupported (claims.json: "no page read states a 2028 completion").
- Herrmann's belief about travelers' motives: "not checkable here" — "A belief about travelers' motives that no data series can test directly; the implied causal fact is graded in the central author claim."
- Two of five linked pages were non-content pages (a video page and an app-links page).
