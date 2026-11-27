# Judge — q4 — DTN/Progressive Farmer, Bryce Anderson, "Drought maintains tight grip on Lower Mississippi" (2026-11-23), 2026-11-27

**Question/claim:** Fact-check the article: drought covers almost 94% of the Lower Mississippi region (more than double late July's ~42%); D2-D4 shares by state; almost 56% of Arkansas in D4; record-warm and 3rd/4th/5th driest Aug-Oct; precipitation under 50% (Aug) and under 10% (Oct) of normal; 5-9F above normal; the "20 to 30 inches" Drought Monitor quote; El Nino relief.

Rank: [askamerica, everyman, expert]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files open with 123456789 and contain 987654321 (pass); askamerica ran the extension's verbatim news prompt (no fingerprint by design). Grep of the everyman and expert answers for "askamerica" found no match. No unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Article, NIDIS 11-12 update, USDM national summary | Article, NIDIS, USDM page, warehouse (weather.drought_monitor_weekly, weather.climate_area_monthly, geo.counties) | Article, NIDIS, USDM summary 11-17 |
| Vintage | USDM Nov 10 (NIDIS) | Jul 28, Nov 10, Nov 17 weekly; nClimDiv 1895-2026 | USDM Nov 10 (NIDIS) |
| Headline figures | 94% supported; AR D4 "almost 50%" vs 56% unresolved | 94.7% (Nov 10), 97.7% (Nov 17); AR D4 55.7%; D2-D4 AR 90.7, MS 80.0, LA 85.0; temp rank 1 in all three; precip Aug 50/51/60%, Oct 26/30/20% | 94.2% supported; AR D4 56% not confirmed; D2-D4, temp, precip not checkable |
| Pinocchio grade | 1 | 1 | about 1 |
| Claims graded | 10 | 10 | about 6, in one paragraph |
| Central claim verdict | 94% supported; "more than double" not checkable | Mostly true; 42% is D0-D4, 94% is D1-D4; like-for-like change understated | 94% supported; "more than double" not verified |
| "20 to 30 inches" quote | Supported (USDM text found) | Unsupported (page garbled, not contradicted) | Verbatim match |
| Visual | None | dashboard.png and report.html | agent-dashboard.png and agent-report.html |

## Verification

Central claim: I fetched the NIDIS update (2026-11-12). It states 94% of the three-state region in D1-D4 (data valid 11/10), AR D4 "almost 50%", 97% of AR in D1-D4. It gives no precipitation percentages and no temperature departures, only ranks (3rd/4th/5th driest, warmest on record). So the 94% is supported by the agency page. All three arms agree.

Askamerica adds what the web pages cannot: the warehouse gives D1-D4 of 94.7% on Nov 10, which matches NIDIS 94.2% closely. It also shows the July base mixes categories (D0-D4 about 49% vs the article's 42%; D1-D4 about 23%). That is a real framing point that the other two arms could only call "not checkable." Warehouse figures are county land-area estimates, which askamerica states.

AR D4: everyman and expert saw only "almost 50%" and could not confirm 56%. Askamerica reports 55.7% on Nov 17 from the warehouse. Consistent with a later USDM map. Open item: the orchestrator should live-check the Nov 17 AR D4 and the D2-D4 state shares (MS 80.0% is at the margin).

Precipitation: askamerica grades the Aug/Oct "under 50% / under 10%" claim partially false from nClimDiv statewide values (Aug 50/51/60%, Oct 26/30/20%). It notes the article may describe local hot spots. This is calibrated, but it rests on a statewide-versus-station comparison, so it needs a live query check (open item). Everyman and expert correctly declined to grade it.

Quote: everyman and expert both located the USDM text ("20 to 30 inches of rain in a 3-month span"); expert calls it verbatim, everyman notes the article's wording differs slightly ("three-month"). Askamerica could not read the page (garbled encoding) and graded it "unsupported," not false. That is calibrated but a miss against the other two.

Askamerica engine rating vs agent.md: report.html says "Rating: 1 Pinocchio"; agent.md says "Rating: 1 Pinocchio." No mismatch. claims.json holds 10 claims, with verdicts matching the table in agent.md. Minor: the headline says "biased and mostly accurate"; the "biased" word is engine boilerplate and is not discussed in the claims.

No arm graded a claim true or false without evidence. Everyman graded claim 10 and claim 9 with source text. Expert named a "verbatim" match, supported by the USDM page.

## Ranking rationale

1. askamerica: same 1 Pinocchio verdict, but it checked the most claims against primary data (warehouse and NOAA nClimDiv): record-warm rank, driest ranks, AR D4, D2-D4 shares, July baseline category mix, and the precipitation percentages. It found the like-for-like D1-D4 change and the precipitation mismatch that the others could not test. Weakness: it missed the USDM quote that both others read.
2. everyman: ten claims graded one by one with correct "not checkable" labels, NIDIS evidence, useful context on El Nino and the NWS low-flow forecast. It found the quote. It could not check the numbers that need data, so it left four claims ungraded.
3. expert: correct and calibrated, found the quote, but covers fewer claims, in one paragraph, and no claim table. Same unchecked items as everyman with less detail.

What would change the ranking: if a live check shows the warehouse precipitation or D2-D4 figures are wrong, askamerica drops, possibly below everyman. If the USDM quote is judged central, everyman and expert gain.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

Askamerica used recipe `fact-check-a-news-article` (served at seq 1-2 in calls.jsonl). It reached the warehouse for drought and climate. The one thing everyman and expert did that askamerica did not: read the USDM Nov 17 narrative text for the quote. Askamerica's fetch returned garbled text and it stopped. The recipe could say to retry the USDM summary through another path (for example the USDM text or JSON services, or a web search for the quoted phrase) before grading "unsupported." No warehouse table holds USDM narrative text.

Askamerica is #1, so no cause classification is needed beyond the quote miss.

Proposed resolution: b — add to `fact-check-a-news-article` a step to retry an unreadable primary page via a quoted-phrase web search or alternate USDM URL before grading a quote unsupported.

## Corpus gaps stated

- "The USDM page loaded with garbled encoding, so the phrase could not be read. It was not contradicted." (claim "The Nov 17 Drought Monitor summary said 20-30 inches of rain are needed", graded Unsupported; claims.json adds "no warehouse table carries USDM narrative text.")
- Caveats: "Not checked: the Southern Regional Climate Center's station-level figures and the USDM Nov 17 narrative text."
- "Station-level figures were not verified" (temperature 5-9F, graded mostly true on statewide departures).
