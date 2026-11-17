# Judge — news q5 bostonglobe-nh-wild-river-fire-historic

**Story:** "NH's historic Wild River Fire" (Boston Globe, 2026-11-12). Claim of interest: 127 other fires have burned 180 acres this year against a typical 250, "outstripped four times over"; the Wild River Fire is the largest in the White Mountain National Forest since 1918.

Rank: [expert, askamerica, everyman]

Judged 2026-11-17 on three answers produced the same day (this resume run).

## Why
- **expert (1st):** 1 Pinocchio, 18 claims and quotes. Read the US Drought Monitor's state figures directly (93.40% D0+, 40.46% D1+, week of Nov 10). Did the "four times over" arithmetic explicitly: the 180 acres is 0.72x of a typical year, and the 4x only appears when the Wild River Fire is added (4.0x at 817 acres). Found that the Forest Service called the 1918 comparison "unofficial" and the Globe dropped the hedge, which no other persona found. Reconciled the acreage across mapping revisions (921 to 856 to 817) and the crew count (87 on Nov 11, 58 on Nov 12).
- **askamerica (2nd):** engine rating 0 Pinocchios, 9 claims. Used two warehouse tables: `disasters.wildfire_perimeters` for the current mapped size (817 acres) and `weather.drought_monitor_weekly` for drought (92.8% / 38.4%). It found the same "four times over" problem and the stale lede acreage, and found that the linked tick article is an avoidance guide that says nothing about fire, which no other persona found. Its drought figure is an unweighted mean of NH's 10 county percentages rather than the state's area figure, so it reads 38.4% where USDM's state figure is 40.46%. It reached the right verdict, but by a method the existing recipe `test-weighted-aggregate-against-its-largest-unit` warns against. It extracted half as many claims as expert and graded the crew-count claim unsupported where expert found the Nov 11 source.
- **everyman (3rd):** 1 Pinocchio. Worked from a WebFetch summary of the article. It caught the stale acreage and the 180-vs-250 oddity, but left the drought figures unchecked against USDM's own numbers and had fewer sources.

What would change the ranking: askamerica and expert reach the same overall verdict. Expert's extra breadth (18 vs 9 claims), exact USDM figures and the dropped-hedge catch decide it. askamerica's tick-link catch is the only point where it led.

## Recipe check
No new recipe. Both the drought shortcut and its fix are already covered by `test-weighted-aggregate-against-its-largest-unit` (askamerica called `find_recipe` once, for "fact-check news article"). Expert's dropped-hedge check (compare the source's own qualifier with the article's paraphrase) is general fact-checking practice. askamerica already does it as part of fidelity grading when a source is cited, and the 1918 claim was not grouped as fidelity.

## Resolution
The loss came from breadth and one dropped-hedge catch, not from a warehouse gap. The one real gap the story exposed affected all three personas equally: no persona could test the 127-fires / 180-acres figure. Filed as govdata-ops#1300 (no wildfire occurrence counts by state and year; WFIGS incident locations, FPA FOD).

## Shape
Reviewed. 9 `author_claims`, no speakers. The graded figures are the reporter's narrative or attributed to unnamed "firefighters". Matson's quotes are opinion and were not graded. Compliant.

## Severity invariant
Not triggered.
