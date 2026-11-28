# Judge — news q5 — Inside Climate News, "Storm Costs Are Sending Maine's Power Bills Soaring" (2026-11-25), 2026-11-28

**Question/claim:** Fact-check the article (https://insideclimatenews.org/news/25112026/maine-storms-power-bill-costs/). Central claim: Maine electricity prices rose 64% from 2019 to 2025, against 38% nationally.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files open with 123456789 and close with 987654321 (composer-generated); askamerica ran the production news-check prompt verbatim. No unaided arm's agent.md mentions askamerica. A grep of the everyman and expert answers for "askamerica" returned no match, so no unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Article text | WebFetch summary only | HTTP 403; one aggregator (Wisevoter) copy only | WebFetch summary only |
| Data used | Web only; no EIA figure found | Warehouse energy.eia_electricity_prices; Spectrum News, Sun Journal, Bangor Daily News | EIA EPM Table 5.6.A (Sep 2026 levels); Maine Public, BDN, Press Herald |
| Window | Searched 2015-2025, 2014-2024 | 2019 vs 2025, residential and all sectors | Sep 2026 vs Sep 2025 (not the 2019-2025 window) |
| Central claim verdict | "Not confirmed"; no figure for the window | "Partially false": ME resid +54.8% vs US +32.4%; all sectors ME +60.4% vs US +28.7% | "Not checkable"; EIA annual fetch was empty |
| Overall grade | 1 Pinocchio, provisional | 1 Pinocchio, low confidence (agent.md); report.html says "too few to rate" | 1 Pinocchio, provisional |
| Claims graded | 7 rows (3 not checked or not checkable) | 4 (3 true or false, 1 unsupported) | about 8 items (several not checkable) |
| Visual | none | dashboard.png, report.html | agent-dashboard.png, agent-report.html |

Per-arm notes:
- everyman: $69M denial confirmed (Oct 6, unanimous). $18/month marked "Not confirmed" and guessed to be conflated with the $7 temporary piece. FERC refund claim partly confirmed with a contested range ($900M to $1.5B). Storm cost, outage ranking and liability cap "not checked" for lack of time.
- askamerica: $69.3M denial true. $189M filing and spring 2027 decision true. $600M, $14.1M and $18/month "unsupported". Flags the Public Advocate split: supply and gas about $30 of a roughly $67 rise, distribution about $18.
- expert: $18 distribution request versus $7 temporary request separated correctly. Maine 31.29 vs US 18.17 c/kWh (Sep 2026). Single-year CMP/Versant comparison flagged. Quotes not checkable.

## Verification

Central claim, my check. The EIA annual state table could not be parsed through WebFetch (binary xlsx). A web search returned EIA-sourced figures: Maine average retail price 12.78 c (2015) to 22.81 c (2025); Maine 12.65 to 19.62 c (2014-2024) and US 10.44 to 12.99 c. I found no source for 64% vs 38% on a 2019-2025 window. Askamerica's all-sector ME figure ends at 2025 and is consistent with the 22.81 c EIA value (a 2019 value near 14.2 c gives about +60%). So the direction and the gap hold, and the exact 64% and 38% do not reproduce on either measure askamerica tested. This supports askamerica's verdict and shows everyman's and expert's "not confirmed / not checkable" were gaps in evidence rather than findings. Open item for the orchestrator: a live query to confirm the 2019 values in energy.eia_electricity_prices (ME residential 17.89 c looks high against my recollection of EIA; I could not check it). Askamerica also logged `low_coverage:high` and `rollup_contamination:caution` diagnostics on its queries and did not discuss them.

Askamerica overcalls "partially false": it says the article's measure is unknown, then rules the figures do not match. A calibrated verdict would be "not reproduced". It did test two measures and report both, which is more than the others did.

Askamerica claims.json vs report.html: claims.json holds 3 graded claims (1 partially false and central, 1 true, 1 unsupported). The engine rating in report.html is "too few to rate". agent.md states "Provisional rating: 1 Pinocchio, low confidence". Mismatch: agent.md gives a grade the engine declined to compute. The agent.md text does hedge that it covers only the listed figures. agent.md also lists 4 rows (including the $189M claim) while claims.json has 3.

Other claims:
- $69M denial on Oct 6: confirmed by all three (askamerica 69.3M via Spectrum/Sun Journal; everyman 69.3M).
- $18/month: expert and askamerica place it correctly as the distribution component; everyman called it "not confirmed", which missed the Public Advocate split.
- $600M storm costs: no arm verified it. Everyman and expert said not checked or not checkable; askamerica said unsupported. Equal.
- Quotes: no arm verified any. Everyman and expert said so; askamerica said it could not extract them.
- Coverage miss: only everyman checked the FERC refund claim. Only askamerica and expert flagged that storms are not the only driver (askamerica with the $30 of $67 figure; expert with the 2021 single-year caveat).

No arm read the article text. Askamerica's subagent had a client WebFetch tool and per calls.jsonl never tried another route to the article beyond Wisevoter; it did not search for the article text itself. Everyman and expert at least got a summary of the article.

## Ranking rationale

1. askamerica. It alone ran the central claim against EIA data at the stated 2019-2025 window, tested two measures, and reported the result with the limits stated. It separated what it could source from what it could not, and added the driver-share framing from the Public Advocate analysis. Weaknesses: narrow coverage (4 claims), article never read, the "partially false" overcall, and the rating mismatch.
2. expert. Correct handling of the $18 versus $7 split, good sources, EIA price level checked, but the central claim was left unchecked and the check used a different window (Sep 2026 vs Sep 2025).
3. everyman. Widest claim table, and it caught FERC, but the central claim has no EIA figure, the $18 handling is weaker, and it left several claims unchecked for lack of time. Its grade of 1 Pinocchio rests on little evidence.

What would change the ranking: if a live query shows the warehouse 2019 Maine residential value is wrong, askamerica's central verdict loses its basis and expert would rank first. If the article's own text shows 64% and 38% come from a measure askamerica tested, the "partially false" call would be wrong in kind.

## Severity invariant

No flag. Askamerica ranks above everyman.

## Recipe check

- The recipe `fact-check-a-news-article` (recipes.json) was served and used. The central-claim method (test windows and measures, report both) follows it.
- Pattern not reached: when the article URL returns 403, askamerica did not try the client WebFetch, an archive copy or a search for quoted sentences, which the unaided arms effectively did by getting a summary. Its claim set therefore comes from an aggregator. A broadened recipe step is warranted: after a 403, try the client WebFetch and a search on distinctive quoted phrases before settling for one aggregator copy; the warehouse method (energy.eia_electricity_prices, residential and all-sector, stated window) stays as is.
- Pattern to teach: expert and askamerica both split the $18 distribution rise from the $7 temporary request; askamerica's Public Advocate driver split is the better framing.
- Rating hygiene: agent.md should not state a Pinocchio grade when the engine computed "too few to rate".

Askamerica is #1, so no loss classification is needed.

Proposed resolution: b — add a recipe step for unreadable article URLs (client WebFetch and phrase search before aggregator-only), and do not state a numeric grade when the engine says too few to rate.

## Corpus gaps stated

- "The ICN URL returned HTTP 403. The engine's copy search did not find the same story." Reason: article unreadable; only the Wisevoter summary was read.
- "I could not extract the article's own claims or quotes."
- "Storm costs of more than $600 million for 2021 to 2025, $14.1 million a year in base rates, and an $18/month rise ... Only the Wisevoter summary states these. The Public Advocate PDF returned 403." Verdict: unsupported.
- Central claim: "The exact percentages do not match EIA, and the article's measure is unknown."
- claims.json: "No primary source readable; the Public Advocate PDF returned 403." Catalog search for "Maine CMP storm recovery costs 2021-2025" produced no stated table (no explicit "no table" statement in agent.md).
