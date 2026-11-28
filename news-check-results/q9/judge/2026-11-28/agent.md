# Judge — news q9, Brownfield Ag News "NOPA reports record October soybean crush" (2026-11-25), 2026-11-28

**Question/claim:** Fact-check the Brownfield piece: October 2026 NOPA crush of 234 million bushels is an all-time record, 27% above four years ago, a slow western Corn Belt harvest hurt September, the industry can meet the expanded EPA RVOs, and bio-based diesel is 10% of the U.S. distillate pool.

Rank: [expert, everyman, askamerica]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files open with 123456789 and close with 987654321 (composer-generated); askamerica ran the production news-check prompt verbatim. No unaided arm's agent.md mentions askamerica. A grep of the everyman and expert answers for "askamerica" found no match, so no unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web only: Ukragroconsult, EPA, farmdoc, USDA NASS 2022 release | Web (OleoScope, EPA, CME) plus warehouse ag.nass_crop_progress for one claim | Web: MarketScreener, FP Group, Federal Register, dieselnet, EIA |
| Window | Oct 2026 vs Oct 2022 and Oct 2025 | Same, plus harvest progress Sep 2024-2026 | Same |
| Headline figures | 234.19 mbu; +26.9% vs 2022; +2.9% YoY; Sep 196.3 | 234.2 mbu; +27.0%; +2.9% YoY; Sep 196.3; harvest 17% vs 19% vs 26% | 234.191 mbu; +26.96%; about +3% YoY; BBD output needs +60%; 2025 BBD fell 15.8%; EIA 7% in 2023 |
| Pinocchio | 1 | 1 (own judgment; engine said too few to rate) | 1 |
| Claims graded | 6 | 5 | 6 (table in report) |
| Central claim verdict | Supported (secondary) | True | Verified, high confidence |
| Visual | none | dashboard.png | agent-dashboard.png |

## Verification

Central claim checked by WebSearch. MarketScreener, Total Farm Marketing and Barchart/Yahoo all report NOPA October crush of 234.191 million bushels, a record for any month, up 19.3% from September and 2.87% over October 2025. Total Farm Marketing says 3% above a year earlier. I did not reach NOPA's own table. All three arms are supported on 234 mbu and the record.

- 27% claim: 234.19 / 184.464 = 1.2696. All three match (26.9, 27.0, 26.96).
- Context all three found: the 27% uses a four-year base; the year-over-year gain is about 3%.
- EPA RVO date: everyman says Mar 27, askamerica says Mar 26, expert says Apr 1 (Federal Register). The three disagree. Not resolved here. None changes a verdict.
- 10% distillate claim: everyman "not checkable", askamerica "unsupported" (no source found), expert "partial" with an industry figure and EIA 7% in 2023. Expert gave the most evidence. askamerica's "unsupported" is a grade of a claim it had no evidence on; "not checkable" fits better.
- Slow harvest claim: askamerica used the warehouse (17% by Sep 27, 2026 vs 19% in 2025, 26% in 2024). This is national, not western Corn Belt, and it does not test the crush effect. Open item: orchestrator to confirm the ag.nass_crop_progress values by live query. McKeany-Flavell (search result) says September had harvest delays and plant maintenance, which supports "mostly true".
- Extra context: everyman noted NOPA vs USDA NASS coverage, soyoil stocks at 1.17 billion pounds and futures falling. Search confirms 1.171 billion pounds and lower futures. askamerica noted the NASS difference. expert did not.

askamerica engine rating vs agent.md: report.html headline says the claims are "too few to rate". agent.md states 1 Pinocchio and says it is its own judgment. claims.json has five claims, no engine score. This is a disclosed mismatch, not a hidden one, but the product output gives no rating.

## Ranking rationale

1. expert. Correct central verdict, six claims, the strongest evidence on the two hard claims (BBD output +60% and 2025 BBD down 15.8%; EIA 7% share against the 10% claim). It states what it could not check. Its agent.md is short, but the report table is complete.
2. everyman. Same correct verdict and numbers. Six claims, a clear table, good context (NASS vs NOPA, soyoil stocks, one-source interview). Weaker on the 10% claim (found nothing) and it did not reach the NOPA release.
3. askamerica. Correct on the two numeric claims and on 1 Pinocchio, but five claims, no evidence on the 10% claim, a graded "unsupported" with no data, and the engine returned no rating. The warehouse added only a national harvest-progress check that does not test the claim. It left "all-time" scope "not independently checked" although multiple sources say record for any month.

What would change the ranking: if the live query shows the harvest numbers are wrong, askamerica falls further. If everyman's EPA date (Mar 27) or expert's (Apr 1) is shown wrong and askamerica's (Mar 26) right, the gap narrows but not to first place.

## Severity invariant

SEVERITY DEFECT: askamerica at or below everyman. Concrete cause: the warehouse has no NOPA crush, NASS Fats and Oils or EIA biodiesel series, so the connector had nothing for the central claims or the 10% claim, and the engine scorer returned too few claims to rate. everyman did more web work on the same claims (6 vs 5 claims, soyoil and NASS context).

## Recipe check

- Nothing in recipes.json matches crush, NOPA or soy crush (grep found only the NASS crop production qualifier-pinning recipe, which does not cover this).
- Patterns worth teaching: expert and everyman both compared against a second base year (Oct 2025) and a coverage difference between NOPA and NASS. askamerica did the base-year check too. expert's EIA share check for the 10% claim is a pattern askamerica did not reach.
- A recipe for "trade-association figure vs government series" would help, but only once a NASS Fats and Oils table exists. The warehouse-native method then would be ag schema NASS Fats and Oils crush table (govdata-ops #1638) and an EIA biodiesel/renewable diesel table (#1637).
- Cause class: (a). The missing series are the gap.

Proposed resolution: a — ingest the NASS Fats and Oils crush series (#1638) and EIA biodiesel/renewable diesel series (#1637).

## Corpus gaps stated

- "Warehouse: no NOPA crush table (search_catalog, unmatched terms: nopa, crush)."
- claims.json claim 1: "no NOPA table in warehouse"; reason: "preliminary trade-association figure, no 2026 government series found."
- Claim 5 (10% distillate pool): "No source found stating this share; the biodiesel term is absent from the catalog." Also "Search returned no EIA page stating the share; the interview audio could not be parsed as text."
- agent.md: "A government series for 2026 was not found."
- Orchestrator notes: the warehouse has no USDA NASS Fats and Oils crush series (govdata-ops #1638) and no EIA biodiesel/renewable diesel series (#1637). askamerica's report.html says "too few to rate"; agent.md states 1 Pinocchio as its own judgment.
